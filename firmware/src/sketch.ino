#include <WiFi.h>
#include <PubSubClient.h>
#include <DHTesp.h>
#include <time.h>
#include <sys/time.h>
#include <WiFiUdp.h>

const char* WIFI_SSID = "Wokwi-GUEST";
const char* WIFI_PASSWORD = "";

// Wokwi tren VS Code: host.wokwi.internal tro ve may dang chay mo phong
// (Mosquitto trong docker-compose). Neu chay tren wokwi.com thi doi sang
// broker cong cong, vi du "broker.hivemq.com", va sua MQTT_HOST trong .env.
const char* MQTT_SERVER = "host.wokwi.internal";
const int MQTT_PORT = 1883;

// Cau truc topic: iot/<site>/<device_id>/telemetry
const char* SITE = "b23dcat041";
const char* DEVICE_ID = "esp32-01";

const int DHT_PIN = 15;
const int TRIG_PIN = 5;
const int ECHO_PIN = 18;
const int LED_PIN = 2;

const unsigned long SEND_INTERVAL_MS = 5000;

// Wokwi mo phong cham hon thoi gian thuc nen dong ho he thong cua ESP32 bi tre dan.
// Khi bat co nay, moi ban tin lay timestamp truc tiep tu may chu NTP.
// Tren phan cung that dat false de dung dong ho he thong (da dong bo NTP luc khoi dong).
const bool NTP_PER_MESSAGE = true;
const char* NTP_SERVER = "time.google.com";
const unsigned long NTP_EPOCH_OFFSET = 2208988800UL;

WiFiClient wifiClient;
PubSubClient mqttClient(wifiClient);
WiFiUDP ntpUdp;
IPAddress ntpIp;
DHTesp dht;

char mqttTopic[96];
unsigned long lastSend = 0;
unsigned long sequenceNo = 0;

void connectWiFi() {
  Serial.print("Connecting WiFi");
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD, 6);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println(" connected");
  Serial.print("IP Address: ");
  Serial.println(WiFi.localIP());
}

// Dong bo gio thuc qua NTP de gan timestamp (epoch) cho tung ban tin,
// phuc vu do do tre end-to-end va chong trung lap o phia database.
void syncTime() {
  Serial.print("Syncing NTP");
  configTime(0, 0, "pool.ntp.org", "time.google.com");
  while (time(nullptr) < 1700000000) {
    delay(500);
    Serial.print(".");
  }
  Serial.println(" done");
}

// Hoi truc tiep may chu NTP, tra ve epoch (giay + mili giay)
bool queryNtp(unsigned long &sec, unsigned long &ms) {
  if (ntpIp == IPAddress((uint32_t)0) && !WiFi.hostByName(NTP_SERVER, ntpIp)) {
    return false;
  }

  uint8_t pkt[48] = {0};
  pkt[0] = 0x1B;  // LI = 0, version = 3, mode = 3 (client)

  while (ntpUdp.parsePacket() > 0) {
    ntpUdp.flush();  // bo cac phan hoi cu den tre
  }
  ntpUdp.beginPacket(ntpIp, 123);
  ntpUdp.write(pkt, sizeof(pkt));
  if (!ntpUdp.endPacket()) {
    return false;
  }

  unsigned long start = millis();
  while (millis() - start < 1000) {
    if (ntpUdp.parsePacket() >= 48) {
      ntpUdp.read(pkt, sizeof(pkt));
      // Transmit timestamp: byte 40..43 la giay, 44..47 la phan le cua giay
      uint32_t ntpSec = ((uint32_t)pkt[40] << 24) | ((uint32_t)pkt[41] << 16) |
                        ((uint32_t)pkt[42] << 8) | pkt[43];
      uint32_t frac = ((uint32_t)pkt[44] << 24) | ((uint32_t)pkt[45] << 16) |
                      ((uint32_t)pkt[46] << 8) | pkt[47];
      if (ntpSec == 0) {
        return false;
      }
      sec = ntpSec - NTP_EPOCH_OFFSET;
      ms = (unsigned long)(((uint64_t)frac * 1000) >> 32);
      return true;
    }
    delay(5);
  }
  return false;
}

void connectMQTT() {
  while (!mqttClient.connected()) {
    String clientId = String("ESP32-Bai2-") + DEVICE_ID;
    Serial.print("Connecting MQTT...");
    if (mqttClient.connect(clientId.c_str())) {
      Serial.println(" connected");
    } else {
      Serial.printf(" failed, rc=%d. Retry in 2 s\n", mqttClient.state());
      delay(2000);
    }
  }
}

float readDistanceCm() {
  digitalWrite(TRIG_PIN, LOW);
  delayMicroseconds(2);
  digitalWrite(TRIG_PIN, HIGH);
  delayMicroseconds(10);
  digitalWrite(TRIG_PIN, LOW);

  unsigned long duration = pulseIn(ECHO_PIN, HIGH, 30000);
  if (duration == 0) {
    return NAN;
  }
  return duration * 0.0343f / 2.0f;
}

void setup() {
  Serial.begin(115200);

  pinMode(TRIG_PIN, OUTPUT);
  pinMode(ECHO_PIN, INPUT);
  pinMode(LED_PIN, OUTPUT);
  digitalWrite(LED_PIN, LOW);

  dht.setup(DHT_PIN, DHTesp::DHT22);

  connectWiFi();
  syncTime();
  ntpUdp.begin(2390);

  snprintf(mqttTopic, sizeof(mqttTopic), "iot/%s/%s/telemetry", SITE, DEVICE_ID);
  mqttClient.setServer(MQTT_SERVER, MQTT_PORT);
  mqttClient.setBufferSize(384);
}

void loop() {
  if (WiFi.status() != WL_CONNECTED) {
    connectWiFi();
  }
  if (!mqttClient.connected()) {
    connectMQTT();
  }
  mqttClient.loop();

  unsigned long now = millis();
  if (now - lastSend < SEND_INTERVAL_MS) {
    return;
  }
  lastSend = now;

  TempAndHumidity data = dht.getTempAndHumidity();
  float distance = readDistanceCm();

  if (!isfinite(data.temperature) ||
      !isfinite(data.humidity) ||
      !isfinite(distance)) {
    Serial.println("Invalid sensor data - skip publish");
    return;
  }

  sequenceNo++;

  // ts la epoch tinh bang mili giay, ghep tu giay + 3 chu so mili giay
  unsigned long tsSec, tsMs;
  if (!NTP_PER_MESSAGE || !queryNtp(tsSec, tsMs)) {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    tsSec = tv.tv_sec;
    tsMs = tv.tv_usec / 1000;
  }

  char payload[256];
  snprintf(
      payload,
      sizeof(payload),
      "{\"device_id\":\"%s\","
      "\"ts\":%lu%03lu,"
      "\"seq\":%lu,"
      "\"temperature\":%.2f,"
      "\"humidity\":%.2f,"
      "\"distance_cm\":%.2f,"
      "\"rssi\":%d}",
      DEVICE_ID,
      tsSec,
      tsMs,
      sequenceNo,
      data.temperature,
      data.humidity,
      distance,
      WiFi.RSSI());

  bool ok = mqttClient.publish(mqttTopic, payload);
  Serial.printf("%s | publish=%s\n", payload, ok ? "OK" : "FAILED");

  digitalWrite(LED_PIN, HIGH);
  delay(80);
  digitalWrite(LED_PIN, LOW);
}
