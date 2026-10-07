"""Thu thập dữ liệu: subscribe MQTT -> validate -> ghi InfluxDB (bucket thô).

Chạy: python collector.py
"""
import json
import logging
import math
import socket
import struct
import time
from collections import deque

import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

import config

MAX_CLOCK_SKEW_MS = 24 * 3600 * 1000
MAX_DEVICE_LAG_MS = 10_000  # ts lệch giờ nhận quá mức này thì coi đồng hồ thiết bị không tin được
MAX_PENDING = 10_000
NTP_SERVER = "time.google.com"  # cùng máy chủ NTP với firmware
NTP_EPOCH_OFFSET = 2208988800
NTP_REFRESH_S = 300

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("collector.log", encoding="utf-8")],
)
log = logging.getLogger("collector")

influx = InfluxDBClient(url=config.INFLUX_URL, token=config.INFLUX_TOKEN, org=config.INFLUX_ORG)
write_api = influx.write_api(write_options=SYNCHRONOUS)

# Điểm chưa ghi được (database tạm ngắt) được giữ lại để ghi bù ở lần sau
pending = deque(maxlen=MAX_PENDING)
last_seq = {}
clock = {"offset_ms": 0, "checked": 0.0, "last_write_ms": 0.0}
stats = {"ok": 0, "invalid": 0, "lost": 0, "duplicate": 0, "ts_fallback": 0}


def is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def ntp_offset_ms():
    """Độ lệch (ms) của đồng hồ máy này so với máy chủ NTP: giờ NTP - giờ máy."""
    packet = bytes([0x1B]) + bytes(47)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(2)
        sent = time.time()
        sock.sendto(packet, (NTP_SERVER, 123))
        data, _ = sock.recvfrom(48)
        received = time.time()
    seconds, fraction = struct.unpack("!II", data[40:48])
    server_time = seconds - NTP_EPOCH_OFFSET + fraction / 2**32
    return round((server_time - (sent + received) / 2) * 1000)


def now_ms():
    """Giờ hiện tại theo NTP, để so được với ts mà thiết bị cũng lấy từ NTP."""
    if time.time() - clock["checked"] > NTP_REFRESH_S:
        clock["checked"] = time.time()
        try:
            clock["offset_ms"] = ntp_offset_ms()
            log.info("Đồng hồ máy lệch %+d ms so với %s", clock["offset_ms"], NTP_SERVER)
        except OSError as exc:
            log.warning("Không hỏi được NTP, giữ độ lệch %+d ms: %s", clock["offset_ms"], exc)
    return time.time_ns() // 1_000_000 + clock["offset_ms"]


def validate(raw, recv_ms):
    """Trả về dict hợp lệ hoặc ném ValueError kèm lý do."""
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("payload không phải JSON object")

    device_id = data.get("device_id")
    if not isinstance(device_id, str) or not device_id:
        raise ValueError("thiếu device_id")

    for key in ("ts", "seq"):
        if not isinstance(data.get(key), int) or isinstance(data.get(key), bool):
            raise ValueError(f"{key} phải là số nguyên")
    if abs(recv_ms - data["ts"]) > MAX_CLOCK_SKEW_MS:
        raise ValueError(f"ts lệch quá xa so với giờ hệ thống: {data['ts']}")

    for key, (low, high) in config.SENSOR_RANGES.items():
        value = data.get(key)
        if not is_number(value):
            raise ValueError(f"{key} thiếu hoặc không phải số")
        if not low <= value <= high:
            raise ValueError(f"{key}={value} ngoài dải [{low}, {high}]")
    return data


def track_sequence(device_id, seq):
    """Đếm số bản tin bị mất dựa trên khoảng nhảy của seq."""
    prev = last_seq.get(device_id)
    last_seq[device_id] = seq
    if prev is None or seq < prev:  # lần đầu hoặc thiết bị khởi động lại
        return 0
    if seq == prev:
        stats["duplicate"] += 1
        return 0
    return seq - prev - 1


def flush():
    try:
        started = time.perf_counter()
        write_api.write(bucket=config.BUCKET_RAW, record=list(pending))
        clock["last_write_ms"] = (time.perf_counter() - started) * 1000
        pending.clear()
    except Exception as exc:
        log.error("Ghi InfluxDB lỗi, giữ lại %d điểm chờ ghi bù: %s", len(pending), exc)


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        log.error("Kết nối MQTT thất bại: %s", reason_code)
        return
    log.info("Đã kết nối MQTT %s:%d, subscribe %s", config.MQTT_HOST, config.MQTT_PORT, config.MQTT_TOPIC)
    client.subscribe(config.MQTT_TOPIC, qos=1)


def on_disconnect(client, userdata, flags, reason_code, properties):
    log.warning("Mất kết nối MQTT (%s), đang tự kết nối lại...", reason_code)


def on_message(client, userdata, msg):
    recv_ms = now_ms()
    try:
        data = validate(msg.payload, recv_ms)
    except ValueError as exc:
        stats["invalid"] += 1
        log.warning("Bỏ bản tin không hợp lệ trên %s: %s", msg.topic, exc)
        return

    device_id = data["device_id"]
    lost = track_sequence(device_id, data["seq"])
    stats["lost"] += lost
    if lost:
        log.warning("%s mất %d bản tin trước seq=%d", device_id, lost, data["seq"])

    parts = msg.topic.split("/")
    site = parts[1] if len(parts) >= 4 else "unknown"

    # Timestamp lấy từ thiết bị: bản tin gửi lặp sẽ trùng (measurement, tags, time)
    # nên InfluxDB tự ghi đè thay vì tạo bản ghi trùng.
    # Nếu ts lệch quá xa giờ nhận (thiết bị không đồng bộ được NTP) thì dùng giờ nhận
    # làm timestamp và không tính độ trễ cho bản tin đó.
    latency_ms = recv_ms - data["ts"]
    device_time_ok = abs(latency_ms) <= MAX_DEVICE_LAG_MS
    if not device_time_ok:
        stats["ts_fallback"] += 1
        log.warning("%s seq=%d có ts lệch %d ms, dùng giờ nhận làm timestamp", device_id, data["seq"], latency_ms)
    point = (
        Point(config.MEASUREMENT_RAW)
        .tag("device_id", device_id)
        .tag("site", site)
        .field("seq", data["seq"])
        .field("lost", lost)
        .field("ts_fallback", int(not device_time_ok))
        # Thời gian ghi InfluxDB của bản tin liền trước (bản tin này chưa ghi nên chưa đo được)
        .field("prev_write_ms", float(clock["last_write_ms"]))
        .time(data["ts"] if device_time_ok else recv_ms, WritePrecision.MS)
    )
    if device_time_ok:
        point.field("latency_ms", latency_ms)
    for key in config.SENSOR_FIELDS:
        point.field(key, float(data[key]))
    if is_number(data.get("rssi")):
        point.field("rssi", int(data["rssi"]))

    pending.append(point)
    flush()

    stats["ok"] += 1
    if stats["ok"] % 12 == 0:
        log.info("Thống kê: %s", stats)


def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="collector-bai2")
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    now_ms()  # đo độ lệch đồng hồ trước khi nhận bản tin đầu tiên
    client.connect_async(config.MQTT_HOST, config.MQTT_PORT, keepalive=30)
    try:
        client.loop_forever(retry_first_connection=True)
    except KeyboardInterrupt:
        log.info("Dừng collector. Thống kê cuối: %s", stats)
    finally:
        influx.close()


if __name__ == "__main__":
    main()
