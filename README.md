# Bài thực hành số 2 - Thu thập, lưu trữ và tiền xử lý dữ liệu IoT

Pipeline: **ESP32 (Wokwi) → MQTT (Mosquitto) → collector.py → InfluxDB → preprocess.py → Grafana / Streamlit**

```
ESP32 + DHT22 + HC-SR04 ──MQTT──> Mosquitto ──> collector.py ──> InfluxDB: iot_raw / sensor_data
   (Wokwi trên VS Code)                         (validate)                │
                                                                preprocess.py
                                                                          │
                              Grafana, Streamlit <── InfluxDB: iot_clean / sensor_clean
```

## Cấu trúc thư mục

| Đường dẫn | Nội dung |
|---|---|
| `firmware/` | Firmware ESP32 (PlatformIO + Wokwi) |
| `collector.py` | Subscribe MQTT, validate, ghi InfluxDB |
| `preprocess.py` | Làm sạch, outlier, resampling, đặc trưng, chuẩn hóa |
| `app.py` | App giám sát Streamlit |
| `tools/simulate_device.py` | Thiết bị giả lập để kiểm thử khi không mở Wokwi |
| `tools/send_invalid.py` | Gửi bản tin sai định dạng để kiểm tra validate |
| `tools/benchmark.py` | Đo hiệu năng ghi và truy vấn InfluxDB |
| `docker-compose.yml`, `mosquitto/`, `grafana/` | Hạ tầng và cấu hình |
| `.env.example` | Mẫu file cấu hình |

## Yêu cầu

- Docker Desktop, Python 3.10+
- VS Code với extension PlatformIO và Wokwi Simulator

## Cách chạy

1. Tạo file cấu hình và đổi mật khẩu, token:
   ```
   copy .env.example .env
   ```
2. Khởi động Mosquitto, InfluxDB, Grafana:
   ```
   docker compose up -d
   ```
3. Cài thư viện Python:
   ```
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   ```
4. Chạy collector (để chạy suốt buổi thực hành):
   ```
   python collector.py
   ```
5. Chạy thiết bị: mở thư mục `firmware/` bằng VS Code, build bằng PlatformIO, rồi
   `F1` → **Wokwi: Start Simulator**. Serial Monitor phải in `publish=OK`.
   Không mở Wokwi thì dùng `python tools/simulate_device.py`.
6. Chạy tiền xử lý (lặp lại mỗi 60 giây):
   ```
   python preprocess.py --range 1h --every 60
   ```
7. Xem kết quả:
   - Grafana: http://localhost:3000/d/iot-bai2 (xem không cần đăng nhập; sửa thì dùng admin / `GRAFANA_PASSWORD`)
   - Streamlit: `streamlit run app.py` → http://localhost:8501
   - InfluxDB UI: http://localhost:8086

## Định dạng dữ liệu

Topic: `iot/<site>/<device_id>/telemetry`, ví dụ `iot/b23dcat041/esp32-01/telemetry`

```json
{"device_id":"esp32-01","ts":1791356400123,"seq":42,
 "temperature":27.50,"humidity":61.00,"distance_cm":120.40,"rssi":-60}
```

`ts` là epoch mili giây lấy qua NTP trên thiết bị, `seq` là số thứ tự bản tin.

## Schema InfluxDB

| | Dữ liệu thô | Dữ liệu đã xử lý |
|---|---|---|
| Bucket | `iot_raw` | `iot_clean` |
| Retention | 7 ngày | 30 ngày |
| Measurement | `sensor_data` | `sensor_clean` |
| Tags | `device_id`, `site` | `device_id` |
| Fields | `temperature`, `humidity`, `distance_cm`, `rssi`, `seq`, `lost`, `latency_ms`, `prev_write_ms`, `ts_fallback` | với mỗi đại lượng `x`: `x`, `x_roll`, `x_delta`, `x_norm` |
| Timestamp | `ts` của thiết bị (ms) | mốc đầu cửa sổ 1 phút |

- **Trùng lặp:** timestamp lấy từ thiết bị nên bản tin gửi lặp trùng (measurement, tags, time) và bị InfluxDB ghi đè.
- **Mất mát:** collector so sánh `seq` liên tiếp, ghi số bản tin mất vào field `lost`. Khi InfluxDB ngắt, điểm dữ liệu được giữ trong bộ đệm (tối đa 10 000 điểm) và ghi bù khi kết nối lại.
- **Validate:** bản tin sai JSON, thiếu khóa, sai kiểu, NaN hoặc ngoài dải vật lý bị loại và ghi log.

## Các bước tiền xử lý

1. Phát hiện outlier bằng IQR trên từng mẫu thô (ngoài `Q1 − 1.5·IQR` .. `Q3 + 1.5·IQR`), coi như giá trị thiếu.
2. Đưa về lưới thời gian đều 15 giây để lộ ra các khoảng thiếu.
3. Nội suy theo thời gian cho khoảng thiếu tối đa 60 giây; khoảng dài hơn để trống.
4. Resampling trung bình theo cửa sổ 1 phút.
5. Đặc trưng: trung bình trượt 5 cửa sổ (`_roll`), độ biến thiên (`_delta`), chuẩn hóa z-score (`_norm`).
6. Ghi vào `iot_clean / sensor_clean`.

## Đo độ trễ

`latency_ms = thời điểm collector nhận − ts của thiết bị`. Xem trung bình, p95, max trên Grafana hoặc Streamlit.
Hai đầu dùng chung mốc thời gian NTP: firmware lấy `ts` trực tiếp từ máy chủ NTP cho từng bản tin (`NTP_PER_MESSAGE`, cần thiết vì Wokwi mô phỏng chậm hơn thời gian thực), collector hiệu chỉnh độ lệch đồng hồ máy theo cùng máy chủ.

## Tạo dữ liệu thử nghiệm trên Wokwi

Không bấm tạm dừng mô phỏng: đồng hồ của ESP32 sẽ dừng theo và làm sai phép đo độ trễ.

- **Outlier:** bấm vào DHT22, kéo nhiệt độ lên rất cao (ví dụ 75 °C) trong khoảng 10 giây rồi trả về giá trị cũ.
- **Dữ liệu thiếu:** `docker compose stop mosquitto`, chờ 40 giây, rồi `docker compose start mosquitto`.
- **Dữ liệu không hợp lệ:** `python tools/send_invalid.py` (collector phải báo loại 4 bản tin).

## Đo hiệu năng InfluxDB

```
python tools/benchmark.py
```

Đo tốc độ ghi từng điểm, ghi theo lô và thời gian truy vấn trên một bucket tạm, lưu kết quả vào `benchmark.json`.

## Ghi chú

- Firmware dùng `host.wokwi.internal` để ESP32 mô phỏng truy cập Mosquitto trên máy; chỉ hoạt động với Wokwi trên VS Code. Nếu chạy trên wokwi.com, đổi `MQTT_SERVER` trong `firmware/src/sketch.ino` và `MQTT_HOST` trong `.env` sang một broker công cộng (ví dụ `broker.hivemq.com`).
- Không commit file `.env`.
