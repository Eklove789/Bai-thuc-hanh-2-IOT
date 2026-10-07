"""Cấu hình dùng chung, đọc từ file .env."""
import os

from dotenv import load_dotenv

load_dotenv()

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "iot/+/+/telemetry")

INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_ORG = os.getenv("INFLUX_ORG", "ptit")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "")
BUCKET_RAW = os.getenv("INFLUX_BUCKET_RAW", "iot_raw")
BUCKET_CLEAN = os.getenv("INFLUX_BUCKET_CLEAN", "iot_clean")
RETENTION_CLEAN_DAYS = int(os.getenv("INFLUX_RETENTION_CLEAN_DAYS", "30"))

MEASUREMENT_RAW = "sensor_data"
MEASUREMENT_CLEAN = "sensor_clean"

# Các đại lượng đo và dải giá trị hợp lệ (theo datasheet DHT22 / HC-SR04)
SENSOR_RANGES = {
    "temperature": (-40.0, 80.0),
    "humidity": (0.0, 100.0),
    # HC-SR04 đo tối đa 400 cm; chừa biên vì sai số quy đổi thời gian -> khoảng cách
    "distance_cm": (2.0, 450.0),
}
SENSOR_FIELDS = list(SENSOR_RANGES)
