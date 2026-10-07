"""Gửi vài bản tin sai định dạng để kiểm tra khâu validate của collector.

Chạy: python tools/send_invalid.py
"""
import os
import sys
import time

import paho.mqtt.publish as publish

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402

TOPIC = "iot/b23dcat041/esp32-01/telemetry"
now_ms = int(time.time() * 1000)

BAD_MESSAGES = [
    "day khong phai JSON",
    '{"device_id":"esp32-01","ts":%d,"seq":1,"temperature":"abc","humidity":60,"distance_cm":100}' % now_ms,
    '{"device_id":"esp32-01","ts":%d,"seq":1,"temperature":25,"humidity":150,"distance_cm":100}' % now_ms,
    '{"device_id":"esp32-01","temperature":25,"humidity":60}',
]

for message in BAD_MESSAGES:
    publish.single(TOPIC, message, hostname=config.MQTT_HOST, port=config.MQTT_PORT)
    print("Đã gửi:", message)
