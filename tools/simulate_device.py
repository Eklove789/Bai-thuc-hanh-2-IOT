"""Thiết bị giả lập để kiểm thử pipeline khi không mở Wokwi.

Cố ý chèn outlier, bỏ bản tin (mất gói) và bản tin sai định dạng.
Chạy: python tools/simulate_device.py --interval 1
"""
import argparse
import json
import math
import os
import random
import sys
import time

import paho.mqtt.client as mqtt

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402

parser = argparse.ArgumentParser()
parser.add_argument("--device", default="sim-01")
parser.add_argument("--interval", type=float, default=5.0)
args = parser.parse_args()

topic = f"iot/b23dcat041/{args.device}/telemetry"
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"sim-{args.device}")
client.connect(config.MQTT_HOST, config.MQTT_PORT)
client.loop_start()

seq = 0
while True:
    seq += 1
    t = time.time()
    payload = {
        "device_id": args.device,
        "ts": int(t * 1000),
        "seq": seq,
        "temperature": round(27 + 2 * math.sin(t / 120) + random.gauss(0, 0.2), 2),
        "humidity": round(60 + 5 * math.cos(t / 180) + random.gauss(0, 0.5), 2),
        "distance_cm": round(120 + random.gauss(0, 2), 2),
        "rssi": -60,
    }
    roll = random.random()
    if roll < 0.03:
        payload["temperature"] = 65.0          # outlier nhưng vẫn trong dải vật lý
    elif roll < 0.05:
        payload["humidity"] = 150.0            # sai dải -> collector phải loại
    if 0.05 <= roll < 0.08:
        print(f"seq={seq} bị bỏ (giả lập mất gói)")
    else:
        client.publish(topic, json.dumps(payload))
        print(payload)
    time.sleep(args.interval)
