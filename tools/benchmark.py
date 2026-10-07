"""Đo hiệu năng ghi và truy vấn InfluxDB, lưu kết quả vào benchmark.json.

Ghi vào bucket tạm iot_bench (xóa sau khi đo) nên không ảnh hưởng dữ liệu thực nghiệm.
Chạy: python tools/benchmark.py
"""
import json
import os
import random
import statistics
import sys
import time

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import config  # noqa: E402

BENCH_BUCKET = "iot_bench"
SINGLE_POINTS = 300
BATCH_POINTS = 5000
BATCH_SIZE = 500
QUERY_REPEATS = 5


def make_points(count, device):
    now_ms = int(time.time() * 1000)
    return [
        Point(config.MEASUREMENT_RAW)
        .tag("device_id", device)
        .tag("site", "bench")
        .field("temperature", 25 + random.random())
        .field("humidity", 60 + random.random())
        .field("distance_cm", 100 + random.random())
        .field("seq", i)
        .time(now_ms - (count - i) * 1000, WritePrecision.MS)
        for i in range(count)
    ]


def time_query(client, flux):
    durations = []
    for _ in range(QUERY_REPEATS):
        started = time.perf_counter()
        tables = client.query_api().query(flux)
        durations.append((time.perf_counter() - started) * 1000)
    rows = sum(len(table.records) for table in tables)
    return statistics.median(durations), rows


def main():
    result = {"measured_at": time.strftime("%Y-%m-%d %H:%M:%S")}
    with InfluxDBClient(url=config.INFLUX_URL, token=config.INFLUX_TOKEN, org=config.INFLUX_ORG) as client:
        buckets = client.buckets_api()
        old = buckets.find_bucket_by_name(BENCH_BUCKET)
        if old is not None:
            buckets.delete_bucket(old)
        bucket = buckets.create_bucket(bucket_name=BENCH_BUCKET, org=config.INFLUX_ORG)
        write_api = client.write_api(write_options=SYNCHRONOUS)
        try:
            # Ghi từng điểm một, giống cách collector đang ghi
            durations = []
            for point in make_points(SINGLE_POINTS, "bench-single"):
                started = time.perf_counter()
                write_api.write(bucket=BENCH_BUCKET, record=point)
                durations.append((time.perf_counter() - started) * 1000)
            result["single_write_ms_mean"] = statistics.mean(durations)
            result["single_write_ms_p95"] = statistics.quantiles(durations, n=20)[18]
            result["single_points_per_s"] = 1000 / statistics.mean(durations)

            # Ghi theo lô
            points = make_points(BATCH_POINTS, "bench-batch")
            started = time.perf_counter()
            for i in range(0, BATCH_POINTS, BATCH_SIZE):
                write_api.write(bucket=BENCH_BUCKET, record=points[i:i + BATCH_SIZE])
            elapsed = time.perf_counter() - started
            result["batch_size"] = BATCH_SIZE
            result["batch_points_per_s"] = BATCH_POINTS / elapsed

            # Truy vấn
            result["query_bench_ms"], result["query_bench_rows"] = time_query(client, f'''
from(bucket: "{BENCH_BUCKET}") |> range(start: -3h)
  |> filter(fn: (r) => r._measurement == "{config.MEASUREMENT_RAW}" and r.device_id == "bench-batch")
  |> filter(fn: (r) => r._field == "temperature")''')
            result["query_raw_1h_ms"], result["query_raw_1h_rows"] = time_query(client, f'''
from(bucket: "{config.BUCKET_RAW}") |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "{config.MEASUREMENT_RAW}")''')
            result["query_clean_1h_ms"], result["query_clean_1h_rows"] = time_query(client, f'''
from(bucket: "{config.BUCKET_CLEAN}") |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "{config.MEASUREMENT_CLEAN}")''')
        finally:
            buckets.delete_bucket(bucket)

    out = os.path.join(ROOT, "benchmark.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    for key, value in result.items():
        print(f"{key}: {value:.2f}" if isinstance(value, float) else f"{key}: {value}")


if __name__ == "__main__":
    main()
