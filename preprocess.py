"""Tiền xử lý: đọc bucket thô -> làm sạch -> resample -> đặc trưng -> ghi bucket sạch.

Chạy một lần:      python preprocess.py --range 1h
Chạy định kỳ:      python preprocess.py --range 1h --every 60
"""
import argparse
import logging
import time

import pandas as pd
from influxdb_client import BucketRetentionRules, InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from sklearn.preprocessing import StandardScaler

import config

GRID = "15s"            # lưới thời gian đều để lộ ra các khoảng thiếu dữ liệu
MAX_GAP_STEPS = 4       # chỉ nội suy khoảng thiếu <= 4 bước (60 s)
RESAMPLE_WINDOW = "1min"
ROLLING_WINDOW = 5
# Sàn cho IQR: dữ liệu mô phỏng gần như hằng số nên IQR có thể bằng 0
MIN_IQR = {"temperature": 1.0, "humidity": 2.0, "distance_cm": 5.0}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("preprocess")


def ensure_clean_bucket(client):
    buckets = client.buckets_api()
    if buckets.find_bucket_by_name(config.BUCKET_CLEAN) is None:
        rule = BucketRetentionRules(type="expire", every_seconds=config.RETENTION_CLEAN_DAYS * 86400)
        buckets.create_bucket(bucket_name=config.BUCKET_CLEAN, retention_rules=rule, org=config.INFLUX_ORG)
        log.info("Đã tạo bucket %s (retention %d ngày)", config.BUCKET_CLEAN, config.RETENTION_CLEAN_DAYS)


def read_raw(client, time_range):
    field_filter = " or ".join(f'r._field == "{f}"' for f in config.SENSOR_FIELDS)
    keep = ", ".join(f'"{c}"' for c in ["_time", "device_id", *config.SENSOR_FIELDS])
    flux = f'''
from(bucket: "{config.BUCKET_RAW}")
  |> range(start: -{time_range})
  |> filter(fn: (r) => r._measurement == "{config.MEASUREMENT_RAW}")
  |> filter(fn: (r) => {field_filter})
  |> pivot(rowKey: ["_time", "device_id"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: [{keep}])
'''
    df = client.query_api().query_data_frame(flux)
    if isinstance(df, list):
        df = pd.concat(df, ignore_index=True)
    return df


def outlier_mask(values, col):
    """Đánh dấu các mẫu nằm ngoài [Q1 - 1.5*IQR, Q3 + 1.5*IQR]."""
    q1, q3 = values.quantile([0.25, 0.75])
    iqr = max(q3 - q1, MIN_IQR[col])
    return (values < q1 - 1.5 * iqr) | (values > q3 + 1.5 * iqr)


def clean_device(df):
    """Trả về (DataFrame đã xử lý theo cửa sổ 1 phút, dict thống kê)."""
    series = df.set_index("_time").sort_index()[config.SENSOR_FIELDS]

    report = {"raw_points": len(series), "outliers": 0}

    # 1. Phát hiện outlier bằng IQR trên từng mẫu thô, coi như giá trị thiếu
    for col in config.SENSOR_FIELDS:
        mask = outlier_mask(series[col], col)
        report["outliers"] += int(mask.sum())
        series.loc[mask, col] = float("nan")

    # 2. Đưa về lưới thời gian đều -> khoảng thiếu thành NaN
    grid = series.resample(GRID).mean()
    report["missing"] = int(grid.isna().any(axis=1).sum())

    # 3. Nội suy theo thời gian cho khoảng thiếu ngắn; khoảng dài để nguyên NaN
    grid = grid.interpolate(method="time", limit=MAX_GAP_STEPS, limit_area="inside")

    # 4. Resampling theo cửa sổ thời gian
    out = grid.resample(RESAMPLE_WINDOW).mean().dropna(how="all")

    # 5. Đặc trưng: trung bình trượt, độ biến thiên, chuẩn hóa z-score
    for col in config.SENSOR_FIELDS:
        out[f"{col}_roll"] = out[col].rolling(ROLLING_WINDOW, min_periods=1).mean()
        out[f"{col}_delta"] = out[col].diff()
    valid = out[config.SENSOR_FIELDS].dropna()
    if not valid.empty:
        norm_cols = [f"{col}_norm" for col in config.SENSOR_FIELDS]
        out.loc[valid.index, norm_cols] = StandardScaler().fit_transform(valid)

    report["clean_points"] = len(out)
    return out, report


def run(client, time_range):
    raw = read_raw(client, time_range)
    if raw.empty:
        log.warning("Không có dữ liệu thô trong %s gần nhất", time_range)
        return

    write_api = client.write_api(write_options=SYNCHRONOUS)
    for device_id, df in raw.groupby("device_id"):
        out, report = clean_device(df)
        out["device_id"] = device_id
        # Timestamp theo mốc phút nên chạy lại sẽ ghi đè, không tạo bản ghi trùng
        write_api.write(
            bucket=config.BUCKET_CLEAN,
            record=out,
            data_frame_measurement_name=config.MEASUREMENT_CLEAN,
            data_frame_tag_columns=["device_id"],
        )
        log.info("%s: %s", device_id, report)


def main():
    parser = argparse.ArgumentParser(description="Tiền xử lý dữ liệu IoT")
    parser.add_argument("--range", default="1h", help="khoảng thời gian đọc, ví dụ 30m, 1h, 24h")
    parser.add_argument("--every", type=int, default=0, help="lặp lại sau mỗi N giây (0 = chạy một lần)")
    args = parser.parse_args()

    with InfluxDBClient(url=config.INFLUX_URL, token=config.INFLUX_TOKEN, org=config.INFLUX_ORG) as client:
        ensure_clean_bucket(client)
        while True:
            run(client, args.range)
            if args.every <= 0:
                break
            time.sleep(args.every)


if __name__ == "__main__":
    main()
