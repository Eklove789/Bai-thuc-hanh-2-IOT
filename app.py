"""App giám sát bằng Streamlit.

Chạy: streamlit run app.py
"""
import time

import pandas as pd
import streamlit as st
from influxdb_client import InfluxDBClient

import config

LABELS = {"temperature": "Nhiệt độ (°C)", "humidity": "Độ ẩm (%RH)", "distance_cm": "Khoảng cách (cm)"}

st.set_page_config(page_title="Giám sát IoT - Bài 2", layout="wide")


@st.cache_resource
def get_client():
    return InfluxDBClient(url=config.INFLUX_URL, token=config.INFLUX_TOKEN, org=config.INFLUX_ORG)


def query(bucket, measurement, time_range):
    flux = f'''
from(bucket: "{bucket}")
  |> range(start: -{time_range})
  |> filter(fn: (r) => r._measurement == "{measurement}")
  |> pivot(rowKey: ["_time", "device_id"], columnKey: ["_field"], valueColumn: "_value")
'''
    df = get_client().query_api().query_data_frame(flux)
    if isinstance(df, list):
        df = pd.concat(df, ignore_index=True) if df else pd.DataFrame()
    if df.empty:
        return df
    return df.set_index("_time").sort_index()


st.title("Giám sát dữ liệu IoT - Bài thực hành số 2")

with st.sidebar:
    time_range = st.selectbox("Khoảng thời gian", ["15m", "1h", "6h", "24h"], index=1)
    auto_refresh = st.checkbox("Tự làm mới mỗi 5 giây", value=True)

try:
    raw = query(config.BUCKET_RAW, config.MEASUREMENT_RAW, time_range)
    clean = query(config.BUCKET_CLEAN, config.MEASUREMENT_CLEAN, time_range)
except Exception as exc:
    st.error(f"Không đọc được InfluxDB: {exc}")
    st.stop()

if raw.empty:
    st.info("Chưa có dữ liệu thô. Hãy chạy mô phỏng Wokwi và collector.py.")
else:
    devices = sorted(raw["device_id"].unique())
    device = st.sidebar.selectbox("Thiết bị", devices)
    raw = raw[raw["device_id"] == device]
    if not clean.empty:
        clean = clean[clean["device_id"] == device]

    latest = raw.iloc[-1]
    cols = st.columns(3)
    for col, field in zip(cols, config.SENSOR_FIELDS):
        col.metric(LABELS[field], f"{latest[field]:.1f}")

    st.subheader("Chất lượng pipeline")
    received = len(raw)
    lost = int(raw["lost"].sum())
    latency = raw["latency_ms"]
    cols = st.columns(5)
    cols[0].metric("Bản tin nhận", received)
    cols[1].metric("Bản tin mất", lost, f"{100 * lost / (received + lost):.2f} %", delta_color="off")
    cols[2].metric("Độ trễ TB (ms)", f"{latency.mean():.0f}")
    cols[3].metric("Độ trễ p95 (ms)", f"{latency.quantile(0.95):.0f}")
    cols[4].metric("Độ trễ max (ms)", f"{latency.max():.0f}")

    tab_raw, tab_clean, tab_latency = st.tabs(["Dữ liệu thô", "Dữ liệu đã xử lý", "Độ trễ"])
    with tab_raw:
        for field in config.SENSOR_FIELDS:
            st.caption(LABELS[field])
            st.line_chart(raw[field])
    with tab_clean:
        if clean.empty:
            st.info("Chưa có dữ liệu đã xử lý. Hãy chạy preprocess.py.")
        else:
            for field in config.SENSOR_FIELDS:
                st.caption(f"{LABELS[field]} - trung bình 1 phút và trung bình trượt")
                st.line_chart(clean[[field, f"{field}_roll"]])
            st.dataframe(clean.drop(columns=["result", "table", "_start", "_stop", "_measurement"], errors="ignore").tail(20))
    with tab_latency:
        st.line_chart(latency)

if auto_refresh:
    time.sleep(5)
    st.rerun()
