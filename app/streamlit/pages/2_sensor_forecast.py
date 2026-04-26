from __future__ import annotations
import datetime
import sys
from pathlib import Path
_STREAMLIT_DIR = Path(__file__).resolve().parents[1]
if str(_STREAMLIT_DIR) not in sys.path:
    sys.path.insert(0, str(_STREAMLIT_DIR))
import pandas as pd
import streamlit as st
from components import api_client as api
from components import prophet_trainer as pt
from components.cards import status_badge
from components.charts import forecast_chart
from components.filters import filter_row_forecast


# Set Page title
st.set_page_config(page_title="Sensor Forecast", layout="wide")
st.title("Time Series Sensor Forecast")


# ─── Fetch metrics list สำหรับ filter dropdowns ───────────
try:
    metrics_data = api.get("/api/sensor/available-metrics")
    metrics_list = metrics_data.get("rows", [])
    
except Exception as exc:
    st.error(f"Cannot reach API: {exc}")
    st.stop()

if not metrics_list:
    st.error("No sensor metrics defined in DIM_METRIC")
    st.stop()


# ─── Filters ──────────────────────────────────────────────
filters = filter_row_forecast(metrics_list)
machine = filters["machine"]
metric = filters["metric"]
horizon_hours = int(filters["horizon"].split()[0])
train_clicked = filters["train_clicked"]


# Lookup threshold สำหรับ metric ที่เลือก (จาก DIM_METRIC.critical_threshold)
metric_meta = next((m for m in metrics_list if m.get("metric_name") == metric), {})
threshold = metric_meta.get("critical_threshold")


# ─── Fetch historical (last 7 days) ──────────────────────
end_date = datetime.date.today()
all_history: list[dict] = []

for i in range(7):
    d = end_date - datetime.timedelta(days=i)
    try:
        chunk = api.get("/api/sensor/by-machine-15min",
                        {"date": d.isoformat(), "metric": metric})
        rows = chunk.get("rows", [])
        # filter ตาม machine ที่เลือก (response มีหลาย machine)
        machine_rows = [r for r in rows if r.get("machine_code") == machine]
        all_history.extend(machine_rows)
    except Exception:
        # หากวันใดดึงไม่ได้ ก็ skip (ดึง 7 วันต่อเนื่อง — บางวันอาจไม่มีข้อมูล)
        continue


history_df = pd.DataFrame(all_history)

if not history_df.empty:
    # Prophet input: ds (datetime) + y (target value)
    history_df = history_df.rename(columns={
        "window_start": "ds",
        "avg_value": "y",
    })
    # format='mixed' — Oracle TIMESTAMP บางแถวมี fractional seconds บางแถวไม่มี
    history_df["ds"] = pd.to_datetime(history_df["ds"], format="mixed")
    history_df = history_df.sort_values("ds").reset_index(drop=True)


# ─── Train trigger ────────────────────────────────────────
if train_clicked:
    if history_df.empty or len(history_df) < 30:
        st.error(f"Need ≥30 historical points; got {len(history_df)}")
    else:
        try:
            pt.trigger_training(machine, metric, history_df[["ds", "y"]])
            st.success(
                f"Training started for {machine} / {metric}. "
                f"Refresh ใน 30 วินาที"
            )
        except ImportError:
            st.error(
                "Prophet ยังไม่ได้ install — รัน `pip install prophet` "
                "(ใช้เวลา 5-10 นาที)"
            )
        except Exception as e:
            st.error(f"Training failed: {e}")


# ─── A: Model status card ─────────────────────────────────
status = pt.model_status(machine, metric)

with st.container(border=True):
    cols = st.columns([3, 1])
    with cols[0]:
        if status["exists"]:
            st.markdown(f"**{machine} / {metric}**  \n{status['status_text']}")
        else:
            st.markdown(f"**{machine} / {metric}**  \n_{status['status_text']}_")
    with cols[1]:
        if status["exists"]:
            st.markdown(status_badge("Ready", "success"),
                        unsafe_allow_html=True)
        elif "Training" in status["status_text"]:
            st.markdown(status_badge("Training", "warning"),
                        unsafe_allow_html=True)
        else:
            st.markdown(status_badge("Not ready", "danger"),
                        unsafe_allow_html=True)


st.divider()


# ─── B: Forecast chart ──────────────────────────────────────
st.subheader("Forecast")

if not status["exists"]:
    st.info("Train a model first to see the forecast")
else:
    try:
        forecast_df = pt.predict(machine, metric, horizon_hours)
    except Exception as exc:
        st.error(f"Predict failed: {exc}")
        forecast_df = None

    if forecast_df is None or forecast_df.empty:
        st.warning("Could not generate forecast")
    else:
        st.plotly_chart(
            forecast_chart(history_df[["ds", "y"]], forecast_df, threshold),
            use_container_width=True,
        )


