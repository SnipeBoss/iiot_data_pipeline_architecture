from __future__ import annotations
import os
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv



# โหลด .env จาก repo root (ขึ้น 2 ชั้นจาก app/streamlit/dashboard.py)
_REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_REPO_ROOT / ".env", override=False)

API_URL = os.environ.get("DASHBOARD_API_URL", "http://localhost:8000")
TOKEN = os.environ.get("ORACLE_API_TOKEN", "")


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {TOKEN}"} if TOKEN else {}


@st.cache_data(ttl=300)                 # 5-min cache (ตรงกับ 15-min refresh)
def _get(path: str, **params) -> dict[str, Any]:
    r = requests.get(f"{API_URL}{path}", headers=_headers(), params=params, timeout=20)
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# Page config + sidebar
# ---------------------------------------------------------------------------


st.set_page_config(page_title="Process Performance Dashboard", layout="wide")
st.title("🔋 Battery Process Performance Dashboard")
st.caption(f"Data refresh every 15 min · source: {API_URL}")

# Date picker
try:
    dates = _get("/api/production/available-dates").get("dates", [])
except Exception as exc:
    st.error(f"Cannot reach API: {exc}")
    st.stop()

if not dates:
    st.warning("ยังไม่มีข้อมูลใน DW — รัน Airflow DAG ก่อน")
    st.stop()

selected_date = st.sidebar.selectbox("เลือกวัน", dates)
if st.sidebar.button("🔄 Refresh cache"):
    st.cache_data.clear()
    st.rerun()


# ---------------------------------------------------------------------------
# Tab 1 — Production Overview
# ---------------------------------------------------------------------------


tab_prod, tab_quality, tab_sensor, tab_machine = st.tabs([
    "Production", "Quality", "Sensor per Batch", "Machine Status (15-min)",
])


with tab_prod:
    summary = _get("/api/production/summary", date=selected_date).get("summary", {})
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Batches", summary.get("total_batches", 0))
    c2.metric("Total Output", summary.get("total_out", 0))
    c3.metric("Avg Yield %", f"{summary.get('avg_yield_pct', 0):.2f}%")
    c4.metric("Avg Duration", f"{summary.get('avg_duration_min', 0):.1f} min")

    st.subheader("Batches for " + selected_date)
    batches = _get("/api/production/by-batch", date=selected_date).get("rows", [])
    if batches:
        df = pd.DataFrame(batches)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("ไม่มี batch ในวันที่เลือก")


# ---------------------------------------------------------------------------
# Tab 2 — Quality / Defect Rate
# ---------------------------------------------------------------------------


with tab_quality:
    qc = _get("/api/quality/defect-rate", date=selected_date)
    overall = qc.get("overall", {})
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Sampled", overall.get("total_sampled", 0))
    c2.metric("Passed", overall.get("total_passed", 0))
    c3.metric("Failed", overall.get("total_failed", 0))
    c4.metric("Defect %", f"{overall.get('overall_defect_pct', 0):.2f}%")

    st.subheader("QC per batch")
    per_batch = qc.get("per_batch", [])
    if per_batch:
        df = pd.DataFrame(per_batch)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("ไม่มี qc record ในวันที่เลือก")


# ---------------------------------------------------------------------------
# Tab 3 — Sensor per Batch
# ---------------------------------------------------------------------------


with tab_sensor:
    batches = _get("/api/production/by-batch", date=selected_date).get("rows", [])
    if not batches:
        st.info("ไม่มี batch ในวันที่เลือก")
    else:
        batch_ids = [b["batch_src_id"] for b in batches]
        chosen = st.selectbox("เลือก batch", batch_ids)
        sensor = _get("/api/sensor/by-batch", batch_src_id=chosen).get("rows", [])
        if not sensor:
            st.info("ไม่มี sensor data ภายใน window ของ batch นี้")
        else:
            df = pd.DataFrame(sensor)
            st.subheader(f"Sensor parameters of batch #{chosen}")
            st.dataframe(df, use_container_width=True)

            # Line chart ต่อ metric
            for metric in df["metric_name"].unique():
                sub = df[df["metric_name"] == metric].copy()
                sub["window_start"] = pd.to_datetime(sub["window_start"], format="mixed")
                pivot = sub.pivot_table(
                    index="window_start", columns="machine_name", values="avg_value"
                )
                st.write(f"**{metric}** ({sub['unit'].iloc[0] or ''})")
                st.line_chart(pivot)


# ---------------------------------------------------------------------------
# Tab 4 — Machine Status per 15-min
# ---------------------------------------------------------------------------


with tab_machine:
    rows = _get("/api/production/per-machine-15min", date=selected_date).get("rows", [])
    if not rows:
        st.info("ไม่มี sensor state ในวันที่เลือก")
    else:
        df = pd.DataFrame(rows)
        df["window_start"] = pd.to_datetime(df["window_start"], format="mixed")
        st.subheader("Machine state (1=RUNNING, 0=FAULT)")
        pivot = df.pivot_table(
            index="window_start", columns="machine_name", values="avg_state"
        )
        st.line_chart(pivot)

        st.subheader("Raw state log")
        st.dataframe(df, use_container_width=True)
