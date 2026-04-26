"""Page 3 — Schedule Adherence

Components:
  A: 4 KPI cards (Total / On-time / Minor late / Late)
  B: Slippage histogram
  C: Slippage trend (avg per date)
  D: Gantt chart per order (interactive drilldown)
  E: Batch detail table
"""
from __future__ import annotations

import sys
from pathlib import Path

_STREAMLIT_DIR = Path(__file__).resolve().parents[1]
if str(_STREAMLIT_DIR) not in sys.path:
    sys.path.insert(0, str(_STREAMLIT_DIR))

import pandas as pd
import streamlit as st

from components import api_client as api
from components.cards import kpi_row
from components.charts import batch_gantt, slippage_histogram, slippage_trend
from components.filters import filter_row_schedule


st.set_page_config(page_title="Schedule Adherence", layout="wide")
st.title("Schedule Adherence")


# ─── Filters ──────────────────────────────────────────────
filters = filter_row_schedule()
period = filters["period"]
status_filter = filters["status"]


# ─── Fetch data ───────────────────────────────────────────
try:
    data = api.get("/api/analytics/schedule-adherence", {"period": period})
except Exception as exc:
    st.error(f"Cannot reach API: {exc}")
    st.stop()

df = pd.DataFrame(data.get("rows", []))

if df.empty:
    st.warning("No schedule data for selected period")
    st.stop()


# Apply client-side status filter
if status_filter != "All" and "adherence_status" in df.columns:
    df = df[df["adherence_status"] == status_filter]


# ─── A: KPI cards ─────────────────────────────────────────
total = len(df)
on_time = (df["adherence_status"] == "ON_TIME").sum() if "adherence_status" in df.columns else 0
minor = (df["adherence_status"] == "MINOR_LATE").sum() if "adherence_status" in df.columns else 0
late = (df["adherence_status"] == "LATE").sum() if "adherence_status" in df.columns else 0

pct = lambda n: f"{(n / total * 100):.1f}%" if total else "0%"

kpi_row([
    {"label": "Total batches", "value": f"{total}"},
    {"label": "On-time",       "value": pct(on_time)},
    {"label": "Minor late",    "value": pct(minor)},
    {"label": "Late",          "value": pct(late)},
])


st.divider()


# ─── B + C: Histogram + Trend (side-by-side) ──────────────
cols = st.columns(2)

with cols[0]:
    st.subheader("Slippage distribution")
    if "slippage_min" in df.columns:
        st.plotly_chart(slippage_histogram(df), use_container_width=True)
    else:
        st.info("slippage_min column missing")

with cols[1]:
    st.subheader("Slippage trend")
    if "slippage_min" in df.columns:
        st.plotly_chart(slippage_trend(df), use_container_width=True)
    else:
        st.info("slippage_min column missing")


st.divider()


# ─── D: Gantt chart per order (interactive drilldown) ─────
st.subheader("Order Gantt timeline")

if "order_src_id" in df.columns:
    orders = sorted(df["order_src_id"].dropna().astype(int).unique().tolist())
    if not orders:
        st.info("No orders to drill into")
    else:
        selected_order = st.selectbox("Select order to view batches",
                                      orders, key="gantt_order")

        # Fetch batch timeline สำหรับ order ที่เลือก
        try:
            timeline_data = api.get(
                "/api/scheduling/batch-timeline",
                {"order_id": int(selected_order)},
            )
            timeline_df = pd.DataFrame(timeline_data.get("rows", []))

            if not timeline_df.empty:
                # Convert timestamp columns ทั้งหมด → datetime
                for col in ["batch_planned_start", "batch_planned_end",
                            "start_time", "end_time",
                            "actual_start", "actual_end"]:
                    if col in timeline_df.columns:
                        # format='mixed' — Oracle TIMESTAMP บางแถวมี fractional seconds บางแถวไม่มี
                        timeline_df[col] = pd.to_datetime(
                            timeline_df[col], format="mixed", errors="coerce"
                        )

                st.plotly_chart(batch_gantt(timeline_df),
                                use_container_width=True)
            else:
                st.info(f"No batch timeline for order {selected_order}")
        except Exception as e:
            st.warning(f"Could not load timeline: {e}")
else:
    st.info("Order data not available in response")


st.divider()


# ─── E: Batch detail table (top 50 by slippage desc) ──────
st.subheader("Batch detail")

display_cols = [
    "batch_src_id", "order_src_id", "model_code", "line_name", "shift_name",
    "batch_planned_start", "actual_start",
    "planned_min", "actual_min",
    "slippage_min", "adherence_status",
    "yield_rate",
]
available = [c for c in display_cols if c in df.columns]

if "slippage_min" in df.columns:
    table = df[available].sort_values(
        "slippage_min", ascending=False, na_position="last"
    ).head(50)
else:
    table = df[available].head(50)

st.dataframe(
    table,
    use_container_width=True,
    hide_index=True,
)

st.caption(f"Showing top 50 of {len(df)} batches • sorted by slippage desc")
