from __future__ import annotations
import sys
from pathlib import Path

_STREAMLIT_DIR = Path(__file__).resolve().parents[1]
if str(_STREAMLIT_DIR) not in sys.path:
    sys.path.insert(0, str(_STREAMLIT_DIR))

import pandas as pd
import streamlit as st

# Import API Connection
from components import api_client as api
from components.cards import kpi_row

# Import Chart 
from components.charts import (
    defect_pareto_chart,
    defect_rate_by_model_chart,
    oee_trend_chart,
)

# Import Filters 
from components.filters import filter_row_oee_defect





"""
Page 1 — OEE + Defect Rate

Components:
  A: 4 KPI cards (OEE / Availability / Performance / Quality)
  B: OEE trend line chart
  C: Defect Pareto bar+line
  D: Defect rate by battery model (จาก batch-features aggregation)
  E: Defect detail table
"""

# Set Page Title
st.set_page_config(
    page_title="OEE & Defect", 
    layout="wide"
)

# Set Title
st.title("OEE and Defect Rate")


# Filters Selection Columns
filters = filter_row_oee_defect()

# Get Period
period = filters["period"]


# ─── Fetch data (cached, shared across components) ────────
try:


    oee_data = api.get(
        "/api/analytics/oee-daily", 
        {"period": period}
    )
    
    
    pareto_data = api.get(
        "/api/analytics/defect-pareto", 
        {"period": period}
    )


except Exception as exc:
    st.error(f"Cannot reach API: {exc}")
    st.stop()



# Turn to Dataframe
oee_df = pd.DataFrame(oee_data.get("rows", []))
pareto_df = pd.DataFrame(pareto_data.get("rows", []))


# ─── A: KPI cards ─────────────────────────────────────────

def _to_pct(val) -> float:
    """
    แปลง decimal (0-1) → percentage (0-100), guard NaN/None
    OEE endpoint ส่ง availability/performance/quality/oee เป็น decimal (0-1)
    แสดงเป็น % โดย × 100 (ห่ามใส่ %_pct ใน column ถ้า endpoint ไม่ได้ส่ง)
    """
    if pd.isna(val) or val is None:
        return 0.0
    
    return float(val) * 100



if oee_df.empty:
    st.warning("No OEE data for selected period")

else:

    # Add *_pct columns เพื่อให้ chart builder ใช้ — ถ้า endpoint ไม่ได้ pre-format
    for raw, pct_col in [
        ("oee", "oee_pct"),
        ("availability", "availability_pct"),
        ("performance", "performance_pct"),
        ("quality", "quality_pct"),
    ]:
        if raw in oee_df.columns and pct_col not in oee_df.columns:
            oee_df[pct_col] = oee_df[raw].apply(_to_pct)

    # Calculated to average
    avg_oee = oee_df["oee_pct"].mean() if "oee_pct" in oee_df.columns else 0.0
    avg_avail = oee_df["availability_pct"].mean() if "availability_pct" in oee_df.columns else 0.0
    avg_perf = oee_df["performance_pct"].mean() if "performance_pct" in oee_df.columns else 0.0
    avg_qual = oee_df["quality_pct"].mean() if "quality_pct" in oee_df.columns else 0.0

    # Save as dict
    kpi_row([
        {"label": "OEE",          "value": f"{avg_oee:.1f}%"},
        {"label": "Availability", "value": f"{avg_avail:.1f}%"},
        {"label": "Performance",  "value": f"{avg_perf:.1f}%"},
        {"label": "Quality",      "value": f"{avg_qual:.1f}%"},
    ])



st.divider()


# ─── B: OEE trend ─────────────────────────────────────────
st.subheader("OEE trend")

if not oee_df.empty:

    # OEE endpoint return date_id (YYYYMMDD number) — convert เป็น full_date สำหรับ chart x-axis
    if "full_date" not in oee_df.columns and "date_id" in oee_df.columns:
        oee_df["full_date"] = pd.to_datetime(
            oee_df["date_id"].astype(int).astype(str), format="%Y%m%d"
        )


    # Aggregate ราย date ในกรณีมี (date × line × shift) หลาย row
    if "full_date" in oee_df.columns:

        # Group to trends
        trend = (oee_df.groupby("full_date", as_index=False)
                       .agg(
                           {
                               c: "mean" for c in ["oee_pct", "availability_pct", "performance_pct", "quality_pct"]
                               if c in oee_df.columns
                            }
                        )
                )
        
        # Plot Chart 
        st.plotly_chart(oee_trend_chart(trend), use_container_width=True)

    else:
        st.info("No date column to plot trend")
else:
    st.info("No data to plot")


st.divider()


# ─── C: Defect Pareto ─────────────────────────────────────
st.subheader("Defect Pareto")
if not pareto_df.empty:

    # Plot to Chart 
    st.plotly_chart(defect_pareto_chart(pareto_df), use_container_width=True)

else:
    st.info("No defect data")


st.divider()


# ─── D: Defect rate by battery model ──────────────────────
st.subheader("Defect rate by battery model")

try:

    # Calling batch-features and set to dataframe    
    features_data = api.get("/api/analytics/batch-features")
    features_df = pd.DataFrame(features_data.get("rows", []))


except Exception as exc:
    st.warning(f"batch-features unavailable: {exc}")
    features_df = pd.DataFrame()


if not features_df.empty and "model_id" in features_df.columns:

    # batch-features ส่ง model_id (surrogate) ไม่มี model_name โดยตรง
    # ใช้ model_id เป็น label ชั่วคราว (TODO: extend endpoint ให้ JOIN DIM_BATTERY_MODEL)
    if "defect_rate_pct" in features_df.columns:
    
    
        model_defect = (features_df.groupby("model_id", as_index=False)
                                    .agg(defect_rate_pct=("defect_rate_pct", "mean"),
                                         n_batches=("batch_src_id", "count")))
    
    
        # Rename for Chart
        model_defect = model_defect.rename(columns={"model_id": "model_name"})

        # Set name
        model_defect["model_name"] = "Model " + model_defect["model_name"].astype(str)
    
        # Plot to Chart
        st.plotly_chart(defect_rate_by_model_chart(model_defect), use_container_width=True)
    

        st.caption(f"Aggregated from {len(features_df)} batches in DW")
    
    
    else:
        st.info("defect_rate_pct missing in batch-features response")
else:
    st.info("No batch features data — DW may be empty or schema incomplete")


st.divider()




# ─── E: Defect detail table ───────────────────────────────
st.subheader("Defect detail")
if not pareto_df.empty:

    # column order ตามความสำคัญสำหรับ analyst
    display_cols = ["defect_code", "defect_type", "category", "parent_code",
                    "qty_affected", "total_qty_affected",
                    "pct_of_total", "severity", "occurrence_count"]
    
    available = [c for c in display_cols if c in pareto_df.columns]

    # sort ตาม qty descending
    sort_col = next((c for c in ["qty_affected", "total_qty_affected"]
                     if c in pareto_df.columns), None)
    
    # Filters
    table = pareto_df[available].sort_values(sort_col, ascending=False) if sort_col else pareto_df[available]

    # Show as Dataframe
    st.dataframe(
        table,
        use_container_width=True,
        hide_index=True,
    )
    
else:
    st.info("No defects to show")
