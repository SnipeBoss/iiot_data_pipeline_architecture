"""Battery Manufacturing — OEE Dashboard.

Reads exclusively from the FastAPI Oracle service (no DB access from here).
Run locally:
    .venv/bin/streamlit run app/streamlit/dashboard.py

Env vars (optional — both have sane defaults):
    DASHBOARD_API_URL      default http://localhost:8000
    ORACLE_API_TOKEN       bearer token; blank = no auth required by server
"""

from __future__ import annotations

import os

import pandas as pd
import requests
import streamlit as st

API_URL = os.environ.get("DASHBOARD_API_URL", "http://localhost:8000")
TOKEN = os.environ.get("ORACLE_API_TOKEN", "")


# ---------------------------------------------------------------------------
# HTTP helpers — cached for 30 s so rapid re-renders don't hammer Oracle.
# ---------------------------------------------------------------------------


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {TOKEN}"} if TOKEN else {}


@st.cache_data(ttl=30, show_spinner=False)
def fetch(path: str, params: dict | None = None) -> dict:
    r = requests.get(f"{API_URL}{path}", params=params, headers=_headers(), timeout=30)
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# Page setup
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Battery OEE Dashboard",
    page_icon=None,
    layout="wide",
)

st.title("Battery Manufacturing — OEE Dashboard")
st.caption(
    f"Live from Oracle DW @ `{API_URL}`  ·  "
    "Each tile aggregates FACT_* tables populated by the Airflow ETL."
)


# ---------------------------------------------------------------------------
# Sidebar — date picker driven by what's actually in FACT_OEE
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("Controls")
    try:
        dates = fetch("/api/oee/available-dates")["dates"]
    except requests.RequestException as exc:
        st.error(f"Could not reach API at {API_URL}: {exc}")
        st.stop()

    if not dates:
        st.warning(
            "No FACT_OEE rows yet. Run the `etl_supabase_to_oracle` and "
            "`sp_load_dw` DAGs first."
        )
        st.stop()

    selected_date = st.selectbox("Date", dates, index=0)

    if st.button("Refresh now"):
        fetch.clear()
        st.rerun()


# ---------------------------------------------------------------------------
# Top section — OEE KPIs for the selected date
# ---------------------------------------------------------------------------

oee_payload = fetch("/api/oee/daily", {"date": selected_date})
oee_df = pd.DataFrame(oee_payload["rows"])

if oee_df.empty:
    st.info(f"No OEE rows for {selected_date}.")
    st.stop()

avg_oee = oee_df["oee_pct"].dropna().mean() if "oee_pct" in oee_df else None
avg_a = oee_df["availability_pct"].dropna().mean() if "availability_pct" in oee_df else None
avg_p = oee_df["performance_pct"].dropna().mean() if "performance_pct" in oee_df else None
avg_q = oee_df["quality_pct"].dropna().mean() if "quality_pct" in oee_df else None


def _fmt_pct(v) -> str:
    return "—" if pd.isna(v) or v is None else f"{v:.1f}%"


st.subheader(f"Overall OEE — {selected_date}")
col1, col2, col3, col4 = st.columns(4)
col1.metric("OEE (avg of machines)", _fmt_pct(avg_oee))
col2.metric("Availability",           _fmt_pct(avg_a))
col3.metric("Performance",            _fmt_pct(avg_p))
col4.metric("Quality",                _fmt_pct(avg_q))

st.caption(
    "Note: these are arithmetic averages across machines for the selected day. "
    "The *weekly-trend* section below recomputes OEE from additive measures "
    "(the statistically correct way)."
)


# ---------------------------------------------------------------------------
# Per-machine OEE — bar chart + table
# ---------------------------------------------------------------------------

st.subheader("Per-machine breakdown")

left, right = st.columns([2, 3])
with left:
    st.bar_chart(oee_df.set_index("machine_name")[["oee_pct"]], height=320)

with right:
    display_df = oee_df.rename(columns={
        "machine_name": "Machine",
        "machine_type": "Type",
        "availability_pct": "A %",
        "performance_pct": "P %",
        "quality_pct": "Q %",
        "oee_pct": "OEE %",
        "planned_time_min": "Planned (min)",
        "actual_run_min": "Actual (min)",
        "downtime_min": "Down (min)",
        "units_produced": "Produced",
        "units_good": "Good",
    })
    st.dataframe(display_df, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Secondary sections — quality, maintenance, inventory, trend
# ---------------------------------------------------------------------------

tab_quality, tab_maint, tab_inv, tab_trend = st.tabs([
    "Quality by stage", "Maintenance (MTBF/MTTR)", "Inventory snapshot", "Weekly OEE trend",
])

with tab_quality:
    q_df = pd.DataFrame(fetch("/api/quality/defect-by-stage")["rows"])
    if q_df.empty:
        st.info("No rows in FACT_QUALITY yet.")
    else:
        q_df = q_df.rename(columns={
            "stage_name": "Stage",
            "sequence_no": "Seq",
            "avg_defect_pct": "Defect %",
            "total_samples": "Samples",
            "total_passes": "Pass",
            "total_fails": "Fail",
        })
        st.dataframe(q_df, use_container_width=True, hide_index=True)
        st.bar_chart(q_df.set_index("Stage")["Defect %"], height=300)

with tab_maint:
    m_df = pd.DataFrame(fetch("/api/maintenance/mtbf-mttr")["rows"])
    if m_df.empty:
        st.info("No BREAKDOWN rows in FACT_MAINTENANCE yet.")
    else:
        m_df = m_df.rename(columns={
            "machine_name": "Machine",
            "breakdown_count": "Breakdowns",
            "avg_downtime_min": "Avg downtime (min)",
            "total_downtime_min": "Total downtime (min)",
            "most_recent_issue": "Last issue code",
        })
        st.dataframe(m_df, use_container_width=True, hide_index=True)
        st.caption(
            "MTBF/MTTR: *Mean Time Between Failures* and *Mean Time To Repair*. "
            "For full MTBF we need multi-week history — this view shows per-machine "
            "totals from `FACT_MAINTENANCE`."
        )

with tab_inv:
    i_df = pd.DataFrame(fetch("/api/inventory/latest")["rows"])
    if i_df.empty:
        st.info("No FACT_INVENTORY rows yet — run `SP_LOAD_FACT_INVENTORY`.")
    else:
        as_of = i_df["as_of_date"].iloc[0] if "as_of_date" in i_df.columns else "—"
        st.caption(f"As of **{as_of}** — current closing balances from Supabase.")
        i_df = i_df.rename(columns={
            "material_name": "Material",
            "unit": "Unit",
            "hazard_class": "Hazard",
            "qty_opening": "Opening",
            "qty_received": "Received",
            "qty_consumed": "Consumed",
            "qty_closing": "Closing",
        }).drop(columns=["as_of_date"], errors="ignore")
        st.dataframe(i_df, use_container_width=True, hide_index=True)

with tab_trend:
    t_df = pd.DataFrame(fetch("/api/oee/weekly-trend")["rows"])
    if t_df.empty:
        st.info("No FACT_OEE rows yet.")
    else:
        t_df["period"] = t_df["year"].astype(str) + "-W" + t_df["week_number"].astype(str).str.zfill(2)
        pivot = t_df.pivot(index="period", columns="machine_name", values="weekly_oee_pct")
        st.line_chart(pivot, height=320)
        st.caption(
            "OEE recomputed weekly from the sum of additive measures "
            "(planned / downtime / produced / good) — NOT an average of daily "
            "percentages."
        )
