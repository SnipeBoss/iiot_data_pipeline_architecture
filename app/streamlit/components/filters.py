from __future__ import annotations
import streamlit as st


PERIOD_OPTIONS = [
    "Today", 
    "This week", 
    "Last 7 days", 
    "Last 30 days"
]


def period_selector(key: str = "period") -> str:
    """
    Period dropdown — map ไปยัง FastAPI ?period= param
    """
    return st.selectbox(
        "Period", 
        PERIOD_OPTIONS, 
        index=2, 
        key=key
    )




def filter_row_oee_defect() -> dict:
    """
    Filter row ของ Page 1 (OEE + Defect)

    Returns: {"period", "line", "shift", "model"}
    """
    cols = st.columns([2, 2, 2, 2, 1])

    # Box Selection
    with cols[0]:
        period = period_selector(key="oee_period")
    

    with cols[1]:
        line = st.selectbox("Line", ["All", "L01"], key="oee_line")
    
    with cols[2]:
        shift = st.selectbox("Shift", ["All", "DAY", "NIGHT"], key="oee_shift")
    
    with cols[3]:
        model = st.selectbox("Battery model",
                             ["All", "60AH", "75AH", "100AH"],
                             key="oee_model")
    with cols[4]:
        st.write(" ")  # vertical alignment
        refresh = st.button("Refresh", key="oee_refresh",
                            use_container_width=True)

    if refresh:
        st.cache_data.clear()
        st.rerun()

    return {
        "period": period, 
        "line": line, 
        "shift": shift, 
        "model": model
    }





def filter_row_forecast(metrics: list[dict]) -> dict:
    """
    Filter row ของ Page 2 (Sensor Forecast)

    Args:
        metrics: list dict จาก /api/sensor/available-metrics
                 (มี key: metric_name, machine_code)

    Returns: {"machine", "metric", "horizon", "train_clicked"}
    """

    
    machines = sorted({m["machine_code"] for m in metrics
                       if m.get("machine_code")})
    
    
    # # Fall backs
    # if not machines:
    #     machines = ["M01", "M02", "M03"]   

    
    cols = st.columns([2, 2, 2, 2, 2])
    with cols[0]:
        machine = st.selectbox("Machine", machines, key="fcst_machine")

    # filter metrics ตาม machine ที่เลือก (รวม metric ที่ machine_code=NULL = ทุกเครื่อง)
    machine_metrics = [m["metric_name"] for m in metrics
                       if m.get("machine_code") == machine
                       or m.get("machine_code") is None]
    
    # # Fall backs
    # if not machine_metrics:
    #     machine_metrics = ["temperature_c"]   

    with cols[1]:
        metric = st.selectbox("Metric", machine_metrics, key="fcst_metric")
    
    with cols[2]:
        horizon = st.selectbox("Horizon",
                               ["6 hours", "12 hours", "24 hours"],
                               index=0, key="fcst_horizon")
    with cols[3]:
        train = st.button("Train model", key="fcst_train",
                          use_container_width=True)
    with cols[4]:
        refresh = st.button("Refresh", key="fcst_refresh",
                            use_container_width=True)

    if refresh:
        st.cache_data.clear()
        st.rerun()

    return {
        "machine": machine,
        "metric": metric,
        "horizon": horizon,
        "train_clicked": train,
    }




def filter_row_schedule() -> dict:
    """
    Filter row ของ Page 3 (Schedule Adherence)

    Returns: {"period", "line", "status", "model"}
    """
    cols = st.columns([2, 2, 2, 2, 1])
    with cols[0]:
        period = period_selector(key="sched_period")
    with cols[1]:
        line = st.selectbox("Line", ["All", "L01"], key="sched_line")
    with cols[2]:
        status = st.selectbox("Status",
                              ["All", "ON_TIME", "MINOR_LATE", "LATE"],
                              key="sched_status")
    with cols[3]:
        model = st.selectbox("Battery model",
                             ["All", "60AH", "75AH", "100AH"],
                             key="sched_model")
    with cols[4]:
        st.write(" ")
        refresh = st.button("Refresh", key="sched_refresh",
                            use_container_width=True)

    if refresh:
        st.cache_data.clear()
        st.rerun()

    return {"period": period, "line": line, "status": status, "model": model}
