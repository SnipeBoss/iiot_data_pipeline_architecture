from __future__ import annotations

from fastapi import APIRouter, Depends

from app.api.dw_api.deps import query_rows, require_token


"""Dashboard endpoints — อ่าน DW facts สำหรับ Streamlit UI

Endpoints ที่ใช้จริง 7 ตัว (3 sub-domain):
- /api/sensor/*    — sensor metrics + 15-min telemetry (Page 2)
- /api/scheduling/batch-timeline — Gantt drilldown (Page 3)
- /api/analytics/* — replace Oracle views (Page 1, 2, 3 + ML)

Analytics endpoints ทำหน้าที่แทน Oracle views ที่ AI03 ไม่มี privilege สร้าง:
- /api/analytics/oee-daily          ← V_OEE_DAILY equivalent
- /api/analytics/defect-pareto      ← V_DEFECT_PARETO equivalent
- /api/analytics/schedule-adherence ← V_SCHEDULE_ADHERENCE equivalent
- /api/analytics/batch-features     ← V_BATCH_FEATURES equivalent (ML feature matrix)
"""


router = APIRouter(
    prefix="/api",
    dependencies=[Depends(require_token)],
    tags=["dashboard"],
)


# =============================================================================
# SQL queries
# =============================================================================


_SQL_SENSOR_METRICS = """
    SELECT metric_id, metric_name, unit, machine_code, description
      FROM DIM_METRIC ORDER BY metric_id
"""


_SQL_SENSOR_BY_MACHINE_15MIN = """
    SELECT dm.machine_code,
           fs.window_start,
           fs.window_end,
           fs.avg_value,
           fs.min_value,
           fs.max_value
      FROM FACT_SENSOR fs
      JOIN DIM_MACHINE dm  ON fs.machine_id = dm.machine_id
      JOIN DIM_METRIC  dmt ON fs.metric_id  = dmt.metric_id
      JOIN DIM_DATE    d   ON fs.date_id    = d.date_id
     WHERE d.full_date = ?
       AND dmt.metric_name = ?
     ORDER BY fs.window_start, dm.machine_code
"""


_SQL_BATCH_TIMELINE = """
    SELECT p.batch_src_id,
           p.batch_planned_start,
           p.batch_planned_end,
           p.start_time   AS actual_start,
           p.end_time     AS actual_end,
           p.slippage_min
      FROM FACT_PRODUCTION p
     WHERE p.order_src_id = ?
     ORDER BY p.batch_planned_start
"""


# --- Replaces V_OEE_DAILY ---
# OEE = Availability × Performance × Quality
#   A = (planned - downtime) / planned
#   P = qty_out / qty_planned
#   Q = qty_passed / qty_inspected
_SQL_V_OEE_DAILY = """
    WITH
    production AS (
        SELECT p.date_id, p.line_id, p.shift_id,
               SUM(p.qty_out)                AS total_qty_out,
               SUM(p.qty_planned)            AS total_qty_planned,
               SUM(p.duration_min)           AS total_run_min,
               SUM(p.batch_est_duration_min) AS total_planned_min
          FROM FACT_PRODUCTION p
         WHERE p.date_id >= ?
         GROUP BY p.date_id, p.line_id, p.shift_id
    ),
    quality AS (
        SELECT q.date_id, q.line_id, q.shift_id,
               SUM(q.qty_inspected) AS total_inspected,
               SUM(q.qty_passed)    AS total_passed
          FROM FACT_QUALITY q
         WHERE q.date_id >= ?
         GROUP BY q.date_id, q.line_id, q.shift_id
    ),
    downtime AS (
        SELECT d.date_id, d.line_id, d.shift_id,
               SUM(d.duration_min) AS total_down_min
          FROM FACT_DOWNTIME d
         WHERE d.is_planned = 'N'
           AND d.date_id >= ?
         GROUP BY d.date_id, d.line_id, d.shift_id
    )
    SELECT p.date_id, p.line_id, p.shift_id,
           CASE WHEN p.total_planned_min > 0 THEN
                (p.total_planned_min - NVL(d.total_down_min, 0)) / p.total_planned_min
                ELSE 0 END AS availability,
           CASE WHEN p.total_qty_planned > 0 THEN
                p.total_qty_out / p.total_qty_planned
                ELSE 0 END AS performance,
           CASE WHEN q.total_inspected > 0 THEN
                q.total_passed / q.total_inspected
                ELSE 0 END AS quality,
           CASE WHEN p.total_planned_min > 0
                 AND p.total_qty_planned > 0
                 AND q.total_inspected > 0
                THEN (p.total_planned_min - NVL(d.total_down_min, 0)) / p.total_planned_min
                   * p.total_qty_out / p.total_qty_planned
                   * q.total_passed / q.total_inspected
                ELSE 0 END AS oee,
           p.total_qty_out, p.total_qty_planned,
           NVL(d.total_down_min, 0) AS downtime_min,
           q.total_inspected, q.total_passed
      FROM production p
      LEFT JOIN quality  q ON q.date_id = p.date_id
                          AND q.line_id = p.line_id
                          AND q.shift_id = p.shift_id
      LEFT JOIN downtime d ON d.date_id = p.date_id
                          AND d.line_id = p.line_id
                          AND d.shift_id = p.shift_id
     ORDER BY p.date_id, p.line_id, p.shift_id
"""


# --- Replaces V_DEFECT_PARETO ---
_SQL_V_DEFECT_PARETO = """
    SELECT d.parent_code      AS category,
           d.defect_code      AS defect_type,
           d.severity,
           COUNT(*)           AS occurrence_count,
           SUM(f.qty_affected) AS total_qty_affected
      FROM FACT_DEFECT      f
      JOIN DIM_DEFECT_TYPE  d ON f.defect_id = d.defect_id
     WHERE d.is_leaf = 'Y'
       AND f.date_id >= ?
     GROUP BY d.parent_code, d.defect_code, d.severity
     ORDER BY total_qty_affected DESC
"""


# --- Replaces V_SCHEDULE_ADHERENCE ---
_SQL_V_SCHEDULE_ADHERENCE = """
    SELECT p.prod_id,
           p.date_id,
           p.batch_src_id,
           p.order_src_id,
           bm.model_code,
           dl.line_name,
           ds.shift_name,
           p.qty_planned,
           p.qty_out,
           p.batch_planned_start,
           p.batch_planned_end,
           p.start_time AS actual_start,
           p.end_time   AS actual_end,
           p.batch_est_duration_min AS planned_min,
           p.duration_min           AS actual_min,
           p.slippage_min,
           CASE
               WHEN p.slippage_min <= 5  THEN 'ON_TIME'
               WHEN p.slippage_min <= 15 THEN 'MINOR_LATE'
               ELSE 'LATE'
           END AS adherence_status,
           p.yield_rate
      FROM FACT_PRODUCTION    p
      JOIN DIM_BATTERY_MODEL  bm ON p.model_id = bm.model_id
      JOIN DIM_LINE           dl ON p.line_id  = dl.line_id
      JOIN DIM_SHIFT          ds ON p.shift_id = ds.shift_id
     WHERE p.date_id >= ?
     ORDER BY p.start_time
"""


# --- Replaces V_BATCH_FEATURES (ML feature matrix) ---
# 21 features รวม target (defect_rate_pct + qty_failed)
# ใช้โดย LightGBM trainer (consume ผ่าน HTTP)
_SQL_V_BATCH_FEATURES = """
    SELECT
        p.prod_id,
        p.batch_src_id,
        p.order_src_id,
        p.model_id,
        p.line_id,
        EXTRACT(HOUR FROM p.start_time) AS hour_of_day,
        p.qty_planned,
        p.duration_min,
        p.batch_est_duration_min,
        p.slippage_min,
        p.slippage_min / NULLIF(p.batch_est_duration_min, 0) AS slippage_ratio,
        AVG(CASE WHEN m.metric_name = 'temperature_c' THEN s.avg_value END) AS temp_avg,
        MAX(CASE WHEN m.metric_name = 'temperature_c' THEN s.max_value END) AS temp_max,
        STDDEV(CASE WHEN m.metric_name = 'temperature_c' THEN s.avg_value END) AS temp_std,
        AVG(CASE WHEN m.metric_name = 'vibration_g' THEN s.avg_value END) AS vib_avg,
        MAX(CASE WHEN m.metric_name = 'vibration_g' THEN s.max_value END) AS vib_max,
        STDDEV(CASE WHEN m.metric_name = 'vibration_g' THEN s.avg_value END) AS vib_std,
        AVG(CASE WHEN m.metric_name = 'cycle_count' THEN s.avg_value END) AS cycle_avg,
        AVG(CASE WHEN m.metric_name = 'current_a' THEN s.avg_value END) AS current_avg,
        MAX(CASE WHEN m.metric_name = 'current_a' THEN s.max_value END) AS current_max,
        AVG(CASE WHEN m.metric_name = 'voltage_v' THEN s.avg_value END) AS voltage_avg,
        MIN(CASE WHEN m.metric_name = 'voltage_v' THEN s.min_value END) AS voltage_min,
        MAX(CASE WHEN m.metric_name = 'voltage_v' THEN s.max_value END) AS voltage_max,
        q.defect_rate_pct,
        q.qty_failed
      FROM FACT_PRODUCTION p
      JOIN FACT_QUALITY    q ON p.batch_src_id = q.batch_src_id
      LEFT JOIN FACT_SENSOR s
             ON s.window_start < p.end_time
            AND s.window_end   > p.start_time
      LEFT JOIN DIM_METRIC m ON s.metric_id = m.metric_id
     WHERE q.defect_rate_pct IS NOT NULL
     GROUP BY
        p.prod_id, p.batch_src_id, p.order_src_id,
        p.model_id, p.line_id,
        EXTRACT(HOUR FROM p.start_time),
        p.qty_planned, p.duration_min,
        p.batch_est_duration_min, p.slippage_min,
        q.defect_rate_pct, q.qty_failed
     ORDER BY p.prod_id
"""


# =============================================================================
# Helpers
# =============================================================================


def _period_to_start_date_id(period: str) -> int:
    """map period string → start date_id (YYYYMMDD) สำหรับ WHERE filter"""
    from datetime import date, timedelta
    today = date.today()
    if period == "Today":
        start = today
    elif period == "This week":
        start = today - timedelta(days=today.weekday())
    else:  # "Last 7 days" / default
        start = today - timedelta(days=7)
    return int(start.strftime("%Y%m%d"))


# =============================================================================
# Sensor endpoints (Page 2)
# =============================================================================


@router.get("/sensor/available-metrics")
def sensor_available_metrics() -> dict:
    """List ของ metric (DIM_METRIC) สำหรับ dropdown"""
    return {"rows": query_rows(_SQL_SENSOR_METRICS)}


@router.get("/sensor/by-machine-15min")
def sensor_by_machine_15min(date: str, metric: str) -> dict:
    """Sensor value ราย 15 นาที แยก machine (line chart)"""
    rows = query_rows(_SQL_SENSOR_BY_MACHINE_15MIN, [date, metric])
    return {"date": date, "metric": metric, "rows": rows}


# =============================================================================
# Scheduling endpoints (Page 3)
# =============================================================================


@router.get("/scheduling/batch-timeline")
def scheduling_batch_timeline(order_id: int) -> dict:
    """Per-batch planned vs actual timeline สำหรับ Gantt drilldown"""
    rows = query_rows(_SQL_BATCH_TIMELINE, [order_id])
    return {"order_id": order_id, "rows": rows}


# =============================================================================
# Analytics endpoints (replace Oracle CREATE VIEW privs ที่ไม่มี)
# =============================================================================


@router.get("/analytics/oee-daily")
def analytics_oee_daily(period: str = "Last 7 days") -> dict:
    """OEE daily per (date × line × shift) — replaces V_OEE_DAILY"""
    start_id = _period_to_start_date_id(period)
    # 3 sub-queries ใช้ start_id เหมือนกัน — ส่ง 3 ครั้ง
    rows = query_rows(_SQL_V_OEE_DAILY, [start_id, start_id, start_id])
    return {"period": period, "rows": rows}


@router.get("/analytics/defect-pareto")
def analytics_defect_pareto(period: str = "Last 7 days") -> dict:
    """Defect Pareto — count + qty_affected per (category × type) — replaces V_DEFECT_PARETO

    Pareto pct คำนวณฝั่ง client (Streamlit) เพื่อหลบ ORA window function ใน 10g
    """
    start_id = _period_to_start_date_id(period)
    rows = query_rows(_SQL_V_DEFECT_PARETO, [start_id])
    total = sum((r.get("total_qty_affected") or 0) for r in rows)
    for r in rows:
        qty = r.get("total_qty_affected") or 0
        r["pct_of_total"] = round(qty / total * 100, 2) if total else 0.0
    return {"period": period, "total_qty_affected": total, "rows": rows}


@router.get("/analytics/schedule-adherence")
def analytics_schedule_adherence(period: str = "Last 7 days") -> dict:
    """Slippage per batch + adherence status — replaces V_SCHEDULE_ADHERENCE"""
    start_id = _period_to_start_date_id(period)
    rows = query_rows(_SQL_V_SCHEDULE_ADHERENCE, [start_id])
    return {"period": period, "rows": rows}


@router.get("/analytics/batch-features")
def analytics_batch_features() -> dict:
    """ML feature matrix — 1 row = 1 batch กับ 21 features + 2 targets

    Replaces V_BATCH_FEATURES
    """
    rows = query_rows(_SQL_V_BATCH_FEATURES)
    return {"rowcount": len(rows), "rows": rows}
