from __future__ import annotations

from fastapi import APIRouter, Depends

from app.api.deps import query_rows, require_token


"""Dashboard / reporting endpoints — อ่าน DW facts สำหรับ Streamlit UI

Group เป็น 3 sub-domain:
- /api/production/*  — batch-level + 15-min machine status
- /api/quality/*     — defect rate
- /api/sensor/*      — sensor telemetry ราย 15 นาที

ทุก endpoint รัน SQL เดียว คืน {"rows": [...]} หรือ object ตาม shape
"""


router = APIRouter(
    prefix="/api",
    dependencies=[Depends(require_token)],
    tags=["dashboard"],
)


# =============================================================================
# SQL queries (เก็บเป็น constant แยกจาก handler เพื่ออ่านง่าย)
# =============================================================================


_SQL_AVAILABLE_DATES = """
    SELECT DISTINCT d.full_date AS the_date
      FROM DIM_DATE d
     WHERE d.date_id IN (
        SELECT date_id FROM FACT_PRODUCTION
        UNION SELECT date_id FROM FACT_QUALITY
        UNION SELECT date_id FROM FACT_SENSOR
     )
     ORDER BY d.full_date DESC
"""


_SQL_PRODUCTION_BY_BATCH = """
    SELECT p.batch_src_id,
           p.order_src_id,
           dp.product_name,
           p.qty_planned,
           p.qty_out,
           ROUND(p.yield_rate * 100, 2) AS yield_pct,
           p.start_time,
           p.end_time,
           p.duration_min
      FROM FACT_PRODUCTION p
      JOIN DIM_DATE d     ON p.date_id    = d.date_id
      LEFT JOIN DIM_PRODUCT dp ON p.product_id = dp.product_id
     WHERE d.full_date = ?
     ORDER BY p.start_time
"""


_SQL_PRODUCTION_SUMMARY = """
    SELECT COUNT(*) AS total_batches,
           SUM(p.qty_planned) AS total_planned,
           SUM(p.qty_out) AS total_out,
           ROUND(AVG(p.yield_rate) * 100, 2) AS avg_yield_pct,
           ROUND(AVG(p.duration_min), 2) AS avg_duration_min
      FROM FACT_PRODUCTION p
      JOIN DIM_DATE d ON p.date_id = d.date_id
     WHERE d.full_date = ?
"""


_SQL_QUALITY_OVERALL = """
    SELECT SUM(q.qty_sampled) AS total_sampled,
           SUM(q.qty_passed)  AS total_passed,
           SUM(q.qty_failed)  AS total_failed,
           ROUND(SUM(q.qty_failed) / NULLIF(SUM(q.qty_sampled), 0) * 100, 2) AS overall_defect_pct
      FROM FACT_QUALITY q
      JOIN DIM_DATE d ON q.date_id = d.date_id
     WHERE d.full_date = ?
"""


_SQL_QUALITY_PER_BATCH = """
    SELECT q.batch_src_id,
           q.qty_sampled,
           q.qty_passed,
           q.qty_failed,
           q.defect_rate_pct,
           q.inspected_at
      FROM FACT_QUALITY q
      JOIN DIM_DATE d ON q.date_id = d.date_id
     WHERE d.full_date = ?
     ORDER BY q.inspected_at
"""


_SQL_SENSOR_METRICS = """
    SELECT metric_id, metric_name, unit, machine_name, description
      FROM DIM_METRIC ORDER BY metric_id
"""


_SQL_SENSOR_BY_BATCH = """
    SELECT dm.machine_name,
           dmt.metric_name,
           dmt.unit,
           fs.window_start,
           fs.window_end,
           fs.avg_value,
           fs.min_value,
           fs.max_value,
           fs.sample_count
      FROM FACT_SENSOR fs
      JOIN DIM_MACHINE dm  ON fs.machine_id = dm.machine_id
      JOIN DIM_METRIC  dmt ON fs.metric_id  = dmt.metric_id
      JOIN FACT_PRODUCTION fp
           ON fs.window_start >= fp.start_time
          AND fs.window_end   <= fp.end_time
     WHERE fp.batch_src_id = ?
     ORDER BY fs.window_start, dm.machine_name, dmt.metric_name
"""


_SQL_SENSOR_BY_MACHINE_15MIN = """
    SELECT dm.machine_name,
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
     ORDER BY fs.window_start, dm.machine_name
"""


_SQL_MACHINE_STATUS_15MIN = """
    SELECT dm.machine_name,
           fs.window_start,
           fs.window_end,
           ROUND(fs.avg_value, 2) AS avg_state,
           CASE WHEN fs.avg_value >= 0.5 THEN 'RUNNING' ELSE 'FAULT' END AS status
      FROM FACT_SENSOR fs
      JOIN DIM_MACHINE dm  ON fs.machine_id = dm.machine_id
      JOIN DIM_METRIC  dmt ON fs.metric_id  = dmt.metric_id
      JOIN DIM_DATE    d   ON fs.date_id    = d.date_id
     WHERE dmt.metric_name = 'machine_state_num'
       AND d.full_date = ?
     ORDER BY fs.window_start, dm.machine_name
"""


# =============================================================================
# Production endpoints
# =============================================================================


@router.get("/production/available-dates")
def production_available_dates() -> dict:
    """วันที่มี FACT_* rows (ใช้เป็น date picker ใน dashboard)"""
    rows = query_rows(_SQL_AVAILABLE_DATES)
    return {"dates": [r["the_date"][:10] if r["the_date"] else None for r in rows]}


@router.get("/production/by-batch")
def production_by_batch(date: str) -> dict:
    """ทุก batch ของวันที่เลือก + yield + duration"""
    rows = query_rows(_SQL_PRODUCTION_BY_BATCH, [date])
    return {"date": date, "rows": rows}


@router.get("/production/summary")
def production_summary(date: str) -> dict:
    """KPI รวม: total batches / total out / avg yield / avg duration"""
    rows = query_rows(_SQL_PRODUCTION_SUMMARY, [date])
    return {"date": date, "summary": rows[0] if rows else {}}


@router.get("/production/per-machine-15min")
def production_per_machine_15min(date: str) -> dict:
    """Machine status (RUNNING/FAULT) ต่อเครื่อง ต่อ 15-min window

    derive จาก machine_state_num: avg >= 0.5 = RUNNING
    """
    rows = query_rows(_SQL_MACHINE_STATUS_15MIN, [date])
    return {"date": date, "rows": rows}


# =============================================================================
# Quality endpoints
# =============================================================================


@router.get("/quality/defect-rate")
def quality_defect_rate(date: str) -> dict:
    """Defect rate รวม + per batch ของวันที่เลือก"""
    overall = query_rows(_SQL_QUALITY_OVERALL, [date])
    per_batch = query_rows(_SQL_QUALITY_PER_BATCH, [date])
    return {
        "date": date,
        "overall": overall[0] if overall else {},
        "per_batch": per_batch,
    }


# =============================================================================
# Sensor endpoints
# =============================================================================


@router.get("/sensor/available-metrics")
def sensor_available_metrics() -> dict:
    """List ของ metric (DIM_METRIC) สำหรับ dropdown"""
    return {"rows": query_rows(_SQL_SENSOR_METRICS)}


@router.get("/sensor/by-batch")
def sensor_by_batch(batch_src_id: int) -> dict:
    """Sensor parameter ราย 15 นาที ที่อยู่ภายใน window ของ batch นั้น"""
    rows = query_rows(_SQL_SENSOR_BY_BATCH, [batch_src_id])
    return {"batch_src_id": batch_src_id, "rows": rows}


@router.get("/sensor/by-machine-15min")
def sensor_by_machine_15min(date: str, metric: str) -> dict:
    """Sensor value ราย 15 นาที แยกตามเครื่อง (สำหรับ line chart)"""
    rows = query_rows(_SQL_SENSOR_BY_MACHINE_15MIN, [date, metric])
    return {"date": date, "metric": metric, "rows": rows}
