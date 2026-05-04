from __future__ import annotations
from fastapi import APIRouter, Depends
from app.api.dw_api.deps import query_rows, require_token


"""
Dashboard endpoints — อ่าน DW facts สำหรับ Streamlit UI

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


# Set Router
router = APIRouter(
    prefix="/api",
    dependencies=[Depends(require_token)],
    tags=["dashboard"],
)



def _period_to_start_date_id(period: str) -> int:
    """
    Helpers functions
        map period string → start date_id (YYYYMMDD) สำหรับ WHERE filter
    """
    from datetime import date, timedelta
    today = date.today()

    if period == "Today":
        start = today

    elif period == "This week":
        start = today - timedelta(days=today.weekday())

    elif period == "Last 30 days":
        start = today - timedelta(days=30)

    else:  # "Last 7 days" / default
        start = today - timedelta(days=7)

    return int(start.strftime("%Y%m%d"))







_SQL_SENSOR_METRICS = """
    SELECT metric_id, metric_name, unit, machine_code, description
    FROM DIM_METRIC ORDER BY metric_id
"""

@router.get("/sensor/available-metrics")
def sensor_available_metrics() -> dict:
    """
    Sensor endpoints (Page 2)
    List ของ metric (DIM_METRIC) สำหรับ dropdown
    """
    return {
        "rows": query_rows(_SQL_SENSOR_METRICS)
    }







_SQL_SENSOR_BY_MACHINE_15MIN = """
    SELECT machine.machine_code,
           sensor.window_start,
           sensor.window_end,
           sensor.avg_value,
           sensor.min_value,
           sensor.max_value
      FROM FACT_SENSOR  sensor
      JOIN DIM_MACHINE  machine  ON sensor.machine_id = machine.machine_id
      JOIN DIM_METRIC   metric   ON sensor.metric_id  = metric.metric_id
      JOIN DIM_DATE     date_dim ON sensor.date_id    = date_dim.date_id
     WHERE date_dim.full_date  = ?
       AND metric.metric_name  = ?
     ORDER BY sensor.window_start, machine.machine_code
"""
"""
SELECT machine.machine_code,
       sensor.window_start,
       sensor.window_end,
       sensor.avg_value,
       sensor.min_value,
       sensor.max_value
  FROM FACT_SENSOR  sensor
  JOIN DIM_MACHINE  machine  ON sensor.machine_id = machine.machine_id
  JOIN DIM_METRIC   metric   ON sensor.metric_id  = metric.metric_id
  JOIN DIM_DATE     date_dim ON sensor.date_id    = date_dim.date_id
 WHERE date_dim.full_date  = TO_DATE('2026-04-25', 'YYYY-MM-DD')
   AND metric.metric_name  = 'temperature_c'
 ORDER BY sensor.window_start, machine.machine_code;

"""

@router.get("/sensor/by-machine-15min")
def sensor_by_machine_15min(date: str, metric: str) -> dict:
    """
    Sensor endpoints (Page 2)
    Sensor value ราย 15 นาที แยก machine (line chart)
    """
    rows = query_rows(_SQL_SENSOR_BY_MACHINE_15MIN, [date, metric])
    
    return {
        "date": date, 
        "metric": metric, 
        "rows": rows
    }







_SQL_BATCH_TIMELINE = """
    SELECT production.batch_src_id,
           production.batch_planned_start,
           production.batch_planned_end,
           production.start_time AS actual_start,
           production.end_time   AS actual_end,
           production.slippage_min
      FROM FACT_PRODUCTION production
     WHERE production.order_src_id = ?
     ORDER BY production.batch_planned_start
"""
"""
SELECT production.batch_src_id,
       production.batch_planned_start,
       production.batch_planned_end,
       production.start_time AS actual_start,
       production.end_time   AS actual_end,
       production.slippage_min
  FROM FACT_PRODUCTION production
 WHERE production.order_src_id = 1
 ORDER BY production.batch_planned_start;
"""

@router.get("/scheduling/batch-timeline")
def scheduling_batch_timeline(order_id: int) -> dict:
    """
    Scheduling endpoints (Page 3)
    Per-batch planned vs actual timeline สำหรับ Gantt drilldown
    """
    rows = query_rows(_SQL_BATCH_TIMELINE, [order_id])
    return {
        "order_id": order_id, 
        "rows": rows
    }








# OEE = Availability × Performance × Quality
#   A = (planned - downtime) / planned
#   P = qty_out / qty_planned
#   Q = qty_passed / qty_inspected
_SQL_V_OEE_DAILY = """
    WITH
    production_agg AS (
        SELECT FACT_PRODUCTION.date_id,
               FACT_PRODUCTION.line_id,
               FACT_PRODUCTION.shift_id,
               SUM(FACT_PRODUCTION.qty_out)                AS total_qty_out,
               SUM(FACT_PRODUCTION.qty_planned)            AS total_qty_planned,
               SUM(FACT_PRODUCTION.duration_min)           AS total_run_min,
               SUM(FACT_PRODUCTION.batch_est_duration_min) AS total_planned_min
          FROM FACT_PRODUCTION
         WHERE FACT_PRODUCTION.date_id >= ?
         GROUP BY FACT_PRODUCTION.date_id,
                  FACT_PRODUCTION.line_id,
                  FACT_PRODUCTION.shift_id
    ),
    quality_agg AS (
        SELECT FACT_QUALITY.date_id,
               FACT_QUALITY.line_id,
               FACT_QUALITY.shift_id,
               SUM(FACT_QUALITY.qty_inspected) AS total_inspected,
               SUM(FACT_QUALITY.qty_passed)    AS total_passed
          FROM FACT_QUALITY
         WHERE FACT_QUALITY.date_id >= ?
         GROUP BY FACT_QUALITY.date_id,
                  FACT_QUALITY.line_id,
                  FACT_QUALITY.shift_id
    ),
    downtime_agg AS (
        SELECT FACT_DOWNTIME.date_id,
               FACT_DOWNTIME.line_id,
               FACT_DOWNTIME.shift_id,
               SUM(FACT_DOWNTIME.duration_min) AS total_down_min
          FROM FACT_DOWNTIME
         WHERE FACT_DOWNTIME.is_planned = 'N'
           AND FACT_DOWNTIME.date_id   >= ?
         GROUP BY FACT_DOWNTIME.date_id,
                  FACT_DOWNTIME.line_id,
                  FACT_DOWNTIME.shift_id
    )


    SELECT production_agg.date_id,
           production_agg.line_id,
           production_agg.shift_id,
    
           CASE WHEN production_agg.total_planned_min > 0 THEN
                (production_agg.total_planned_min - NVL(downtime_agg.total_down_min, 0))
                / production_agg.total_planned_min
                ELSE 0 END AS availability,
    
           CASE WHEN production_agg.total_qty_planned > 0 THEN
                production_agg.total_qty_out / production_agg.total_qty_planned
                ELSE 0 END AS performance,
           
           CASE WHEN quality_agg.total_inspected > 0 THEN
                quality_agg.total_passed / quality_agg.total_inspected
                ELSE 0 END AS quality,
           
           CASE WHEN production_agg.total_planned_min > 0
                 AND production_agg.total_qty_planned > 0
                 AND quality_agg.total_inspected      > 0
                THEN (production_agg.total_planned_min - NVL(downtime_agg.total_down_min, 0))
                       / production_agg.total_planned_min

                   * production_agg.total_qty_out     / production_agg.total_qty_planned

                   * quality_agg.total_passed         / quality_agg.total_inspected

                ELSE 0 END AS oee,
           
           production_agg.total_qty_out,
           production_agg.total_qty_planned,
           NVL(downtime_agg.total_down_min, 0) AS downtime_min,
           quality_agg.total_inspected,
           quality_agg.total_passed
      
      FROM production_agg
      LEFT JOIN quality_agg  ON quality_agg.date_id  = production_agg.date_id
                            AND quality_agg.line_id  = production_agg.line_id
                            AND quality_agg.shift_id = production_agg.shift_id
      LEFT JOIN downtime_agg ON downtime_agg.date_id  = production_agg.date_id
                            AND downtime_agg.line_id  = production_agg.line_id
                            AND downtime_agg.shift_id = production_agg.shift_id
     ORDER BY production_agg.date_id,
              production_agg.line_id,
              production_agg.shift_id
"""


"""
WITH
production_agg AS (
    SELECT FACT_PRODUCTION.date_id,
           FACT_PRODUCTION.line_id,
           FACT_PRODUCTION.shift_id,
           SUM(FACT_PRODUCTION.qty_out)                AS total_qty_out,
           SUM(FACT_PRODUCTION.qty_planned)            AS total_qty_planned,
           SUM(FACT_PRODUCTION.duration_min)           AS total_run_min,
           SUM(FACT_PRODUCTION.batch_est_duration_min) AS total_planned_min
      FROM FACT_PRODUCTION
     WHERE FACT_PRODUCTION.date_id >= 20260418
     GROUP BY FACT_PRODUCTION.date_id,
              FACT_PRODUCTION.line_id,
              FACT_PRODUCTION.shift_id
),
quality_agg AS (
    SELECT FACT_QUALITY.date_id,
           FACT_QUALITY.line_id,
           FACT_QUALITY.shift_id,
           SUM(FACT_QUALITY.qty_inspected) AS total_inspected,
           SUM(FACT_QUALITY.qty_passed)    AS total_passed
      FROM FACT_QUALITY
     WHERE FACT_QUALITY.date_id >= 20260418
     GROUP BY FACT_QUALITY.date_id,
              FACT_QUALITY.line_id,
              FACT_QUALITY.shift_id
),
downtime_agg AS (
    SELECT FACT_DOWNTIME.date_id,
           FACT_DOWNTIME.line_id,
           FACT_DOWNTIME.shift_id,
           SUM(FACT_DOWNTIME.duration_min) AS total_down_min
      FROM FACT_DOWNTIME
     WHERE FACT_DOWNTIME.is_planned = 'N'
       AND FACT_DOWNTIME.date_id   >= 20260418
     GROUP BY FACT_DOWNTIME.date_id,
              FACT_DOWNTIME.line_id,
              FACT_DOWNTIME.shift_id
)
SELECT production_agg.date_id,
       production_agg.line_id,
       production_agg.shift_id,
       CASE WHEN production_agg.total_planned_min > 0 THEN
            (production_agg.total_planned_min - NVL(downtime_agg.total_down_min, 0))
            / production_agg.total_planned_min
            ELSE 0 END AS availability,
       CASE WHEN production_agg.total_qty_planned > 0 THEN
            production_agg.total_qty_out / production_agg.total_qty_planned
            ELSE 0 END AS performance,
       CASE WHEN quality_agg.total_inspected > 0 THEN
            quality_agg.total_passed / quality_agg.total_inspected
            ELSE 0 END AS quality,
       CASE WHEN production_agg.total_planned_min > 0
             AND production_agg.total_qty_planned > 0
             AND quality_agg.total_inspected      > 0
            THEN (production_agg.total_planned_min - NVL(downtime_agg.total_down_min, 0))
                   / production_agg.total_planned_min
               * production_agg.total_qty_out     / production_agg.total_qty_planned
               * quality_agg.total_passed         / quality_agg.total_inspected
            ELSE 0 END AS oee,
       production_agg.total_qty_out,
       production_agg.total_qty_planned,
       NVL(downtime_agg.total_down_min, 0) AS downtime_min,
       quality_agg.total_inspected,
       quality_agg.total_passed
  FROM production_agg
  LEFT JOIN quality_agg  ON quality_agg.date_id  = production_agg.date_id
                        AND quality_agg.line_id  = production_agg.line_id
                        AND quality_agg.shift_id = production_agg.shift_id
  LEFT JOIN downtime_agg ON downtime_agg.date_id  = production_agg.date_id
                        AND downtime_agg.line_id  = production_agg.line_id
                        AND downtime_agg.shift_id = production_agg.shift_id
 ORDER BY production_agg.date_id,
          production_agg.line_id,
          production_agg.shift_id;

"""
@router.get("/analytics/oee-daily")
def analytics_oee_daily(period: str = "Last 7 days") -> dict:
    """
    OEE daily per (date × line × shift) — replaces V_OEE_DAILY
    Analytics endpoints (replace Oracle CREATE VIEW privs ที่ไม่มี)
    """
    start_id = _period_to_start_date_id(period)

    # 3 sub-queries ใช้ start_id เหมือนกัน — ส่ง 3 ครั้ง
    rows = query_rows(_SQL_V_OEE_DAILY, [start_id, start_id, start_id])
    
    return {
        "period": period, 
        "rows": rows
    }







_SQL_V_DEFECT_PARETO = """
    SELECT defect_type.parent_code   AS category,
           defect_type.defect_code   AS defect_type,
           defect_type.severity,
           COUNT(*)                  AS occurrence_count,
           SUM(defect.qty_affected)  AS total_qty_affected

      FROM FACT_DEFECT      defect
      JOIN DIM_DEFECT_TYPE  defect_type ON defect.defect_id = defect_type.defect_id
     WHERE defect_type.is_leaf  = 'Y'
       AND defect.date_id      >= ?
     GROUP BY defect_type.parent_code,
              defect_type.defect_code,
              defect_type.severity
     ORDER BY total_qty_affected DESC
"""

"""
SELECT defect_type.parent_code   AS category,
       defect_type.defect_code   AS defect_type,
       defect_type.severity,
       COUNT(*)                  AS occurrence_count,
       SUM(defect.qty_affected)  AS total_qty_affected
  FROM FACT_DEFECT      defect
  JOIN DIM_DEFECT_TYPE  defect_type ON defect.defect_id = defect_type.defect_id
 WHERE defect_type.is_leaf  = 'Y'
   AND defect.date_id      >= 20260418
 GROUP BY defect_type.parent_code,
          defect_type.defect_code,
          defect_type.severity
 ORDER BY total_qty_affected DESC;

"""

@router.get("/analytics/defect-pareto")
def analytics_defect_pareto(period: str = "Last 7 days") -> dict:
    """
    Defect Pareto — count + qty_affected per (category × type) — replaces V_DEFECT_PARETO

    Pareto pct คำนวณฝั่ง client (Streamlit) เพื่อหลบ ORA window function ใน 10g
    
    Analytics endpoints (replace Oracle CREATE VIEW privs ที่ไม่มี)
    """
    start_id = _period_to_start_date_id(period)
    rows = query_rows(_SQL_V_DEFECT_PARETO, [start_id])
    total = sum((r.get("total_qty_affected") or 0) for r in rows)
    for r in rows:
        qty = r.get("total_qty_affected") or 0
        r["pct_of_total"] = round(qty / total * 100, 2) if total else 0.0
    return {"period": period, "total_qty_affected": total, "rows": rows}






_SQL_V_SCHEDULE_ADHERENCE = """
    SELECT production.prod_id,
           production.date_id,
           production.batch_src_id,
           production.order_src_id,
           model.model_code,
           prod_line.line_name,
           prod_shift.shift_name,
           production.qty_planned,
           production.qty_out,
           production.batch_planned_start,
           production.batch_planned_end,
           production.start_time            AS actual_start,
           production.end_time              AS actual_end,
           production.batch_est_duration_min AS planned_min,
           production.duration_min          AS actual_min,
           production.slippage_min,
           CASE
               WHEN production.slippage_min <=  5 THEN 'ON_TIME'
               WHEN production.slippage_min <= 15 THEN 'MINOR_LATE'
               ELSE 'LATE'
           END                              AS adherence_status,
           production.yield_rate

      FROM FACT_PRODUCTION    production
      JOIN DIM_BATTERY_MODEL  model      ON production.model_id = model.model_id
      JOIN DIM_LINE           prod_line  ON production.line_id  = prod_line.line_id
      JOIN DIM_SHIFT          prod_shift ON production.shift_id = prod_shift.shift_id
     
     WHERE production.date_id >= ?
     ORDER BY production.start_time
"""

"""
SELECT production.prod_id,
       production.date_id,
       production.batch_src_id,
       production.order_src_id,
       model.model_code,
       prod_line.line_name,
       prod_shift.shift_name,
       production.qty_planned,
       production.qty_out,
       production.batch_planned_start,
       production.batch_planned_end,
       production.start_time            AS actual_start,
       production.end_time              AS actual_end,
       production.batch_est_duration_min AS planned_min,
       production.duration_min          AS actual_min,
       production.slippage_min,
       CASE
           WHEN production.slippage_min <=  5 THEN 'ON_TIME'
           WHEN production.slippage_min <= 15 THEN 'MINOR_LATE'
           ELSE 'LATE'
       END                              AS adherence_status,
       production.yield_rate
  FROM FACT_PRODUCTION    production
  JOIN DIM_BATTERY_MODEL  model      ON production.model_id = model.model_id
  JOIN DIM_LINE           prod_line  ON production.line_id  = prod_line.line_id
  JOIN DIM_SHIFT          prod_shift ON production.shift_id = prod_shift.shift_id
 WHERE production.date_id >= 20260418
 ORDER BY production.start_time;

"""

@router.get("/analytics/schedule-adherence")
def analytics_schedule_adherence(period: str = "Last 7 days") -> dict:
    """
    Slippage per batch + adherence status — replaces V_SCHEDULE_ADHERENCE
    
    Analytics endpoints (replace Oracle CREATE VIEW privs ที่ไม่มี)
    """
    
    start_id = _period_to_start_date_id(period)
    rows = query_rows(_SQL_V_SCHEDULE_ADHERENCE, [start_id])
    return {"period": period, "rows": rows}










# --- Replaces V_BATCH_FEATURES (ML feature matrix) ---
# 21 features รวม target (defect_rate_pct + qty_failed)
# Consumer: Streamlit Page 1 — aggregate เป็น "Defect rate by battery model" chart
_SQL_V_BATCH_FEATURES = """
    SELECT
        production.prod_id,
        production.batch_src_id,
        production.order_src_id,
        production.model_id,
        production.line_id,
        EXTRACT(HOUR FROM production.start_time) AS hour_of_day,
        production.qty_planned,
        production.duration_min,
        production.batch_est_duration_min,
        production.slippage_min,
        production.slippage_min / NULLIF(production.batch_est_duration_min, 0) AS slippage_ratio,
        AVG(CASE WHEN metric.metric_name = 'temperature_c' THEN sensor.avg_value END) AS temp_avg,
        MAX(CASE WHEN metric.metric_name = 'temperature_c' THEN sensor.max_value END) AS temp_max,
        STDDEV(CASE WHEN metric.metric_name = 'temperature_c' THEN sensor.avg_value END) AS temp_std,
        AVG(CASE WHEN metric.metric_name = 'vibration_g'   THEN sensor.avg_value END) AS vib_avg,
        MAX(CASE WHEN metric.metric_name = 'vibration_g'   THEN sensor.max_value END) AS vib_max,
        STDDEV(CASE WHEN metric.metric_name = 'vibration_g' THEN sensor.avg_value END) AS vib_std,
        AVG(CASE WHEN metric.metric_name = 'cycle_count'   THEN sensor.avg_value END) AS cycle_avg,
        AVG(CASE WHEN metric.metric_name = 'current_a'     THEN sensor.avg_value END) AS current_avg,
        MAX(CASE WHEN metric.metric_name = 'current_a'     THEN sensor.max_value END) AS current_max,
        AVG(CASE WHEN metric.metric_name = 'voltage_v'     THEN sensor.avg_value END) AS voltage_avg,
        MIN(CASE WHEN metric.metric_name = 'voltage_v'     THEN sensor.min_value END) AS voltage_min,
        MAX(CASE WHEN metric.metric_name = 'voltage_v'     THEN sensor.max_value END) AS voltage_max,
        quality.defect_rate_pct,
        quality.qty_failed
      FROM FACT_PRODUCTION    production
      JOIN FACT_QUALITY       quality ON production.batch_src_id = quality.batch_src_id
      LEFT JOIN FACT_SENSOR   sensor  ON sensor.window_start < production.end_time
                                     AND sensor.window_end   > production.start_time
      LEFT JOIN DIM_METRIC    metric  ON sensor.metric_id    = metric.metric_id
     WHERE quality.defect_rate_pct IS NOT NULL
     GROUP BY production.prod_id,
              production.batch_src_id,
              production.order_src_id,
              production.model_id,
              production.line_id,
              EXTRACT(HOUR FROM production.start_time),
              production.qty_planned,
              production.duration_min,
              production.batch_est_duration_min,
              production.slippage_min,
              quality.defect_rate_pct,
              quality.qty_failed
     ORDER BY production.prod_id
"""

"""
SELECT
    production.prod_id,
    production.batch_src_id,
    production.order_src_id,
    production.model_id,
    production.line_id,
    EXTRACT(HOUR FROM production.start_time) AS hour_of_day,
    production.qty_planned,
    production.duration_min,
    production.batch_est_duration_min,
    production.slippage_min,
    production.slippage_min / NULLIF(production.batch_est_duration_min, 0) AS slippage_ratio,
    AVG(CASE WHEN metric.metric_name = 'temperature_c' THEN sensor.avg_value END) AS temp_avg,
    MAX(CASE WHEN metric.metric_name = 'temperature_c' THEN sensor.max_value END) AS temp_max,
    STDDEV(CASE WHEN metric.metric_name = 'temperature_c' THEN sensor.avg_value END) AS temp_std,
    AVG(CASE WHEN metric.metric_name = 'vibration_g'   THEN sensor.avg_value END) AS vib_avg,
    MAX(CASE WHEN metric.metric_name = 'vibration_g'   THEN sensor.max_value END) AS vib_max,
    STDDEV(CASE WHEN metric.metric_name = 'vibration_g' THEN sensor.avg_value END) AS vib_std,
    AVG(CASE WHEN metric.metric_name = 'cycle_count'   THEN sensor.avg_value END) AS cycle_avg,
    AVG(CASE WHEN metric.metric_name = 'current_a'     THEN sensor.avg_value END) AS current_avg,
    MAX(CASE WHEN metric.metric_name = 'current_a'     THEN sensor.max_value END) AS current_max,
    AVG(CASE WHEN metric.metric_name = 'voltage_v'     THEN sensor.avg_value END) AS voltage_avg,
    MIN(CASE WHEN metric.metric_name = 'voltage_v'     THEN sensor.min_value END) AS voltage_min,
    MAX(CASE WHEN metric.metric_name = 'voltage_v'     THEN sensor.max_value END) AS voltage_max,
    quality.defect_rate_pct,
    quality.qty_failed
  FROM FACT_PRODUCTION    production
  JOIN FACT_QUALITY       quality ON production.batch_src_id = quality.batch_src_id
  LEFT JOIN FACT_SENSOR   sensor  ON sensor.window_start < production.end_time
                                 AND sensor.window_end   > production.start_time
  LEFT JOIN DIM_METRIC    metric  ON sensor.metric_id    = metric.metric_id
 WHERE quality.defect_rate_pct IS NOT NULL
 GROUP BY production.prod_id,
          production.batch_src_id,
          production.order_src_id,
          production.model_id,
          production.line_id,
          EXTRACT(HOUR FROM production.start_time),
          production.qty_planned,
          production.duration_min,
          production.batch_est_duration_min,
          production.slippage_min,
          quality.defect_rate_pct,
          quality.qty_failed
 ORDER BY production.prod_id;

"""

@router.get("/analytics/batch-features")
def analytics_batch_features() -> dict:
    """
    ML feature matrix — 1 row = 1 batch กับ 21 features + 2 targets
    """
    rows = query_rows(_SQL_V_BATCH_FEATURES)
    return {"rowcount": len(rows), "rows": rows}
