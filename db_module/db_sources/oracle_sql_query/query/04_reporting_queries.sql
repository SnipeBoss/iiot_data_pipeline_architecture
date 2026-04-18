-- =============================================================================
-- Reporting queries — ตาม GM dashboard requirements (NEW_ARCHITECTURE.md §RQ)
-- =============================================================================
-- รันแยกทีละ query ผ่าน FastAPI /sql/query หรือ ad-hoc บน Oracle SQL client
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q1. Production overview ต่อ batch (ของวันที่เลือก)
-- ใช้สำหรับ table/KPI ใน dashboard
-- -----------------------------------------------------------------------------
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
 WHERE d.full_date = DATE '2026-04-18'
 ORDER BY p.start_time;


-- -----------------------------------------------------------------------------
-- Q2. Defect rate overall + per-batch (ของวันที่เลือก)
-- -----------------------------------------------------------------------------
SELECT q.batch_src_id,
       q.qty_sampled,
       q.qty_passed,
       q.qty_failed,
       q.defect_rate_pct,
       q.inspected_at
  FROM FACT_QUALITY q
  JOIN DIM_DATE d ON q.date_id = d.date_id
 WHERE d.full_date = DATE '2026-04-18'
 ORDER BY q.inspected_at;

-- Aggregate defect rate ของทั้งวัน
SELECT d.full_date,
       SUM(q.qty_sampled) AS total_sampled,
       SUM(q.qty_passed)  AS total_passed,
       SUM(q.qty_failed)  AS total_failed,
       ROUND(SUM(q.qty_failed) / NULLIF(SUM(q.qty_sampled), 0) * 100, 2) AS overall_defect_pct
  FROM FACT_QUALITY q
  JOIN DIM_DATE d ON q.date_id = d.date_id
 WHERE d.full_date = DATE '2026-04-18'
 GROUP BY d.full_date;


-- -----------------------------------------------------------------------------
-- Q3. Sensor parameter per batch (15-min window)
-- JOIN FACT_SENSOR กับ FACT_PRODUCTION ผ่าน time overlap
-- -----------------------------------------------------------------------------
SELECT fp.batch_src_id,
       dm.machine_name,
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
 WHERE fp.batch_src_id = 1      -- parameter: batch_src_id
 ORDER BY fs.window_start, dm.machine_name, dmt.metric_name;


-- -----------------------------------------------------------------------------
-- Q4. Production per machine per 15-min window
-- ใช้ค่าจาก FACT_SENSOR: count rows ที่ machine_state_num = 1 ใน window
-- คำนวณ % runtime = running windows / total windows
-- -----------------------------------------------------------------------------
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
   AND d.full_date = DATE '2026-04-18'
 ORDER BY fs.window_start, dm.machine_name;


-- -----------------------------------------------------------------------------
-- Q5. Available dates (สำหรับ dashboard date picker)
-- -----------------------------------------------------------------------------
SELECT DISTINCT d.full_date
  FROM DIM_DATE d
 WHERE d.date_id IN (
    SELECT date_id FROM FACT_PRODUCTION
    UNION SELECT date_id FROM FACT_QUALITY
    UNION SELECT date_id FROM FACT_SENSOR
 )
 ORDER BY d.full_date DESC;
