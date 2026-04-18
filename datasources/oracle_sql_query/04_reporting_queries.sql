-- =============================================================================
-- [B]-4 Reporting Queries — 5 read-only queries over the DW.
-- Run each separately against Oracle AI03 (e.g. via the Oracle API /sql/query).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q1. OEE by machine by date
-- -----------------------------------------------------------------------------
SELECT m.machine_name,
       d.full_date,
       f.availability_pct,
       f.performance_pct,
       f.quality_pct,
       f.oee_pct
  FROM FACT_OEE     f
  JOIN DIM_MACHINE  m ON f.machine_id = m.machine_id
  JOIN DIM_DATE     d ON f.date_id    = d.date_id
 ORDER BY d.full_date DESC, f.oee_pct DESC;


-- -----------------------------------------------------------------------------
-- Q2. Defect rate by stage
-- -----------------------------------------------------------------------------
SELECT s.stage_name,
       s.sequence_no                  AS stage_seq,
       ROUND(AVG(fq.defect_rate_pct), 2) AS avg_defect_pct,
       SUM(fq.fail_count)             AS total_fails,
       SUM(fq.pass_count)             AS total_passes,
       MAX(fq.top_defect_param)       AS common_defect
  FROM FACT_QUALITY fq
  JOIN DIM_STAGE    s ON fq.stage_id = s.stage_id
 GROUP BY s.stage_name, s.sequence_no
 ORDER BY s.sequence_no;


-- -----------------------------------------------------------------------------
-- Q3. Material consumption (needs FACT_INVENTORY — Phase 6b)
-- -----------------------------------------------------------------------------
SELECT mat.material_name,
       mat.unit,
       SUM(fi.qty_consumed) AS total_consumed,
       SUM(fi.qty_received) AS total_received,
       MIN(fi.qty_closing)  AS min_stock_seen,
       MAX(fi.qty_closing)  AS max_stock_seen
  FROM FACT_INVENTORY fi
  JOIN DIM_MATERIAL   mat ON fi.material_id = mat.material_id
 GROUP BY mat.material_name, mat.unit
 ORDER BY total_consumed DESC;


-- -----------------------------------------------------------------------------
-- Q4. MTBF / MTTR by machine  (breakdowns only)
-- -----------------------------------------------------------------------------
SELECT m.machine_name,
       COUNT(*)                       AS breakdown_count,
       ROUND(AVG(fm.downtime_min), 2) AS avg_downtime_min,
       SUM(fm.downtime_min)           AS total_downtime_min,
       MAX(fm.issue_code)             AS most_recent_issue
  FROM FACT_MAINTENANCE fm
  JOIN DIM_MACHINE      m ON fm.machine_id = m.machine_id
 WHERE fm.event_type = 'BREAKDOWN'
 GROUP BY m.machine_name
 ORDER BY total_downtime_min DESC;


-- -----------------------------------------------------------------------------
-- Q5. Weekly OEE trend — recalculated from raw additive measures
--     (avoid averaging percentages; re-derive from sums)
-- -----------------------------------------------------------------------------
SELECT d.year,
       d.week_number,
       m.machine_name,
       ROUND(
           (  (SUM(f.planned_time_min) - SUM(f.downtime_min)) / NULLIF(SUM(f.planned_time_min), 0)
            * (SUM(f.units_produced * m.ideal_cycle_sec / 60.0) / NULLIF(SUM(f.actual_run_min), 0))
            * (SUM(f.units_good) / NULLIF(SUM(f.units_produced), 0))
           ) * 100
       , 2) AS weekly_oee_pct
  FROM FACT_OEE    f
  JOIN DIM_DATE    d ON f.date_id    = d.date_id
  JOIN DIM_MACHINE m ON f.machine_id = m.machine_id
 GROUP BY d.year, d.week_number, m.machine_name, m.ideal_cycle_sec
 ORDER BY d.year, d.week_number, m.machine_name;
