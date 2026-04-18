-- =============================================================================
-- FN_CALC_OEE  +  SP_LOAD_FACT_*  (deliverable [C])
--
-- All procs are idempotent for a given (date_id): they DELETE rows matching
-- the date first, then INSERT fresh. Call with `TRUNC(SYSDATE)` or any
-- historic DATE — see sp_load_dw DAG for scheduling.
--
-- Staging inputs are whatever the Airflow extract DAGs loaded for the day.
-- If STG is empty the proc writes no rows and exits cleanly.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- FN_CALC_OEE — pure function, returns OEE % given additive measures.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION FN_CALC_OEE(
    p_planned_min  IN NUMBER,
    p_downtime_min IN NUMBER,
    p_actual_run   IN NUMBER,
    p_units_prod   IN NUMBER,
    p_ideal_cycle  IN NUMBER,   -- seconds
    p_units_good   IN NUMBER
) RETURN NUMBER IS
    v_a NUMBER;
    v_p NUMBER;
    v_q NUMBER;
BEGIN
    v_a := (p_planned_min - p_downtime_min) / NULLIF(p_planned_min, 0);
    v_p := (p_units_prod * p_ideal_cycle / 60.0) / NULLIF(p_actual_run, 0);
    v_q := p_units_good / NULLIF(p_units_prod, 0);
    RETURN ROUND(LEAST(v_a, 1) * LEAST(v_p, 1) * LEAST(v_q, 1) * 100, 2);
EXCEPTION
    WHEN ZERO_DIVIDE THEN RETURN 0;
    WHEN OTHERS      THEN RETURN NULL;
END;
/


-- -----------------------------------------------------------------------------
-- SP_LOAD_FACT_OEE — 3 rows per call (one per instrumented machine).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_LOAD_FACT_OEE(p_date IN DATE) IS
    v_date_id      NUMBER;
    v_actual_run   NUMBER;
    v_units_prod   NUMBER;
    v_down_min     NUMBER;
    v_units_good   NUMBER;
    c_planned      CONSTANT NUMBER := 480;   -- 8h planned time (hardcoded)
BEGIN
    SELECT date_id INTO v_date_id
    FROM DIM_DATE
    WHERE full_date = TRUNC(p_date);

    DELETE FROM FACT_OEE WHERE date_id = v_date_id;

    FOR rec IN (
        SELECT machine_id, machine_src_id, ideal_cycle_sec
        FROM DIM_MACHINE
    ) LOOP
        -- Actual run time + units produced at this machine's stage, for the day.
        -- Stage IDs happen to equal machine_src_id for instrumented machines
        -- (M01→stage 1 smelting, M02→stage 5 assembly, M03→stage 8 charging)
        -- per the master data in 02_master_data.sql, so this join works.
        SELECT NVL(SUM(
                  EXTRACT(DAY    FROM (completed_at - started_at)) * 24 * 60
                + EXTRACT(HOUR   FROM (completed_at - started_at)) * 60
                + EXTRACT(MINUTE FROM (completed_at - started_at))
               ), 0),
               NVL(SUM(qty_produced), 0)
          INTO v_actual_run, v_units_prod
          FROM STG_PRODUCTION_BATCH
         WHERE completed_at IS NOT NULL
           AND TRUNC(completed_at) = TRUNC(p_date)
           AND stage_id = (
               SELECT sequence_no FROM DIM_STAGE
                WHERE machine_name = (
                    SELECT machine_name FROM DIM_MACHINE
                     WHERE machine_id = rec.machine_id
                )
                AND ROWNUM = 1
           );

        -- Downtime minutes from maintenance log for the day (any type).
        SELECT NVL(SUM(downtime_min), 0) INTO v_down_min
          FROM STG_MAINTENANCE_LOG
         WHERE machine_id = rec.machine_src_id
           AND TRUNC(started_at) = TRUNC(p_date);

        -- Units that passed QC at this stage.
        SELECT NVL(COUNT(*), 0) INTO v_units_good
          FROM STG_QC_RESULT qr
          JOIN STG_QC_INSPECTION qi ON qr.qc_id = qi.qc_id
         WHERE qr.pass_fail = 'PASS'
           AND TRUNC(qi.inspected_at) = TRUNC(p_date)
           AND qi.stage_id = (
               SELECT sequence_no FROM DIM_STAGE
                WHERE machine_name = (
                    SELECT machine_name FROM DIM_MACHINE
                     WHERE machine_id = rec.machine_id
                )
                AND ROWNUM = 1
           );

        INSERT INTO FACT_OEE (
            oee_id, date_id, machine_id,
            planned_time_min, actual_run_min, downtime_min,
            units_produced, units_good,
            availability_pct, performance_pct, quality_pct, oee_pct
        ) VALUES (
            SEQ_FACT_OEE.NEXTVAL, v_date_id, rec.machine_id,
            c_planned, v_actual_run, v_down_min,
            v_units_prod, v_units_good,
            ROUND((c_planned - v_down_min) / NULLIF(c_planned, 0) * 100, 2),
            ROUND((v_units_prod * rec.ideal_cycle_sec / 60.0) / NULLIF(v_actual_run, 0) * 100, 2),
            ROUND(v_units_good / NULLIF(v_units_prod, 0) * 100, 2),
            FN_CALC_OEE(c_planned, v_down_min, v_actual_run,
                        v_units_prod, rec.ideal_cycle_sec, v_units_good)
        );
    END LOOP;
    COMMIT;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('Date not found in DIM_DATE: ' || TO_CHAR(p_date));
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
/


-- -----------------------------------------------------------------------------
-- SP_LOAD_FACT_QUALITY — one row per inspection on the date.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_LOAD_FACT_QUALITY(p_date IN DATE) IS
    v_date_id NUMBER;
BEGIN
    SELECT date_id INTO v_date_id
    FROM DIM_DATE WHERE full_date = TRUNC(p_date);

    DELETE FROM FACT_QUALITY WHERE date_id = v_date_id;

    INSERT INTO FACT_QUALITY (
        quality_id, date_id, stage_id,
        samples_taken, pass_count, fail_count,
        defect_rate_pct, top_defect_param
    )
    SELECT SEQ_FACT_QUALITY.NEXTVAL,
           v_date_id,
           ds.stage_id,
           inspection.samples_taken,
           inspection.pass_count,
           inspection.fail_count,
           CASE WHEN inspection.total > 0
                THEN ROUND(inspection.fail_count / inspection.total * 100, 2)
                ELSE 0 END,
           inspection.top_param
      FROM (
        SELECT qi.stage_id,
               SUM(qi.sample_qty) AS samples_taken,
               SUM(CASE WHEN qr.pass_fail = 'PASS' THEN 1 ELSE 0 END) AS pass_count,
               SUM(CASE WHEN qr.pass_fail = 'FAIL' THEN 1 ELSE 0 END) AS fail_count,
               COUNT(qr.result_id) AS total,
               MAX(qr.parameter) AS top_param
          FROM STG_QC_INSPECTION qi
          JOIN STG_QC_RESULT qr ON qi.qc_id = qr.qc_id
         WHERE TRUNC(qi.inspected_at) = TRUNC(p_date)
         GROUP BY qi.stage_id
      ) inspection
      JOIN DIM_STAGE ds ON ds.sequence_no = inspection.stage_id;

    COMMIT;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('No DIM_DATE row for ' || TO_CHAR(p_date));
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
/


-- -----------------------------------------------------------------------------
-- SP_LOAD_FACT_MAINTENANCE — one row per maintenance event on the date.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_LOAD_FACT_MAINTENANCE(p_date IN DATE) IS
    v_date_id NUMBER;
BEGIN
    SELECT date_id INTO v_date_id
    FROM DIM_DATE WHERE full_date = TRUNC(p_date);

    DELETE FROM FACT_MAINTENANCE WHERE date_id = v_date_id;

    INSERT INTO FACT_MAINTENANCE (
        maintenance_id, date_id, machine_id,
        event_type, downtime_min, mtbf_hrs, mttr_min, issue_code
    )
    SELECT SEQ_FACT_MAINTENANCE.NEXTVAL,
           v_date_id,
           dm.machine_id,
           stg.type,
           stg.downtime_min,
           NULL,   -- MTBF needs historical window; compute at reporting time
           stg.downtime_min AS mttr_min,
           stg.issue_code
      FROM STG_MAINTENANCE_LOG stg
      JOIN DIM_MACHINE dm ON dm.machine_src_id = stg.machine_id
     WHERE TRUNC(stg.started_at) = TRUNC(p_date);

    COMMIT;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('No DIM_DATE row for ' || TO_CHAR(p_date));
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
/


-- -----------------------------------------------------------------------------
-- SP_LOAD_FACT_PRODUCTION — one row per (machine, stage) on the date.
-- -----------------------------------------------------------------------------
-- Oracle 10g forbids SEQ.NEXTVAL inside INSERT ... SELECT (ORA-02287) — use
-- a cursor FOR loop and read the sequence per row.
CREATE OR REPLACE PROCEDURE SP_LOAD_FACT_PRODUCTION(p_date IN DATE) IS
    v_date_id NUMBER;
BEGIN
    SELECT date_id INTO v_date_id
    FROM DIM_DATE WHERE full_date = TRUNC(p_date);

    DELETE FROM FACT_PRODUCTION WHERE date_id = v_date_id;

    FOR rec IN (
        SELECT dm.machine_id AS machine_id,
               ds.stage_id   AS stage_id,
               SUM(stg.qty_produced) AS units_produced,
               SUM(
                   EXTRACT(DAY    FROM (stg.completed_at - stg.started_at)) * 86400 +
                   EXTRACT(HOUR   FROM (stg.completed_at - stg.started_at)) * 3600 +
                   EXTRACT(MINUTE FROM (stg.completed_at - stg.started_at)) * 60
               ) AS total_duration_sec
          FROM STG_PRODUCTION_BATCH stg
          JOIN DIM_STAGE ds ON ds.sequence_no = stg.stage_id
          LEFT JOIN DIM_MACHINE dm ON dm.machine_name = ds.machine_name
         WHERE stg.completed_at IS NOT NULL
           AND TRUNC(stg.completed_at) = TRUNC(p_date)
         GROUP BY dm.machine_id, ds.stage_id
    ) LOOP
        INSERT INTO FACT_PRODUCTION (
            production_id, date_id, machine_id, stage_id,
            units_produced, avg_cycle_time_sec, batch_duration_min, yield_rate
        ) VALUES (
            SEQ_FACT_PRODUCTION.NEXTVAL,
            v_date_id,
            rec.machine_id,
            rec.stage_id,
            rec.units_produced,
            CASE WHEN rec.units_produced > 0
                 THEN ROUND(rec.total_duration_sec / rec.units_produced, 2)
                 ELSE NULL END,
            ROUND(rec.total_duration_sec / 60.0, 2),
            NULL
        );
    END LOOP;

    COMMIT;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('No DIM_DATE row for ' || TO_CHAR(p_date));
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
/
