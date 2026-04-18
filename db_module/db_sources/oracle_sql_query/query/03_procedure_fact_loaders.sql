-- =============================================================================
-- Fact loader procedures (3 ตัว) — Merge mode (15-min compatible)
-- =============================================================================
-- เหตุผลเปลี่ยนจาก DELETE-by-date → DELETE-by-key:
--   DAG รันทุก 15 นาที → STG มีแค่ window ล่าสุด
--   ถ้า SP ทำ DELETE WHERE date_id = :d จะล้าง FACT ทั้งวันแล้ว rebuild จาก
--   STG window เดียว → FACT เหลือแค่ 15 นาที ไม่ใช่ทั้งวัน
--
-- วิธีแก้: ลบเฉพาะ row ที่ match กับ STG ปัจจุบัน (per-batch หรือ per-window)
-- แล้ว INSERT ใหม่ → idempotent ที่ระดับ key แทน date
-- =============================================================================


-- -----------------------------------------------------------------------------
-- SP_LOAD_FACT_PRODUCTION — merge by batch_src_id
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_LOAD_FACT_PRODUCTION(p_date IN DATE) IS
BEGIN
    -- ลบ row ที่ batch_src_id ตรงกับ STG ปัจจุบัน (เผื่อ rerun window เดิม)
    DELETE FROM FACT_PRODUCTION
     WHERE batch_src_id IN (
        SELECT batch_id FROM STG_PRODUCTION_BATCH
         WHERE end_time IS NOT NULL
     );

    FOR rec IN (
        SELECT stg.batch_id, stg.order_id, stg.product_id,
               stg.qty_planned, stg.qty_out,
               stg.start_time, stg.end_time,
               dp.product_id AS dim_product_id,
               TO_NUMBER(TO_CHAR(stg.end_time, 'YYYYMMDD')) AS date_id
          FROM STG_PRODUCTION_BATCH stg
          LEFT JOIN DIM_PRODUCT dp ON dp.product_src_id = stg.product_id
         WHERE stg.end_time IS NOT NULL
    ) LOOP
        INSERT INTO FACT_PRODUCTION (
            prod_id, date_id, product_id,
            batch_src_id, order_src_id,
            qty_planned, qty_out, yield_rate,
            start_time, end_time, duration_min
        ) VALUES (
            SEQ_FACT_PRODUCTION.NEXTVAL,
            rec.date_id,
            rec.dim_product_id,
            rec.batch_id,
            rec.order_id,
            rec.qty_planned,
            rec.qty_out,
            CASE WHEN rec.qty_planned > 0 AND rec.qty_out IS NOT NULL
                 THEN ROUND(rec.qty_out / rec.qty_planned, 4)
                 ELSE NULL END,
            rec.start_time,
            rec.end_time,
            ROUND(
                (EXTRACT(DAY    FROM (rec.end_time - rec.start_time)) * 24 * 60 +
                 EXTRACT(HOUR   FROM (rec.end_time - rec.start_time)) * 60 +
                 EXTRACT(MINUTE FROM (rec.end_time - rec.start_time)) +
                 EXTRACT(SECOND FROM (rec.end_time - rec.start_time)) / 60),
                2)
        );
    END LOOP;
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
/


-- -----------------------------------------------------------------------------
-- SP_LOAD_FACT_QUALITY — merge by batch_src_id (qc_record 1:1 กับ batch)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_LOAD_FACT_QUALITY(p_date IN DATE) IS
BEGIN
    DELETE FROM FACT_QUALITY
     WHERE batch_src_id IN (SELECT batch_id FROM STG_QC_RECORD);

    FOR rec IN (
        SELECT batch_id, qty_sampled, qty_passed, qty_failed, inspected_at,
               TO_NUMBER(TO_CHAR(inspected_at, 'YYYYMMDD')) AS date_id
          FROM STG_QC_RECORD
    ) LOOP
        INSERT INTO FACT_QUALITY (
            quality_id, date_id, batch_src_id,
            qty_sampled, qty_passed, qty_failed,
            defect_rate_pct, inspected_at
        ) VALUES (
            SEQ_FACT_QUALITY.NEXTVAL,
            rec.date_id,
            rec.batch_id,
            rec.qty_sampled,
            rec.qty_passed,
            rec.qty_failed,
            CASE WHEN rec.qty_sampled > 0
                 THEN ROUND(rec.qty_failed / rec.qty_sampled * 100, 2)
                 ELSE 0 END,
            rec.inspected_at
        );
    END LOOP;
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
/


-- -----------------------------------------------------------------------------
-- SP_LOAD_FACT_SENSOR — merge by (machine_id, metric_id, window_start)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_LOAD_FACT_SENSOR(p_date IN DATE) IS
BEGIN
    -- ลบ row ที่ window ตรงกับ STG (กัน duplicate ถ้า rerun window เดิม)
    DELETE FROM FACT_SENSOR
     WHERE (machine_id, metric_id, window_start) IN (
        SELECT dm.machine_id, dmt.metric_id, stg.window_start
          FROM STG_SENSOR_AGG stg
          JOIN DIM_MACHINE dm  ON dm.machine_name = stg.machine_name
          JOIN DIM_METRIC  dmt ON dmt.metric_name = stg.metric_name
     );

    FOR rec IN (
        SELECT stg.window_start, stg.window_end,
               stg.avg_value, stg.min_value, stg.max_value, stg.sample_count,
               dm.machine_id, dmt.metric_id,
               TO_NUMBER(TO_CHAR(stg.window_start, 'YYYYMMDD')) AS date_id
          FROM STG_SENSOR_AGG stg
          JOIN DIM_MACHINE dm  ON dm.machine_name = stg.machine_name
          JOIN DIM_METRIC  dmt ON dmt.metric_name = stg.metric_name
    ) LOOP
        INSERT INTO FACT_SENSOR (
            sensor_id, date_id, machine_id, metric_id,
            window_start, window_end,
            avg_value, min_value, max_value, sample_count
        ) VALUES (
            SEQ_FACT_SENSOR.NEXTVAL,
            rec.date_id,
            rec.machine_id,
            rec.metric_id,
            rec.window_start,
            rec.window_end,
            rec.avg_value,
            rec.min_value,
            rec.max_value,
            rec.sample_count
        );
    END LOOP;
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
/
