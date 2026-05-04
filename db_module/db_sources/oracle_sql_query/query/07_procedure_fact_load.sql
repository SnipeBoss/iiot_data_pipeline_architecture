-- ============================================================
-- Stored Procedure สำหรับโหลด FACT (transform STG → FACT)
--
-- รูปแบบ: ใช้ business key เป็นตัวอ้างอิง (idempotent ที่ระดับ key)
-- เหตุผล: DAG รันทุก 15 นาที อาจรันซ้ำในช่วง window ที่ทับกัน
--         → ลบ row ที่ key ตรงกับ STG ปัจจุบัน แล้วค่อย insert ใหม่
-- ============================================================

-- ┌──────────────────────────────────────────────────────────┐
-- │  Helper function: derive shift_id จาก timestamp          │
-- │  DAY:   07:30-16:30 → shift_id=1                         │
-- │  NIGHT: 17:30-06:30 → shift_id=2                         │
-- └──────────────────────────────────────────────────────────┘
CREATE OR REPLACE FUNCTION FN_GET_SHIFT_ID(p_ts TIMESTAMP)
RETURN NUMBER AS
    v_minutes_of_day NUMBER;
BEGIN
    IF p_ts IS NULL THEN RETURN NULL; END IF;

    -- แปลงเวลาในวันให้เป็น "นาทีตั้งแต่เที่ยงคืน" เพื่อเปรียบเทียบง่าย
    v_minutes_of_day := EXTRACT(HOUR   FROM p_ts) * 60
                      + EXTRACT(MINUTE FROM p_ts);

    -- DAY: 07:30 (450) <= t < 16:30 (990)
    -- NIGHT: t >= 17:30 (1050) หรือ t < 06:30 (390)
    IF v_minutes_of_day >= 450 AND v_minutes_of_day < 990 THEN
        RETURN 1;  -- กะกลางวัน
    ELSE
        RETURN 2;  -- กะกลางคืน (รวมช่วงคาบเกี่ยวก่อน/หลังกะเพื่อความเรียบง่าย)
    END IF;
END FN_GET_SHIFT_ID;
/






-- ┌──────────────────────────────────────────────────────────┐
-- │  SP_LOAD_FACT_PRODUCTION                                 │
-- │  Source: STG_PRODUCTION_BATCH (เฉพาะ batch ที่ปิดแล้ว)   │
-- │  Derive: shift_id, batch_planned (FIFO), slippage        │
-- └──────────────────────────────────────────────────────────┘
CREATE OR REPLACE PROCEDURE SP_LOAD_FACT_PRODUCTION AS
BEGIN
    -- Step 1: DELETE row ที่ key match กับ STG (idempotency)
    DELETE FROM FACT_PRODUCTION
    WHERE batch_src_id IN (SELECT batch_id FROM STG_PRODUCTION_BATCH);

    -- Step 2: INSERT พร้อม lookup FK และคำนวณคอลัมน์ derive
    -- ใช้ cursor FOR-LOOP เพราะ Oracle 10g ห้ามใช้ SEQ.NEXTVAL ใน INSERT...SELECT
    FOR rec IN (
        SELECT 
            stg.batch_id,
            stg.order_id,
            stg.line_id,
            stg.model_id,
            stg.qty_planned,
            stg.qty_out,
            stg.start_time,
            stg.end_time,
            stg.order_planned_start,
            stg.order_planned_end,
            stg.order_total_qty,
            -- จำนวนสะสมก่อนหน้า batch นี้ (เรียงแบบ FIFO ตาม batch_id)
            (SELECT NVL(SUM(qty_planned), 0)
               FROM STG_PRODUCTION_BATCH inner_stg
              WHERE inner_stg.order_id = stg.order_id
                AND inner_stg.batch_id < stg.batch_id) AS cum_qty_before
        FROM STG_PRODUCTION_BATCH stg
        WHERE stg.end_time IS NOT NULL
    ) LOOP
        DECLARE
            v_order_dur_min  NUMBER;
            v_batch_share    NUMBER;
            v_batch_est_min  NUMBER;
            v_planned_start  TIMESTAMP;
            v_planned_end    TIMESTAMP;
            v_actual_dur_min NUMBER;
            v_slippage_min   NUMBER;
            v_dim_line_id    NUMBER;
            v_dim_model_id   NUMBER;
            v_dim_shift_id   NUMBER;
            v_dim_date_id    NUMBER;
        BEGIN
            -- คำนวณแผน batch แบบ FIFO (แบ่งสัดส่วนเวลาตาม qty)
            v_order_dur_min := (CAST(rec.order_planned_end AS DATE)
                              - CAST(rec.order_planned_start AS DATE)) * 24 * 60;
            v_batch_share   := rec.qty_planned / NULLIF(rec.order_total_qty, 0);
            v_batch_est_min := v_order_dur_min * v_batch_share;
            v_planned_start := rec.order_planned_start
                             + (rec.cum_qty_before / NULLIF(rec.order_total_qty,0))
                               * (rec.order_planned_end - rec.order_planned_start);
            v_planned_end   := v_planned_start
                             + NUMTODSINTERVAL(v_batch_est_min * 60, 'SECOND');

            -- Slippage = ระยะเวลาจริง - ระยะเวลาที่วางแผนไว้
            v_actual_dur_min := (CAST(rec.end_time AS DATE)
                               - CAST(rec.start_time AS DATE)) * 24 * 60;
            v_slippage_min   := v_actual_dur_min - v_batch_est_min;

            -- Lookup DIM (แปลง business key → surrogate key)
            SELECT line_id INTO v_dim_line_id
              FROM DIM_LINE WHERE line_src_id = rec.line_id;
            
            SELECT model_id INTO v_dim_model_id
              FROM DIM_BATTERY_MODEL WHERE model_src_id = rec.model_id;
            
            v_dim_shift_id := FN_GET_SHIFT_ID(rec.start_time);
            v_dim_date_id  := TO_NUMBER(TO_CHAR(rec.end_time, 'YYYYMMDD'));
            
            INSERT INTO FACT_PRODUCTION (
                prod_id, date_id, line_id, shift_id, model_id,
                batch_src_id, order_src_id,
                qty_planned, qty_out, duration_min, yield_rate,
                order_planned_start, order_planned_end,
                batch_planned_start, batch_planned_end,
                batch_est_duration_min, slippage_min,
                start_time, end_time
            ) VALUES (
                SEQ_FACT_PRODUCTION.NEXTVAL,
                v_dim_date_id, v_dim_line_id, v_dim_shift_id, v_dim_model_id,
                rec.batch_id, rec.order_id,
                rec.qty_planned, rec.qty_out, v_actual_dur_min,
                rec.qty_out / NULLIF(rec.qty_planned, 0),
                rec.order_planned_start, rec.order_planned_end,
                v_planned_start, v_planned_end,
                v_batch_est_min, v_slippage_min,
                rec.start_time, rec.end_time
            );
        END;
    END LOOP;
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END SP_LOAD_FACT_PRODUCTION;
/







-- ┌──────────────────────────────────────────────────────────┐
-- │  SP_LOAD_FACT_QUALITY                                    │
-- │  Source: STG_QC_RECORD JOIN STG_PRODUCTION_BATCH         │
-- │  (ต้องดึง context ของ batch เพื่อหา dim line/model/shift)│
-- └──────────────────────────────────────────────────────────┘
CREATE OR REPLACE PROCEDURE SP_LOAD_FACT_QUALITY AS
BEGIN
    DELETE FROM FACT_QUALITY
    WHERE qc_src_id IN (SELECT qc_id FROM STG_QC_RECORD);
    
    FOR rec IN (
        SELECT 
            qc.qc_id,
            qc.batch_id,
            qc.qty_inspected,
            qc.qty_passed,
            qc.qty_failed,
            qc.inspected_at,
            b.line_id    AS src_line_id,
            b.model_id   AS src_model_id,
            b.start_time AS batch_start
        FROM STG_QC_RECORD qc
        JOIN STG_PRODUCTION_BATCH b ON b.batch_id = qc.batch_id
    ) LOOP
        DECLARE
            v_dim_line_id  NUMBER;
            v_dim_model_id NUMBER;
        BEGIN
            SELECT line_id INTO v_dim_line_id
              FROM DIM_LINE WHERE line_src_id = rec.src_line_id;
            
            SELECT model_id INTO v_dim_model_id
              FROM DIM_BATTERY_MODEL WHERE model_src_id = rec.src_model_id;
            
            INSERT INTO FACT_QUALITY (
                quality_id, date_id, line_id, shift_id, model_id,
                qc_src_id, batch_src_id,
                qty_inspected, qty_passed, qty_failed, defect_rate_pct,
                inspected_at
            ) VALUES (
                SEQ_FACT_QUALITY.NEXTVAL,
                TO_NUMBER(TO_CHAR(rec.inspected_at, 'YYYYMMDD')),
                v_dim_line_id,
                FN_GET_SHIFT_ID(rec.batch_start),
                v_dim_model_id,
                rec.qc_id, rec.batch_id,
                rec.qty_inspected, rec.qty_passed, rec.qty_failed,
                rec.qty_failed / NULLIF(rec.qty_inspected, 0) * 100,
                rec.inspected_at
            );
        END;
    END LOOP;
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END SP_LOAD_FACT_QUALITY;
/







-- ┌──────────────────────────────────────────────────────────┐
-- │  SP_LOAD_FACT_DEFECT                                     │
-- │  Source: STG_QC_DEFECT JOIN STG_QC_RECORD JOIN STG_BATCH │
-- │  Lookup: DIM_DEFECT_TYPE โดยใช้ defect_code              │
-- └──────────────────────────────────────────────────────────┘
CREATE OR REPLACE PROCEDURE SP_LOAD_FACT_DEFECT AS
BEGIN
    DELETE FROM FACT_DEFECT
    WHERE qc_src_id IN (SELECT qc_id FROM STG_QC_DEFECT);
    
    FOR rec IN (
        SELECT 
            qd.qc_id,
            qd.defect_code,
            qd.qty_affected,
            qc.inspected_at,
            qc.batch_id,
            b.line_id   AS src_line_id,
            b.model_id  AS src_model_id
        FROM STG_QC_DEFECT qd
        JOIN STG_QC_RECORD qc ON qc.qc_id = qd.qc_id
        JOIN STG_PRODUCTION_BATCH b ON b.batch_id = qc.batch_id
    ) LOOP
        DECLARE
            v_dim_line_id   NUMBER;
            v_dim_model_id  NUMBER;
            v_dim_defect_id NUMBER;
            -- Oracle 10g ไม่มี CONTINUE keyword (เพิ่มใน 11g R1)
            -- ใช้ flag pattern: ถ้า lookup ไม่เจอ ตั้ง v_skip=TRUE แล้วข้าม INSERT
            v_skip          BOOLEAN := FALSE;
        BEGIN
            SELECT line_id INTO v_dim_line_id
              FROM DIM_LINE WHERE line_src_id = rec.src_line_id;

            SELECT model_id INTO v_dim_model_id
              FROM DIM_BATTERY_MODEL WHERE model_src_id = rec.src_model_id;

            -- Lookup DIM_DEFECT_TYPE จาก defect_code
            -- ถ้าไม่เจอใน DIM (data quality issue) ให้ข้าม row นั้นเงียบ ๆ
            BEGIN
                SELECT defect_id INTO v_dim_defect_id
                  FROM DIM_DEFECT_TYPE
                 WHERE defect_code = rec.defect_code
                   AND is_leaf = 'Y';   -- รับเฉพาะ leaf node เท่านั้น
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    v_skip := TRUE;   -- ข้าม code ที่ไม่มีใน DIM (เลี่ยง CONTINUE สำหรับ Oracle 10g)
            END;

            IF NOT v_skip THEN
                INSERT INTO FACT_DEFECT (
                    defect_fact_id, date_id, line_id, model_id, defect_id,
                    qc_src_id, batch_src_id, qty_affected
                ) VALUES (
                    SEQ_FACT_DEFECT.NEXTVAL,
                    TO_NUMBER(TO_CHAR(rec.inspected_at, 'YYYYMMDD')),
                    v_dim_line_id, v_dim_model_id, v_dim_defect_id,
                    rec.qc_id, rec.batch_id, rec.qty_affected
                );
            END IF;
        END;
    END LOOP;
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END SP_LOAD_FACT_DEFECT;
/







-- ┌──────────────────────────────────────────────────────────┐
-- │  SP_LOAD_FACT_DOWNTIME                                   │
-- │  Source: STG_DOWNTIME_EVENT (เฉพาะ event ที่ปิดแล้ว)     │
-- │  Filter: end_ts IS NOT NULL (ไม่โหลด event ที่ยังเปิดอยู่)│
-- └──────────────────────────────────────────────────────────┘
CREATE OR REPLACE PROCEDURE SP_LOAD_FACT_DOWNTIME AS
BEGIN
    DELETE FROM FACT_DOWNTIME
    WHERE event_src_id IN (
        SELECT event_id FROM STG_DOWNTIME_EVENT WHERE end_ts IS NOT NULL
    );
    
    FOR rec IN (
        SELECT * FROM STG_DOWNTIME_EVENT WHERE end_ts IS NOT NULL
    ) LOOP
        DECLARE
            v_dim_line_id    NUMBER;
            v_dim_machine_id NUMBER;
        BEGIN
            SELECT line_id INTO v_dim_line_id
              FROM DIM_LINE WHERE line_src_id = rec.line_id;
            
            SELECT machine_id INTO v_dim_machine_id
              FROM DIM_MACHINE WHERE machine_src_id = rec.machine_id;
            
            INSERT INTO FACT_DOWNTIME (
                downtime_id, date_id, line_id, shift_id, machine_id,
                event_src_id, batch_src_id, reason_code, is_planned,
                duration_min, start_ts, end_ts
            ) VALUES (
                SEQ_FACT_DOWNTIME.NEXTVAL,
                TO_NUMBER(TO_CHAR(rec.start_ts, 'YYYYMMDD')),
                v_dim_line_id,
                FN_GET_SHIFT_ID(rec.start_ts),
                v_dim_machine_id,
                rec.event_id, rec.batch_id, rec.reason_code, rec.is_planned,
                rec.duration_min, rec.start_ts, rec.end_ts
            );
        END;
    END LOOP;
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END SP_LOAD_FACT_DOWNTIME;
/







-- ┌──────────────────────────────────────────────────────────┐
-- │  SP_LOAD_FACT_SENSOR                                     │
-- │  Source: STG_SENSOR_AGG (มาจาก Flux aggregation 15 นาที) │
-- │  Lookup: DIM_MACHINE จาก machine_code, DIM_METRIC จาก name│
-- └──────────────────────────────────────────────────────────┘
CREATE OR REPLACE PROCEDURE SP_LOAD_FACT_SENSOR AS
BEGIN
    -- ใช้ composite key (machine_id, metric_id, window_start) เป็นตัวลบ
    -- เพราะ (machine, metric) เดียวกันมีหลาย window ได้
    DELETE FROM FACT_SENSOR
    WHERE (machine_id, metric_id, window_start) IN (
        SELECT dm.machine_id, dmt.metric_id, stg.window_start
          FROM STG_SENSOR_AGG stg
          JOIN DIM_MACHINE dm ON dm.machine_code = stg.machine_code
          JOIN DIM_METRIC dmt ON dmt.metric_name = stg.metric_name
    );
    
    FOR rec IN (
        SELECT 
            dm.machine_id  AS dim_machine_id,
            dmt.metric_id  AS dim_metric_id,
            stg.window_start,
            stg.window_end,
            stg.avg_value,
            stg.min_value,
            stg.max_value,
            stg.sample_count
        FROM STG_SENSOR_AGG stg
        JOIN DIM_MACHINE dm ON dm.machine_code = stg.machine_code
        JOIN DIM_METRIC dmt ON dmt.metric_name = stg.metric_name
    ) LOOP
        INSERT INTO FACT_SENSOR (
            sensor_id, date_id, machine_id, metric_id,
            window_start, window_end,
            avg_value, min_value, max_value, sample_count
        ) VALUES (
            SEQ_FACT_SENSOR.NEXTVAL,
            TO_NUMBER(TO_CHAR(rec.window_start, 'YYYYMMDD')),
            rec.dim_machine_id, rec.dim_metric_id,
            rec.window_start, rec.window_end,
            rec.avg_value, rec.min_value, rec.max_value, rec.sample_count
        );
    END LOOP;
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END SP_LOAD_FACT_SENSOR;
/






-- ┌──────────────────────────────────────────────────────────┐
-- │  Master orchestrator — โหลด FACT ทุกตัวตามลำดับ dependency│
-- │  ลำดับ: PRODUCTION → QUALITY → DEFECT → DOWNTIME → SENSOR │
-- └──────────────────────────────────────────────────────────┘
CREATE OR REPLACE PROCEDURE SP_LOAD_ALL_FACTS AS
BEGIN
    SP_LOAD_FACT_PRODUCTION;
    SP_LOAD_FACT_QUALITY;
    SP_LOAD_FACT_DEFECT;        -- ขึ้นกับ QC_RECORD + PRODUCTION_BATCH
    SP_LOAD_FACT_DOWNTIME;
    SP_LOAD_FACT_SENSOR;        -- ไม่ขึ้นกับ OLTP
END SP_LOAD_ALL_FACTS;
/