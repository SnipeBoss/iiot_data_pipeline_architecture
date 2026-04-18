-- =============================================================================
-- SP_LOAD_DIM_DATE — สร้าง row ใน DIM_DATE ครอบคลุม window ที่กำหนด
-- =============================================================================
-- idempotent: date_id = YYYYMMDD → รันซ้ำบน window ที่ทับกันจะเป็น no-op (MERGE)
--
-- ISO week anchoring สำหรับ day_of_week ให้ locale-independent:
--   TRUNC(d) - TRUNC(d,'IW') → 0=Mon..6=Sun  (บวก 1 ได้ 1..7)
-- ถ้าใช้ TO_CHAR(d, 'D') จะขึ้นกับ NLS_TERRITORY ของ session → flaky
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_LOAD_DIM_DATE(p_start IN DATE, p_days IN NUMBER) IS
    v_d    DATE;
    v_dow  NUMBER;   -- 1=จันทร์ .. 7=อาทิตย์ (ISO-8601)
BEGIN
    FOR i IN 0 .. p_days - 1 LOOP
        v_d   := p_start + i;
        v_dow := (TRUNC(v_d) - TRUNC(v_d, 'IW')) + 1;

        MERGE INTO DIM_DATE dst
        USING (
            SELECT TO_NUMBER(TO_CHAR(v_d, 'YYYYMMDD')) AS date_id FROM DUAL
        ) src
        ON (dst.date_id = src.date_id)
        WHEN NOT MATCHED THEN INSERT (
            date_id, full_date, day_of_week, week_number,
            month_number, quarter, year
        ) VALUES (
            src.date_id,
            v_d,
            v_dow,
            TO_NUMBER(TO_CHAR(v_d, 'IW')),
            TO_NUMBER(TO_CHAR(v_d, 'MM')),
            TO_NUMBER(TO_CHAR(v_d, 'Q')),
            TO_NUMBER(TO_CHAR(v_d, 'YYYY'))
        );
    END LOOP;
    COMMIT;
END;
/
