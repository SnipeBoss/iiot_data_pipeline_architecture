-- =============================================================================
-- SP_LOAD_DIM_DATE — generate DIM_DATE rows across a window, idempotent.
-- date_id = YYYYMMDD so a re-run on an overlapping window is a no-op (MERGE).
-- Day-of-week uses ISO-week anchoring so weekends are locale-independent:
--   TRUNC(d) - TRUNC(d,'IW') returns 0=Mon .. 6=Sun
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_LOAD_DIM_DATE(p_start IN DATE, p_days IN NUMBER) IS
    v_d     DATE;
    v_dow   NUMBER;   -- 1=Mon .. 7=Sun
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
            month_number, month_name, quarter, year, is_weekend, is_holiday
        ) VALUES (
            src.date_id,
            v_d,
            v_dow,
            TO_NUMBER(TO_CHAR(v_d, 'IW')),
            TO_NUMBER(TO_CHAR(v_d, 'MM')),
            TRIM(TO_CHAR(v_d, 'Month', 'NLS_DATE_LANGUAGE=ENGLISH')),
            TO_NUMBER(TO_CHAR(v_d, 'Q')),
            TO_NUMBER(TO_CHAR(v_d, 'YYYY')),
            CASE WHEN v_dow IN (6, 7) THEN 'Y' ELSE 'N' END,
            'N'
        );
    END LOOP;
    COMMIT;
END;
/
