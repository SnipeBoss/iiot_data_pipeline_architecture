-- =============================================================================
-- Truncate + Reload — ล้างและสร้าง DW ใหม่ทั้งหมด (เก็บ DIM_DATE + DIM_METRIC ไว้)
-- =============================================================================
-- ลำดับการรัน:
--   1. TRUNCATE facts (FK ชี้ไป DIM → ต้องล้างก่อน)
--   2. TRUNCATE DIM_MACHINE / DIM_PRODUCT  (ไม่แตะ DIM_DATE, DIM_METRIC)
--   3. Re-seed DIM_MACHINE / DIM_PRODUCT จาก Supabase ผ่าน
--      sync_dimensions_from_supabase.py
--   4. Re-run SP_LOAD_FACT_* ทุกวันที่ต้องการ
--
-- TRUNCATE facts ไม่ต้อง drop/recreate sequence (Oracle 10g)
-- แต่ถ้าอยาก reset sequence ให้ start ที่ 1 ต้อง DROP + CREATE
-- =============================================================================


-- Step 1: Truncate facts
TRUNCATE TABLE FACT_SENSOR;
TRUNCATE TABLE FACT_QUALITY;
TRUNCATE TABLE FACT_PRODUCTION;


-- Step 2: Truncate dims (เก็บ DIM_DATE + DIM_METRIC)
TRUNCATE TABLE DIM_PRODUCT;
TRUNCATE TABLE DIM_MACHINE;


-- Step 3: Reset sequences (optional — Oracle 10g ต้อง DROP + CREATE)
DROP SEQUENCE SEQ_DIM_MACHINE;
DROP SEQUENCE SEQ_DIM_PRODUCT;
DROP SEQUENCE SEQ_FACT_PRODUCTION;
DROP SEQUENCE SEQ_FACT_QUALITY;
DROP SEQUENCE SEQ_FACT_SENSOR;

CREATE SEQUENCE SEQ_DIM_MACHINE     START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE SEQ_DIM_PRODUCT     START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE SEQ_FACT_PRODUCTION START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE SEQ_FACT_QUALITY    START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE SEQ_FACT_SENSOR     START WITH 1 INCREMENT BY 1 NOCACHE;


-- Step 4: Verification
SELECT 'DIM_DATE'         tbl, COUNT(*) n FROM DIM_DATE UNION ALL
SELECT 'DIM_METRIC',           COUNT(*)   FROM DIM_METRIC UNION ALL
SELECT 'DIM_MACHINE',          COUNT(*)   FROM DIM_MACHINE UNION ALL
SELECT 'DIM_PRODUCT',          COUNT(*)   FROM DIM_PRODUCT UNION ALL
SELECT 'FACT_PRODUCTION',      COUNT(*)   FROM FACT_PRODUCTION UNION ALL
SELECT 'FACT_QUALITY',         COUNT(*)   FROM FACT_QUALITY UNION ALL
SELECT 'FACT_SENSOR',          COUNT(*)   FROM FACT_SENSOR;


-- -----------------------------------------------------------------------------
-- ตัวอย่าง full-rebuild command:
-- -----------------------------------------------------------------------------
-- .venv/bin/python db_module/db_sources/oracle_sql_query/run_sql_file.py \
--     db_module/db_sources/oracle_sql_query/query/05_truncate_and_reload.sql
-- .venv/bin/python db_module/db_sources/oracle_sql_query/sync_dimensions_from_supabase.py
-- # แล้ว re-run SP ทุกวันที่ต้องการ:
-- for d in 2026-04-18; do
--   curl -s -X POST http://localhost:8000/sp/call \
--     -H "Authorization: Bearer $ORACLE_API_TOKEN" \
--     -H "Content-Type: application/json" \
--     -d "{\"name\":\"SP_LOAD_FACT_PRODUCTION\",\"args\":[\"$d\"]}"
-- done
