-- =============================================================================
-- [B]-5 Truncate + Reload — rebuild the entire DW from STG + Supabase dims.
-- Preserves DIM_DATE (it's a calendar, not sourced from OLTP).
--
-- Order:
--   1. TRUNCATE facts (FKs point at dims, so clear facts first).
--   2. TRUNCATE DIM_MACHINE / PRODUCT / STAGE / MATERIAL  (NOT DIM_DATE).
--   3. Re-seed dims from Supabase via seed_dims.py (run outside this SQL).
--   4. Re-run all SP_LOAD_FACT_* for the target dates (run via API or DAG).
--
-- The dim re-seed and SP re-runs are intentionally NOT SQL — they live in
-- Python so they can read Supabase. This file only handles the TRUNCATE +
-- verification queries.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Step 1: truncate facts (FK-referencing tables go first).
-- -----------------------------------------------------------------------------
TRUNCATE TABLE FACT_MAINTENANCE;
TRUNCATE TABLE FACT_INVENTORY;
TRUNCATE TABLE FACT_QUALITY;
TRUNCATE TABLE FACT_PRODUCTION;
TRUNCATE TABLE FACT_OEE;


-- -----------------------------------------------------------------------------
-- Step 2: truncate dims. Keep DIM_DATE — it's a 5-year calendar, no source.
-- -----------------------------------------------------------------------------
TRUNCATE TABLE DIM_MATERIAL;
TRUNCATE TABLE DIM_STAGE;
TRUNCATE TABLE DIM_PRODUCT;
TRUNCATE TABLE DIM_MACHINE;


-- -----------------------------------------------------------------------------
-- Step 3: sequences — not strictly required (MAX is reset) but clean.
-- Oracle 10g can't ALTER SEQUENCE RESTART, so drop + recreate.
-- -----------------------------------------------------------------------------
DROP SEQUENCE SEQ_DIM_MACHINE;
DROP SEQUENCE SEQ_DIM_PRODUCT;
DROP SEQUENCE SEQ_DIM_STAGE;
DROP SEQUENCE SEQ_DIM_MATERIAL;
DROP SEQUENCE SEQ_FACT_OEE;
DROP SEQUENCE SEQ_FACT_PRODUCTION;
DROP SEQUENCE SEQ_FACT_QUALITY;
DROP SEQUENCE SEQ_FACT_INVENTORY;
DROP SEQUENCE SEQ_FACT_MAINTENANCE;

CREATE SEQUENCE SEQ_DIM_MACHINE       START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE SEQ_DIM_PRODUCT       START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE SEQ_DIM_STAGE         START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE SEQ_DIM_MATERIAL      START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE SEQ_FACT_OEE          START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE SEQ_FACT_PRODUCTION   START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE SEQ_FACT_QUALITY      START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE SEQ_FACT_INVENTORY    START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE SEQ_FACT_MAINTENANCE  START WITH 1 INCREMENT BY 1 NOCACHE;


-- -----------------------------------------------------------------------------
-- Step 4: verification — run after seed_dims.py + SP re-runs.
-- Expect rowcounts: DIM_MACHINE=3, DIM_PRODUCT=3, DIM_STAGE=10, DIM_MATERIAL=5.
-- -----------------------------------------------------------------------------
SELECT 'DIM_DATE'     tbl, COUNT(*) n FROM DIM_DATE UNION ALL
SELECT 'DIM_MACHINE',      COUNT(*)   FROM DIM_MACHINE UNION ALL
SELECT 'DIM_PRODUCT',      COUNT(*)   FROM DIM_PRODUCT UNION ALL
SELECT 'DIM_STAGE',        COUNT(*)   FROM DIM_STAGE UNION ALL
SELECT 'DIM_MATERIAL',     COUNT(*)   FROM DIM_MATERIAL UNION ALL
SELECT 'FACT_OEE',         COUNT(*)   FROM FACT_OEE UNION ALL
SELECT 'FACT_PRODUCTION',  COUNT(*)   FROM FACT_PRODUCTION UNION ALL
SELECT 'FACT_QUALITY',     COUNT(*)   FROM FACT_QUALITY UNION ALL
SELECT 'FACT_INVENTORY',   COUNT(*)   FROM FACT_INVENTORY UNION ALL
SELECT 'FACT_MAINTENANCE', COUNT(*)   FROM FACT_MAINTENANCE;


-- -----------------------------------------------------------------------------
-- Full-rebuild driver — Python equivalent:
-- -----------------------------------------------------------------------------
-- .venv/bin/python datasources/oracle_sql_query/apply_ddl.py \
--     datasources/oracle_sql_query/05_truncate_and_reload.sql
-- .venv/bin/python datasources/oracle_sql_query/seed_dims.py
-- # Then re-run SPs for every date you want in the DW, e.g.:
-- for d in 2026-04-15 2026-04-16 2026-04-17; do
--   for sp in SP_LOAD_FACT_OEE SP_LOAD_FACT_QUALITY \
--             SP_LOAD_FACT_MAINTENANCE SP_LOAD_FACT_PRODUCTION; do
--     curl -s -X POST http://localhost:8000/sp/call \
--       -H "Authorization: Bearer $ORACLE_API_TOKEN" \
--       -H "Content-Type: application/json" \
--       -d "{\"name\":\"$sp\",\"args\":[\"$d\"]}"
--   done
-- done
