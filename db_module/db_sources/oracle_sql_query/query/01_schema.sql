-- =============================================================================
-- Oracle DW Schema (AI03) — ตาม NEW_ARCHITECTURE.md
-- 3 STG + 4 DIM + 3 FACT
-- =============================================================================
-- Oracle 10g compatibility notes:
--   * ห้ามใช้ `GENERATED ALWAYS AS IDENTITY` (ต้อง 12c+) → ใช้ SEQUENCE
--   * Each statement ปิดด้วย `;` บรรทัดเดียว; PL/SQL block ปิดด้วย `/`
-- =============================================================================


-- -----------------------------------------------------------------------------
-- DIM_DATE — surrogate key = YYYYMMDD; populate ผ่าน SP_LOAD_DIM_DATE
-- -----------------------------------------------------------------------------
CREATE TABLE DIM_DATE (
    date_id      NUMBER PRIMARY KEY,    -- YYYYMMDD เช่น 20260418
    full_date    DATE          NOT NULL,
    day_of_week  NUMBER(1),
    week_number  NUMBER(2),
    month_number NUMBER(2),
    quarter      NUMBER(1),
    year         NUMBER(4)
)
;


-- -----------------------------------------------------------------------------
-- DIM_MACHINE — 3 เครื่อง (M01/M02/M03); machine_name ต้องตรง tag ใน InfluxDB
-- -----------------------------------------------------------------------------
CREATE TABLE DIM_MACHINE (
    machine_id      NUMBER PRIMARY KEY,
    machine_src_id  NUMBER        NOT NULL,  -- machine_id จาก Supabase
    machine_name    VARCHAR2(10)  NOT NULL,  -- "M01" / "M02" / "M03"
    line_name       VARCHAR2(50)             -- denormalized
)
;
CREATE SEQUENCE SEQ_DIM_MACHINE START WITH 1 INCREMENT BY 1 NOCACHE
;


-- -----------------------------------------------------------------------------
-- DIM_PRODUCT
-- -----------------------------------------------------------------------------
CREATE TABLE DIM_PRODUCT (
    product_id      NUMBER PRIMARY KEY,
    product_src_id  NUMBER        NOT NULL,  -- product_id จาก Supabase
    product_name    VARCHAR2(100) NOT NULL
)
;
CREATE SEQUENCE SEQ_DIM_PRODUCT START WITH 1 INCREMENT BY 1 NOCACHE
;


-- -----------------------------------------------------------------------------
-- DIM_METRIC — catalog ของ sensor metric จาก NodeRED/InfluxDB
-- เพิ่ม sensor ใหม่ = INSERT row เท่านั้น ไม่แตะ schema อื่น
-- -----------------------------------------------------------------------------
CREATE TABLE DIM_METRIC (
    metric_id    NUMBER PRIMARY KEY,
    metric_name  VARCHAR2(50)  NOT NULL,  -- ต้องตรง field name ใน InfluxDB
    unit         VARCHAR2(20),
    machine_name VARCHAR2(10),            -- NULL = ทุกเครื่อง
    description  VARCHAR2(200)
)
;

-- Master seed ตาม NodeRED flow จริง (6 metric)
INSERT INTO DIM_METRIC VALUES (1, 'temperature_c',     'celsius', 'M01', 'Furnace temperature')
;
INSERT INTO DIM_METRIC VALUES (2, 'machine_state_num', 'binary',  NULL,  '1=RUNNING / 0=FAULT')
;
INSERT INTO DIM_METRIC VALUES (3, 'cycle_count',       'count',   'M02', 'Assembly cycle count')
;
INSERT INTO DIM_METRIC VALUES (4, 'vibration_g',       'g-force', 'M02', 'Vibration level')
;
INSERT INTO DIM_METRIC VALUES (5, 'current_a',         'ampere',  'M03', 'Charging current')
;
INSERT INTO DIM_METRIC VALUES (6, 'voltage_v',         'volt',    'M03', 'Charging voltage')
;


-- -----------------------------------------------------------------------------
-- FACT_PRODUCTION — grain: 1 batch = 1 full line run (no machine_id)
-- -----------------------------------------------------------------------------
CREATE TABLE FACT_PRODUCTION (
    prod_id       NUMBER        PRIMARY KEY,
    date_id       NUMBER        REFERENCES DIM_DATE(date_id),
    product_id    NUMBER        REFERENCES DIM_PRODUCT(product_id),
    batch_src_id  NUMBER        NOT NULL,  -- Supabase batch_id (degenerate dim)
    order_src_id  NUMBER        NOT NULL,  -- Supabase order_id (degenerate dim)
    qty_planned   NUMBER(8)     NOT NULL,
    qty_out       NUMBER(8),               -- NULL = ยังไม่เสร็จ
    yield_rate    NUMBER(5,4),             -- qty_out / qty_planned (SP คำนวณ)
    start_time    TIMESTAMP     NOT NULL,  -- เข้า M01
    end_time      TIMESTAMP,               -- ออก M03
    duration_min  NUMBER(8,2),             -- SP คำนวณ
    loaded_at     TIMESTAMP     DEFAULT SYSDATE
)
;
CREATE SEQUENCE SEQ_FACT_PRODUCTION START WITH 1 INCREMENT BY 1 NOCACHE
;


-- -----------------------------------------------------------------------------
-- FACT_QUALITY — grain: 1 QC record ต่อ batch (no machine_id; QC = end of line)
-- -----------------------------------------------------------------------------
CREATE TABLE FACT_QUALITY (
    quality_id       NUMBER       PRIMARY KEY,
    date_id          NUMBER       REFERENCES DIM_DATE(date_id),
    batch_src_id     NUMBER       NOT NULL,
    qty_sampled      NUMBER(8)    NOT NULL,
    qty_passed       NUMBER(8)    NOT NULL,
    qty_failed       NUMBER(8)    NOT NULL,
    defect_rate_pct  NUMBER(5,2),            -- qty_failed / qty_sampled * 100
    inspected_at     TIMESTAMP,
    loaded_at        TIMESTAMP    DEFAULT SYSDATE
)
;
CREATE SEQUENCE SEQ_FACT_QUALITY START WITH 1 INCREMENT BY 1 NOCACHE
;


-- -----------------------------------------------------------------------------
-- FACT_SENSOR — grain: 1 row ต่อ (machine × metric × 15-min window)
-- 6 metrics × 3 machines = สูงสุด 18 row ต่อ window แต่จริง filter ตาม
-- DIM_METRIC.machine_name (บาง metric ไม่ได้ทุกเครื่อง)
-- -----------------------------------------------------------------------------
CREATE TABLE FACT_SENSOR (
    sensor_id    NUMBER        PRIMARY KEY,
    date_id      NUMBER        REFERENCES DIM_DATE(date_id),
    machine_id   NUMBER        REFERENCES DIM_MACHINE(machine_id),
    metric_id    NUMBER        REFERENCES DIM_METRIC(metric_id),
    window_start TIMESTAMP     NOT NULL,
    window_end   TIMESTAMP     NOT NULL,
    avg_value    NUMBER(12,4)  NOT NULL,
    min_value    NUMBER(12,4),
    max_value    NUMBER(12,4),
    sample_count NUMBER(6),                 -- ~900 จาก 1Hz × 15min
    loaded_at    TIMESTAMP     DEFAULT SYSDATE
)
;
CREATE SEQUENCE SEQ_FACT_SENSOR START WITH 1 INCREMENT BY 1 NOCACHE
;


-- -----------------------------------------------------------------------------
-- STG_PRODUCTION_BATCH — raw extract จาก Supabase `production_batch`
-- (เฉพาะ batch ที่ end_time IS NOT NULL)
-- -----------------------------------------------------------------------------
CREATE TABLE STG_PRODUCTION_BATCH (
    batch_id        NUMBER,
    order_id        NUMBER,
    product_id      NUMBER,
    qty_planned     NUMBER,
    qty_out         NUMBER,
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    src_system      VARCHAR2(20)  DEFAULT 'SUPABASE',
    pipeline_run_id VARCHAR2(100),
    loaded_at       TIMESTAMP     DEFAULT SYSDATE
)
;


-- -----------------------------------------------------------------------------
-- STG_QC_RECORD — raw extract จาก Supabase `qc_record`
-- -----------------------------------------------------------------------------
CREATE TABLE STG_QC_RECORD (
    qc_id           NUMBER,
    batch_id        NUMBER,
    qty_sampled     NUMBER,
    qty_passed      NUMBER,
    qty_failed      NUMBER,
    inspected_at    TIMESTAMP,
    src_system      VARCHAR2(20)  DEFAULT 'SUPABASE',
    pipeline_run_id VARCHAR2(100),
    loaded_at       TIMESTAMP     DEFAULT SYSDATE
)
;


-- -----------------------------------------------------------------------------
-- STG_SENSOR_AGG — 15-min aggregate จาก InfluxDB
-- Airflow เขียน 1 row ต่อ (machine × metric × window) ~18 row ต่อ 15 นาที
-- -----------------------------------------------------------------------------
CREATE TABLE STG_SENSOR_AGG (
    machine_name    VARCHAR2(10),   -- "M01" ใช้ lookup DIM_MACHINE
    metric_name     VARCHAR2(50),   -- "temperature_c" ใช้ lookup DIM_METRIC
    window_start    TIMESTAMP,
    window_end      TIMESTAMP,
    avg_value       NUMBER(12,4),
    min_value       NUMBER(12,4),
    max_value       NUMBER(12,4),
    sample_count    NUMBER(6),
    src_system      VARCHAR2(20)  DEFAULT 'INFLUXDB',
    pipeline_run_id VARCHAR2(100),
    loaded_at       TIMESTAMP     DEFAULT SYSDATE
)
;


-- -----------------------------------------------------------------------------
-- Index สำหรับ reporting query (filter by date_id + FK join)
-- -----------------------------------------------------------------------------
CREATE INDEX idx_prod_date    ON FACT_PRODUCTION (date_id)
;
CREATE INDEX idx_prod_batch   ON FACT_PRODUCTION (batch_src_id)
;
CREATE INDEX idx_qual_date    ON FACT_QUALITY    (date_id)
;
CREATE INDEX idx_qual_batch   ON FACT_QUALITY    (batch_src_id)
;
CREATE INDEX idx_sensor_date_machine ON FACT_SENSOR (date_id, machine_id)
;
CREATE INDEX idx_sensor_window       ON FACT_SENSOR (window_start, window_end)
;
