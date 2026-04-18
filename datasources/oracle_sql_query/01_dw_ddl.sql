-- =============================================================================
-- Oracle DDL — Staging + Data Warehouse, all under AI03 schema.
-- No CREATE USER. Table names keep their STG_/DIM_/FACT_ prefixes but live in AI03.
-- =============================================================================
-- Oracle 10g compatibility notes:
--   * No GENERATED AS IDENTITY — surrogate keys use explicit sequences.
--   * Facts drop before dims (FK dependency).
--   * Each statement separated by a bare `;` on its own line-ending so the
--     Python applier can split reliably. PL/SQL blocks (anonymous or named)
--     must be terminated with `/` on its own line.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Idempotent teardown — drop any pre-existing objects in reverse-FK order.
-- -----------------------------------------------------------------------------
BEGIN
    FOR r IN (
        SELECT 'DROP TABLE ' || table_name || ' CASCADE CONSTRAINTS PURGE' AS stmt
        FROM user_tables
        WHERE table_name IN (
            'FACT_OEE','FACT_PRODUCTION','FACT_QUALITY','FACT_INVENTORY','FACT_MAINTENANCE',
            'DIM_DATE','DIM_MACHINE','DIM_PRODUCT','DIM_STAGE','DIM_MATERIAL',
            'STG_PRODUCTION_BATCH','STG_QC_INSPECTION','STG_QC_RESULT',
            'STG_MAINTENANCE_LOG','STG_SENSOR_AGG'
        )
    ) LOOP
        EXECUTE IMMEDIATE r.stmt;
    END LOOP;

    FOR r IN (
        SELECT 'DROP SEQUENCE ' || sequence_name AS stmt
        FROM user_sequences
        WHERE sequence_name IN (
            'SEQ_DIM_MACHINE','SEQ_DIM_PRODUCT','SEQ_DIM_STAGE','SEQ_DIM_MATERIAL',
            'SEQ_FACT_OEE','SEQ_FACT_PRODUCTION','SEQ_FACT_QUALITY',
            'SEQ_FACT_INVENTORY','SEQ_FACT_MAINTENANCE'
        )
    ) LOOP
        EXECUTE IMMEDIATE r.stmt;
    END LOOP;
END;
/

-- -----------------------------------------------------------------------------
-- Staging — raw extract buffer. TRUNCATE + INSERT on each ETL run (idempotent).
-- -----------------------------------------------------------------------------

CREATE TABLE STG_PRODUCTION_BATCH (
    batch_id         NUMBER,
    order_id         NUMBER,
    line_id          NUMBER,
    stage_id         NUMBER,
    started_at       TIMESTAMP,
    completed_at     TIMESTAMP,
    qty_produced     NUMBER,
    src_system       VARCHAR2(20)  DEFAULT 'SUPABASE',
    pipeline_run_id  VARCHAR2(100),
    loaded_at        TIMESTAMP DEFAULT SYSTIMESTAMP
)
;

CREATE TABLE STG_QC_INSPECTION (
    qc_id            NUMBER,
    batch_id         NUMBER,
    stage_id         NUMBER,
    sample_qty       NUMBER,
    inspected_at     TIMESTAMP,
    src_system       VARCHAR2(20)  DEFAULT 'SUPABASE',
    pipeline_run_id  VARCHAR2(100),
    loaded_at        TIMESTAMP DEFAULT SYSTIMESTAMP
)
;

CREATE TABLE STG_QC_RESULT (
    result_id        NUMBER,
    qc_id            NUMBER,
    parameter        VARCHAR2(50),
    measured_value   NUMBER,
    spec_min         NUMBER,
    spec_max         NUMBER,
    pass_fail        VARCHAR2(4),
    src_system       VARCHAR2(20)  DEFAULT 'SUPABASE',
    pipeline_run_id  VARCHAR2(100),
    loaded_at        TIMESTAMP DEFAULT SYSTIMESTAMP
)
;

CREATE TABLE STG_MAINTENANCE_LOG (
    log_id           NUMBER,
    machine_id       NUMBER,
    type             VARCHAR2(20),
    started_at       TIMESTAMP,
    ended_at         TIMESTAMP,
    downtime_min     NUMBER,
    issue_code       VARCHAR2(10),
    src_system       VARCHAR2(20)  DEFAULT 'SUPABASE',
    pipeline_run_id  VARCHAR2(100),
    loaded_at        TIMESTAMP DEFAULT SYSTIMESTAMP
)
;

CREATE TABLE STG_SENSOR_AGG (
    machine_id       VARCHAR2(20),
    run_date         DATE,
    avg_temp_c       NUMBER(8,2),
    total_cycles     NUMBER,
    avg_vibration_g  NUMBER(8,4),
    avg_current_a    NUMBER(8,2),
    avg_voltage_v    NUMBER(8,2),
    src_system       VARCHAR2(20)  DEFAULT 'INFLUXDB',
    pipeline_run_id  VARCHAR2(100),
    loaded_at        TIMESTAMP DEFAULT SYSTIMESTAMP
)
;

-- -----------------------------------------------------------------------------
-- Dimensions
-- -----------------------------------------------------------------------------

-- DIM_DATE: surrogate key = YYYYMMDD (no sequence needed). 5 years of rows.
CREATE TABLE DIM_DATE (
    date_id       NUMBER        PRIMARY KEY,
    full_date     DATE          NOT NULL,
    day_of_week   NUMBER,
    week_number   NUMBER,
    month_number  NUMBER,
    month_name    VARCHAR2(20),
    quarter       NUMBER,
    year          NUMBER,
    is_weekend    CHAR(1)       DEFAULT 'N',
    is_holiday    CHAR(1)       DEFAULT 'N'
)
;

CREATE TABLE DIM_MACHINE (
    machine_id       NUMBER        PRIMARY KEY,
    machine_src_id   NUMBER,
    machine_name     VARCHAR2(50),
    machine_type     VARCHAR2(20),
    line_name        VARCHAR2(50),
    ideal_cycle_sec  NUMBER
)
;
CREATE SEQUENCE SEQ_DIM_MACHINE START WITH 1 INCREMENT BY 1 NOCACHE
;

CREATE TABLE DIM_PRODUCT (
    product_id       NUMBER        PRIMARY KEY,
    product_src_id   NUMBER,
    sku              VARCHAR2(30),
    product_name     VARCHAR2(100),
    voltage_v        NUMBER(5,2),
    capacity_ah      NUMBER(6,2)
)
;
CREATE SEQUENCE SEQ_DIM_PRODUCT START WITH 1 INCREMENT BY 1 NOCACHE
;

CREATE TABLE DIM_STAGE (
    stage_id         NUMBER        PRIMARY KEY,
    stage_src_id     NUMBER,
    stage_name       VARCHAR2(50),
    sequence_no      NUMBER,
    machine_name     VARCHAR2(50),
    ideal_cycle_sec  NUMBER
)
;
CREATE SEQUENCE SEQ_DIM_STAGE START WITH 1 INCREMENT BY 1 NOCACHE
;

CREATE TABLE DIM_MATERIAL (
    material_id      NUMBER        PRIMARY KEY,
    material_src_id  NUMBER,
    material_name    VARCHAR2(100),
    material_type    VARCHAR2(50),
    unit             VARCHAR2(20),
    hazard_class     VARCHAR2(20)
)
;
CREATE SEQUENCE SEQ_DIM_MATERIAL START WITH 1 INCREMENT BY 1 NOCACHE
;

-- -----------------------------------------------------------------------------
-- Facts
-- -----------------------------------------------------------------------------

CREATE TABLE FACT_OEE (
    oee_id            NUMBER        PRIMARY KEY,
    date_id           NUMBER        NOT NULL,
    machine_id        NUMBER        NOT NULL,
    product_id        NUMBER,
    planned_time_min  NUMBER,
    actual_run_min    NUMBER,
    downtime_min      NUMBER,
    units_planned     NUMBER,
    units_produced    NUMBER,
    units_good        NUMBER,
    availability_pct  NUMBER(5,2),
    performance_pct   NUMBER(5,2),
    quality_pct       NUMBER(5,2),
    oee_pct           NUMBER(5,2),
    CONSTRAINT fk_oee_date    FOREIGN KEY (date_id)    REFERENCES DIM_DATE(date_id),
    CONSTRAINT fk_oee_machine FOREIGN KEY (machine_id) REFERENCES DIM_MACHINE(machine_id),
    CONSTRAINT fk_oee_product FOREIGN KEY (product_id) REFERENCES DIM_PRODUCT(product_id)
)
;
CREATE SEQUENCE SEQ_FACT_OEE START WITH 1 INCREMENT BY 1 NOCACHE
;

CREATE TABLE FACT_PRODUCTION (
    production_id       NUMBER        PRIMARY KEY,
    date_id             NUMBER        NOT NULL,
    machine_id          NUMBER,
    stage_id            NUMBER,
    product_id          NUMBER,
    units_produced      NUMBER,
    avg_cycle_time_sec  NUMBER,
    batch_duration_min  NUMBER,
    yield_rate          NUMBER(6,4),
    CONSTRAINT fk_prod_date    FOREIGN KEY (date_id)    REFERENCES DIM_DATE(date_id),
    CONSTRAINT fk_prod_machine FOREIGN KEY (machine_id) REFERENCES DIM_MACHINE(machine_id),
    CONSTRAINT fk_prod_stage   FOREIGN KEY (stage_id)   REFERENCES DIM_STAGE(stage_id),
    CONSTRAINT fk_prod_product FOREIGN KEY (product_id) REFERENCES DIM_PRODUCT(product_id)
)
;
CREATE SEQUENCE SEQ_FACT_PRODUCTION START WITH 1 INCREMENT BY 1 NOCACHE
;

CREATE TABLE FACT_QUALITY (
    quality_id        NUMBER        PRIMARY KEY,
    date_id           NUMBER        NOT NULL,
    product_id        NUMBER,
    stage_id          NUMBER,
    samples_taken     NUMBER,
    pass_count        NUMBER,
    fail_count        NUMBER,
    defect_rate_pct   NUMBER(5,2),
    top_defect_param  VARCHAR2(50),
    CONSTRAINT fk_qual_date    FOREIGN KEY (date_id)    REFERENCES DIM_DATE(date_id),
    CONSTRAINT fk_qual_product FOREIGN KEY (product_id) REFERENCES DIM_PRODUCT(product_id),
    CONSTRAINT fk_qual_stage   FOREIGN KEY (stage_id)   REFERENCES DIM_STAGE(stage_id)
)
;
CREATE SEQUENCE SEQ_FACT_QUALITY START WITH 1 INCREMENT BY 1 NOCACHE
;

CREATE TABLE FACT_INVENTORY (
    inventory_id  NUMBER        PRIMARY KEY,
    date_id       NUMBER        NOT NULL,
    material_id   NUMBER,
    qty_opening   NUMBER,
    qty_received  NUMBER,
    qty_consumed  NUMBER,
    qty_closing   NUMBER,
    stock_value   NUMBER,
    CONSTRAINT fk_inv_date     FOREIGN KEY (date_id)     REFERENCES DIM_DATE(date_id),
    CONSTRAINT fk_inv_material FOREIGN KEY (material_id) REFERENCES DIM_MATERIAL(material_id)
)
;
CREATE SEQUENCE SEQ_FACT_INVENTORY START WITH 1 INCREMENT BY 1 NOCACHE
;

CREATE TABLE FACT_MAINTENANCE (
    maintenance_id  NUMBER        PRIMARY KEY,
    date_id         NUMBER        NOT NULL,
    machine_id      NUMBER,
    event_type      VARCHAR2(20),
    downtime_min    NUMBER,
    mtbf_hrs        NUMBER,
    mttr_min        NUMBER,
    issue_code      VARCHAR2(10),
    CONSTRAINT fk_maint_date    FOREIGN KEY (date_id)    REFERENCES DIM_DATE(date_id),
    CONSTRAINT fk_maint_machine FOREIGN KEY (machine_id) REFERENCES DIM_MACHINE(machine_id)
)
;
CREATE SEQUENCE SEQ_FACT_MAINTENANCE START WITH 1 INCREMENT BY 1 NOCACHE
;

-- -----------------------------------------------------------------------------
-- Indexes tuned for reporting queries in CLAUDE.md §10
-- -----------------------------------------------------------------------------

CREATE INDEX idx_oee_date_machine   ON FACT_OEE         (date_id, machine_id)
;
CREATE INDEX idx_prod_date_stage    ON FACT_PRODUCTION  (date_id, stage_id)
;
CREATE INDEX idx_qual_date_stage    ON FACT_QUALITY     (date_id, stage_id)
;
CREATE INDEX idx_maint_machine_date ON FACT_MAINTENANCE (machine_id, date_id)
;
CREATE INDEX idx_inv_date_material  ON FACT_INVENTORY   (date_id, material_id)
;
