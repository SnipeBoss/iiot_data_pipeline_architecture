-- 1. FACT_PRODUCTION — grain: 1 batch
CREATE TABLE FACT_PRODUCTION (

    -- Surrogate
    prod_id              NUMBER       PRIMARY KEY,
    
    -- Conformed DIM FKs
    date_id              NUMBER       NOT NULL REFERENCES DIM_DATE(date_id),
    line_id              NUMBER       NOT NULL REFERENCES DIM_LINE(line_id),
    shift_id             NUMBER       NOT NULL REFERENCES DIM_SHIFT(shift_id),
    model_id             NUMBER       NOT NULL REFERENCES DIM_BATTERY_MODEL(model_id),
    
    -- Degenerate dims (business keys, no DIM table)
    batch_src_id         NUMBER       NOT NULL,
    order_src_id         NUMBER       NOT NULL,
    
    -- Additive measures
    qty_planned          NUMBER       NOT NULL,
    qty_out              NUMBER       NOT NULL,
    duration_min         NUMBER(8,2),
    
    -- Semi-additive (precomputed, do not SUM across) -> qty_out / qty_planned
    yield_rate           NUMBER(5,4),                    
    
    -- Schedule adherence (Page 3)
    order_planned_start  TIMESTAMP,
    order_planned_end    TIMESTAMP,

    -- derived FIFO split
    batch_planned_start  TIMESTAMP,                      
    batch_planned_end    TIMESTAMP,
    batch_est_duration_min NUMBER(8,2),

    -- actual - planned
    slippage_min         NUMBER(8,2),                    
    
    -- Actual times
    start_time           TIMESTAMP,
    end_time             TIMESTAMP,
    
    -- Lineage
    loaded_at            TIMESTAMP    DEFAULT SYSTIMESTAMP NOT NULL
);
CREATE SEQUENCE SEQ_FACT_PRODUCTION 
START WITH 1 INCREMENT BY 1 NOCACHE;



-- 2. FACT_QUALITY — grain: 1 QC inspection (1:1 with batch)
CREATE TABLE FACT_QUALITY (
    quality_id           NUMBER       PRIMARY KEY,
    
    date_id              NUMBER       NOT NULL REFERENCES DIM_DATE(date_id),
    line_id              NUMBER       NOT NULL REFERENCES DIM_LINE(line_id),
    shift_id             NUMBER       NOT NULL REFERENCES DIM_SHIFT(shift_id),
    model_id             NUMBER       NOT NULL REFERENCES DIM_BATTERY_MODEL(model_id),
    
    -- Degenerate dims
    qc_src_id            NUMBER       NOT NULL,
    batch_src_id         NUMBER       NOT NULL,
    
    -- Additive
    qty_inspected        NUMBER       NOT NULL,
    qty_passed           NUMBER       NOT NULL,
    qty_failed           NUMBER       NOT NULL,
    
    -- Semi-additive
    -- qty_failed/qty_inspected*100
    defect_rate_pct      NUMBER(5,2),                    
    
    inspected_at         TIMESTAMP    NOT NULL,
    loaded_at            TIMESTAMP    DEFAULT SYSTIMESTAMP NOT NULL
);
CREATE SEQUENCE SEQ_FACT_QUALITY 
START WITH 1 INCREMENT BY 1 NOCACHE;



-- 3. FACT_DEFECT — grain: 1 defect type per QC (M:N junction)
CREATE TABLE FACT_DEFECT (
    defect_fact_id       NUMBER       PRIMARY KEY,
    
    date_id              NUMBER       NOT NULL REFERENCES DIM_DATE(date_id),
    line_id              NUMBER       NOT NULL REFERENCES DIM_LINE(line_id),
    model_id             NUMBER       NOT NULL REFERENCES DIM_BATTERY_MODEL(model_id),
    defect_id            NUMBER       NOT NULL REFERENCES DIM_DEFECT_TYPE(defect_id),
    
    -- Degenerate dims
    qc_src_id            NUMBER       NOT NULL,
    batch_src_id         NUMBER       NOT NULL,
    
    -- Additive
    qty_affected         NUMBER       NOT NULL,
    
    loaded_at            TIMESTAMP    DEFAULT SYSTIMESTAMP NOT NULL
);
CREATE SEQUENCE SEQ_FACT_DEFECT 
START WITH 1 INCREMENT BY 1 NOCACHE;



-- 4. FACT_DOWNTIME — grain: 1 downtime event per machine
CREATE TABLE FACT_DOWNTIME (
    downtime_id          NUMBER       PRIMARY KEY,
    
    date_id              NUMBER       NOT NULL REFERENCES DIM_DATE(date_id),
    line_id              NUMBER       NOT NULL REFERENCES DIM_LINE(line_id),
    shift_id             NUMBER       NOT NULL REFERENCES DIM_SHIFT(shift_id),
    machine_id           NUMBER       NOT NULL REFERENCES DIM_MACHINE(machine_id),
    
    -- Degenerate dims
    event_src_id         NUMBER       NOT NULL,
    batch_src_id         NUMBER,                         -- nullable: PM after-hours
    reason_code          VARCHAR2(30) NOT NULL,          -- degenerate (no DIM_REASON)
    is_planned           CHAR(1)      NOT NULL CHECK (is_planned IN ('Y','N')),
    
    -- Additive
    duration_min         NUMBER(8,2)  NOT NULL,
    
    start_ts             TIMESTAMP    NOT NULL,
    end_ts               TIMESTAMP    NOT NULL,
    loaded_at            TIMESTAMP    DEFAULT SYSTIMESTAMP NOT NULL
);
CREATE SEQUENCE SEQ_FACT_DOWNTIME 
START WITH 1 INCREMENT BY 1 NOCACHE;



-- 5. FACT_SENSOR — grain: 1 (machine × metric × 15-min window)
CREATE TABLE FACT_SENSOR (
    sensor_id            NUMBER       PRIMARY KEY,
    
    date_id              NUMBER       NOT NULL REFERENCES DIM_DATE(date_id),
    machine_id           NUMBER       NOT NULL REFERENCES DIM_MACHINE(machine_id),
    metric_id            NUMBER       NOT NULL REFERENCES DIM_METRIC(metric_id),
    
    window_start         TIMESTAMP    NOT NULL,
    window_end           TIMESTAMP    NOT NULL,
    
    -- Semi-additive (avg/min/max over time, NOT additive across metrics)
    avg_value            NUMBER(12,4),
    min_value            NUMBER(12,4),
    max_value            NUMBER(12,4),
    
    -- Additive (for sample completeness check)
    sample_count         NUMBER(6),                      -- ~900 expected for 1Hz
    
    loaded_at            TIMESTAMP    DEFAULT SYSTIMESTAMP NOT NULL
);
CREATE SEQUENCE SEQ_FACT_SENSOR 
START WITH 1 INCREMENT BY 1 NOCACHE;