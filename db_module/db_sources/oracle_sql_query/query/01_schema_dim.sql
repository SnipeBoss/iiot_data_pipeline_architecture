-- 1. DIM_DATE — calendar (smart key YYYYMMDD, no sequence)
CREATE TABLE DIM_DATE (
    date_id           NUMBER       PRIMARY KEY,
    full_date         DATE         NOT NULL,
    year              NUMBER(4)    NOT NULL,
    quarter           NUMBER(1)    NOT NULL,
    month_number      NUMBER(2)    NOT NULL,
    month_name        VARCHAR2(10) NOT NULL,
    week_number       NUMBER(2)    NOT NULL,
    day_of_month      NUMBER(2)    NOT NULL,
    day_of_week       NUMBER(1)    NOT NULL,
    day_name          VARCHAR2(10) NOT NULL,
    is_weekend        CHAR(1)      DEFAULT 'N' NOT NULL CHECK (is_weekend IN ('Y','N')),
    is_holiday        CHAR(1)      DEFAULT 'N' NOT NULL CHECK (is_holiday IN ('Y','N'))
);


-- 2. DIM_LINE — production lines (conformed, future-extensible)
CREATE TABLE DIM_LINE (
    line_id           NUMBER       PRIMARY KEY,
    line_src_id       NUMBER       NOT NULL UNIQUE,
    line_code         VARCHAR2(10) NOT NULL,
    line_name         VARCHAR2(50) NOT NULL,
    area              VARCHAR2(50),
    process_type      VARCHAR2(30),
    is_active         CHAR(1)      DEFAULT 'Y' NOT NULL CHECK (is_active IN ('Y','N'))
);


-- Create the sequence for running
CREATE SEQUENCE SEQ_DIM_LINE 
START WITH 1 INCREMENT BY 1 NOCACHE;


-- 3. DIM_SHIFT —  (DAY/NIGHT, seeded inline)
CREATE TABLE DIM_SHIFT (
    shift_id          NUMBER       PRIMARY KEY,
    shift_code        VARCHAR2(10) NOT NULL UNIQUE,
    shift_name        VARCHAR2(20) NOT NULL,
    start_hour        NUMBER(2)    NOT NULL,
    start_minute      NUMBER(2)    NOT NULL,
    end_hour          NUMBER(2)    NOT NULL,
    end_minute        NUMBER(2)    NOT NULL,
    crosses_midnight  CHAR(1)      NOT NULL CHECK (crosses_midnight IN ('Y','N'))
);


-- 4. DIM_BATTERY_MODEL — products (conformed, denormalized)
CREATE TABLE DIM_BATTERY_MODEL (
    model_id            NUMBER       PRIMARY KEY,
    model_src_id        NUMBER       NOT NULL UNIQUE,
    model_code          VARCHAR2(20) NOT NULL,
    model_name          VARCHAR2(100) NOT NULL,
    spec_plate_count    NUMBER,
    spec_weight_kg      NUMBER(5,2),
    spec_terminal_type  VARCHAR2(10),
    dim_length_mm       NUMBER(6,1),
    dim_width_mm        NUMBER(6,1),
    dim_height_mm       NUMBER(6,1),
    capacity_class      VARCHAR2(20),
    chemistry           VARCHAR2(20),
    is_active           CHAR(1)      DEFAULT 'Y' NOT NULL CHECK (is_active IN ('Y','N'))
);

-- For Running Number
CREATE SEQUENCE SEQ_DIM_BATTERY_MODEL 
START WITH 1 INCREMENT BY 1 NOCACHE;



-- 5. DIM_MACHINE — equipment (denormalized line attributes)
CREATE TABLE DIM_MACHINE (
    machine_id          NUMBER       PRIMARY KEY,
    machine_src_id      NUMBER       NOT NULL UNIQUE,

    -- M01/M02/M03 — match Influx tag
    machine_code        VARCHAR2(20) NOT NULL UNIQUE,    
    machine_type        VARCHAR2(30) NOT NULL,
    sequence_position   NUMBER       NOT NULL,
    line_id             NUMBER       NOT NULL REFERENCES DIM_LINE(line_id),
    line_name           VARCHAR2(50) NOT NULL,
    install_date        DATE,
    is_active           CHAR(1)      DEFAULT 'Y' NOT NULL CHECK (is_active IN ('Y','N'))
);

-- For Running Number
CREATE SEQUENCE SEQ_DIM_MACHINE 
START WITH 1 INCREMENT BY 1 NOCACHE;



-- 6. DIM_METRIC — sensor metric definitions (seeded inline, no sequence)
CREATE TABLE DIM_METRIC (
    metric_id           NUMBER       PRIMARY KEY,

    -- match Influx field exactly
    metric_name         VARCHAR2(50) NOT NULL UNIQUE,    
    unit                VARCHAR2(20),

    -- NULL = all machines
    machine_code        VARCHAR2(20),                    
    normal_min          NUMBER(12,4),
    normal_max          NUMBER(12,4),
    critical_threshold  NUMBER(12,4),
    description         VARCHAR2(200)
);


-- 7. DIM_DEFECT_TYPE — recursive hierarchy (denormalized parent_code)
CREATE TABLE DIM_DEFECT_TYPE (
    defect_id           NUMBER       PRIMARY KEY,
    defect_code         VARCHAR2(30) NOT NULL UNIQUE,
    parent_defect_id    NUMBER       REFERENCES DIM_DEFECT_TYPE(defect_id),

    -- denormalized for query
    parent_code         VARCHAR2(30),                    
    hierarchy_level     NUMBER(1)    NOT NULL,
    is_leaf             CHAR(1)      NOT NULL CHECK (is_leaf IN ('Y','N')),
    description         VARCHAR2(200) NOT NULL,
    severity            NUMBER(1)    CHECK (severity BETWEEN 1 AND 5),

    -- ROOT/LEAF
    category            VARCHAR2(20)                     
);

-- Running Number
CREATE SEQUENCE SEQ_DIM_DEFECT_TYPE 
START WITH 1 INCREMENT BY 1 NOCACHE;