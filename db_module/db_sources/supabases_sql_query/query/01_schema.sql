-- ============================================================
-- Line COS Battery Assembly MES — OLTP Schema
-- 12 tables, PostgreSQL
-- Naming convention: FK ใช้ชื่อเดียวกับ PK ของ parent
-- ============================================================

-- 1. production_line
CREATE TABLE production_line (
    line_id     SERIAL PRIMARY KEY,
    name        VARCHAR(50) NOT NULL,
    area        VARCHAR(50)
);

-- 2. machine
CREATE TABLE machine (
    machine_id          SERIAL PRIMARY KEY,
    line_id             INT NOT NULL REFERENCES production_line(line_id),
    machine_code        VARCHAR(20) NOT NULL UNIQUE,
    machine_type        VARCHAR(30) NOT NULL,
    sequence_position   INT NOT NULL,
    install_date        DATE
);

-- 3. battery_model
CREATE TABLE battery_model (
    model_id            SERIAL PRIMARY KEY,
    model_code          VARCHAR(20) NOT NULL UNIQUE,
    name                VARCHAR(100) NOT NULL,
    spec_plate_count    INT,
    spec_weight_kg      NUMERIC(5,2),
    spec_terminal_type  VARCHAR(10),
    casing_part_no      VARCHAR(20),
    cover_part_no       VARCHAR(20),
    dim_length_mm       NUMERIC(6,1),
    dim_width_mm        NUMERIC(6,1),
    dim_height_mm       NUMERIC(6,1),
    is_active           CHAR(1) NOT NULL DEFAULT 'Y' CHECK (is_active IN ('Y','N'))
);

-- 4. defect_type (recursive hierarchy)
CREATE TABLE defect_type (
    defect_code     VARCHAR(30) PRIMARY KEY,
    parent_code     VARCHAR(30) REFERENCES defect_type(defect_code),
    description     VARCHAR(200) NOT NULL,
    severity        INT CHECK (severity BETWEEN 1 AND 5),
    category        VARCHAR(20),
    CONSTRAINT chk_no_self_parent CHECK (parent_code IS NULL OR parent_code <> defect_code)
);

-- 5. batch_status (state machine lookup)
CREATE TABLE batch_status (
    status_code     VARCHAR(20) PRIMARY KEY,
    description     VARCHAR(100),
    is_finished     CHAR(1) NOT NULL DEFAULT 'N' CHECK (is_finished IN ('Y','N'))
);

-- 6. event_reason (shared lookup)
CREATE TABLE event_reason (
    reason_code     VARCHAR(30) PRIMARY KEY,
    description     VARCHAR(200) NOT NULL,
    category        VARCHAR(30) NOT NULL,
    is_planned      CHAR(1) NOT NULL DEFAULT 'N' CHECK (is_planned IN ('Y','N'))
);

-- 7. production_order
CREATE TABLE production_order (
    order_id        SERIAL PRIMARY KEY,
    model_id        INT NOT NULL REFERENCES battery_model(model_id),
    qty_ordered     INT NOT NULL CHECK (qty_ordered > 0),
    planned_start   TIMESTAMP NOT NULL,
    planned_end     TIMESTAMP NOT NULL,
    CONSTRAINT chk_planned_window CHECK (planned_end > planned_start)
);

-- 8. production_batch
CREATE TABLE production_batch (
    batch_id        SERIAL PRIMARY KEY,
    order_id        INT NOT NULL REFERENCES production_order(order_id),
    line_id         INT NOT NULL REFERENCES production_line(line_id),
    status_code     VARCHAR(20) NOT NULL REFERENCES batch_status(status_code),
    qty_planned     INT NOT NULL CHECK (qty_planned > 0),
    qty_out         INT DEFAULT 0 CHECK (qty_out >= 0),
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    CONSTRAINT chk_time_logic CHECK (end_time IS NULL OR end_time > start_time)
);

-- 9. batch_status_event (event log, append-only)
CREATE TABLE batch_status_event (
    event_id        SERIAL PRIMARY KEY,
    batch_id        INT NOT NULL REFERENCES production_batch(batch_id),
    status_code     VARCHAR(20) NOT NULL REFERENCES batch_status(status_code),
    reason_code     VARCHAR(30) REFERENCES event_reason(reason_code),
    event_ts        TIMESTAMP NOT NULL,
    notes           TEXT,
    CONSTRAINT uq_batch_event_ts UNIQUE (batch_id, event_ts)
);

-- 10. qc_record
CREATE TABLE qc_record (
    qc_id           SERIAL PRIMARY KEY,
    batch_id        INT NOT NULL REFERENCES production_batch(batch_id),
    qty_inspected   INT NOT NULL CHECK (qty_inspected > 0),
    qty_passed      INT NOT NULL CHECK (qty_passed >= 0),
    qty_failed      INT NOT NULL CHECK (qty_failed >= 0),
    inspected_at    TIMESTAMP NOT NULL,
    CONSTRAINT chk_qc_total CHECK (qty_passed + qty_failed = qty_inspected)
);


-- 11. qc_defect (M:N junction with attribute)
CREATE TABLE qc_defect (
    qc_id           INT NOT NULL REFERENCES qc_record(qc_id),
    defect_code     VARCHAR(30) NOT NULL REFERENCES defect_type(defect_code),
    qty_affected    INT NOT NULL CHECK (qty_affected > 0),
    notes           TEXT,
    PRIMARY KEY (qc_id, defect_code)
);


-- 12. downtime_event
CREATE TABLE downtime_event (
    event_id        SERIAL PRIMARY KEY,
    machine_id      INT NOT NULL REFERENCES machine(machine_id),
    batch_id        INT REFERENCES production_batch(batch_id),
    reason_code     VARCHAR(30) NOT NULL REFERENCES event_reason(reason_code),
    start_ts        TIMESTAMP NOT NULL,
    end_ts          TIMESTAMP,
    duration_min    NUMERIC(8,2),
    CONSTRAINT chk_downtime_window CHECK (end_ts IS NULL OR end_ts > start_ts)
);





-- ============================================================
-- Indexes
-- ============================================================
CREATE INDEX idx_machine_line          ON machine(line_id);
CREATE INDEX idx_order_model           ON production_order(model_id);
CREATE INDEX idx_batch_order           ON production_batch(order_id);
CREATE INDEX idx_batch_line            ON production_batch(line_id);
CREATE INDEX idx_batch_status_code     ON production_batch(status_code);
CREATE INDEX idx_event_batch_ts        ON batch_status_event(batch_id, event_ts);
CREATE INDEX idx_event_reason          ON batch_status_event(reason_code);
CREATE INDEX idx_qc_batch              ON qc_record(batch_id);
CREATE INDEX idx_qc_inspected_at       ON qc_record(inspected_at);
CREATE INDEX idx_defect_parent         ON defect_type(parent_code);
CREATE INDEX idx_downtime_machine      ON downtime_event(machine_id);
CREATE INDEX idx_downtime_start        ON downtime_event(start_ts);
CREATE INDEX idx_downtime_reason       ON downtime_event(reason_code);
