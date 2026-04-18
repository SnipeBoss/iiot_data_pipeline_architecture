-- =============================================================================
-- Battery Manufacturing OLTP — Supabase PostgreSQL schema
-- 17 tables across 5 domains. Source of truth for deliverable [A].
-- =============================================================================
-- Apply via Supabase SQL editor or psql once SUPABASE_* env vars are filled in.
-- All tables default to the `public` schema. Run once; re-running will fail
-- on existing objects — use 02_reset.sql (not yet written) if you need to drop.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Domain 1 — Infrastructure
-- -----------------------------------------------------------------------------

CREATE TABLE production_line (
    line_id              SERIAL PRIMARY KEY,
    name                 VARCHAR(50)  NOT NULL,
    area                 VARCHAR(50),
    capacity_batches_hr  INTEGER
);

CREATE TABLE machine (
    machine_id       SERIAL PRIMARY KEY,
    name             VARCHAR(50) NOT NULL,
    type             VARCHAR(20) NOT NULL
        CHECK (type IN ('FURNACE','CUTTER','MILL','PRESS','ASSEMBLER','CHARGER','TESTER')),
    line_id          INTEGER REFERENCES production_line(line_id),
    ideal_cycle_sec  INTEGER NOT NULL,
    status           VARCHAR(20) DEFAULT 'ACTIVE'
        CHECK (status IN ('ACTIVE','INACTIVE','MAINTENANCE','DECOMMISSIONED'))
);

CREATE TABLE process_stage (
    stage_id         SERIAL PRIMARY KEY,
    name             VARCHAR(50) NOT NULL,
    sequence         INTEGER NOT NULL,
    machine_id       INTEGER REFERENCES machine(machine_id),  -- NULL = no sensor
    ideal_cycle_sec  INTEGER
);

CREATE TABLE product (
    product_id   SERIAL PRIMARY KEY,
    sku          VARCHAR(30) UNIQUE NOT NULL,
    name         VARCHAR(100),
    voltage_v    DECIMAL(5,2),
    capacity_ah  DECIMAL(6,2)
);

-- -----------------------------------------------------------------------------
-- Domain 2 — Material Master
-- -----------------------------------------------------------------------------

CREATE TABLE raw_material (
    material_id   SERIAL PRIMARY KEY,
    name          VARCHAR(100) NOT NULL,
    type          VARCHAR(50),
    unit          VARCHAR(20),
    hazard_class  VARCHAR(20)
);

CREATE TABLE bill_of_material (
    bom_id        SERIAL PRIMARY KEY,
    product_id    INTEGER NOT NULL REFERENCES product(product_id),
    material_id   INTEGER NOT NULL REFERENCES raw_material(material_id),
    qty_per_unit  DECIMAL(10,4) NOT NULL CHECK (qty_per_unit > 0),
    unit          VARCHAR(20),
    UNIQUE (product_id, material_id)
);

CREATE TABLE supplier (
    supplier_id     SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,
    contact         VARCHAR(200),
    lead_time_days  INTEGER CHECK (lead_time_days >= 0)
);

-- -----------------------------------------------------------------------------
-- Domain 3 — Procurement & Inventory
-- -----------------------------------------------------------------------------

CREATE TABLE raw_material_po (
    po_id          SERIAL PRIMARY KEY,
    material_id    INTEGER NOT NULL REFERENCES raw_material(material_id),
    supplier_id    INTEGER NOT NULL REFERENCES supplier(supplier_id),
    qty_ordered    DECIMAL(12,3) NOT NULL CHECK (qty_ordered > 0),
    order_date     DATE NOT NULL,
    expected_date  DATE,
    status         VARCHAR(20) DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','CONFIRMED','SHIPPED','RECEIVED','CANCELLED'))
);

CREATE TABLE raw_material_receipt (
    receipt_id     SERIAL PRIMARY KEY,
    po_id          INTEGER NOT NULL REFERENCES raw_material_po(po_id),
    qty_received   DECIMAL(12,3) NOT NULL CHECK (qty_received > 0),
    received_date  DATE NOT NULL
);

CREATE TABLE inventory (
    inventory_id   SERIAL PRIMARY KEY,
    material_id    INTEGER NOT NULL UNIQUE REFERENCES raw_material(material_id),
    qty_on_hand    DECIMAL(12,3) NOT NULL DEFAULT 0 CHECK (qty_on_hand >= 0),
    qty_reserved   DECIMAL(12,3) DEFAULT 0 CHECK (qty_reserved >= 0),
    reorder_level  DECIMAL(12,3),
    warehouse_loc  VARCHAR(50),
    updated_at     TIMESTAMP DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- Domain 4 — Production
-- -----------------------------------------------------------------------------

CREATE TABLE production_order (
    order_id         SERIAL PRIMARY KEY,
    product_id       INTEGER NOT NULL REFERENCES product(product_id),
    qty_ordered      INTEGER NOT NULL CHECK (qty_ordered > 0),
    priority         VARCHAR(10) DEFAULT 'NORMAL'
        CHECK (priority IN ('HIGH','NORMAL','LOW')),
    scheduled_start  DATE,
    scheduled_end    DATE,
    status           VARCHAR(20) DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','IN_PROGRESS','COMPLETED','CANCELLED'))
);

CREATE TABLE production_batch (
    batch_id      SERIAL PRIMARY KEY,
    order_id      INTEGER NOT NULL REFERENCES production_order(order_id),
    line_id       INTEGER REFERENCES production_line(line_id),
    stage_id      INTEGER REFERENCES process_stage(stage_id),
    started_at    TIMESTAMP NOT NULL,
    completed_at  TIMESTAMP,
    qty_produced  INTEGER DEFAULT 0 CHECK (qty_produced >= 0),
    CHECK (completed_at IS NULL OR completed_at >= started_at)
);

CREATE TABLE finished_good (
    fg_id        SERIAL PRIMARY KEY,
    batch_id     INTEGER NOT NULL REFERENCES production_batch(batch_id),
    serial_no    VARCHAR(50) UNIQUE NOT NULL,
    produced_at  TIMESTAMP DEFAULT NOW(),
    qc_status    VARCHAR(20) DEFAULT 'PENDING'
        CHECK (qc_status IN ('PENDING','PASS','FAIL','QUARANTINE'))
);

CREATE TABLE material_consumption (
    consumption_id  SERIAL PRIMARY KEY,
    batch_id        INTEGER NOT NULL REFERENCES production_batch(batch_id),
    material_id     INTEGER NOT NULL REFERENCES raw_material(material_id),
    qty_used        DECIMAL(12,4) NOT NULL CHECK (qty_used > 0),
    consumed_at     TIMESTAMP DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- Domain 5 — Quality & Maintenance
-- -----------------------------------------------------------------------------

CREATE TABLE qc_inspection (
    qc_id         SERIAL PRIMARY KEY,
    batch_id      INTEGER NOT NULL REFERENCES production_batch(batch_id),
    stage_id      INTEGER REFERENCES process_stage(stage_id),
    sample_qty    INTEGER NOT NULL CHECK (sample_qty > 0),
    inspected_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE qc_result (
    result_id       SERIAL PRIMARY KEY,
    qc_id           INTEGER NOT NULL REFERENCES qc_inspection(qc_id),
    parameter       VARCHAR(50),
    measured_value  DECIMAL(12,4),
    spec_min        DECIMAL(12,4),
    spec_max        DECIMAL(12,4),
    pass_fail       VARCHAR(4) CHECK (pass_fail IN ('PASS','FAIL'))
);

CREATE TABLE maintenance_log (
    log_id        SERIAL PRIMARY KEY,
    machine_id    INTEGER NOT NULL REFERENCES machine(machine_id),
    type          VARCHAR(20) NOT NULL
        CHECK (type IN ('BREAKDOWN','PREVENTIVE','CHANGEOVER')),
    started_at    TIMESTAMP NOT NULL,
    ended_at      TIMESTAMP,
    downtime_min  INTEGER CHECK (downtime_min >= 0),
    issue_code    VARCHAR(10),
    CHECK (ended_at IS NULL OR ended_at >= started_at)
);

-- -----------------------------------------------------------------------------
-- Indexes for ETL extract queries
-- -----------------------------------------------------------------------------

CREATE INDEX idx_batch_completed ON production_batch(completed_at);
CREATE INDEX idx_batch_stage     ON production_batch(stage_id);
CREATE INDEX idx_qc_inspected    ON qc_inspection(inspected_at);
CREATE INDEX idx_qc_result_qc    ON qc_result(qc_id);
CREATE INDEX idx_maint_machine   ON maintenance_log(machine_id, started_at);
CREATE INDEX idx_fg_batch        ON finished_good(batch_id);
CREATE INDEX idx_mc_batch        ON material_consumption(batch_id);
