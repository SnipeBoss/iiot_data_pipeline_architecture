-- =============================================================================
-- OLTP Schema (Supabase PostgreSQL) — 6 ตาราง ตาม NEW_ARCHITECTURE.md
-- =============================================================================
-- โดเมน: Battery Process Performance POC (1 line, 3 machines: M01/M02/M03)
--
-- ลบ (เทียบกับ schema เก่า):
--   - material/BOM/supplier/procurement/inventory (Domain 2-3)
--   - process_stage (batch 1 ตัว = run ตลอดทั้ง line ไม่แยก stage)
--   - finished_good / material_consumption
--   - qc_inspection + qc_result → รวมเป็น qc_record เดียว
--   - maintenance_log (ใช้ machine_state_num จาก IIoT แทน)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. production_line — สายการผลิต (POC มีสายเดียว)
-- -----------------------------------------------------------------------------
CREATE TABLE production_line (
    line_id  SERIAL PRIMARY KEY,
    name     VARCHAR(50) NOT NULL,
    area     VARCHAR(50)
);


-- -----------------------------------------------------------------------------
-- 2. machine — เครื่องจักร 3 ตัว; name ต้องตรงกับ tag ใน InfluxDB (M01/M02/M03)
-- -----------------------------------------------------------------------------
CREATE TABLE machine (
    machine_id  SERIAL PRIMARY KEY,
    name        VARCHAR(10) NOT NULL UNIQUE,  -- "M01" / "M02" / "M03"
    line_id     INTEGER REFERENCES production_line(line_id)
);


-- -----------------------------------------------------------------------------
-- 3. product — รุ่น battery ที่ผลิต
-- -----------------------------------------------------------------------------
CREATE TABLE product (
    product_id  SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL
);


-- -----------------------------------------------------------------------------
-- 4. production_order — Process Engineer สร้างก่อน (= แผน)
-- -----------------------------------------------------------------------------
CREATE TABLE production_order (
    order_id       SERIAL PRIMARY KEY,
    line_id        INTEGER   REFERENCES production_line(line_id),
    product_id     INTEGER   REFERENCES product(product_id),
    qty_ordered    INTEGER   NOT NULL CHECK (qty_ordered > 0),
    planned_start  TIMESTAMP NOT NULL,
    planned_end    TIMESTAMP NOT NULL,
    CHECK (planned_end >= planned_start)
);


-- -----------------------------------------------------------------------------
-- 5. production_batch — Operator สร้างตอนรัน (= execution)
-- 1 order มีได้หลาย batch (กรณีหยุดกลางคัน แล้ว split)
-- start_time = เข้า M01 ; end_time = ออก M03 (ใช้ map กับ InfluxDB window)
-- -----------------------------------------------------------------------------
CREATE TABLE production_batch (
    batch_id    SERIAL PRIMARY KEY,
    order_id    INTEGER   REFERENCES production_order(order_id),
    qty_planned INTEGER   NOT NULL CHECK (qty_planned > 0),
    qty_out     INTEGER   CHECK (qty_out IS NULL OR qty_out >= 0),
    start_time  TIMESTAMP NOT NULL,
    end_time    TIMESTAMP,
    CHECK (end_time IS NULL OR end_time >= start_time)
);


-- -----------------------------------------------------------------------------
-- 6. qc_record — QC ตรวจที่ end of line (1 batch = 1 qc_record)
-- QC สุ่ม 5% ของ qty_out แล้ว pass/fail
-- -----------------------------------------------------------------------------
CREATE TABLE qc_record (
    qc_id        SERIAL PRIMARY KEY,
    batch_id     INTEGER   REFERENCES production_batch(batch_id) UNIQUE,
    qty_sampled  INTEGER   NOT NULL CHECK (qty_sampled > 0),
    qty_passed   INTEGER   NOT NULL CHECK (qty_passed >= 0),
    qty_failed   INTEGER   NOT NULL CHECK (qty_failed >= 0),
    inspected_at TIMESTAMP DEFAULT NOW(),
    CHECK (qty_passed + qty_failed = qty_sampled)
);


-- -----------------------------------------------------------------------------
-- Index — ปรับตาม ETL extract pattern (filter by end_time / inspected_at + FK join)
-- -----------------------------------------------------------------------------
CREATE INDEX idx_batch_end_time   ON production_batch(end_time);
CREATE INDEX idx_batch_start_time ON production_batch(start_time);
CREATE INDEX idx_batch_order      ON production_batch(order_id);
CREATE INDEX idx_qc_batch         ON qc_record(batch_id);
CREATE INDEX idx_qc_inspected     ON qc_record(inspected_at);
