-- =================================================================================
-- 1. STG_PRODUCTION_BATCH
-- ข้อมูลสรุปการผลิตราย Batch ที่รวม (Join) ข้อมูลแผนการผลิตจาก Supabase ไว้ด้วยกัน
-- ใช้สำหรับวิเคราะห์ความคืบหน้าการผลิตเทียบกับแผน (Planned vs Actual)
-- =================================================================================
CREATE TABLE STG_PRODUCTION_BATCH (
    -- Source business keys
    batch_id             NUMBER       NOT NULL,
    order_id             NUMBER       NOT NULL,
    line_id              NUMBER       NOT NULL,
    model_id             NUMBER       NOT NULL,
    
    -- Source measures
    qty_planned          NUMBER       NOT NULL,
    qty_out              NUMBER,
    start_time           TIMESTAMP,
    end_time             TIMESTAMP,
    
    -- Planning (joined from production_order)
    order_planned_start  TIMESTAMP,
    order_planned_end    TIMESTAMP,

    -- sum(qty_planned) per order
    order_total_qty      NUMBER,                         
    
    -- Lineage
    src_system           VARCHAR2(20) NOT NULL,
    pipeline_run_id      VARCHAR2(50) NOT NULL,
    loaded_at            TIMESTAMP    DEFAULT SYSTIMESTAMP NOT NULL
);


-- =================================================================================
-- 2. STG_QC_RECORD
-- ข้อมูลผลการตรวจสอบคุณภาพ (Quality Control) ราย Batch
-- เก็บจำนวนที่ตรวจ (Inspected), ผ่าน (Passed) และไม่ผ่าน (Failed)
-- =================================================================================
CREATE TABLE STG_QC_RECORD (
    qc_id                NUMBER       NOT NULL,
    batch_id             NUMBER       NOT NULL,
    qty_inspected        NUMBER       NOT NULL,
    qty_passed           NUMBER       NOT NULL,
    qty_failed           NUMBER       NOT NULL,
    inspected_at         TIMESTAMP    NOT NULL,
    
    src_system           VARCHAR2(20) NOT NULL,
    pipeline_run_id      VARCHAR2(50) NOT NULL,
    loaded_at            TIMESTAMP    DEFAULT SYSTIMESTAMP NOT NULL
);


-- =================================================================================
-- 3. STG_QC_DEFECT
-- ตารางรายละเอียดของเสีย (Defect Details) ในลักษณะ Many-to-Many
-- ระบุรหัสของเสีย (Defect Code) และจำนวนที่พบ เพื่อวิเคราะห์ Pareto ของปัญหา
-- =================================================================================
CREATE TABLE STG_QC_DEFECT (
    qc_id                NUMBER       NOT NULL,
    defect_code          VARCHAR2(30) NOT NULL,
    qty_affected         NUMBER       NOT NULL,
    
    src_system           VARCHAR2(20) NOT NULL,
    pipeline_run_id      VARCHAR2(50) NOT NULL,
    loaded_at            TIMESTAMP    DEFAULT SYSTIMESTAMP NOT NULL
);


-- =================================================================================
-- 4. STG_DOWNTIME_EVENT
-- บันทึกเหตุการณ์ที่เครื่องจักรหยุดทำงาน (Downtime) 
-- มีการแยกประเภท Planned/Unplanned และคำนวณระยะเวลา (Duration) มาจาก Source แล้ว
-- =================================================================================
CREATE TABLE STG_DOWNTIME_EVENT (
    event_id             NUMBER       NOT NULL,
    machine_id           NUMBER       NOT NULL,          
    machine_code         VARCHAR2(20) NOT NULL,          
    line_id              NUMBER       NOT NULL,
    batch_id             NUMBER,                         
    reason_code          VARCHAR2(30) NOT NULL,
    is_planned           CHAR(1)      NOT NULL,
    start_ts             TIMESTAMP    NOT NULL,
    end_ts               TIMESTAMP    NOT NULL,
    duration_min         NUMBER(8,2)  NOT NULL,          
    
    src_system           VARCHAR2(20) NOT NULL,
    pipeline_run_id      VARCHAR2(50) NOT NULL,
    loaded_at            TIMESTAMP    DEFAULT SYSTIMESTAMP NOT NULL
);


-- =================================================================================
-- 5. STG_SENSOR_AGG
-- ข้อมูลสรุปสถิติจาก Sensor (IoT) ที่ดึงมาจาก InfluxDB
-- เก็บค่า Avg, Min, Max ในแต่ละช่วงเวลา (Window) เพื่อลดปริมาณ Data Load
-- =================================================================================
CREATE TABLE STG_SENSOR_AGG (
    machine_code         VARCHAR2(20) NOT NULL,          
    metric_name          VARCHAR2(50) NOT NULL,          
    window_start         TIMESTAMP    NOT NULL,
    window_end           TIMESTAMP    NOT NULL,
    avg_value            NUMBER(12,4),
    min_value            NUMBER(12,4),
    max_value            NUMBER(12,4),
    sample_count         NUMBER(6),
    
    src_system           VARCHAR2(20) DEFAULT 'INFLUXDB' NOT NULL,
    pipeline_run_id      VARCHAR2(50) NOT NULL,
    loaded_at            TIMESTAMP    DEFAULT SYSTIMESTAMP NOT NULL
);


-- =================================================================================
-- 6. STG_LINE
-- ข้อมูลมาสเตอร์ของสายการผลิต (Production Line) แบ่งตามโซนพื้นที่ทำงาน
-- =================================================================================
CREATE TABLE STG_LINE (
    line_id              NUMBER       NOT NULL,          
    name                 VARCHAR2(50) NOT NULL,
    area                 VARCHAR2(50),
    
    src_system           VARCHAR2(20) NOT NULL,
    pipeline_run_id      VARCHAR2(50) NOT NULL,
    loaded_at            TIMESTAMP    DEFAULT SYSTIMESTAMP NOT NULL
);


-- =================================================================================
-- 7. STG_BATTERY_MODEL
-- ข้อมูลสเปกและรายละเอียดทางเทคนิคของแบตเตอรี่แต่ละรุ่น
-- ใช้เป็นค่าอ้างอิง (Reference) ในการคำนวณมาตรฐานการผลิต
-- =================================================================================
CREATE TABLE STG_BATTERY_MODEL (
    model_id             NUMBER       NOT NULL,          
    model_code           VARCHAR2(20) NOT NULL,
    name                 VARCHAR2(100) NOT NULL,
    spec_plate_count     NUMBER,
    spec_weight_kg       NUMBER(5,2),
    spec_terminal_type   VARCHAR2(10),
    dim_length_mm        NUMBER(6,1),
    dim_width_mm         NUMBER(6,1),
    dim_height_mm        NUMBER(6,1),
    is_active            CHAR(1)      NOT NULL,
    
    src_system           VARCHAR2(20) NOT NULL,
    pipeline_run_id      VARCHAR2(50) NOT NULL,
    loaded_at            TIMESTAMP    DEFAULT SYSTIMESTAMP NOT NULL
);


-- =================================================================================
-- 8. STG_MACHINE
-- ข้อมูลมาสเตอร์ของเครื่องจักรและลำดับการติดตั้งใน Production Line
-- ใช้เชื่อมโยงเหตุการณ์ Downtime และ Sensor Data เข้ากับสายการผลิต
-- =================================================================================
CREATE TABLE STG_MACHINE (
    machine_id           NUMBER       NOT NULL,          
    line_id              NUMBER       NOT NULL,          
    machine_code         VARCHAR2(20) NOT NULL,
    machine_type         VARCHAR2(30) NOT NULL,
    sequence_position    NUMBER       NOT NULL,
    install_date         DATE,
    is_active            CHAR(1)      NOT NULL,
    
    src_system           VARCHAR2(20) NOT NULL,
    pipeline_run_id      VARCHAR2(50) NOT NULL,
    loaded_at            TIMESTAMP    DEFAULT SYSTIMESTAMP NOT NULL
);