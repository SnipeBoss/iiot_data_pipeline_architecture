# From OEE (Over Engineer) to Process Performances Dashboard and Data Egnineer Architecture


## Problems Righe Now -> Over Engineering / Spagetthi Code (Developer Cant work from this)
ปัจจุบัน Architecture ใหญ่เกินไป เราต้องมีการปรับเปลี่ยนโครงสร้างทั้งหมด
แก้ไขยังไง : 
- ตัดส่วนของการเบิกของ หรือ Material ออกไปให้หมด และ Follwing ตาม Business Requirements ใหม่
- ลดความเป็น Spagethi Code


## Techstack
| Layer | Tech | Role |
|---|---|---|
| OLTP | Supabase (PostgreSQL) | Production orders, batches, QC |
| Streaming | NodeRED → InfluxDB 2.0 (AWS) | 1 Hz sensor simulation (1min interval) |
| Orchestration | Apache Airflow 2.8 (local Docker) | 3 DAGs: Supabase extract, Influx aggregate, SP chain |
| Serving layer | FastAPI (local uvicorn) | HTTP wrapper around Oracle 10g JDBC |
| Data warehouse | Oracle 10.2.0.3 @ KMITL (`AI03` schema) | 5 STG + 5 DIM + 5 FACT tables |
| Dashboard | Streamlit | Date picker, KPIs, per-machine performances |


ฺ# Business Requirements : 

## What Business Needed? (RQ)
Our Role : SI Company 
Scenarios : บริษัทนึงต้องการทำระบบในการวัดผล Process ทั้งโรงงาน โดยอยากให้เราเริ่มที่ Process เดียวก่อนเพื่อทำ POC ทั้งระบบ โดยไม่ใหญ่มากและเหมาะกับระยะเวลาสั้น ๆ (ห้าม Overengineering และเกิน Scope) แล้วจากนั้นหากผ่านจะให้มีการไปพัฒนาต่อที่ Process อื่น ๆ

Actor :
1. Process Engineer ดูแลเกี่ยวกับการสั่งจำนวน Battery ที่ต้องส่งเข้าใน Process และ Setup ระบบ IIoT
2. Quality Control มีหน้าที่นำ Finised Good มาตรวจสอบและจดบันทึกในฐานข้อมูลแบบ Manual 
3. General Manager มีหน้าที่วิเคราะห์ข้อมูลจาก Dashboard OEE

โดย Process ที่เค้าต้องการจะทำคือ Process การผลิต Battery ประกอบด้วยเครื่องจักร 3 ส่วน
M01 - Smelting Furnances
M02 - Plate Assembly Unit
M03 - Formation Charger

ปัจจุบันข้อมูลจะมาจาก 2 แหล่ง
1. IIoT โดยข้อมูลมาจาก NodeRED และ Inject เข้าที่ InfluxDB Time Sereis Databases
2. ระบบ ERP โดย Actor คือ Process Engineer ที่จะมีการ
- ใส่ข้อมูลในระบบว่าวันนี้จะผลิต Production line อะไร
- แต่ละเครื่องจะไม่ได้มี Status การทำงาน ต้องดูจาก IIoT ว่า Status เป็นยังไง
- เมื่อครบกระบวนการจะมีการส่งข้อมูลการผลิตให้กับ Quality Control (QC)
- QC มีการสุ่ม Random 5% เพื่อทำการทดสอบว่า Finished Good มี Defect กี่ Percent

General Manager อยากดูอะไร ?
- อัตราการ Update ข้อมูลที่ Dashboard คือ 15 นาที (ETL Injection Airflow)
- อยากรู้ว่า Production 3 Machines แยกกันเป็นยังไง และรวมทั้ง Process เป็นยังไง (ราย 15 นาที)
- อยากรุู้ว่ามีอัตรา Defect เท่าไหร่
- อยากรู้ว่า Machine Status แต่ละ Batches มีค่า Parameter เป็นเท่าไหร่ (ราย 15 นาที)

# OLTP Supabases

ดังนั้น Entity ที่ต้องมีคือ

CREATE TABLE production_line (
    line_id  SERIAL PRIMARY KEY,
    name     VARCHAR(50) NOT NULL,
    area     VARCHAR(50)
);

CREATE TABLE machine (
    machine_id  SERIAL PRIMARY KEY,
    name        VARCHAR(10) NOT NULL,  -- "M01" ต้องตรงกับ tag ใน InfluxDB
    line_id     INTEGER REFERENCES production_line(line_id)
);
-- Master data: M01, M02, M03

CREATE TABLE product (
    product_id  SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL
);


-- Process Engineer สร้างก่อน = แผน
CREATE TABLE production_order (
    order_id       SERIAL PRIMARY KEY,
    line_id        INTEGER   REFERENCES production_line(line_id),
    product_id     INTEGER   REFERENCES product(product_id),
    qty_ordered    INTEGER   NOT NULL,
    planned_start  TIMESTAMP NOT NULL,
    planned_end    TIMESTAMP NOT NULL
);

-- Operator สร้างตอนรัน = execution
-- 1 order มีได้หลาย batch (กรณีหยุดกลางคัน แล้ว split)
CREATE TABLE production_batch (
    batch_id    SERIAL PRIMARY KEY,
    order_id    INTEGER   REFERENCES production_order(order_id),
    qty_planned INTEGER   NOT NULL,   -- จำนวนที่วางแผนรอบนี้
    qty_out     INTEGER,              -- จำนวนที่ออก M03 จริง (ใส่หลังเสร็จ)
    start_time  TIMESTAMP NOT NULL,   -- เข้า M01 → map กับ InfluxDB
    end_time    TIMESTAMP             -- ออก M03 → map กับ InfluxDB
);


-- 1 batch มีได้ 1 qc_record (QC ที่ end of line หลัง batch เสร็จ)
CREATE TABLE qc_record (
    qc_id        SERIAL PRIMARY KEY,
    batch_id     INTEGER   REFERENCES production_batch(batch_id) UNIQUE,
    qty_sampled  INTEGER   NOT NULL,   -- random 5% ของ qty_out
    qty_passed   INTEGER   NOT NULL,
    qty_failed   INTEGER   NOT NULL,
    inspected_at TIMESTAMP DEFAULT NOW()
);


## Old Sources 
ตอนนี้ส่วน OLTP ใหญ่เกินไป อันนี้อันเก่า
**Supabase (17 tables, 3NF)** — see
`db_module/db_sources/supabases_sql_query/query/01_schema.sql`. 5 domains:

1. Infrastructure: `production_line`, `machine`, `process_stage`, `product`
2. Material master: `raw_material`, `bill_of_material`, `supplier`
3. Procurement & inventory: `raw_material_po`, `raw_material_receipt`, `inventory`
4. Production: `production_order`, `production_batch`, `finished_good`, `material_consumption`
5. Quality & maintenance: `qc_inspection`, `qc_result`, `maintenance_log`



# Data Warehouse New Design

จากนั้นไปแก้ไข Oracle Databases เพราะมันมีการ Overengineering เกินไป ต้องล้อตาม Business Requirements โดยยังมีการจัดเก็บที่ Staging อยู่แล้วจึงเก็บที่ Data Warehouse

## Data Warehouse Databases
CREATE TABLE AI03.DIM_DATE (
    date_id      NUMBER PRIMARY KEY,  -- format YYYYMMDD เช่น 20240115
    full_date    DATE          NOT NULL,
    day_of_week  NUMBER(1),
    week_number  NUMBER(2),
    month_number NUMBER(2),
    quarter      NUMBER(1),
    year         NUMBER(4)
);

CREATE TABLE AI03.DIM_MACHINE (
    machine_id      NUMBER PRIMARY KEY,
    machine_src_id  NUMBER        NOT NULL,  -- machine_id จาก Supabase
    machine_name    VARCHAR2(10)  NOT NULL,  -- "M01" ต้องตรงกับ tag ใน InfluxDB
    line_name       VARCHAR2(50)             -- denormalized
);
-- M01 = Smelting Furnace, M02 = Plate Assembly, M03 = Formation Charger


CREATE TABLE AI03.DIM_PRODUCT (
    product_id      NUMBER PRIMARY KEY,
    product_src_id  NUMBER        NOT NULL,  -- product_id จาก Supabase
    product_name    VARCHAR2(100) NOT NULL
);


CREATE TABLE AI03.DIM_METRIC (
    metric_id    NUMBER PRIMARY KEY,
    metric_name  VARCHAR2(50)  NOT NULL,  -- ต้องตรงกับ field name ใน InfluxDB
    unit         VARCHAR2(20),
    machine_name VARCHAR2(10),            -- NULL = ทุกเครื่อง
    description  VARCHAR2(200)
);

-- Master data ตาม NodeRED flow ที่ให้มา
INSERT INTO AI03.DIM_METRIC VALUES (1, 'temperature_c',    'celsius', 'M01', 'Furnace temperature');
INSERT INTO AI03.DIM_METRIC VALUES (2, 'machine_state_num','binary',  NULL,  '1=RUNNING / 0=FAULT');
INSERT INTO AI03.DIM_METRIC VALUES (3, 'cycle_count',      'count',   'M02', 'Assembly cycle count');
INSERT INTO AI03.DIM_METRIC VALUES (4, 'vibration_g',      'g-force', 'M02', 'Vibration level');
INSERT INTO AI03.DIM_METRIC VALUES (5, 'current_a',        'ampere',  'M03', 'Charging current');
INSERT INTO AI03.DIM_METRIC VALUES (6, 'voltage_v',        'volt',    'M03', 'Charging voltage');
-- เพิ่ม sensor ใหม่ = INSERT row เท่านั้น ไม่แตะ schema อื่น


// FACT_PRODUCTION — grain: 1 batch = 1 full line run
ไม่มี machine_id เพราะ 1 batch วิ่งทุกเครื่อง
CREATE TABLE AI03.FACT_PRODUCTION (
    prod_id      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_id      NUMBER        REFERENCES AI03.DIM_DATE(date_id),
    product_id   NUMBER        REFERENCES AI03.DIM_PRODUCT(product_id),
    batch_src_id NUMBER        NOT NULL,  -- batch_id จาก Supabase (degenerate dim)
    order_src_id NUMBER        NOT NULL,  -- order_id จาก Supabase (degenerate dim)
    qty_planned  NUMBER(8)     NOT NULL,
    qty_out      NUMBER(8),               -- NULL ถ้า batch ยังไม่เสร็จ
    yield_rate   NUMBER(5,4),             -- qty_out / qty_planned (SP คำนวณ)
    start_time   TIMESTAMP     NOT NULL,  -- เข้า M01 → ใช้ map กับ FACT_SENSOR
    end_time     TIMESTAMP,               -- ออก M03 → ใช้ map กับ FACT_SENSOR
    duration_min NUMBER(8,2),             -- SP คำนวณ
    loaded_at    TIMESTAMP     DEFAULT SYSDATE
);


FACT_QUALITY — grain: 1 QC record ต่อ batch
ไม่มี machine_id เพราะ QC ตรวจที่ end of line
CREATE TABLE AI03.FACT_QUALITY (
    quality_id      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_id         NUMBER       REFERENCES AI03.DIM_DATE(date_id),
    batch_src_id    NUMBER       NOT NULL,   -- trace กลับ batch ได้
    qty_sampled     NUMBER(8)    NOT NULL,   -- 5% random ของ qty_out
    qty_passed      NUMBER(8)    NOT NULL,
    qty_failed      NUMBER(8)    NOT NULL,
    defect_rate_pct NUMBER(5,2),             -- qty_failed / qty_sampled * 100
    inspected_at    TIMESTAMP,
    loaded_at       TIMESTAMP    DEFAULT SYSDATE
);


CREATE TABLE AI03.FACT_SENSOR (
    sensor_id    NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_id      NUMBER        REFERENCES AI03.DIM_DATE(date_id),
    machine_id   NUMBER        REFERENCES AI03.DIM_MACHINE(machine_id),
    metric_id    NUMBER        REFERENCES AI03.DIM_METRIC(metric_id),
    window_start TIMESTAMP     NOT NULL,   -- เริ่มต้น 15min window
    window_end   TIMESTAMP     NOT NULL,   -- สิ้นสุด 15min window
    avg_value    NUMBER(12,4)  NOT NULL,   -- ค่าเฉลี่ยใน window
    min_value    NUMBER(12,4),             -- ค่าต่ำสุดใน window
    max_value    NUMBER(12,4),             -- ค่าสูงสุดใน window
    sample_count NUMBER(6),               -- จำนวน data points ที่ aggregate (ควรได้ ~900 จาก 1Hz × 15min)
    loaded_at    TIMESTAMP     DEFAULT SYSDATE
);


-- Source: Supabase production_batch (WHERE end_time IS NOT NULL)
CREATE TABLE AI03.STG_PRODUCTION_BATCH (
    batch_id    NUMBER, order_id    NUMBER, product_id  NUMBER,
    qty_planned NUMBER, qty_out     NUMBER,
    start_time  TIMESTAMP, end_time TIMESTAMP,
    src_system      VARCHAR2(20)  DEFAULT 'SUPABASE',
    pipeline_run_id VARCHAR2(100),
    loaded_at       TIMESTAMP     DEFAULT SYSDATE
);

-- Source: Supabase qc_record
CREATE TABLE AI03.STG_QC_RECORD (
    qc_id        NUMBER, batch_id     NUMBER,
    qty_sampled  NUMBER, qty_passed   NUMBER, qty_failed   NUMBER,
    inspected_at TIMESTAMP,
    src_system      VARCHAR2(20)  DEFAULT 'SUPABASE',
    pipeline_run_id VARCHAR2(100),
    loaded_at       TIMESTAMP     DEFAULT SYSDATE
);

-- Source: InfluxDB aggregate 15min
-- Airflow เขียน 1 row ต่อ (machine × metric) ต่อ window
-- = 6 metrics × 3 machines = 18 rows ต่อ 15 นาที
CREATE TABLE AI03.STG_SENSOR_AGG (
    machine_name VARCHAR2(10),   -- "M01" ใช้ lookup DIM_MACHINE
    metric_name  VARCHAR2(50),   -- "temperature_c" ใช้ lookup DIM_METRIC
    window_start TIMESTAMP, window_end TIMESTAMP,
    avg_value    NUMBER(12,4), min_value NUMBER(12,4),
    max_value    NUMBER(12,4), sample_count NUMBER(6),
    src_system      VARCHAR2(20)  DEFAULT 'INFLUXDB',
    pipeline_run_id VARCHAR2(100),
    loaded_at       TIMESTAMP     DEFAULT SYSDATE
);



## Old Sources
โดย Oracle เก่าเป็นแบบนี้
**Oracle STG (7 tables)** — raw extract buffer,
`db_module/db_sources/oracle_sql_query/01_dw_ddl.sql` + `06_inventory_pipeline.sql`:

| Table | Source | Grain |
|---|---|---|
| `STG_PRODUCTION_BATCH` | Supabase `production_batch` | 1 batch × 1 stage |
| `STG_QC_INSPECTION` | Supabase `qc_inspection` | 1 QC event |
| `STG_QC_RESULT` | Supabase `qc_result` | 1 measurement |
| `STG_MAINTENANCE_LOG` | Supabase `maintenance_log` | 1 event |
| `STG_SENSOR_AGG` | InfluxDB aggregate | 1 machine × 8h window |
| `STG_INVENTORY` | Supabase `inventory` snapshot | 1 material, current |
| `STG_MATERIAL_CONSUMPTION` | Supabase `material_consumption` | 1 consumption event |

Every STG row carries lineage columns: `src_system`, `pipeline_run_id`,
`loaded_at`.

**Oracle DW (5 + 5 star schema)** —
`db_module/db_sources/oracle_sql_query/01_dw_ddl.sql`:

- **Dims** (surrogate keys via sequences, `*_src_id` preserves source PK):
  `DIM_DATE` (5 years pre-populated), `DIM_MACHINE`, `DIM_PRODUCT`,
  `DIM_STAGE`, `DIM_MATERIAL`.
- **Facts**: `FACT_OEE` (one row per machine per day), `FACT_PRODUCTION`,
  `FACT_QUALITY`, `FACT_INVENTORY`, `FACT_MAINTENANCE`.



หากมีข้อสงสัยถามเพิ่มเติมได้