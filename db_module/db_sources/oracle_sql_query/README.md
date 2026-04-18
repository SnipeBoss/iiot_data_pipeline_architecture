# Oracle Data Warehouse — DDL + Procedures

Data Warehouse layer บน Oracle 10g (schema `AI03` ของ KMITL) ประกอบด้วย SQL DDL,
stored procedure, reporting query และ Python script สำหรับ apply/verify/sync

> สำหรับวิธีติดตั้ง end-to-end ของทั้งระบบ → ดู [README ที่ repo root](../../../README.md)
> สำหรับ schema ฉบับเต็ม → ดู [NEW_ARCHITECTURE.md](../../../markdown/NEW_ARCHITECTURE.md)

---

## Table of Contents

- [โครงสร้างแบบ 3 ชั้น](#โครงสร้างแบบ-3-ชั้น-staging--dimension--fact)
- [Staging Layer (STG_*)](#staging-layer-stg_)
- [Dimension Layer (DIM_*)](#dimension-layer-dim_)
- [Fact Layer (FACT_*)](#fact-layer-fact_)
- [Fact Loader Procedures (SP_LOAD_FACT_*)](#fact-loader-procedures-sp_load_fact_)
- [File Layout](#file-layout)
- [Scripts](#scripts)
- [Oracle 10g Gotchas](#oracle-10g-gotchas)

---

## โครงสร้างแบบ 3 ชั้น (Staging → Dimension → Fact)

DW ใช้ **Kimball star schema** — แยกข้อมูลเป็น 3 ชั้นตามหน้าที่:

```
          Supabase (OLTP)                     InfluxDB (sensor)
              │                                      │
              │ extract via DAG                      │ 15-min aggregate via DAG
              ▼                                      ▼
        ┌──────────────────────────────────────────────────┐
        │  STG (Staging)  ─  buffer ดิบ ไม่มี FK            │
        │  STG_PRODUCTION_BATCH                            │
        │  STG_QC_RECORD                                   │
        │  STG_SENSOR_AGG                                  │
        └──────────────────────┬───────────────────────────┘
                               │ transform ผ่าน SP_LOAD_FACT_*
                               ▼
        ┌──────────────────────────────────────────────────┐
        │  DIM (Dimension)  ─  master data + catalog       │
        │  DIM_DATE · DIM_MACHINE · DIM_PRODUCT · DIM_METRIC│
        └──────────────────────┬───────────────────────────┘
                               │ FK-joined
                               ▼
        ┌──────────────────────────────────────────────────┐
        │  FACT (Measure)  ─  measurable events + KPIs     │
        │  FACT_PRODUCTION · FACT_QUALITY · FACT_SENSOR    │
        └──────────────────────────────────────────────────┘
                               │
                               ▼
                      FastAPI → Dashboard
```

**หลักคิด:**
- **Staging** = บ้านพักชั่วคราวของข้อมูลดิบ — เก็บเหมือน source ตรง ๆ ไม่แปลง format
- **Dimension** = master data "ใคร/อะไร/เมื่อไหร่" ที่ fact ชี้อ้างอิง
- **Fact** = event ที่วัดได้ (produce, fail, sense) — มี measure ที่ SUM/AVG ได้

---

## Staging Layer (STG_*)

### หลักการ

Staging คือ **buffer ดิบ** ระหว่าง source กับ DW:
- **ไม่มี FK** — เพราะ source อาจส่ง data มาไม่ตรงลำดับ ทำให้ constraint หยุด pipeline
- **ไม่มี surrogate key** — เก็บ business key ตรงจาก source
- **ไม่มี index สำหรับ reporting** — STG ใช้แค่อ่าน-แล้วจบ ไม่ได้ serve dashboard
- **มี lineage columns เสมอ** — `src_system`, `pipeline_run_id`, `loaded_at`
  ใช้ debug ตอน data ผิด: รู้ว่ารันมาจาก DAG run ไหน เมื่อไหร่

### 3 ตารางใน STG

| ตาราง | Grain | Source | Extract DAG |
|---|---|---|---|
| `STG_PRODUCTION_BATCH` | 1 batch (completed only) | Supabase `production_batch WHERE end_time IS NOT NULL` | `etl_supabase_to_oracle` |
| `STG_QC_RECORD` | 1 QC record | Supabase `qc_record` | `etl_supabase_to_oracle` |
| `STG_SENSOR_AGG` | 1 machine × metric × 15-min window | InfluxDB aggregate (mean) | `etl_influxdb_to_oracle` |

### Load Pattern

DAG รันทุก 15 นาที — **merge-by-key** (ไม่ใช่ truncate) เพื่อให้ STG สะสมข้อมูลได้
หลาย window ภายในวันเดียว

```sql
-- pseudo code ใน _oracle_api.bulk_insert:
DELETE FROM STG_x WHERE <business_key> IN (<new batch>);
INSERT INTO STG_x VALUES ...;
```

---

## Dimension Layer (DIM_*)

### หลักการ

Dimension = **ตาราง lookup** ที่ fact ชี้อ้างอิง เพื่อ slice & dice KPI ได้:
"OEE ของเครื่องไหน วันไหน metric อะไร"

- **Surrogate key** — PK ไม่ใช่ business key ของ source (`machine_id` ใน DIM ≠ `machine_id` ใน Supabase)
  ใช้ `SEQUENCE` + `NEXTVAL` generate อัตโนมัติ
- **Source key เก็บใน `*_src_id`** — เผื่อ reverse-lookup กลับไปหา source
- **Denormalize ได้** — DIM ควร flat (เช่น `DIM_MACHINE.line_name` copy มาจาก
  `production_line` โดยไม่ต้อง JOIN) → reporting query เร็วขึ้น
- **SCD Type 1** (overwrite) เป็น default — POC ไม่ต้อง track history

### 4 ตารางใน DIM

| ตาราง | Grain | Source | Seed By |
|---|---|---|---|
| `DIM_DATE` | 1 วัน | `SP_LOAD_DIM_DATE(p_start, p_days)` | manual (pre-populate 1 ปี) |
| `DIM_MACHINE` | 1 เครื่อง | Supabase `machine` + `production_line` | `sync_dimensions_from_supabase.py` |
| `DIM_PRODUCT` | 1 product | Supabase `product` | `sync_dimensions_from_supabase.py` |
| `DIM_METRIC` | 1 sensor metric | hardcode ใน `01_schema.sql` | auto (6 row seed on apply) |

### Key ที่น่าสังเกต

**`DIM_DATE.date_id` = YYYYMMDD** (ไม่ใช่ sequence)
- อ่านง่าย (20260418 ≡ 2026-04-18)
- Portable — join ข้าม DB ได้ถ้า design เหมือนกัน
- ไม่ต้อง JOIN ก็รู้ว่าเป็นวันไหน

**`DIM_MACHINE.machine_name`** ต้องเป็น `"M01"/"M02"/"M03"` **ตรงกับ tag ใน InfluxDB เป๊ะ ๆ**
- เพราะ `SP_LOAD_FACT_SENSOR` ใช้ `JOIN ON dm.machine_name = stg.machine_name` เพื่อ map
  ระหว่าง source (InfluxDB tag) กับ DW surrogate key
- ถ้าผิด 1 ตัวอักษร → FACT_SENSOR จะว่าง เพราะ JOIN ไม่ match

**`DIM_METRIC` seeded in schema** — 6 row insert อยู่ใน `01_schema.sql` โดยตรง:
- เพิ่ม sensor ใหม่ = INSERT row เดียว ไม่แตะ schema อื่น
- ไม่ต้อง sync จาก source เหมือน DIM_MACHINE/PRODUCT เพราะ metric name = field name ใน Flux query
  (hardcode ใน DAG อยู่แล้ว)

---

## Fact Layer (FACT_*)

### หลักการ

Fact = **ตารางวัดผล** ที่ record event พร้อม measure — เป็น center ของ star schema

- **Grain ชัดเจน** — ระบุว่า 1 row = อะไร (1 batch? 1 QC? 1 sensor window?)
- **FK ชี้ DIM** — ทุก `*_id` ใน FACT ต้องอ้างอิง DIM (ยกเว้น degenerate dim)
- **Additive measures** — column ที่ SUM/AVG/COUNT ได้ (qty, duration, yield)
- **Precomputed % ได้** (เช่น `defect_rate_pct`, `yield_rate`) — แต่ **ห้ามเฉลี่ย %** ใน query
  ต้อง re-derive จาก measure ดิบถ้าต้องการ aggregate หลาย row
- **Degenerate dimensions** — business key ที่ไม่คุ้มสร้าง DIM แยก
  (`batch_src_id`, `order_src_id`) เก็บใน FACT ตรง ๆ

### 3 ตารางใน FACT

| ตาราง | Grain | FK ไปที่ | Degenerate dim |
|---|---|---|---|
| `FACT_PRODUCTION` | **1 batch** (complete line run) | DIM_DATE, DIM_PRODUCT | batch_src_id, order_src_id |
| `FACT_QUALITY` | **1 QC record** (end-of-line) | DIM_DATE | batch_src_id |
| `FACT_SENSOR` | **1 (machine × metric × 15-min window)** | DIM_DATE, DIM_MACHINE, DIM_METRIC | — |

### ข้อสังเกตสำคัญ

**FACT_PRODUCTION ไม่มี `machine_id`**
- เพราะ 1 batch วิ่งผ่านทั้ง 3 เครื่อง (M01 → M02 → M03) — ไม่มีเครื่องเดียวที่ "ผลิต" batch นี้
- ต้องการดู per-machine? → JOIN `FACT_SENSOR` ตาม `window_start BETWEEN fp.start_time AND fp.end_time`

**FACT_QUALITY ไม่มี `machine_id`**
- QC ตรวจที่ **end of line** หลัง batch เสร็จ — ไม่ผูกกับเครื่องใดเครื่องหนึ่ง

**FACT_SENSOR เก็บทั้ง avg/min/max + sample_count**
- ถ้าเก็บแค่ avg → reporting ไม่รู้ว่า 15 นาทีนั้นค่ากระโดดหรือเปล่า
- `sample_count` (~900 สำหรับ 1 Hz × 15 min) ช่วย flag window ที่ sensor down

---

## Fact Loader Procedures (`SP_LOAD_FACT_*`)

### Merge-by-key (ไม่ใช่ merge-by-date) — จุดเปลี่ยนสำคัญ

**Context:** DAG รันทุก 15 นาที → STG มีแค่ window ล่าสุด ~18 row

**ปัญหาของ `DELETE WHERE date_id = :d`:**
```sql
-- WRONG — ลบ FACT ทั้งวันแล้ว rebuild จาก STG 15 นาทีเดียว
DELETE FROM FACT_SENSOR WHERE date_id = :d;
INSERT FROM STG...;   -- 18 row
-- ผลลัพธ์: FACT_SENSOR เหลือแค่ 18 row (ของ 15 นาทีล่าสุด) ทั้ง ๆ ที่ควรมี 4×18=72 ของ 1 ชั่วโมง
```

**Fix: ลบเฉพาะ row ที่ match กับ STG ปัจจุบัน**
```sql
-- RIGHT — idempotent ที่ระดับ key ไม่ใช่ระดับ date
DELETE FROM FACT_SENSOR
 WHERE (machine_id, metric_id, window_start) IN (
    SELECT dm.machine_id, dmt.metric_id, stg.window_start
      FROM STG_SENSOR_AGG stg
      JOIN DIM_MACHINE dm  ON dm.machine_name = stg.machine_name
      JOIN DIM_METRIC  dmt ON dmt.metric_name = stg.metric_name
 );
INSERT FROM STG...;
```

### Key ที่ใช้ delete ต่อ SP

| SP | Delete key | เหตุผล |
|---|---|---|
| `SP_LOAD_FACT_PRODUCTION` | `batch_src_id` | 1 batch = 1 row เสมอ; rerun ต้อง overwrite |
| `SP_LOAD_FACT_QUALITY` | `batch_src_id` | QC 1:1 กับ batch |
| `SP_LOAD_FACT_SENSOR` | `(machine_id, metric_id, window_start)` | composite unique ใน scope ของ 15-min window |

### การคำนวณ `date_id`

SP ไม่รับ parameter แล้ว (ไม่มี `p_date`) — ใช้ `TO_NUMBER(TO_CHAR(<timestamp>, 'YYYYMMDD'))`
คำนวณจากข้อมูลใน STG โดยตรง

---

## File Layout

```
oracle_sql_query/
├── README.md                              ← ไฟล์นี้
├── query/                                  ← SQL DDL + SP + reporting
│   ├── 01_schema.sql                       ← ตาราง + sequence + index + DIM_METRIC seed
│   ├── 02_procedure_dim_date.sql           ← SP_LOAD_DIM_DATE (populate calendar)
│   ├── 03_procedure_fact_loaders.sql       ← 3 fact loader SPs (merge-by-key)
│   ├── 04_reporting_queries.sql            ← 5 query สำหรับ dashboard (reference)
│   └── 05_truncate_and_reload.sql          ← admin: reset fact + dim
│
├── run_sql_file.py                         ← apply SQL file (รองรับ `/` PL/SQL terminator)
├── sync_dimensions_from_supabase.py        ← seed DIM_MACHINE + DIM_PRODUCT จาก Supabase
└── verify_warehouse_schema.py              ← assertion 10 table + 5 seq + 4 proc
```

### ลำดับรันครั้งแรก (bootstrap DW)

```bash
# 1. Apply schema ทั้งหมด
.venv/bin/python db_module/db_sources/oracle_sql_query/run_sql_file.py \
    db_module/db_sources/oracle_sql_query/query/01_schema.sql

.venv/bin/python db_module/db_sources/oracle_sql_query/run_sql_file.py \
    db_module/db_sources/oracle_sql_query/query/02_procedure_dim_date.sql

.venv/bin/python db_module/db_sources/oracle_sql_query/run_sql_file.py \
    db_module/db_sources/oracle_sql_query/query/03_procedure_fact_loaders.sql

# 2. Verify — ต้องได้ 10 tables + 5 sequences + 4 procedures
.venv/bin/python db_module/db_sources/oracle_sql_query/verify_warehouse_schema.py

# 3. Populate DIM_DATE (1 ปี)
.venv/bin/python -c "from db_module.db_conn import OracleConnector; \
    conn = OracleConnector().connect(); cur = conn.cursor(); \
    cur.execute(\"BEGIN SP_LOAD_DIM_DATE(DATE '2026-01-01', 365); END;\"); \
    conn.commit()"

# 4. Seed DIM_MACHINE + DIM_PRODUCT จาก Supabase
.venv/bin/python db_module/db_sources/oracle_sql_query/sync_dimensions_from_supabase.py
```

---

## Scripts

### `run_sql_file.py <path>`

Apply SQL file ต่อ AI03 — รองรับทั้ง plain SQL (แยก `;`) และ PL/SQL block (แยก `/` บรรทัดเดียว)

- **Fail-fast:** error statement แรก → rollback ทั้งหมด + exit 1
- ใช้ PL/SQL detection heuristic (คำขึ้นต้น `BEGIN`/`DECLARE`/`CREATE OR REPLACE`/`CREATE PROCEDURE`/...)

### `sync_dimensions_from_supabase.py`

One-shot ETL: Supabase master data → Oracle DIM

- `machine` + `production_line` → `DIM_MACHINE` (denormalize `line_name`)
- `product` → `DIM_PRODUCT`
- **DELETE** ตาราง DIM ก่อน INSERT ใหม่ (idempotent)
- ต้องรัน **หลัง** Supabase มี master data แล้ว

### `verify_warehouse_schema.py`

Smoke-test: ตรวจว่า `AI03` มี object ครบตามที่คาดหวัง

- 10 tables (3 STG + 4 DIM + 3 FACT)
- 5 sequences (SEQ_DIM_MACHINE, SEQ_DIM_PRODUCT, SEQ_FACT_PRODUCTION, SEQ_FACT_QUALITY, SEQ_FACT_SENSOR)
- 4 procedures (SP_LOAD_DIM_DATE + 3 fact loaders)

ใช้หลัง apply DDL เพื่อยืนยันว่า schema พร้อม

---

## Oracle 10g Gotchas

**1. ไม่มี `GENERATED AS IDENTITY`** (ต้อง Oracle 12c+)
→ ใช้ `SEQUENCE` + `NEXTVAL` ทุก surrogate PK

**2. `SEQ.NEXTVAL` ห้ามใช้ใน `INSERT...SELECT`** (ORA-02287)
→ ใช้ cursor `FOR...LOOP` + row-by-row INSERT แทน (ดูตัวอย่างใน `SP_LOAD_FACT_PRODUCTION`)

**3. `ALTER SEQUENCE ... RESTART` ไม่มีใน 10g**
→ ต้อง `DROP SEQUENCE` + `CREATE SEQUENCE` ใหม่ (ดู `05_truncate_and_reload.sql`)

**4. JDBC thin driver ต้องใช้ `thinLogonCapability=o3`**
→ ตั้งใน `OracleConnector._JVM_ARGS` แล้ว

**5. Thai locale JVM convert Gregorian → Buddhist calendar**
→ บังคับ `-Duser.language=en -Duser.country=US` + `ALTER SESSION SET NLS_CALENDAR='GREGORIAN'`
ทุก session (อยู่ใน `OracleConnector.connect()`)

**6. `TO_CHAR(d, 'D')` return locale-dependent weekday**
→ ใน `SP_LOAD_DIM_DATE` ใช้ ISO-week anchoring แทน:
`(TRUNC(d) - TRUNC(d, 'IW')) + 1` → 1=Mon..7=Sun เสมอ
