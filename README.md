# Unified IIoT Data Architecture — Battery Assembly Line COS

End-to-end data pipeline สำหรับ **สายการผลิต Battery Assembly Line COS**
(1 line, 3 machines: M01/M02/M03) ที่ ingest ข้อมูลจาก 2 sources
(Supabase OLTP + InfluxDB IIoT sensor) → Oracle 10g Data Warehouse
(Kimball star schema 20 tables) → FastAPI HTTP layer → Streamlit dashboard 3 pages
พร้อม Prophet time-series forecast

```
┌─────────────────────┐  ┌─────────────────────┐
│  Supabase (OLTP)    │  │  InfluxDB (IIoT)    │
│  Postgres 15        │  │  v2.x bucket        │
│  12 tables / cloud  │  │  1 Hz sensor / AWS  │
└──────────┬──────────┘  └──────────┬──────────┘
           │ psycopg2               │ Flux + influxdb-client
           ▼                        ▼
┌─────────────────────────────────────────────────┐
│  Apache Airflow 2.8 (Docker compose, port 8088) │
│  4 DAGs:                                        │
│    etl_supabase_to_oracle    */15 * * * *       │
│    etl_influxdb_to_oracle    */15 * * * *       │
│    sp_load_dw                5,20,35,50 * * * * │
│    sync_dim_supabase         0 2 * * * (nightly)│
└──────────────────────┬──────────────────────────┘
                       │ HTTP (Bearer token)
                       │ /sql/bulk-insert + /sp/call
                       ▼
┌─────────────────────────────────────────────────┐
│  FastAPI (port 8000, host JVM, JDBC thin)       │
│  - dw_api/operational.py                        │
│      /health, /sp/call, /sql/bulk-insert        │
│  - dashboard_api/dashboard.py                   │
│      /api/sensor/*, /api/scheduling/*,          │
│      /api/analytics/* (replace Oracle views)    │
└──────────────────────┬──────────────────────────┘
                       │ JDBC thin (ojdbc8.jar)
                       ▼
┌─────────────────────────────────────────────────┐
│  Oracle 10g (KMITL AI03) — 20 tables            │
│    7 DIM (1827+1+2+3+3+6+20 rows)               │
│    5 FACT (PRODUCTION/QUALITY/DEFECT/           │
│            DOWNTIME/SENSOR)                     │
│    8 STG (5 OLTP staging + 3 DIM source)        │
│    9 SEQ + 10 PROC + 1 FN + 27 IDX              │
└──────────────────────┬──────────────────────────┘
                       │ HTTP cached (TTL 5 min)
                       ▼
┌─────────────────────────────────────────────────┐
│  Streamlit Dashboard (port 8501, 3 pages)       │
│    Page 1: OEE & Defect (4 KPI + 3 charts)      │
│    Page 2: Sensor Forecast (Prophet)            │
│    Page 3: Schedule Adherence (Gantt + KPI)     │
└─────────────────────────────────────────────────┘
```

> **Last refactor:** 2026-04-26 (NEW_ARCHITECTURE) — DW เหลือ 6 OLTP-driven STG + 4 DIM + 3 FACT (เปลี่ยน จาก grain เดิม) + DIM_METRIC; SP เปลี่ยนจาก 3 parallel → 1 master orchestrator
>
> **Production data (mock):** 373 batches, 6,152 sensor windows over 8 วัน

---

## Table of Contents

1. [Problem Statement & Use Cases](#1-problem-statement--use-cases)
2. [Stack Overview & Rationale](#2-stack-overview--rationale)
3. [Repository Layout](#3-repository-layout)
4. [Data Flow (4 sub-flows)](#4-data-flow-4-sub-flows)
5. [Database Layer (`db_module/`)](#5-database-layer-db_module)
   - 5.1 [Connectors](#51-connectors)
   - 5.2 [Supabase OLTP Schema (12 tables)](#52-supabase-oltp-schema-12-tables)
   - 5.3 [InfluxDB IIoT Schema](#53-influxdb-iiot-schema)
   - 5.4 [Oracle DW Schema (20 tables)](#54-oracle-dw-schema-20-tables)
   - 5.5 [Stored Procedures (10 SP + 1 FN)](#55-stored-procedures-10-sp--1-fn)
6. [Application Layer (`app/`)](#6-application-layer-app)
   - 6.1 [FastAPI Service (10 endpoints)](#61-fastapi-service-10-endpoints)
   - 6.2 [Streamlit Dashboard (3 pages)](#62-streamlit-dashboard-3-pages)
7. [Pipeline Layer (Airflow 4 DAGs)](#7-pipeline-layer-airflow-4-dags)
8. [Code Walkthroughs (key algorithms)](#8-code-walkthroughs-key-algorithms)
9. [Setup from Scratch](#9-setup-from-scratch)
10. [Daily Operations](#10-daily-operations)
11. [Smoke Tests](#11-smoke-tests)
12. [Troubleshooting](#12-troubleshooting)
13. [Constraints & Design Decisions](#13-constraints--design-decisions)
14. [Performance Characteristics](#14-performance-characteristics)
15. [Glossary](#15-glossary)

---

## 1. Problem Statement & Use Cases

### 1.1 Manufacturing Context

โรงงานผลิตแบตเตอรี่ตะกั่วกรด Line COS (Cast-On-Strap) ของ GS Battery มีสายการผลิตเดียว
ประกอบด้วย 3 เครื่องเรียงต่อเนื่อง:

| Position | Code | Type | Process |
|---|---|---|---|
| 1 | **M01** | Furnace | หลอมตะกั่วร้อน → cast strap |
| 2 | **M02** | Pasting/Curing | แปะเปลือกแผ่น (paste) + บ่ม |
| 3 | **M03** | Welding | เชื่อม terminal + ประกอบ |

ผลิตได้ 3 รุ่น: **60AH** (Standard), **75AH** (Premium), **100AH** (Heavy)
แบ่ง shift 2 กะ: **Day** (07:30-16:30), **Night** (17:30-06:30 ข้ามวัน)

### 1.2 Business Questions (เป้าหมายของ Dashboard)

| # | คำถาม | Answer ผ่าน |
|---|---|---|
| Q1 | สายผลิตทำงานได้เต็มศักยภาพแค่ไหน? | **OEE** = Availability × Performance × Quality (Page 1) |
| Q2 | ของเสียส่วนใหญ่มาจากไหน? | **Defect Pareto** + per-model breakdown (Page 1) |
| Q3 | เครื่องไหนมีแนวโน้มอุณหภูมิ/แรงดันผิดปกติ? | **Prophet forecast** + threshold (Page 2) |
| Q4 | งานเสร็จตรงเวลาแค่ไหน? | **Slippage** + **Adherence status** (Page 3) |
| Q5 | order ไหนล่าช้า batch ไหนเร็ว? | **Gantt drilldown** ต่อ order (Page 3) |

### 1.3 Why a Data Warehouse (vs. แค่ query OLTP ตรง ๆ)

- **OEE คำนวณข้าม 3 source:** production_batch + qc_record + downtime_event — JOIN cross-table 15-min cadence ใช้ DW efficient กว่า OLTP
- **Sensor 1 Hz × 3 machines × 6 metrics ≈ 18 row/sec** — ต้อง pre-aggregate เป็น 15-min window ใน FACT_SENSOR ก่อนนำไปใช้
- **Star schema** เน้น read-optimized → dashboard query ตอบเร็ว (~< 1s)
- **Surrogate key** stable ข้ามการ sync — FACT FK ไม่ orphan เมื่อ source ขยับ row

---

## 2. Stack Overview & Rationale

| Layer | Tech | Role | Hosted | ทำไมเลือกตัวนี้ |
|---|---|---|---|---|
| **OLTP** | PostgreSQL 15 (Supabase cloud) | 12-table MES (orders, batches, QC, downtime) | Cloud | Free tier + REST API + ใช้ psycopg2 ตรงได้ (ไม่ต้องผ่าน PostgREST) |
| **IIoT source** | InfluxDB 2.x + Telegraf + Mosquitto + Node-RED | 1 Hz sensor (6 fields × 3 machines) | AWS EC2 | Time-series first-class; Flux มี `aggregateWindow` built-in |
| **DW** | Oracle 10.2.0.3 (`AI03`) | Kimball star schema (20 tables) | KMITL `161.246.35.92:1521/orcl` | Free academic infra; ผูกกับ KMITL CSC compute |
| **Driver** | JDBC thin (`ojdbc8.jar`) via `jaydebeapi` + `jpype` | Oracle 10g connectivity | host JVM | python-oracledb thin ไม่รองรับ 10g (ต้อง 12c+); Instant Client ไม่มี ARM64 build |
| **HTTP layer** | FastAPI + uvicorn | JDBC wrapper + analytics endpoints | host (port 8000) | Airflow Docker image ไม่มี Java; bundle ojdbc8 ทุก container แพง → ห่อ JDBC เป็น HTTP service |
| **Pipeline** | Apache Airflow 2.8 | 4 DAGs (3 ETL @15min + 1 DIM sync nightly) | Docker compose (port 8088) | Mature scheduler, UI debug ดี, Python-native task |
| **Dashboard** | Streamlit + Plotly + Prophet | 3 pages (OEE / Forecast / Schedule) | host (port 8501) | สร้าง analytics UI เร็ว; ไม่ต้องเขียน frontend แยก |
| **Forecast** | Prophet 1.1+ (cmdstanpy backend) | Time-series forecast ต่อ (machine × metric) | in-process daemon thread | Robust ต่อ missing data; auto-detect seasonality |

### 2.1 Architectural Decisions

#### a) ทำไมไม่ใช้ Postgres เป็น DW เลย?

โจทย์ให้ใช้ Oracle ของ KMITL — ผูกกับ infra ที่ user มีสิทธิ์ใช้งานฟรี (schema AI03)
แต่ **AI03 ไม่มี privilege CREATE VIEW** → จึงย้าย logic V_OEE_DAILY/V_DEFECT_PARETO/
V_SCHEDULE_ADHERENCE/V_BATCH_FEATURES มาเป็น **FastAPI analytics endpoints** แทน

#### b) ทำไม Airflow ต้อง call HTTP ไม่ใช้ JDBC ตรง?

- Airflow Docker image (apache/airflow:2.8) ไม่มี Java pre-installed
- Bundle ojdbc8.jar + JVM ทุก worker container = image ใหญ่ + cold start ช้า
- **Solution:** ห่อ JDBC เป็น FastAPI service บน host (1 process = 1 JVM)
  Airflow ส่ง HTTP `/sql/bulk-insert` + `/sp/call` แทน

#### c) ทำไม STG = TRUNCATE-then-INSERT?

- STG เป็น **buffer ของ window 15 นาที ล่าสุด** (ไม่ใช่ historical store)
- Idempotent: รัน DAG ซ้ำได้ผลลัพธ์เดิม
- ลด space — STG ไม่บวมเป็น GB

#### d) ทำไม FACT = DELETE-by-key + cursor FOR-LOOP INSERT?

- Oracle 10g **forbids** `SEQ.NEXTVAL` ใน `INSERT...SELECT` (เพิ่งอนุญาตใน 11g R2)
- ต้องเขียนเป็น cursor loop iterate STG → insert ทีละ row
- DELETE ก่อน → idempotent: รันซ้ำกี่ครั้ง row ก็ไม่ duplicate

#### e) ทำไม DIM = MERGE-BY-SRC-ID (ไม่ DELETE)?

- ถ้า DELETE+INSERT — surrogate key (`line_id` ของ DIM_LINE) เปลี่ยน → FACT FK orphan
- **MERGE BY src_id**: ถ้ามีอยู่แล้ว UPDATE, ไม่มี INSERT ใหม่ — surrogate key เสถียร

---

## 3. Repository Layout

```
unified_iiot_data_architecture/
│
├── app/                                    # Application layer (FastAPI + Streamlit)
│   ├── __init__.py
│   ├── api/                                # FastAPI service (host process, port 8000)
│   │   ├── __init__.py
│   │   ├── main.py                         # FastAPI() + 2 router includes
│   │   ├── dw_api/                         # Oracle core (Airflow callers)
│   │   │   ├── __init__.py
│   │   │   ├── deps.py                     # auth + connector singleton + JDBC coercion + query helper
│   │   │   ├── models.py                   # 2 Pydantic: SpCallRequest, BulkInsertRequest
│   │   │   └── operational.py              # /health + /sp/call + /sql/bulk-insert
│   │   └── dashboard_api/
│   │       ├── __init__.py
│   │       └── dashboard.py                # 7 Streamlit-facing endpoints
│   │
│   └── streamlit/                          # Multi-page dashboard (host process, port 8501)
│       ├── __init__.py
│       ├── .streamlit/config.toml          # theme + server config (must run from this dir)
│       ├── dashboard.py                    # Entry: sidebar nav + landing + FastAPI status
│       ├── components/
│       │   ├── __init__.py
│       │   ├── api_client.py               # @st.cache_data wrapper (TTL 5 min) + Bearer
│       │   ├── cards.py                    # kpi_card / kpi_row / status_badge
│       │   ├── charts.py                   # 7 Plotly builders
│       │   ├── filters.py                  # period_selector + 3 filter rows
│       │   └── prophet_trainer.py          # train (daemon thread) + cache + predict
│       ├── pages/
│       │   ├── __init__.py
│       │   ├── 1_oee_defect.py             # Page 1: OEE & Defect
│       │   ├── 2_sensor_forecast.py        # Page 2: Prophet forecast
│       │   └── 3_schedule_adherence.py     # Page 3: Slippage + Gantt
│       └── cache/prophet_models/           # *.pkl cache (gitignored)
│
├── db_module/                              # Database layer
│   ├── __init__.py
│   ├── db_conn/                            # 3 connector classes (Singleton-friendly)
│   │   ├── __init__.py                     # exports OracleConnector, SupabaseConnector, InfluxConnector
│   │   ├── _env.py                         # .env loader + require/get/resolve_path helpers
│   │   ├── oracle/
│   │   │   ├── __init__.py
│   │   │   ├── oracle_connection.py        # OracleConnector — JDBC thin via jaydebeapi/jpype
│   │   │   └── drivers/ojdbc8.jar          # NOT in git — download separately
│   │   ├── supabases/
│   │   │   ├── __init__.py
│   │   │   └── supabase_connection.py      # SupabaseConnector — psycopg2 + SSL
│   │   └── influxdb/
│   │       ├── __init__.py
│   │       └── influx_connection.py        # InfluxConnector — influxdb-client (Flux)
│   │
│   ├── db_sources/                         # Provisioning + mock data
│   │   ├── oracle_sql_query/               # DW provisioning + admin
│   │   │   ├── query/                      # 7 SQL files apply ตามลำดับ
│   │   │   │   ├── 01_schema_dim.sql       # 7 DIM + 4 SEQ
│   │   │   │   ├── 02_schema_fact.sql      # 5 FACT + 5 SEQ
│   │   │   │   ├── 03_schema_staging.sql   # 8 STG (5 OLTP + 3 DIM source)
│   │   │   │   ├── 04_dim_seed.sql         # DIM_DATE 1827 rows + DIM_SHIFT/METRIC/DEFECT inline
│   │   │   │   ├── 05_indexes.sql          # 27 indexes
│   │   │   │   ├── 06_procedure_dim_sync.sql   # 3 SP_SYNC_DIM_* + 1 SP_SYNC_ALL_DIMS
│   │   │   │   └── 07_procedure_fact_load.sql  # FN_GET_SHIFT_ID + 5 SP_LOAD_FACT_* + SP_LOAD_ALL_FACTS
│   │   │   ├── run_sql_file.py             # DDL applier (handles `;` and `/`)
│   │   │   ├── verify_warehouse_schema.py  # Schema integrity check
│   │   │   └── sync_dimensions_from_supabase.py  # CLI alt to nightly DAG
│   │   │
│   │   ├── supabases_sql_query/            # OLTP provisioning + mock data
│   │   │   ├── query/
│   │   │   │   ├── 01_schema.sql           # 12-table MES schema
│   │   │   │   ├── 02_trigger_functions.sql# 2 trigger fn + 2 triggers
│   │   │   │   ├── 03_master_data.sql      # 1 line + 3 machines + 3 models + lookups
│   │   │   │   └── 04_mock_data.sql        # generated 8-day mock
│   │   │   ├── apply_supabase.py           # transactional 4-file applier + audit
│   │   │   └── generate_mock_data.py       # mock generator (FIFO, beta yield)
│   │   │
│   │   └── iiot_container/                 # Edge stack (Node-RED + Mosquitto + Telegraf)
│   │
│   └── pipeline/                           # Airflow orchestration
│       ├── docker-compose.yml              # Airflow 2.8 stack (LocalExecutor + Postgres meta)
│       └── airflow/
│           └── dags/
│               ├── _oracle_api.py          # HTTP helpers: health/bulk_insert/call_sp/as_iso
│               ├── _supabase.py            # supabase_cursor() ctx mgr
│               ├── etl_supabase_to_oracle.py   # */15 — OLTP → STG (4 tasks parallel)
│               ├── etl_influxdb_to_oracle.py   # */15 — Flux agg → STG_SENSOR_AGG
│               ├── sp_load_dw.py               # 5,20,35,50 — call SP_LOAD_ALL_FACTS
│               └── sync_dim_supabase.py        # 0 2 * * * — DIM sync nightly
│
├── markdown/                               # Detailed planning + spec docs
│   ├── 1. INFLUXDB.md                      # InfluxDB schema + connector + Flux flow
│   ├── 1. RECREATE_CODE.md                 # OLTP recreate spec
│   ├── 1. RECREATE_CODE_REPORT.md          # OLTP recreate execution report
│   ├── 2. AS_IS_DW_CODE.md                 # historical DW snapshot (pre-NEW_ARCH)
│   ├── 2. DOCUMENT_DW_RECREATE.md          # DW recreate execution report
│   ├── 2. ER_DIAGRAM_DW.md                 # ER diagram + relationships
│   ├── 2. RECREATE_DW.md                   # DW recreate spec
│   ├── 4. API.md                           # app/ deep-dive
│   └── 5. DASHBOARD_API_AI.md              # Streamlit build spec
│
├── test/                                   # Smoke tests (read-only)
│
├── .env                                    # Secrets (gitignored)
├── .env.example                            # Template
├── requirements.txt
└── README.md                               # this file
```

---

## 4. Data Flow (4 sub-flows)

### 4.1 Ingest — Supabase OLTP → Oracle STG (every 15 min)

```
┌────────────────────────────────┐
│  Airflow scheduler             │
│  cron: */15 * * * *            │
└───────────────┬────────────────┘
                │ trigger
                ▼
┌────────────────────────────────┐
│  etl_supabase_to_oracle DAG    │
│                                │
│  [1] check_oracle_api          │
│       (HTTP GET /health)       │
│                                │
│  [2-5] 4 parallel extracts:    │
│   ├─ extract_production_batch  │
│   ├─ extract_qc_record         │
│   ├─ extract_qc_defect         │
│   └─ extract_downtime_event    │
└───────────────┬────────────────┘
                │ psycopg2 + HTTP /sql/bulk-insert (truncate=True)
                ▼
┌────────────────────────────────┐
│  Oracle STG (truncate-load)    │
│   STG_PRODUCTION_BATCH         │
│   STG_QC_RECORD                │
│   STG_QC_DEFECT                │
│   STG_DOWNTIME_EVENT           │
└────────────────────────────────┘
```

### 4.2 Ingest — InfluxDB → Oracle STG_SENSOR_AGG (every 15 min)

```
┌────────────────────────────────┐
│  etl_influxdb_to_oracle DAG    │
│                                │
│  [1] check_oracle_api          │
│  [2] aggregate_sensor          │
│       Flux: aggregateWindow    │
│       (mean/min/max/count)     │
│       group by machine + field │
└───────────────┬────────────────┘
                │ HTTP /sql/bulk-insert
                │ + EXPECTED_METRICS guard
                │ + EXPECTED_MACHINES guard
                ▼
┌────────────────────────────────┐
│  STG_SENSOR_AGG                │
│   3 machines × 6 metrics       │
│   = ≤ 18 rows / window         │
└────────────────────────────────┘
```

### 4.3 Transform — STG → FACT (5 min after ingest)

```
┌────────────────────────────────┐
│  sp_load_dw DAG                │
│  cron: 5,20,35,50 * * * *      │
└───────────────┬────────────────┘
                │ HTTP /sp/call {"name":"SP_LOAD_ALL_FACTS"}
                ▼
┌────────────────────────────────┐
│  SP_LOAD_ALL_FACTS (master)    │
│   ├─ SP_LOAD_FACT_PRODUCTION   │
│   │   (FIFO derive + slippage) │
│   ├─ SP_LOAD_FACT_QUALITY      │
│   │   (defect_rate_pct calc)   │
│   ├─ SP_LOAD_FACT_DEFECT       │
│   │   (DIM_DEFECT_TYPE lookup) │
│   ├─ SP_LOAD_FACT_DOWNTIME     │
│   │   (closed events only)     │
│   └─ SP_LOAD_FACT_SENSOR       │
│       (composite delete-key)   │
│                                │
│  Pattern per SP:               │
│   1. DELETE FROM FACT          │
│      WHERE key IN (SELECT      │
│           key FROM STG)        │
│   2. FOR rec IN (SELECT FROM   │
│         STG JOIN DIM):         │
│        INSERT INTO FACT        │
│        VALUES (SEQ.NEXTVAL...) │
└───────────────┬────────────────┘
                ▼
┌────────────────────────────────┐
│  Oracle FACT (5 tables)        │
│   FACT_PRODUCTION   373 rows   │
│   FACT_QUALITY      373 rows   │
│   FACT_DEFECT        29 rows   │
│   FACT_DOWNTIME      26 rows   │
│   FACT_SENSOR     6,152 rows   │
└────────────────────────────────┘
```

### 4.4 DIM Sync — Supabase master → Oracle DIM (nightly 02:00 UTC)

```
┌────────────────────────────────┐
│  sync_dim_supabase DAG         │
│  cron: 0 2 * * *               │
│                                │
│  [1] check_oracle_api          │
│  [2-4] 3 parallel extracts:    │
│   ├─ extract_production_line   │
│   ├─ extract_battery_model     │
│   └─ extract_machine           │
│         ↓                      │
│  [5] sp_sync_all_dims          │
└───────────────┬────────────────┘
                ▼
┌────────────────────────────────┐
│  STG_LINE/BATTERY_MODEL/MACHINE│
│  → SP_SYNC_ALL_DIMS            │
│                                │
│  Pattern per SP:               │
│   MERGE INTO DIM USING (       │
│     SELECT ... FROM STG)       │
│   ON (d.src_id = s.src_id)     │
│   WHEN MATCHED THEN UPDATE SET │
│   WHEN NOT MATCHED THEN INSERT │
│     (SEQ.NEXTVAL, ...)         │
└───────────────┬────────────────┘
                ▼
┌────────────────────────────────┐
│  DIM_LINE / DIM_BATTERY_MODEL  │
│  / DIM_MACHINE (surrogate key  │
│  preserved across syncs)       │
└────────────────────────────────┘
```

### 4.5 Read — Streamlit → FastAPI → Oracle DW (real-time, cached 5 min)

```
┌─────────────────────────┐
│  Streamlit page         │
│  (Page 1/2/3)           │
└────────────┬────────────┘
             │ requests.get + Bearer token
             │ (via api_client.py — st.cache_data TTL=300s)
             ▼
┌─────────────────────────┐
│  FastAPI endpoint       │
│  /api/sensor/*          │
│  /api/scheduling/*      │
│  /api/analytics/*       │
└────────────┬────────────┘
             │ query_rows(sql, params)
             │ → coerce(v) per cell → JSON-safe
             ▼
┌─────────────────────────┐
│  Oracle DW              │
│  via JDBC thin (singleton OracleConnector) │
└────────────┬────────────┘
             │ list[dict] (lower-cased columns)
             ▼
┌─────────────────────────┐
│  Streamlit              │
│  pd.DataFrame → Plotly  │
└─────────────────────────┘
```

---

## 5. Database Layer (`db_module/`)

### 5.1 Connectors

3 connector classes — singleton-friendly, autocommit=False, context-manager จัดการ commit/rollback อัตโนมัติ

#### 5.1.1 `OracleConnector` ([db_module/db_conn/oracle/oracle_connection.py](db_module/db_conn/oracle/oracle_connection.py))

JDBC thin driver via `jaydebeapi` + `jpype`. ทำไมต้องใช้ JDBC:

- Oracle 10.2.0.3 เก่ากว่าที่ `python-oracledb` thin-mode รองรับ (ต้อง 12c+)
- ARM64 (Apple Silicon) ไม่มี Instant Client build → JDBC เป็นทางเดียวที่ใช้ได้

**JVM startup args** (`_JVM_ARGS`):

| Arg | ทำไมต้องใส่ |
|---|---|
| `-Doracle.jdbc.thinLogonCapability=o3` | บังคับ ojdbc8 รองรับ Oracle 10g logon protocol |
| `-Doracle.net.disableOob=true` | ปิด Out-of-Band breaks ป้องกันปัญหา network |
| `-Duser.language=en` | บังคับ ENGLISH locale (กัน Buddhist year) |
| `-Duser.country=US` | บังคับ Gregorian + ENGLISH |

**ALTER SESSION ทุก connection** (`_SESSION_NLS_STATEMENTS`):

```sql
ALTER SESSION SET NLS_CALENDAR='GREGORIAN';
ALTER SESSION SET NLS_DATE_LANGUAGE='ENGLISH';
ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS';
```

Default ของ KMITL server คือ `NLS_CALENDAR='THAI BUDDHA'` → วันที่ดึงออกมา offset +543 ปี

**Lifecycle:**
- JVM start ครั้งเดียวต่อ process (`jpype` constraint — classpath freeze หลัง start)
  → เปลี่ยน `ORACLE_JDBC_JAR` ต้อง restart Python
- Connection `autocommit=False` → ต้อง commit/rollback เอง
- `cursor()` context manager จัดการ transaction อัตโนมัติ

**Required env:**
- `ORACLE_HOST` (e.g. `161.246.35.92`)
- `ORACLE_PORT=1521`, `ORACLE_SERVICE=orcl`
- `ORACLE_USER=AI03`, `ORACLE_PASSWORD`
- `ORACLE_JDBC_JAR=db_module/db_conn/oracle/drivers/ojdbc8.jar`
- `JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`

#### 5.1.2 `SupabaseConnector` ([db_module/db_conn/supabases/supabase_connection.py](db_module/db_conn/supabases/supabase_connection.py))

`psycopg2` + SSL (`sslmode=require`). Connect ตรง PostgreSQL พอร์ต 5432
ไม่ผ่าน PostgREST เพราะ:

1. Connector ตัวเดียวใช้ซ้ำได้กับ Airflow PostgresHook
2. Bulk insert/COPY ผ่าน REST ไม่มีประสิทธิภาพ
3. ลด abstraction layer เวลา debug

**Required env:**
- `SUPABASE_HOST` (e.g. `db.<ref>.supabase.co` — IPv6 only!)
- `SUPABASE_PORT=5432`, `SUPABASE_USER=postgres`, `SUPABASE_PASSWORD`
- `SUPABASE_DB=postgres`, `SUPABASE_SSLMODE=require`

**IPv6 caveat:** Supabase direct host resolves IPv6-only — Docker compose network ต้อง enable IPv6 (กำหนดใน [docker-compose.yml](db_module/pipeline/docker-compose.yml))

#### 5.1.3 `InfluxConnector` ([db_module/db_conn/influxdb/influx_connection.py](db_module/db_conn/influxdb/influx_connection.py))

Wrapper บาง ๆ บน `influxdb_client.InfluxDBClient`:
- `query(flux: str)` → `list[FluxTable]` (ไม่ flatten DataFrame — preserve metadata)
- `client()` context manager สำหรับ raw API access (`write_api`, `delete_api`)

**Required env:**
- `INFLUX_URL` (e.g. `http://<ec2>:8086`), `INFLUX_TOKEN`
- `INFLUX_ORG=iiot_data_architecture` (default `factory`)
- `INFLUX_BUCKET=iiot_data_raw` (default `sensors`)

#### 5.1.4 `_env.py` helpers ([db_module/db_conn/_env.py](db_module/db_conn/_env.py))

| Function | Behavior |
|---|---|
| `load_dotenv(...)` | โหลด `.env` ครั้งเดียวตอน import |
| `get(name, default=None)` | อ่าน env; empty string = default (กัน config "ลืมกรอก") |
| `require(name)` | ถ้าขาด → raise `ConfigError` พร้อมแนะนำให้ copy `.env.example` |
| `resolve_path(raw)` | แปลง relative path → absolute (ยึด repo root) |

**สำคัญ:** Empty string = "ไม่ได้ตั้งค่า" — fail loud แทนที่จะ connect ไปที่ host เปล่า

---

### 5.2 Supabase OLTP Schema (12 tables)

3NF normalized + 2 triggers + 13 indexes. ER summary:

```
production_line ──< machine ──< downtime_event
                 │           >─< event_reason
                 │
                 └< production_batch ──< qc_record ──< qc_defect ──> defect_type
                  │              │             │                       (recursive)
                  │              │             └< batch_status_event ──> batch_status
                  │              └────> production_order ──> battery_model
```

#### 5.2.1 Master / Lookup tables (6)

| Table | PK | Purpose |
|---|---|---|
| **`production_line`** | `line_id SERIAL` | สายผลิต (1 row: COS) |
| **`machine`** | `machine_id SERIAL` | M01/M02/M03 — `machine_code UNIQUE` (ตรงกับ Influx tag) |
| **`battery_model`** | `model_id SERIAL` | 60AH/75AH/100AH — `model_code UNIQUE` + dimensions |
| **`defect_type`** | `defect_code VARCHAR PK` | Recursive (`parent_code FK self`) — 5 ROOT + 15 LEAF |
| **`batch_status`** | `status_code VARCHAR PK` | State machine: `PENDING/STARTED/PAUSED/COMPLETED/...` |
| **`event_reason`** | `reason_code VARCHAR PK` | Shared lookup สำหรับ batch event + downtime — `is_planned Y/N` |

#### 5.2.2 Transactional tables (6)

| Table | Grain | Key Columns | Constraints |
|---|---|---|---|
| **`production_order`** | 1 order = 1 SO | `order_id, model_id, qty_ordered, planned_start, planned_end` | `chk_planned_window: planned_end > planned_start` |
| **`production_batch`** | 1 batch = chunk of order | `batch_id, order_id, line_id, status_code, qty_planned, qty_out, start_time, end_time` | `chk_time_logic: end_time > start_time` |
| **`batch_status_event`** | 1 status change | `event_id, batch_id, status_code, reason_code, event_ts, notes` | append-only; `uq_batch_event_ts` UNIQUE (batch, ts) |
| **`qc_record`** | 1 inspection per batch | `qc_id, batch_id, qty_inspected, qty_passed, qty_failed, inspected_at` | `chk_qc_total: passed + failed = inspected` |
| **`qc_defect`** | M:N `qc × defect_type` | `qc_id, defect_code (composite PK), qty_affected, notes` | `qty_affected > 0` |
| **`downtime_event`** | 1 downtime = machine stop | `event_id, machine_id, batch_id?, reason_code, start_ts, end_ts?, duration_min` | `chk_downtime_window: end_ts > start_ts` |

#### 5.2.3 Triggers ([02_trigger_functions.sql](db_module/db_sources/supabases_sql_query/query/02_trigger_functions.sql))

**`fn_sync_batch_status` (AFTER INSERT batch_status_event):**

```sql
-- Auto sync production_batch.status + start_time + end_time
UPDATE production_batch
SET status_code = NEW.status_code,
    start_time = COALESCE(start_time,
        CASE WHEN NEW.status_code = 'STARTED' THEN NEW.event_ts END),
    end_time = CASE WHEN v_is_finished = 'Y' THEN NEW.event_ts ELSE end_time END
WHERE batch_id = NEW.batch_id;
```

**`fn_compute_downtime_duration` (BEFORE INSERT/UPDATE downtime_event):**

```sql
-- Auto-fill duration_min when end_ts is set
IF NEW.end_ts IS NOT NULL THEN
    NEW.duration_min := EXTRACT(EPOCH FROM (NEW.end_ts - NEW.start_ts)) / 60;
END IF;
```

#### 5.2.4 Mock data (8-day window)

Generated by [generate_mock_data.py](db_module/db_sources/supabases_sql_query/generate_mock_data.py):
- 2-shift continuous (DAY/NIGHT)
- Weighted product mix: 60AH 50% / 75AH 30% / 100AH 20%
- Beta-distributed yield rate (right-skewed, peak ~95%)
- 373 batches × ~3.4 status events/batch + 373 QC records + 29 defects + 26 downtime events

---

### 5.3 InfluxDB IIoT Schema

#### 5.3.1 Data layout

| Element | Value |
|---|---|
| **Bucket** | `iiot_data_raw` (default `sensors`) |
| **Org** | `iiot_data_architecture` (default `factory`) |
| **Measurement** | `station_1` |
| **Tag** | `machine_id` ∈ {M01, M02, M03} |
| **Frequency** | 1 Hz (1 sample/sec) |

#### 5.3.2 Fields (6 metrics)

| `_field` | Unit | Machine | Normal range | Critical |
|---|---|---|---|---|
| `temperature_c` | °C | M01 | 25–70 | 80 |
| `machine_state_num` | binary | All | 0/1 | — |
| `cycle_count` | count | M02 | 0–900 | — |
| `vibration_g` | g-force | M02 | 0–3 | 5 |
| `current_a` | A | M03 | 0–50 | 60 |
| `voltage_v` | V | M03 | 220–240 | 250 |

(จาก [DIM_METRIC seed](db_module/db_sources/oracle_sql_query/query/04_dim_seed.sql) — ต้อง match Influx field name เป๊ะ)

#### 5.3.3 Aggregation strategy (Flux)

ETL DAG ดึงข้อมูล 4 query แยก (mean, min, max, count) → join ใน Python:

```flux
from(bucket:"iiot_data_raw")
  |> range(start: 2026-04-30T00:00:00Z, stop: 2026-04-30T00:15:00Z)
  |> filter(fn:(r) => r._measurement == "station_1")
  |> aggregateWindow(every: 15m, fn: mean, createEmpty: false)
```

Output: `≤ 18 rows/window` (3 machines × 6 metrics)

**Validation guard** ([etl_influxdb_to_oracle.py:142-152](db_module/pipeline/airflow/dags/etl_influxdb_to_oracle.py#L142-L152)):

```python
EXPECTED_METRICS = {"temperature_c", "machine_state_num", "cycle_count",
                    "vibration_g", "current_a", "voltage_v"}
EXPECTED_MACHINES = {"M01", "M02", "M03"}
# raise ValueError ถ้า Influx schema drifts → กัน silent FACT_SENSOR empty
```

---

### 5.4 Oracle DW Schema (20 tables)

#### 5.4.1 Object inventory

| Type | Count | Names |
|---|---:|---|
| Tables | 20 | 7 DIM + 5 FACT + 8 STG |
| Sequences | 9 | 4 DIM + 5 FACT (DIM_DATE/SHIFT/METRIC ใช้ smart key ไม่มี SEQ) |
| Procedures | 10 | 4 SP_SYNC + 6 SP_LOAD |
| Functions | 1 | `FN_GET_SHIFT_ID(p_ts TIMESTAMP) RETURN NUMBER` |
| Indexes | 27 | FK + time-range + composite |
| Views | 0 | replaced by FastAPI analytics endpoints (AI03 ไม่มี CREATE VIEW) |

#### 5.4.2 DIM tables (7)

| Table | Surrogate Key | Source | Rows | Note |
|---|---|---|---:|---|
| **`DIM_DATE`** | `date_id` (smart key `YYYYMMDD`) | seeded 2024-2028 | 1,827 | natural JOIN, no SEQ |
| **`DIM_LINE`** | `line_id SEQ_DIM_LINE` | sync from Supabase `production_line` | 1 | `line_src_id UNIQUE` |
| **`DIM_SHIFT`** | `shift_id` (manual 1=DAY, 2=NIGHT) | seed inline | 2 | `crosses_midnight Y/N` |
| **`DIM_BATTERY_MODEL`** | `model_id SEQ_DIM_BATTERY_MODEL` | sync from Supabase `battery_model` | 3 | + derived `capacity_class`/`chemistry` |
| **`DIM_MACHINE`** | `machine_id SEQ_DIM_MACHINE` | sync from Supabase `machine` | 3 | denormalize `line_name` |
| **`DIM_METRIC`** | `metric_id` (manual 1-6) | seed inline | 6 | `metric_name` ตรง Influx `_field` |
| **`DIM_DEFECT_TYPE`** | `defect_id SEQ_DIM_DEFECT_TYPE` | seed inline (5 ROOT + 15 LEAF) | 20 | recursive (`parent_defect_id FK self`) |

**DIM_DATE smart key:** ทำให้ JOIN `fact.date_id = dim.date_id` natural — ไม่ต้องใช้ surrogate key แยก

**DIM_DEFECT_TYPE hierarchy** (5 categories × 3 leaves each):

```
TERMINAL ─ TERMINAL_LOOSE / TERMINAL_MISALIGNED / TERMINAL_BURN_MARK
COVER    ─ COVER_GAP / COVER_CRACK / COVER_MISFIT
WELDING  ─ WELD_INCOMPLETE / WELD_OVER / WELD_POROSITY
PLATE    ─ PLATE_REVERSED / PLATE_MISSING / PLATE_DOUBLE
CASING   ─ CASING_SCRATCH / CASING_CRACK / CASING_DEFORM
```

#### 5.4.3 FACT tables (5)

| Table | Grain | Key columns | Source | Current rows |
|---|---|---|---|---:|
| **`FACT_PRODUCTION`** | 1 batch | `prod_id, date_id, line_id, shift_id, model_id, batch_src_id, order_src_id, qty_planned, qty_out, duration_min, yield_rate, batch_planned_*, batch_est_duration_min, slippage_min, start_time, end_time` | `STG_PRODUCTION_BATCH` (where end_time NOT NULL) | 373 |
| **`FACT_QUALITY`** | 1 QC inspection (1:1 batch) | `quality_id, date_id, line_id, shift_id, model_id, qc_src_id, batch_src_id, qty_inspected, qty_passed, qty_failed, defect_rate_pct, inspected_at` | `STG_QC_RECORD JOIN STG_PRODUCTION_BATCH` | 373 |
| **`FACT_DEFECT`** | 1 defect type per QC (M:N junction) | `defect_fact_id, date_id, line_id, model_id, defect_id, qc_src_id, batch_src_id, qty_affected` | `STG_QC_DEFECT JOIN STG_QC_RECORD JOIN STG_PRODUCTION_BATCH` | 29 |
| **`FACT_DOWNTIME`** | 1 closed downtime event | `downtime_id, date_id, line_id, shift_id, machine_id, event_src_id, batch_src_id?, reason_code, is_planned, duration_min, start_ts, end_ts` | `STG_DOWNTIME_EVENT` (where end_ts NOT NULL) | 26 |
| **`FACT_SENSOR`** | 1 (machine × metric × 15-min window) | `sensor_id, date_id, machine_id, metric_id, window_start, window_end, avg_value, min_value, max_value, sample_count` | `STG_SENSOR_AGG` | 6,152 |

**FACT_PRODUCTION derived columns** (computed in `SP_LOAD_FACT_PRODUCTION`):
- `yield_rate = qty_out / qty_planned`
- `batch_planned_start/end` — FIFO split ของ order time window ตาม cumulative qty
- `batch_est_duration_min = order_duration × (batch_qty / order_total_qty)`
- `slippage_min = actual_duration - batch_est_duration`
- `shift_id = FN_GET_SHIFT_ID(start_time)`

#### 5.4.4 STG tables (8)

| Type | Tables | Lifecycle |
|---|---|---|
| **OLTP staging (5)** | `STG_PRODUCTION_BATCH`, `STG_QC_RECORD`, `STG_QC_DEFECT`, `STG_DOWNTIME_EVENT`, `STG_SENSOR_AGG` | Truncate-load every 15 min |
| **DIM source staging (3)** | `STG_LINE`, `STG_BATTERY_MODEL`, `STG_MACHINE` | Truncate-load nightly |

ทุก STG มี lineage columns: `src_system VARCHAR2(20)`, `pipeline_run_id VARCHAR2(50)`, `loaded_at TIMESTAMP DEFAULT SYSTIMESTAMP`

#### 5.4.5 Indexes (27)

จัดเป็น 4 กลุ่ม:

```sql
-- FACT_PRODUCTION (6 indexes — date/line/shift/model/batch/slippage)
CREATE INDEX idx_fp_date     ON FACT_PRODUCTION(date_id);
CREATE INDEX idx_fp_line     ON FACT_PRODUCTION(line_id);
CREATE INDEX idx_fp_shift    ON FACT_PRODUCTION(shift_id);
CREATE INDEX idx_fp_model    ON FACT_PRODUCTION(model_id);
CREATE INDEX idx_fp_batch    ON FACT_PRODUCTION(batch_src_id);
CREATE INDEX idx_fp_slippage ON FACT_PRODUCTION(slippage_min);

-- FACT_SENSOR (5 indexes — heaviest table)
CREATE INDEX idx_fs_window    ON FACT_SENSOR(window_start, window_end);
CREATE INDEX idx_fs_composite ON FACT_SENSOR(machine_id, metric_id, window_start);
-- + date_id / machine_id / metric_id

-- + FACT_QUALITY (5), FACT_DEFECT (5), FACT_DOWNTIME (6)
```

---

### 5.5 Stored Procedures (10 SP + 1 FN)

#### 5.5.1 `FN_GET_SHIFT_ID(p_ts TIMESTAMP) RETURN NUMBER`

```sql
-- DAY:   07:30 ≤ t < 16:30  → 1
-- NIGHT: t >= 17:30 OR t < 06:30 → 2
-- handover gap (06:30-07:30, 16:30-17:30) → NIGHT (simplification)
v_minutes_of_day := EXTRACT(HOUR FROM p_ts) * 60 + EXTRACT(MINUTE FROM p_ts);
IF v_minutes_of_day >= 450 AND v_minutes_of_day < 990 THEN
    RETURN 1;  -- DAY
ELSE
    RETURN 2;  -- NIGHT
END IF;
```

#### 5.5.2 DIM Sync SPs ([06_procedure_dim_sync.sql](db_module/db_sources/oracle_sql_query/query/06_procedure_dim_sync.sql))

| SP | Source STG | Target DIM | Special derivation |
|---|---|---|---|
| `SP_SYNC_DIM_LINE` | `STG_LINE` | `DIM_LINE` | `line_code = 'L' \|\| LPAD(line_id, 2, '0')`; derive `process_type` from name |
| `SP_SYNC_DIM_BATTERY_MODEL` | `STG_BATTERY_MODEL` | `DIM_BATTERY_MODEL` | derive `capacity_class` (`60AH→Standard`, `75AH→Premium`, `100AH→Heavy`); set `chemistry='Lead-Acid'` |
| `SP_SYNC_DIM_MACHINE` | `STG_MACHINE JOIN DIM_LINE` | `DIM_MACHINE` | lookup `line_id` (business→surrogate), denormalize `line_name` |
| `SP_SYNC_ALL_DIMS` | (master) | — | run `LINE → BATTERY_MODEL → MACHINE` (DIM_MACHINE FK depends on DIM_LINE) |

**Pattern:** MERGE BY `src_id` (โครงสร้างเดียวกันทุก SP):

```sql
MERGE INTO DIM_LINE d
USING (SELECT line_id AS src_id, ... FROM STG_LINE) src
ON (d.line_src_id = src.src_id)
WHEN MATCHED THEN UPDATE SET d.line_code = src.line_code, ...
WHEN NOT MATCHED THEN INSERT (...) VALUES (SEQ_DIM_LINE.NEXTVAL, ...);
```

#### 5.5.3 FACT Load SPs ([07_procedure_fact_load.sql](db_module/db_sources/oracle_sql_query/query/07_procedure_fact_load.sql))

| SP | Source STG | Target FACT | Special logic |
|---|---|---|---|
| `SP_LOAD_FACT_PRODUCTION` | `STG_PRODUCTION_BATCH` (end_time NOT NULL) | `FACT_PRODUCTION` | FIFO derive `batch_planned_*` + `batch_est_duration_min` + `slippage_min` |
| `SP_LOAD_FACT_QUALITY` | `STG_QC_RECORD JOIN STG_PRODUCTION_BATCH` | `FACT_QUALITY` | compute `defect_rate_pct = qty_failed / qty_inspected * 100` |
| `SP_LOAD_FACT_DEFECT` | `STG_QC_DEFECT JOIN STG_QC_RECORD JOIN STG_PRODUCTION_BATCH` | `FACT_DEFECT` | DIM_DEFECT_TYPE lookup by `defect_code`+`is_leaf='Y'` (skip orphan codes via flag pattern) |
| `SP_LOAD_FACT_DOWNTIME` | `STG_DOWNTIME_EVENT` (end_ts NOT NULL) | `FACT_DOWNTIME` | filter closed events only |
| `SP_LOAD_FACT_SENSOR` | `STG_SENSOR_AGG JOIN DIM_MACHINE/DIM_METRIC` | `FACT_SENSOR` | composite delete-key (machine_id, metric_id, window_start) |
| `SP_LOAD_ALL_FACTS` | (master) | — | order: PRODUCTION → QUALITY → DEFECT → DOWNTIME → SENSOR |

**Pattern (per SP):** DELETE-by-key + cursor FOR-LOOP INSERT:

```sql
-- Step 1: Idempotent delete
DELETE FROM FACT_PRODUCTION
WHERE batch_src_id IN (SELECT batch_id FROM STG_PRODUCTION_BATCH);

-- Step 2: Cursor loop (Oracle 10g forbids SEQ.NEXTVAL in INSERT...SELECT)
FOR rec IN (
    SELECT stg.*, ...
    FROM STG_PRODUCTION_BATCH stg
    WHERE stg.end_time IS NOT NULL
) LOOP
    -- DIM lookups
    SELECT line_id INTO v_dim_line_id
      FROM DIM_LINE WHERE line_src_id = rec.line_id;
    -- ... derive computed columns ...
    INSERT INTO FACT_PRODUCTION (...) VALUES (
        SEQ_FACT_PRODUCTION.NEXTVAL, v_dim_line_id, ...
    );
END LOOP;
COMMIT;
```

**Oracle 10g compatibility tricks:**

```sql
-- (a) SEQ.NEXTVAL in INSERT...SELECT → forbidden
-- WORKAROUND: cursor FOR-LOOP

-- (b) CONTINUE keyword → not available (added in 11g R1)
-- WORKAROUND: flag pattern
DECLARE v_skip BOOLEAN := FALSE;
BEGIN
    BEGIN SELECT ... INTO v_dim_defect_id FROM DIM_DEFECT_TYPE WHERE ...;
    EXCEPTION WHEN NO_DATA_FOUND THEN v_skip := TRUE;
    END;
    IF NOT v_skip THEN INSERT INTO ... END IF;
END;

-- (c) Variable name `current_date` clashes with Oracle built-in CURRENT_DATE
-- WORKAROUND: rename to `v_curr_date` (in 04_dim_seed.sql DECLARE)
```

---

## 6. Application Layer (`app/`)

### 6.1 FastAPI Service (10 endpoints)

**Run:**

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
.venv/bin/uvicorn app.api.main:app --host 0.0.0.0 --port 8000
```

**Auth:** Bearer token จาก `ORACLE_API_TOKEN` (ใส่ใน `.env`) — ทุก endpoint pass `Depends(require_token)`. ปล่อยว่าง = disable auth (dev mode)

#### 6.1.1 `dw_api/operational.py` — 3 generic endpoints (Airflow ใช้)

##### `GET /health`

```bash
curl -H "Authorization: Bearer $ORACLE_API_TOKEN" http://localhost:8000/health
```

Response:
```json
{"status": "ok", "oracle_user": "AI03",
 "oracle_sysdate": "2026-04-30T08:15:23",
 "jdbc_url": "jdbc:oracle:thin:@//161.246.35.92:1521/orcl"}
```

ใช้โดย: ทุก DAG (`check_oracle_api` task — short-circuit ถ้า DB down) + Streamlit sidebar

##### `POST /sp/call`

Body: `SpCallRequest`
```json
{"name": "SP_LOAD_ALL_FACTS", "args": []}
```

Generates: `BEGIN SP_LOAD_ALL_FACTS(?, ?, ...); END;`

ใช้โดย: `sp_load_dw` DAG, `sync_dim_supabase` DAG (call `SP_SYNC_ALL_DIMS`)

##### `POST /sql/bulk-insert`

Body: `BulkInsertRequest`
```json
{
  "table": "STG_PRODUCTION_BATCH",
  "columns": ["batch_id", "order_id", ...],
  "rows": [[1, 100, ...], [2, 100, ...]],
  "truncate": true,
  "pipeline_run_id": "manual__2026-04-30T..."
}
```

Behavior: ถ้า `truncate=True` → `TRUNCATE TABLE` ก่อน → `executemany INSERT`

ใช้โดย: ทุก ETL DAG (5 STG tables)

#### 6.1.2 `dashboard_api/dashboard.py` — 7 domain endpoints (Streamlit ใช้)

ทุก endpoint prefix `/api`. SQL constant อยู่ใน module level (cache โดย Python)

##### Sensor (Page 2)

**`GET /api/sensor/available-metrics`**

```sql
SELECT metric_id, metric_name, unit, machine_code, description
FROM DIM_METRIC ORDER BY metric_id
```

Use: dropdown source

**`GET /api/sensor/by-machine-15min?date=YYYY-MM-DD&metric=temperature_c`**

```sql
SELECT machine.machine_code, sensor.window_start, sensor.window_end,
       sensor.avg_value, sensor.min_value, sensor.max_value
  FROM FACT_SENSOR sensor
  JOIN DIM_MACHINE machine ON sensor.machine_id = machine.machine_id
  JOIN DIM_METRIC  metric  ON sensor.metric_id  = metric.metric_id
  JOIN DIM_DATE    date_dim ON sensor.date_id   = date_dim.date_id
 WHERE date_dim.full_date = ?
   AND metric.metric_name = ?
 ORDER BY sensor.window_start, machine.machine_code
```

Use: 7-day historical pull → Prophet input

##### Scheduling (Page 3)

**`GET /api/scheduling/batch-timeline?order_id=123`**

```sql
SELECT production.batch_src_id,
       production.batch_planned_start,
       production.batch_planned_end,
       production.start_time AS actual_start,
       production.end_time   AS actual_end,
       production.slippage_min
  FROM FACT_PRODUCTION production
 WHERE production.order_src_id = ?
 ORDER BY production.batch_planned_start
```

Use: Gantt drilldown (planned vs actual per batch)

##### Analytics (replace Oracle views)

**`GET /api/analytics/oee-daily?period=Last+7+days`**

OEE = Availability × Performance × Quality
- A = (planned_min - downtime_min) / planned_min
- P = qty_out / qty_planned
- Q = qty_passed / qty_inspected

3 sub-queries (`production_agg / quality_agg / downtime_agg`) `LEFT JOIN` ที่ (date_id, line_id, shift_id) — ดู [dashboard.py:148-224](app/api/dashboard_api/dashboard.py#L148-L224)

Period mapping (`_period_to_start_date_id`):

| `period` string | `start_date_id` (YYYYMMDD) |
|---|---|
| `Today` | today |
| `This week` | Monday of this ISO week |
| `Last 7 days` (default) | today - 7 days |
| `Last 30 days` | today - 7 days (current implementation — TODO: align with label) |

**`GET /api/analytics/defect-pareto?period=Last+7+days`**

```sql
SELECT defect_type.parent_code   AS category,
       defect_type.defect_code   AS defect_type,
       defect_type.severity,
       COUNT(*)                  AS occurrence_count,
       SUM(defect.qty_affected)  AS total_qty_affected
  FROM FACT_DEFECT      defect
  JOIN DIM_DEFECT_TYPE  defect_type ON defect.defect_id = defect_type.defect_id
 WHERE defect_type.is_leaf = 'Y' AND defect.date_id >= ?
 GROUP BY defect_type.parent_code, defect_type.defect_code, defect_type.severity
 ORDER BY total_qty_affected DESC
```

Pareto `pct_of_total` คำนวณฝั่ง client (Streamlit) — เลี่ยง `WINDOW FUNCTION` ที่ Oracle 10g รองรับไม่ครบ

**`GET /api/analytics/schedule-adherence?period=Last+7+days`**

```sql
SELECT production.prod_id, production.batch_src_id, production.order_src_id,
       model.model_code, prod_line.line_name, prod_shift.shift_name,
       production.qty_planned, production.qty_out,
       production.batch_planned_start, production.batch_planned_end,
       production.start_time AS actual_start, production.end_time AS actual_end,
       production.batch_est_duration_min AS planned_min,
       production.duration_min          AS actual_min,
       production.slippage_min,
       CASE WHEN production.slippage_min <=  5 THEN 'ON_TIME'
            WHEN production.slippage_min <= 15 THEN 'MINOR_LATE'
            ELSE 'LATE'
       END AS adherence_status,
       production.yield_rate
  FROM FACT_PRODUCTION    production
  JOIN DIM_BATTERY_MODEL  model      ON production.model_id = model.model_id
  JOIN DIM_LINE           prod_line  ON production.line_id  = prod_line.line_id
  JOIN DIM_SHIFT          prod_shift ON production.shift_id = prod_shift.shift_id
 WHERE production.date_id >= ?
 ORDER BY production.start_time
```

**`GET /api/analytics/batch-features`** (no period filter)

ML feature matrix — 21 features + 2 targets per batch (full table scan):
- `prod_id, batch_src_id, order_src_id, model_id, line_id, hour_of_day`
- `qty_planned, duration_min, batch_est_duration_min, slippage_min, slippage_ratio`
- `temp_avg/max/std, vib_avg/max/std, cycle_avg, current_avg/max, voltage_avg/min/max`
- targets: `defect_rate_pct, qty_failed`

ใช้โดย Streamlit Page 1 (aggregate → defect rate by model chart) — ดู [dashboard.py:341-395](app/api/dashboard_api/dashboard.py#L341-L395)

#### 6.1.3 `dw_api/deps.py` — 4 responsibilities

##### a) Auth — `require_token()`

```python
def require_token(authorization: str | None = Header(default=None)) -> None:
    expected = env_get("ORACLE_API_TOKEN")
    if not expected: return       # dev mode
    if authorization != f"Bearer {expected}":
        raise HTTPException(401, "invalid or missing bearer token")
```

##### b) Connection singleton — `get_connector()`

```python
_connector: OracleConnector | None = None
def get_connector() -> OracleConnector:
    global _connector
    if _connector is None:
        _connector = OracleConnector()    # lazy init: starts JVM
    return _connector
```

JVM start ครั้งเดียวต่อ process — เปลี่ยน `ORACLE_JDBC_JAR` ต้อง restart uvicorn

##### c) JDBC type coercion (2 ทิศทาง)

**Response — JDBC value → JSON-safe** (`coerce`):
- `date/datetime/time` → `isoformat()`
- `bool/int/float/str` → as-is
- `java.lang.String` หรือ Java types → `str(v)`

**Request — ISO string → java.sql.\*** (`parse_iso`):
- `^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}` → `JTimestamp.valueOf(...)`
- `^\d{4}-\d{2}-\d{2}$` → `JDate.valueOf(...)`

`OraclePreparedStatement.setObject(int, datetime.date)` ไม่มี overload — ต้องส่ง `java.sql.Date` object สร้างผ่าน `jpype.JClass`

##### d) Query helper — `query_rows(sql, params=None) -> list[dict]`

- Open-close connection ทุก call (ยังไม่มี pool — request rate ต่ำ ~1/min)
- Lower-case column names (Streamlit อ่าน `row["batch_src_id"]`)
- ใช้ทุก endpoint ใน `dashboard.py`

---

### 6.2 Streamlit Dashboard (3 pages)

**Run** (จาก `app/streamlit/` — `.streamlit/config.toml` ต้องอยู่ relative):

```bash
cd app/streamlit && ../../.venv/bin/streamlit run dashboard.py
```

#### 6.2.1 Entry — [dashboard.py](app/streamlit/dashboard.py)

- Sidebar: nav + FastAPI status indicator (`/health` live check)
- Landing: project info + nav

#### 6.2.2 Components

| Module | Role | Key functions |
|---|---|---|
| [`api_client.py`](app/streamlit/components/api_client.py) | HTTP wrapper | `get(endpoint, params)` — `@st.cache_data(ttl=300)` + Bearer header |
| [`cards.py`](app/streamlit/components/cards.py) | KPI rendering | `kpi_card(label, value)`, `kpi_row(items)`, `status_badge(text, status)` |
| [`charts.py`](app/streamlit/components/charts.py) | 7 Plotly builders | `oee_trend_chart`, `defect_pareto_chart`, `defect_rate_by_model_chart`, `forecast_chart`, `slippage_histogram`, `slippage_trend`, `batch_gantt` |
| [`filters.py`](app/streamlit/components/filters.py) | Filter rows | `period_selector`, `filter_row_oee_defect`, `filter_row_forecast`, `filter_row_schedule` |
| [`prophet_trainer.py`](app/streamlit/components/prophet_trainer.py) | Prophet | `trigger_training(machine, metric, df)` (daemon thread), `model_status(...)`, `predict(...)` |

**Chart palette** (consistent across pages):
- OEE: `#0F6E56` (green)
- Availability: `#534AB7` (purple dashed)
- Performance: `#185FA5` (blue dotted)
- Quality: `#BA7517` (orange dashdot)
- Late/critical: `#E24B4A` (red)
- On-time: `#1D9E75` (green)

#### 6.2.3 Page 1 — [1_oee_defect.py](app/streamlit/pages/1_oee_defect.py)

```
┌─────────────────────────────────────────────────────────┐
│  Filters: Period | Line | Shift | Battery model | ↻     │
├─────────────────────────────────────────────────────────┤
│  A: KPI cards                                           │
│  ┌──────┬──────┬──────┬──────┐                          │
│  │ OEE  │ Avail│ Perf │ Qual │ (% averaged over period) │
│  └──────┴──────┴──────┴──────┘                          │
├─────────────────────────────────────────────────────────┤
│  B: OEE trend (line chart, 4 series stacked)            │
├─────────────────────────────────────────────────────────┤
│  C: Defect Pareto (bar + cumulative % line, top 10)     │
├─────────────────────────────────────────────────────────┤
│  D: Defect rate by battery model (bar)                  │
├─────────────────────────────────────────────────────────┤
│  E: Defect detail table (sorted by qty_affected desc)   │
└─────────────────────────────────────────────────────────┘
```

**Data sources:**
- A + B: `/api/analytics/oee-daily`
- C + E: `/api/analytics/defect-pareto`
- D: `/api/analytics/batch-features` → groupby model_id → mean defect_rate_pct

#### 6.2.4 Page 2 — [2_sensor_forecast.py](app/streamlit/pages/2_sensor_forecast.py)

```
┌─────────────────────────────────────────────────────────┐
│  Filters: Machine | Metric | Horizon | [Train model]    │
├─────────────────────────────────────────────────────────┤
│  A: Status card                                         │
│  ┌─────────────────────────────────┬──────────────────┐ │
│  │ M01 / temperature_c             │  [Ready badge]   │ │
│  │ Trained at 2026-04-30 08:15     │                  │ │
│  └─────────────────────────────────┴──────────────────┘ │
├─────────────────────────────────────────────────────────┤
│  B: Forecast chart                                      │
│      - Historical (blue solid, 7 days × 4 windows/hr)   │
│      - Forecast (orange dashed, +6/12/24 hours)         │
│      - 95% confidence band (orange shaded)              │
│      - Critical threshold (red dashed line)             │
│      - "now" vertical line                              │
└─────────────────────────────────────────────────────────┘
```

**Flow:**
1. Pull 7-day historical: 7 × `/api/sensor/by-machine-15min?date=...&metric=...`
2. Filter rows ตาม `machine_code`
3. Rename `window_start → ds`, `avg_value → y` (Prophet input format)
4. Click "Train model" → `prophet_trainer.trigger_training()` → daemon thread fits Prophet, saves `cache/prophet_models/{machine}_{metric}.pkl`
5. Predict next N hours (15-min steps) → `predict()` → `forecast_chart()`

**Prophet config** ([prophet_trainer.py:89](app/streamlit/components/prophet_trainer.py#L89)):
```python
Prophet(daily_seasonality=True, weekly_seasonality=True, interval_width=0.95)
```

#### 6.2.5 Page 3 — [3_schedule_adherence.py](app/streamlit/pages/3_schedule_adherence.py)

```
┌─────────────────────────────────────────────────────────┐
│  Filters: Period | Line | Status | Model | ↻            │
├─────────────────────────────────────────────────────────┤
│  A: KPI cards                                           │
│  ┌─────────┬─────────┬───────────┬──────┐               │
│  │ Total   │ On-time │ Minor late│ Late │               │
│  └─────────┴─────────┴───────────┴──────┘               │
├──────────────────────────┬──────────────────────────────┤
│  B: Slippage histogram   │  C: Slippage trend           │
│  - 20 bins               │  - avg per date              │
│  - 5/15-min thresholds   │  - 0-line reference          │
├──────────────────────────┴──────────────────────────────┤
│  D: Order Gantt drilldown (selectbox)                   │
│  - planned (gray bar)                                   │
│  - actual (green if on-time, red if >15min late)        │
├─────────────────────────────────────────────────────────┤
│  E: Batch detail (top 50 by slippage desc)              │
└─────────────────────────────────────────────────────────┘
```

**Data sources:**
- A-C, E: `/api/analytics/schedule-adherence`
- D: `/api/scheduling/batch-timeline?order_id=...`

**Adherence thresholds:**
- `slippage_min ≤ 5` → ON_TIME
- `slippage_min ≤ 15` → MINOR_LATE
- otherwise → LATE

---

## 7. Pipeline Layer (Airflow 4 DAGs)

**Stack:** Apache Airflow 2.8 in Docker compose ([docker-compose.yml](db_module/pipeline/docker-compose.yml))

**UI:** http://localhost:8088 (login: `admin / admin`)

**Compose services:**
- `postgres-af` — Airflow metadata DB (Postgres 15)
- `airflow-init` — one-shot `airflow db migrate` + admin user creation
- `airflow-webserver` — UI (port 8088)
- `airflow-scheduler` — scheduler + LocalExecutor

**Network:** IPv6 enabled — Supabase direct host (`db.<ref>.supabase.co`) ตอบ IPv6 only, ต้องเปิด NAT6 บน compose network

**Container → host:** `host.docker.internal:host-gateway` — DAG ส่ง HTTP `http://host.docker.internal:8000` ไป host uvicorn

### 7.1 DAG inventory

| DAG ID | Schedule | Tasks | Latency target |
|---|---|---|---|
| `etl_supabase_to_oracle` | `*/15 * * * *` | 1 healthcheck → 4 parallel extracts | < 30s end-to-end |
| `etl_influxdb_to_oracle` | `*/15 * * * *` | 1 healthcheck → 1 aggregate | < 1 min |
| `sp_load_dw` | `5,20,35,50 * * * *` | 1 healthcheck → 1 SP call | < 30s |
| `sync_dim_supabase` | `0 2 * * *` (nightly) | 1 healthcheck → 3 parallel extracts → 1 SP_SYNC_ALL_DIMS | < 1 min |

### 7.2 Helper modules

#### `_oracle_api.py` ([db_module/pipeline/airflow/dags/_oracle_api.py](db_module/pipeline/airflow/dags/_oracle_api.py))

| Function | Purpose |
|---|---|
| `health()` | `GET /health` — return JSON |
| `bulk_insert(table, columns, rows, truncate, pipeline_run_id)` | `POST /sql/bulk-insert` — return `rowcount` |
| `call_sp(name, args=None)` | `POST /sp/call` — return `{ok, procedure}` |
| `as_iso(v)` | Serialize `datetime/date/Decimal` → JSON-safe before HTTP send |

Reads env: `ORACLE_API_URL` (default `http://host.docker.internal:8000`), `ORACLE_API_TOKEN`, `ORACLE_API_TIMEOUT=120`

#### `_supabase.py`

`supabase_cursor()` — `psycopg2` cursor context manager (auto commit/rollback)

### 7.3 Per-DAG details

#### `etl_supabase_to_oracle` ([etl_supabase_to_oracle.py](db_module/pipeline/airflow/dags/etl_supabase_to_oracle.py))

**Tasks:**

```
check_oracle_api
   ├─→ extract_production_batch  → STG_PRODUCTION_BATCH
   ├─→ extract_qc_record         → STG_QC_RECORD
   ├─→ extract_qc_defect         → STG_QC_DEFECT
   └─→ extract_downtime_event    → STG_DOWNTIME_EVENT
```

**Window:** `[data_interval_start, data_interval_end)` (15-min)

**SQL queries** (filter on `end_time / inspected_at / end_ts`):

```sql
-- production_batch (with order JOIN + cumulative qty)
SELECT b.batch_id, b.order_id, b.line_id, o.model_id,
       b.qty_planned, b.qty_out, b.start_time, b.end_time,
       o.planned_start, o.planned_end,
       (SELECT SUM(qty_planned) FROM production_batch
         WHERE order_id = b.order_id) AS order_total_qty
  FROM production_batch b
  JOIN production_order o ON b.order_id = o.order_id
 WHERE b.end_time IS NOT NULL
   AND b.end_time >= %s AND b.end_time < %s
```

**Important schema renames (2026-04-26):**

| Old | New |
|---|---|
| `product_id` | `model_id` |
| `qty_sampled` | `qty_inspected` |
| `machine_name` | `machine_code` |

#### `etl_influxdb_to_oracle` ([etl_influxdb_to_oracle.py](db_module/pipeline/airflow/dags/etl_influxdb_to_oracle.py))

**Flow:**
1. Query `mean / min / max / count` แยก 4 query (Flux `aggregateWindow`)
2. Join ใน Python ด้วย key `(machine, field, window_end)`
3. Validate `EXPECTED_METRICS` + `EXPECTED_MACHINES` — raise ValueError ถ้า drift
4. `bulk_insert STG_SENSOR_AGG`

**Important fix (2026-04-19):** ก่อนนี้ถ้า min/max key ไม่ match mean → fallback avg (silent → misleading column). ตอนนี้ใช้ `None` เพื่อ surface ปัญหา

**Override:** `INFLUX_RANGE_START` env var — สำหรับ ad-hoc test/backfill (เช่น `-6h`)

#### `sp_load_dw` ([sp_load_dw.py](db_module/pipeline/airflow/dags/sp_load_dw.py))

**Pattern:** schedule offset 5 min หลัง ETL (`5,20,35,50` vs `0,15,30,45`) ให้ STG พร้อมก่อน

**Single task:** `call_sp("SP_LOAD_ALL_FACTS")` → ภายใน orchestrate 5 SPs ต่อเนื่องตาม dependency

**Atomicity:** ถ้า SP ตัวใดตัวหนึ่ง raise → master rollback (PL/SQL atomic per call)

#### `sync_dim_supabase` ([sync_dim_supabase.py](db_module/pipeline/airflow/dags/sync_dim_supabase.py))

**Tasks:**

```
check_oracle_api
   ├─→ extract_production_line  → STG_LINE
   ├─→ extract_battery_model    → STG_BATTERY_MODEL
   └─→ extract_machine          → STG_MACHINE
                ↓ (all converge)
   sp_sync_all_dims (call SP_SYNC_ALL_DIMS)
```

**Pre-condition for FACT load:** DIM ต้องมี data ก่อน SP_LOAD_FACT_PRODUCTION (lookup จะ throw `NO_DATA_FOUND`) → ต้อง trigger DAG นี้ manual 1 ครั้งหลัง deploy ใหม่

---

## 8. Code Walkthroughs (key algorithms)

### 8.1 JDBC Type Coercion (2-way)

**ปัญหา:** JDBC + Python = type mismatch hell. Python `datetime.date` ไม่ work กับ `OraclePreparedStatement.setObject` overload resolver

**Solution** ([deps.py:64-139](app/api/dw_api/deps.py#L64-L139)):

```python
# OUT: JDBC value → JSON-safe primitive
def coerce(v: Any) -> Any:
    if v is None: return None
    if isinstance(v, (dt.date, dt.datetime, dt.time)): return v.isoformat()
    if isinstance(v, (bool, int, float, str)): return v
    return str(v)   # java.lang.String / etc → stringify

# IN: ISO string → java.sql.Date / java.sql.Timestamp
def parse_iso(v: Any) -> Any:
    if not isinstance(v, str): return v
    if re.match(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}", v):
        _, JTimestamp = _jdbc_types()       # lazy resolve via jpype.JClass
        return JTimestamp.valueOf(v.replace("T", " ").rstrip("Z"))
    if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
        JDate, _ = _jdbc_types()
        return JDate.valueOf(v)
    return v
```

**Why `valueOf` not `Date()`/`Timestamp()`:** `jaydebeapi.Date()` factory คืน string ที่ Oracle reject (`ORA-01861`). ต้องสร้าง `java.sql.Date` object ผ่าน `jpype.JClass` แล้วเรียก `.valueOf(str)` (Java factory)

### 8.2 SP_LOAD_FACT_PRODUCTION — FIFO derivation

**ปัญหา:** Order มี `planned_start/planned_end` แต่ batch ภายใน order ต้องคำนวณ "ช่วงเวลาแผนของ batch ตัวนี้" จาก FIFO order → ใช้ proportional split

**Algorithm:**

```sql
-- จาก order range [order_planned_start, order_planned_end] กับ order_total_qty
-- แต่ละ batch มี qty_planned ของตัวเอง

v_order_dur_min := (order_planned_end - order_planned_start) in minutes;
v_batch_share   := batch.qty_planned / order_total_qty;
v_batch_est_min := v_order_dur_min * v_batch_share;

-- cum_qty_before = sum of qty_planned ของ batches ที่ batch_id < ตัวนี้ (FIFO)
v_planned_start := order_planned_start
                 + (cum_qty_before / order_total_qty)
                 * (order_planned_end - order_planned_start);
v_planned_end   := v_planned_start + INTERVAL v_batch_est_min MINUTES;

-- slippage = actual - planned
v_actual_dur_min := (end_time - start_time) in minutes;
v_slippage_min   := v_actual_dur_min - v_batch_est_min;
```

**Example:** Order with 1000 units over 10 hours (600 min). 3 batches: B1=300, B2=400, B3=300.
- B1: share 30%, est 180 min, planned [00:00, 03:00]
- B2: share 40%, est 240 min, planned [03:00, 07:00]
- B3: share 30%, est 180 min, planned [07:00, 10:00]

### 8.3 DELETE-by-key idempotency (FACT load)

**ปัญหา:** DAG อาจ rerun overlapping window — ห้าม duplicate row

**Solution:**

```sql
-- Step 1: ลบ row ที่ business key ตรงกับ STG (ตอนนี้)
DELETE FROM FACT_PRODUCTION
WHERE batch_src_id IN (SELECT batch_id FROM STG_PRODUCTION_BATCH);

-- Step 2: INSERT ใหม่หมด
FOR rec IN (...) LOOP INSERT ... END LOOP;
```

ผลลัพธ์: รัน SP กี่ครั้งก็ได้ row count เดิม (idempotent)

**FACT_SENSOR variant — composite key:**

```sql
DELETE FROM FACT_SENSOR
WHERE (machine_id, metric_id, window_start) IN (
    SELECT dm.machine_id, dmt.metric_id, stg.window_start
      FROM STG_SENSOR_AGG stg
      JOIN DIM_MACHINE dm ON dm.machine_code = stg.machine_code
      JOIN DIM_METRIC  dmt ON dmt.metric_name = stg.metric_name
);
```

(เพราะ same `machine` × `metric` มีหลาย window — ต้อง 3-tuple)

### 8.4 MERGE BY src_id (DIM sync)

**ปัญหา:** ถ้า DELETE+INSERT — surrogate key (`line_id`) เปลี่ยน → FACT FK orphan

**Solution:**

```sql
MERGE INTO DIM_LINE d
USING (SELECT line_id AS src_id, ... FROM STG_LINE) src
ON (d.line_src_id = src.src_id)
-- Existing row: update attributes (surrogate key คงเดิม)
WHEN MATCHED THEN UPDATE SET d.line_code = src.line_code, ...
-- New row: assign new surrogate key
WHEN NOT MATCHED THEN INSERT (line_id, line_src_id, ...)
                      VALUES (SEQ_DIM_LINE.NEXTVAL, src.src_id, ...);
```

`line_src_id` (business key) UNIQUE → 1:1 mapping → surrogate `line_id` เสถียรข้าม sync

### 8.5 Prophet Trainer — Background daemon thread

**ปัญหา:** Prophet fit ใช้เวลา 5-30 วินาที (cmdstanpy compile ครั้งแรก ~10 นาที). UI ห้าม block

**Solution** ([prophet_trainer.py:75-132](app/streamlit/components/prophet_trainer.py#L75-L132)):

```python
_training_status: dict[str, str] = {}  # per-process state

def _train_in_thread(machine, metric, df):
    key = f"{machine}_{metric}"
    _training_status[key] = "training"
    try:
        model = Prophet(daily_seasonality=True, weekly_seasonality=True,
                        interval_width=0.95)
        model.fit(df)
        with open(model_path(machine, metric), "wb") as f:
            pickle.dump(model, f)
        _training_status[key] = "ready"
    except Exception as e:
        _training_status[key] = f"error: {e}"

def trigger_training(machine, metric, df):
    if df.empty or len(df) < 30:
        raise ValueError(f"Need ≥30 historical points; got {len(df)}")
    threading.Thread(target=_train_in_thread,
                     args=(machine, metric, df.copy()),
                     daemon=True).start()
```

UI poll `model_status(machine, metric)` ทุก rerun → แสดง badge "Training/Ready/Error"

### 8.6 Streamlit `pd.to_datetime(format="mixed")`

**ปัญหา:** Oracle TIMESTAMP บางแถวมี fractional seconds บางแถวไม่มี
→ `pd.to_datetime(s)` raise `ValueError: time data ... doesn't match format`

**Solution:**

```python
df["window_start"] = pd.to_datetime(df["window_start"], format="mixed")
```

`format="mixed"` (pandas 2.0+) — auto-detect per row

### 8.7 Plotly `add_vline(x=datetime)` workaround

**ปัญหา:** `add_vline(x=datetime, annotation_text=...)` → Plotly internal `_mean(x)` ทำ `0 + datetime` → `TypeError: unsupported operand`

**Solution** ([charts.py:135-145](app/streamlit/components/charts.py#L135-L145)):

```python
# แยก add_vline (เส้น) + add_annotation (label) เป็น 2 calls
fig.add_vline(x=now.isoformat(), line_dash="dash", line_color="#888780")
fig.add_annotation(x=now.isoformat(), y=1, yref="paper",
                   text="now", showarrow=False, ...)
```

### 8.8 PL/SQL block parser ([run_sql_file.py](db_module/db_sources/oracle_sql_query/run_sql_file.py))

**ปัญหา:** Oracle SQL file ผสม plain SQL (`;` terminator) กับ PL/SQL block (`/` terminator)

**Solution heuristic:**

```python
_PLSQL_BLOCK_STARTS = (
    "BEGIN", "DECLARE",
    "CREATE OR REPLACE PROCEDURE",   # ระบุ object type ชัด
    "CREATE OR REPLACE FUNCTION",
    "CREATE OR REPLACE TRIGGER",
    "CREATE OR REPLACE PACKAGE",
    # NOT "CREATE OR REPLACE" loose — would match `CREATE OR REPLACE VIEW`
    # which uses `;` not `/`
)
```

State machine:
- normal mode: collect lines, statement ends at `;` end-of-line
- หลังเจอ keyword ใน `_PLSQL_BLOCK_STARTS` → enter PL/SQL mode
- PL/SQL mode: collect ทุกอย่าง, terminator คือ `/` บรรทัดเดียว

---

## 9. Setup from Scratch

### 9.1 Prerequisites

- Python 3.12+
- Java 17 (for JDBC) — `brew install openjdk@17`
- Docker (for Airflow)
- KMITL Oracle access — credentials in `.env`
- Supabase project + InfluxDB instance (or use the one defined in `.env`)

### 9.2 Step 1 — Python env

```bash
python3.12 -m venv .venv
.venv/bin/python -m ensurepip                      # bootstrap pip if missing
.venv/bin/python -m pip install -r requirements.txt
# requirements.txt มี prophet>=1.1.5 อยู่แล้ว — install ครั้งแรกใช้ ~5-10 min compile cmdstanpy
```

### 9.3 Step 2 — Oracle JDBC driver

ดาวน์โหลด `ojdbc8.jar` (Oracle 19c+ จะ compatible ลง 10g):

```bash
mkdir -p db_module/db_conn/oracle/drivers
# วาง ojdbc8.jar ที่ db_module/db_conn/oracle/drivers/
```

### 9.4 Step 3 — `.env`

```bash
cp .env.example .env
# แก้ค่าจริง:
#   ORACLE_HOST/PORT/SERVICE/USER/PASSWORD/JDBC_JAR
#   JAVA_HOME (macOS: /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home)
#   SUPABASE_HOST/PASSWORD
#   INFLUX_URL/TOKEN
#   ORACLE_API_TOKEN (random string for auth)
```

### 9.5 Step 4 — Verify connectors

```bash
.venv/bin/python -m pytest test/test_connectors.py -v   # 3 connectors round-trip
.venv/bin/python test/test_connection.py                # Oracle 3-layer probe
```

### 9.6 Step 5 — Apply Supabase OLTP

```bash
.venv/bin/python db_module/db_sources/supabases_sql_query/apply_supabase.py
# applies in order:
#   01_schema.sql           → 12 tables + 13 indexes
#   02_trigger_functions.sql → 2 fn + 2 triggers
#   03_master_data.sql      → 1 line + 3 machines + 3 models + lookups (7+10+20)
#   04_mock_data.sql        → 8-day mock (373 batches)
```

### 9.7 Step 6 — Apply Oracle DW

```bash
for f in 01_schema_dim 02_schema_fact 03_schema_staging \
         04_dim_seed 05_indexes 06_procedure_dim_sync \
         07_procedure_fact_load; do
    .venv/bin/python db_module/db_sources/oracle_sql_query/run_sql_file.py \
        "db_module/db_sources/oracle_sql_query/query/${f}.sql"
done

.venv/bin/python db_module/db_sources/oracle_sql_query/verify_warehouse_schema.py
# Expected: OK — all expected objects present
#   20 tables + 9 sequences + 10 procs + 1 fn + 27 indexes
```

### 9.8 Step 7 — Sync DIMs (one-shot, must run before FACT load)

```bash
.venv/bin/python db_module/db_sources/oracle_sql_query/sync_dimensions_from_supabase.py
# Expected:
#   DIM_LINE = 1
#   DIM_BATTERY_MODEL = 3
#   DIM_MACHINE = 3
```

### 9.9 Step 8 — Start services (3 terminals)

```bash
# Terminal 1: FastAPI
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
.venv/bin/uvicorn app.api.main:app --host 0.0.0.0 --port 8000

# Terminal 2: Airflow
docker compose -f db_module/pipeline/docker-compose.yml up -d --build
# → http://localhost:8088 (admin/admin)

# Terminal 3: Streamlit
cd app/streamlit && ../../.venv/bin/streamlit run dashboard.py --server.port 8501
# → http://localhost:8501
```

### 9.10 Step 9 — Trigger ETL DAGs

ใน Airflow UI → unpause + trigger ทั้ง 4 DAGs ตามลำดับ:

1. `sync_dim_supabase` (รอ 30s) — DIM ต้องมีก่อน FACT
2. `etl_supabase_to_oracle` + `etl_influxdb_to_oracle` (parallel, รอ 2 นาที)
3. `sp_load_dw` (รอ 1 นาที) → FACT populated

### 9.11 Step 10 — Open dashboard

http://localhost:8501

ทดลอง:
- Page 1: เลือก period "Last 7 days" → ควรเห็น OEE ~70-90%, Pareto chart
- Page 2: เลือก M01 / temperature_c / 24 hours → คลิก "Train model" (~30s) → forecast chart
- Page 3: เลือก period → ดู slippage histogram + Gantt drilldown

---

## 10. Daily Operations

### 10.1 Routine

| Frequency | Task | How |
|---|---|---|
| Every 15 min | ETL ทำงานอัตโนมัติ | Airflow scheduler |
| Every 15 min (5 min after) | FACT load | `sp_load_dw` scheduled |
| Nightly 02:00 UTC | DIM sync | `sync_dim_supabase` scheduled |
| Manual | Refresh dashboard | คลิก ↻ ที่ filter row ของแต่ละ page |
| Manual | Train Prophet model | Page 2 → เลือก machine + metric → คลิก "Train model" |

### 10.2 Manual ETL trigger (run นอก schedule)

```bash
COMPOSE=db_module/pipeline/docker-compose.yml
docker compose -f $COMPOSE exec -T airflow-scheduler \
    airflow dags trigger <dag_id>
```

Available `dag_id`: `etl_supabase_to_oracle`, `etl_influxdb_to_oracle`, `sp_load_dw`, `sync_dim_supabase`

### 10.3 Backfill InfluxDB (ad-hoc)

```bash
# ใน .env หรือ shell ตั้ง override:
INFLUX_RANGE_START=-6h docker compose ... airflow dags trigger etl_influxdb_to_oracle
```

### 10.4 Manual SP call (debug)

```bash
TOKEN=$(grep ^ORACLE_API_TOKEN= .env | cut -d= -f2)
curl -X POST http://localhost:8000/sp/call \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "SP_LOAD_FACT_SENSOR", "args": []}'
```

---

## 11. Smoke Tests

### 11.1 Health checks

```bash
TOKEN=$(grep ^ORACLE_API_TOKEN= .env | cut -d= -f2)
BASE=http://localhost:8000

# 1. FastAPI + Oracle
curl -s "$BASE/health" -H "Authorization: Bearer $TOKEN" | jq '{status, oracle_user}'

# 2. Streamlit
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8501/

# 3. Airflow scheduler
docker compose -f db_module/pipeline/docker-compose.yml ps airflow-scheduler
```

### 11.2 Endpoint smoke (ทุก 7 dashboard endpoint)

```bash
TOKEN=$(grep ^ORACLE_API_TOKEN= .env | cut -d= -f2)
BASE=http://localhost:8000
DATE=$(date +%Y-%m-%d)

for ep in \
    "/api/sensor/available-metrics" \
    "/api/sensor/by-machine-15min?date=$DATE&metric=temperature_c" \
    "/api/scheduling/batch-timeline?order_id=1" \
    "/api/analytics/oee-daily?period=Last+7+days" \
    "/api/analytics/defect-pareto?period=Last+7+days" \
    "/api/analytics/schedule-adherence?period=Last+7+days" \
    "/api/analytics/batch-features"; do
    n=$(curl -s "$BASE${ep}" -H "Authorization: Bearer $TOKEN" | jq '(.rows // []) | length')
    echo "  $ep -> $n rows"
done
```

### 11.3 Data consistency (Supabase ↔ Oracle FACT)

```bash
.venv/bin/python -c "
from db_module.db_conn import SupabaseConnector, OracleConnector

# Supabase counts
print('=== Supabase ===')
with SupabaseConnector().cursor() as cur:
    queries = [
        ('production_batch (closed)', 'SELECT COUNT(*) FROM production_batch WHERE end_time IS NOT NULL'),
        ('qc_record',                  'SELECT COUNT(*) FROM qc_record'),
        ('qc_defect',                  'SELECT COUNT(*) FROM qc_defect'),
        ('downtime_event (closed)',    'SELECT COUNT(*) FROM downtime_event WHERE end_ts IS NOT NULL'),
    ]
    for name, sql in queries:
        cur.execute(sql); print(f'  {name}: {cur.fetchone()[0]}')

# Oracle FACT counts
print('=== Oracle FACT ===')
with OracleConnector().cursor() as cur:
    for table in ['FACT_PRODUCTION', 'FACT_QUALITY', 'FACT_DEFECT', 'FACT_DOWNTIME', 'FACT_SENSOR']:
        cur.execute(f'SELECT COUNT(*) FROM {table}'); print(f'  {table}: {cur.fetchone()[0]}')
"
```

Expected: ทุก count match (Supabase closed events = Oracle FACT counts)

### 11.4 Schema integrity ([verify_warehouse_schema.py](db_module/db_sources/oracle_sql_query/verify_warehouse_schema.py))

```bash
.venv/bin/python db_module/db_sources/oracle_sql_query/verify_warehouse_schema.py
# Checks:
#   20 tables (7 DIM + 5 FACT + 8 STG)
#   9 sequences (4 DIM + 5 FACT)
#   10 procedures + 1 function
```

---

## 12. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `ConfigError: Environment variable 'ORACLE_HOST' is not set` | `.env` ไม่ได้โหลด หรือ key ไม่มี | check `.env` exists + has all required vars |
| `Unable to locate a Java Runtime` | `JAVA_HOME` ไม่ตั้ง | `export JAVA_HOME=/opt/homebrew/opt/openjdk@17` หรือใส่ใน `.env` |
| `ORA-01861: literal does not match format string` | Python `datetime.date` ส่งตรง JDBC | ใช้ `prepare_row()` ใน `deps.py` — แปลงเป็น `java.sql.Date` |
| `ORA-00911: invalid character` ตอน apply view/PL/SQL | `_PLSQL_BLOCK_STARTS` match หลวม | ระบุ object type ชัด (PROCEDURE/FUNCTION/TRIGGER/PACKAGE) — `VIEW` ไม่ใช่ PL/SQL block |
| `PLS-00201: identifier 'CONTINUE' must be declared` | Oracle 10g ไม่มี CONTINUE keyword | ใช้ flag pattern (`v_skip BOOLEAN`) — ดู SP_LOAD_FACT_DEFECT |
| `ORA-01031: insufficient privileges` ตอน CREATE VIEW | AI03 ไม่มี CREATE VIEW privilege | views ถูก replace ด้วย FastAPI `/api/analytics/*` endpoints |
| `DIM_DATE.full_date = SYSDATE for all rows` | Variable `current_date` ชนกับ Oracle built-in `CURRENT_DATE` | renamed → `v_curr_date` |
| Streamlit `pd.to_datetime` ValueError | mixed format (some rows have `.fff` microseconds) | ใช้ `format="mixed"` |
| Plotly `add_vline` TypeError: `0 + datetime` | Plotly + pandas 2.x bug ใน `_mean(x)` | split `add_vline` + `add_annotation` แยก call |
| FastAPI cold start ช้า 2-3s | JVM start + ojdbc8 load | normal — singleton หลัง first request, latency กลับเป็น ~50ms |
| FACT empty หลัง trigger DAG | DAG window ไม่ตรง mock data range | manual full ETL หรือ backfill window ที่มี data |
| Streamlit `MissingSchema: Invalid URL 'None/health'` | `dashboard.py` ไม่ได้ load `.env` (sidebar status check) | ใส่ `load_dotenv()` ที่ top + fallback default `http://localhost:8000` |
| `Unknown metric_name(s) ใน Influx ไม่ตรง DIM_METRIC` | Influx schema drift (เพิ่ม metric ใหม่) | เพิ่มใน `EXPECTED_METRICS` + `DIM_METRIC` seed |
| Airflow container `connection refused` ไป localhost:8000 | inside container, `localhost` = container itself | ใช้ `host.docker.internal:8000` (set ใน `docker-compose.yml`) |
| Supabase `connection timed out` | Direct host `db.<ref>.supabase.co` IPv6-only | enable IPv6 ใน Docker network (กำหนดใน compose) |
| `prophet not installed` | `pip install` ก่อนหน้านี้ skip prophet | `.venv/bin/pip install prophet` (~5-10 min compile cmdstanpy) |

---

## 13. Constraints & Design Decisions

### 13.1 Oracle Server (10.2.0.3)

| Constraint | Workaround |
|---|---|
| ไม่รองรับ `CONTINUE` PL/SQL keyword (เพิ่มใน 11g R1) | flag pattern: `v_skip BOOLEAN := FALSE; ... IF NOT v_skip THEN ... END IF;` |
| ไม่รองรับ `SEQ.NEXTVAL ใน INSERT...SELECT` (เพิ่มใน 11g R2) | Cursor FOR-LOOP iterating STG → INSERT one by one |
| Default `NLS_CALENDAR = 'THAI BUDDHA'` | `ALTER SESSION SET NLS_CALENDAR='GREGORIAN'` ทุก connection |
| `python-oracledb` thin requires 12c+ | JDBC thin via `jaydebeapi` + `ojdbc8.jar` + `jpype` |
| AI03 user lacks `CREATE VIEW` privilege | views replaced by FastAPI `/api/analytics/*` endpoints |
| Variable `current_date` ชนกับ built-in `CURRENT_DATE` ใน MERGE INSERT VALUES | rename → `v_curr_date` |
| LIMITED window function support (no `PERCENT_RANK`/`CUME_DIST`?) | client-side computation in FastAPI/Streamlit |

### 13.2 JDBC + JVM

| Constraint | Detail |
|---|---|
| `thinLogonCapability=o3` | Required for Oracle 10g logon protocol — ไม่ใส่จะ login ไม่ผ่าน |
| `disableOob=true` | Prevent Out-of-Band breaks ใน some firewalled networks |
| `user.language=en, user.country=US` | Prevent Buddhist year on wire |
| JVM start ครั้งเดียวต่อ process | jpype constraint — classpath freeze; restart uvicorn เพื่อเปลี่ยน jar |
| `OraclePreparedStatement.setObject(int, datetime.date)` ไม่มี overload | สร้าง `java.sql.Date` ผ่าน `JDate.valueOf(str)` (lazy via jpype) |

### 13.3 Schema Design

- **Smart key for DIM_DATE** (`YYYYMMDD` integer) — natural JOIN, no surrogate
- **MERGE BY src_id** in SP_SYNC_DIM_* — preserve surrogate keys across sync
- **Truncate-and-load STG** — buffer last 15-min window only
- **DELETE-by-key + INSERT** in SP_LOAD_FACT_* — idempotent (rerun ไม่ duplicate)
- **Denormalize `line_name` in DIM_MACHINE** — query saves a JOIN
- **Composite delete-key for FACT_SENSOR** — `(machine_id, metric_id, window_start)` (same machine×metric has many windows)
- **DIM_DEFECT_TYPE recursive but denormalize `parent_code`** — query simpler than CONNECT BY

### 13.4 App Architecture

- **FastAPI HTTP wrapper** — avoid bundling Java in Airflow Docker image
- **OracleConnector singleton** — single uvicorn process; query rate ~1 req/min ไม่ต้องการ pool
- **Streamlit cache TTL 5 min** — DAG cadence 15 min; refresh button clears cache instantly
- **Prophet daemon thread training** — UI ไม่ block; status badge แสดง progress
- **Pages auto-refresh** disabled — user-driven refresh ผ่าน ↻ button

### 13.5 Airflow Pipeline

- **5-min offset** ระหว่าง ETL (`*/15`) และ FACT load (`5,20,35,50`) — STG พร้อมก่อน
- **3-tier retry**: `retries=2, retry_delay=2min` for ETL; `5min` for DIM sync
- **`max_active_runs=1`** — กัน DAG ซ้อนกันถ้า run ก่อนหน้านาน
- **`catchup=False`** — ไม่ backfill ตั้งแต่ `start_date` (ไม่จำเป็นเพราะ STG truncate-load)
- **Schema validation guards** ใน Influx DAG — fail loud ถ้า drift

---

## 14. Performance Characteristics

### 14.1 ETL latency targets

| Step | Target | Actual (mock data) |
|---|---|---|
| Supabase extract (4 tasks parallel) | < 30s | ~5-10s |
| InfluxDB Flux aggregate | < 1 min | ~10-30s |
| `SP_LOAD_ALL_FACTS` | < 30s | ~10-20s |
| **End-to-end (data → FACT)** | **< 6 min** | **~30s-1min** |

### 14.2 Dashboard query latency

| Endpoint | Target | Note |
|---|---|---|
| `/health` | < 100ms (warm) | JVM cold start: ~2-3s |
| `/api/sensor/available-metrics` | < 50ms | 6 rows |
| `/api/sensor/by-machine-15min` | < 200ms | ~280 rows/day |
| `/api/analytics/oee-daily` | < 500ms | ~50 rows/week |
| `/api/analytics/defect-pareto` | < 200ms | ≤ 20 rows |
| `/api/analytics/batch-features` | < 1s | full FACT_PRODUCTION JOIN FACT_QUALITY JOIN FACT_SENSOR |
| `/api/scheduling/batch-timeline` | < 100ms | ~5-10 rows/order |

### 14.3 Streamlit caching

```python
@st.cache_data(ttl=300)              # 5 min
def get(endpoint, params=None):
    ...
```

Hit rate target: ~95% (DAG cadence 15 min vs cache 5 min)

### 14.4 Storage growth (current production)

| Table | Rows | Growth/day (8d sample) |
|---|---:|---:|
| FACT_PRODUCTION | 373 | ~46 batches |
| FACT_QUALITY | 373 | ~46 inspections |
| FACT_DEFECT | 29 | ~3.6 defects |
| FACT_DOWNTIME | 26 | ~3.2 events |
| **FACT_SENSOR** | **6,152** | **~770 windows** |

FACT_SENSOR คือ heavy — 3 machines × 6 metrics × 96 windows/day × 365 days ≈ 631K rows/year per machine. Growth manageable on KMITL Oracle.

---

## 15. Glossary

| Term | Meaning |
|---|---|
| **MES** | Manufacturing Execution System — system controlling shop floor production |
| **OLTP** | Online Transactional Processing — Supabase Postgres in this project |
| **DW** | Data Warehouse — Oracle 10g AI03 schema |
| **OEE** | Overall Equipment Effectiveness = Availability × Performance × Quality |
| **Availability** | (planned_min - downtime_min) / planned_min |
| **Performance** | qty_out / qty_planned |
| **Quality** | qty_passed / qty_inspected |
| **Defect Pareto** | 80/20 rule — top defect types contribute most of total defects |
| **Slippage** | actual_duration - planned_duration (positive = late, negative = early) |
| **Adherence Status** | `ON_TIME` (≤5 min) / `MINOR_LATE` (≤15) / `LATE` (>15) |
| **Surrogate key** | DW-internal sequence-generated PK (vs. business key from OLTP) |
| **Smart key** | Natural-meaningful key (e.g. `DIM_DATE.date_id = 20260430`) |
| **Degenerate dim** | Business key kept in FACT but no DIM table (e.g. `batch_src_id`) |
| **Conformed dim** | Same DIM shared across multiple FACTs (e.g. `DIM_DATE` in all 5 FACTs) |
| **Junk dim** | Small fixed-set dim (e.g. `DIM_SHIFT` with DAY/NIGHT) |
| **Truncate-and-load** | STG pattern — DELETE all → INSERT new (not historical) |
| **Idempotent** | Run multiple times = same result (key for retry-safe DAGs) |
| **JDBC thin** | Pure-Java Oracle driver — no Instant Client native lib needed |
| **jpype** | Python-to-JVM bridge — single JVM per process |
| **Bearer token** | HTTP auth scheme: `Authorization: Bearer <token>` |
| **Prophet** | Facebook's time-series library (cmdstanpy backend) |
| **cmdstanpy** | Stan compiler for Prophet (compile time ~5-10 min ครั้งแรก) |
| **`ds`/`y`** | Prophet input format: `ds` = datetime, `y` = numeric target |
| **15-min window** | Standard ETL cadence + Influx aggregation interval |
| **Flux** | InfluxDB 2.x query language (functional, pipe-based) |
| **MERGE BY src_id** | Upsert pattern preserving surrogate keys |
| **DELETE-by-key + INSERT** | Idempotent FACT load pattern (Oracle 10g compatible) |
| **flag pattern** | PL/SQL workaround for missing CONTINUE keyword (use BOOLEAN) |
| **NLS** | National Language Support — Oracle locale settings |

---

## License & Credits

KMITL Computer Science — IIoT Data Architecture project (2026)

Built by [@SnipeBoss](https://github.com/SnipeBoss) with assistance from Claude Code.

For deeper docs, see:
- [`markdown/2. ER_DIAGRAM_DW.md`](markdown/2.%20ER_DIAGRAM_DW.md) — full ER diagram
- [`markdown/2. RECREATE_DW.md`](markdown/2.%20RECREATE_DW.md) — DW recreate spec
- [`markdown/4. API.md`](markdown/4.%20API.md) — FastAPI deep-dive
- [`markdown/5. DASHBOARD_API_AI.md`](markdown/5.%20DASHBOARD_API_AI.md) — Streamlit build spec
