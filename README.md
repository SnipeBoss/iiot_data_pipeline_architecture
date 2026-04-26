# Unified IIoT Data Architecture

End-to-end data pipeline สำหรับ Battery Assembly Line COS (1 line, 3 machines: M01/M02/M03)
ที่ ingest ข้อมูลจาก 2 sources (Supabase OLTP + InfluxDB sensor) → Oracle 10g Data Warehouse
(Kimball star schema) → FastAPI HTTP layer → Streamlit dashboard 3 pages

```
[Supabase OLTP]   [InfluxDB sensor]
       |                |
       +---> Airflow ETL (every 15 min) 
                        |
                        v
              Oracle 10g DW (AI03)
              7 DIM + 5 FACT + 8 STG
                        |
                        v
                FastAPI (port 8000)
                10 endpoints
                        |
                        v
              Streamlit (port 8501)
              3 pages + Prophet forecast
```

> **Last updated:** 2026-04-26 (post-cleanup, dead code removed)
> **Production data:** 373 batches, 6152 sensor windows over 8 days

---

## Table of Contents

1. [Stack Overview](#1-stack-overview)
2. [Repository Layout](#2-repository-layout)
3. [Data Flow](#3-data-flow)
4. [Database Layer (`db_module/`)](#4-database-layer-db_module)
5. [Application Layer (`app/`)](#5-application-layer-app)
6. [Pipeline Layer (Airflow)](#6-pipeline-layer-airflow)
7. [Oracle DW Schema](#7-oracle-dw-schema)
8. [Setup from Scratch](#8-setup-from-scratch)
9. [Daily Operations](#9-daily-operations)
10. [Smoke Tests](#10-smoke-tests)
11. [Troubleshooting](#11-troubleshooting)
12. [Constraints & Design Decisions](#12-constraints--design-decisions)

---

## 1. Stack Overview

| Layer | Tech | Role | Hosted |
|---|---|---|---|
| **OLTP source** | PostgreSQL via Supabase | 12-table MES (orders, batches, QC, downtime) | Cloud |
| **IIoT source** | InfluxDB 2.x + Telegraf + Node-RED + Mosquitto | 1 Hz sensor (6 fields × 3 machines) | AWS EC2 |
| **DW** | Oracle 10.2.0.3 (`AI03`) | Kimball star schema, 20 tables | KMITL `161.246.35.92:1521/orcl` |
| **HTTP layer** | FastAPI + uvicorn | JDBC wrapper + analytics endpoints | host (port 8000) |
| **Dashboard** | Streamlit + Plotly + Prophet | 3 pages (OEE / Forecast / Schedule) | host (port 8501) |
| **Pipeline** | Apache Airflow 2.8 | 4 DAGs (3 ETL @15min + 1 DIM sync nightly) | Docker compose (port 8088) |
| **Driver** | JDBC thin (`ojdbc8.jar`) via `jaydebeapi` + `jpype` | Oracle 10g connectivity | host JVM |

**Why this stack:**

- **Oracle 10g** — ผูกกับ KMITL infra ที่ใช้ได้ฟรี; ตัวชอบ Java JDBC (python-oracledb thin requires 12c+)
- **FastAPI HTTP wrapper** — Airflow Docker image ไม่มี Java; bundle ojdbc8 ทุก container แพง → ห่อ JDBC เป็น HTTP service บน host
- **Streamlit** — รวดเร็วในการสร้าง analytics UI; ไม่ต้องเขียน frontend แยก
- **Prophet** — defect forecast time-series; cmdstanpy backend (compile ครั้งแรก ~10 นาที)

---

## 2. Repository Layout

```
unified_iiot_data_architecture/
├── app/                                       # Application layer
│   ├── __init__.py
│   ├── api/                                   # FastAPI service (port 8000)
│   │   ├── main.py                            # FastAPI() + 2 router includes
│   │   ├── dw_api/                            # Oracle core (Airflow callers)
│   │   │   ├── deps.py                        # auth + connector singleton + JDBC type coercion + query_rows
│   │   │   ├── models.py                      # 2 Pydantic models (SpCallRequest, BulkInsertRequest)
│   │   │   └── operational.py                 # /health + /sp/call + /sql/bulk-insert
│   │   └── dashboard_api/
│   │       └── dashboard.py                   # 7 domain endpoints (4 analytics + 2 sensor + 1 scheduling)
│   │
│   └── streamlit/                             # Multi-page dashboard (port 8501)
│       ├── .streamlit/config.toml             # theme + server config (must run from this dir)
│       ├── dashboard.py                       # Entry: sidebar nav + landing (renders README)
│       ├── components/
│       │   ├── api_client.py                  # cached HTTP wrapper (TTL 5 min)
│       │   ├── cards.py                       # KPI card renderers
│       │   ├── charts.py                      # 7 Plotly chart builders
│       │   ├── filters.py                     # 3 page-level filter rows
│       │   └── prophet_trainer.py             # Prophet train (background thread) + cache + predict
│       ├── pages/
│       │   ├── 1_oee_defect.py                # Page 1: OEE + Defect (4 KPI + trend + Pareto + model defect + table)
│       │   ├── 2_sensor_forecast.py           # Page 2: Sensor forecast (status card + Prophet chart)
│       │   └── 3_schedule_adherence.py        # Page 3: Schedule (KPI + histogram + trend + Gantt + table)
│       └── cache/prophet_models/              # Prophet .pkl cache (gitignored)
│
├── db_module/                                 # Database layer
│   ├── db_conn/                               # 3 connector classes (singleton-friendly)
│   │   ├── _env.py                            # .env loader + require/get/resolve_path helpers
│   │   ├── oracle/oracle_connection.py        # OracleConnector — JDBC thin via jaydebeapi/jpype
│   │   │   └── drivers/ojdbc8.jar             # NOTE: ดาวน์โหลดเอง (gitignored)
│   │   ├── supabases/supabase_connection.py   # SupabaseConnector — psycopg2 + SSL
│   │   └── influxdb/influx_connection.py      # InfluxConnector — influxdb-client (Flux)
│   │
│   ├── db_sources/
│   │   ├── oracle_sql_query/                  # DW provisioning + admin
│   │   │   ├── query/                         # 7 SQL files apply ตาม order
│   │   │   │   ├── 01_schema_dim.sql          # 7 DIM + 4 sequences
│   │   │   │   ├── 02_schema_fact.sql         # 5 FACT + 5 sequences
│   │   │   │   ├── 03_schema_staging.sql      # 8 STG (5 OLTP + 3 DIM source)
│   │   │   │   ├── 04_dim_seed.sql            # MERGE INTO DIM_DATE/SHIFT/METRIC/DEFECT
│   │   │   │   ├── 05_indexes.sql             # 27 indexes (FK + time-range + composite)
│   │   │   │   ├── 06_procedure_dim_sync.sql  # 4 SP_SYNC_DIM_*
│   │   │   │   └── 07_procedure_fact_load.sql # 1 FN + 6 SP_LOAD_FACT_*
│   │   │   ├── run_sql_file.py                # DDL applier (handles PL/SQL `/` terminator)
│   │   │   ├── verify_warehouse_schema.py     # checks 20 tables + 9 SEQ + 10 PROC + 1 FN exist
│   │   │   └── sync_dimensions_from_supabase.py  # CLI DIM sync (alt to nightly DAG)
│   │   │
│   │   ├── supabases_sql_query/               # OLTP provisioning + mock data
│   │   │   ├── query/
│   │   │   │   ├── 01_schema.sql              # 12-table MES schema
│   │   │   │   ├── 02_trigger_functions.sql   # 2 trigger fn + 2 triggers (status sync, downtime duration)
│   │   │   │   ├── 03_master_data.sql         # 1 line + 3 machines + 3 models + lookups
│   │   │   │   └── 04_mock_data.sql           # generated mock (8-day window)
│   │   │   ├── apply_supabase.py              # transactional apply 4 files + audit
│   │   │   └── generate_mock_data.py          # generator for 04_mock_data.sql
│   │   │
│   │   └── iiot_container/                    # Edge stack (Node-RED + Mosquitto + Telegraf)
│   │
│   └── pipeline/                              # Airflow orchestration
│       ├── docker-compose.yml                 # Airflow 2.8 stack (scheduler + webserver:8088)
│       └── airflow/dags/
│           ├── _oracle_api.py                 # HTTP helper: bulk_insert, call_sp, health, as_iso
│           ├── _supabase.py                   # psycopg2 cursor wrapper
│           ├── etl_supabase_to_oracle.py      # OLTP → STG (4 tasks parallel, every 15 min)
│           ├── etl_influxdb_to_oracle.py      # Influx → STG_SENSOR_AGG (every 15 min)
│           ├── sp_load_dw.py                  # call SP_LOAD_ALL_FACTS (5,20,35,50 * * * *)
│           └── sync_dim_supabase.py           # nightly DIM sync (02:00 UTC)
│
├── markdown/                                  # Project docs + spec files
│   ├── 1. INFLUXDB.md                         # InfluxDB schema + connector + flow
│   ├── 1. RECREATE_CODE.md                    # OLTP recreate spec
│   ├── 2. AS_IS_DW_CODE.md                    # historical snapshot
│   ├── 2. DOCUMENT_DW_RECREATE.md             # DW recreate execution report
│   ├── 2. RECREATE_DW.md                      # DW recreate spec
│   ├── 4. API.md                              # app/ explanation (full)
│   ├── 5. DASHBOARD_API_AI.md                 # Streamlit build spec
│   └── dw_schema_compact.sql                  # all 7 SQL files concatenated
│
├── test/                                      # Smoke tests (read-only)
│   ├── test_connection.py                     # 3-layer Oracle probe
│   ├── test_connectors.py                     # pytest round-trip 3 connectors
│   └── test_create_table.py                   # Oracle write access (CREATE/INSERT/DROP)
│
├── .env                                       # secrets (gitignored)
├── .env.example                               # template
├── requirements.txt
└── README.md                                  # this file
```

---

## 3. Data Flow

### 3.1 Ingest (every 15 min)

```
[Supabase OLTP]                              [InfluxDB sensor 1Hz]
   |                                              |
   | psycopg2                                     | influxdb-client (Flux)
   v                                              v
[etl_supabase_to_oracle DAG]                  [etl_influxdb_to_oracle DAG]
   4 parallel extract tasks                       1 task: aggregateWindow 15m
   - production_batch + JOIN order              (mean/min/max/count for
   - qc_record                                    each machine × field)
   - qc_defect (JOIN qc_record for window)       
   - downtime_event (JOIN machine + reason)      
   |                                              |
   | HTTP POST /sql/bulk-insert (truncate=True)   |
   v                                              v
[Oracle STG_*]
  STG_PRODUCTION_BATCH, STG_QC_RECORD,
  STG_QC_DEFECT, STG_DOWNTIME_EVENT,
  STG_SENSOR_AGG
```

### 3.2 Transform (5 minutes after ingest)

```
[sp_load_dw DAG @ 5,20,35,50 * * * *]
   |
   | HTTP POST /sp/call {"name":"SP_LOAD_ALL_FACTS"}
   v
[Oracle SP_LOAD_ALL_FACTS]
   PRODUCTION → QUALITY → DEFECT → DOWNTIME → SENSOR
   each SP: DELETE-by-key + INSERT cursor FOR-LOOP
            (Oracle 10g forbids SEQ.NEXTVAL ใน INSERT...SELECT)
   |
   v
[Oracle FACT_*]
  FACT_PRODUCTION (1 row = 1 batch)
  FACT_QUALITY    (1 row = 1 QC record)
  FACT_DEFECT     (1 row = 1 defect type per QC, M:N junction)
  FACT_DOWNTIME   (1 row = 1 closed downtime event)
  FACT_SENSOR     (1 row = 1 machine × metric × 15-min window)
```

### 3.3 Dimension sync (nightly 02:00 UTC)

```
[sync_dim_supabase DAG]
   3 parallel: extract_production_line / battery_model / machine
   |
   v HTTP POST /sql/bulk-insert
[STG_LINE, STG_BATTERY_MODEL, STG_MACHINE]
   |
   v HTTP POST /sp/call SP_SYNC_ALL_DIMS
[DIM_LINE, DIM_BATTERY_MODEL, DIM_MACHINE]
   MERGE BY src_id (preserve surrogate keys → FACT FK ไม่ orphan)
```

### 3.4 Read (real-time)

```
[Streamlit page]
   |
   | requests.get + Bearer token
   v
[FastAPI endpoint]
   |
   v query_rows(sql, params)  --→ JDBC → Oracle DW
   v coerce(v) → JSON-safe values
   |
   v
[Streamlit] pd.DataFrame → Plotly chart
```

---

## 4. Database Layer (`db_module/`)

### 4.1 Connectors ([`db_module/db_conn/`](db_module/db_conn/))

3 connector classes สำหรับเชื่อม database 3 ตัว:

#### `OracleConnector` — [`oracle/oracle_connection.py`](db_module/db_conn/oracle/oracle_connection.py)

JDBC thin driver via jaydebeapi + jpype. ทำไม:

- Oracle 10.2.0.3 เก่ากว่าที่ python-oracledb thin-mode รองรับ (12c+)
- ARM64 (Apple Silicon) ไม่มี Instant Client build

**JVM startup args** (`_JVM_ARGS`):
- `thinLogonCapability=o3` — บังคับ ojdbc8 รองรับ 10g
- `disableOob=true` — ปิด Out-of-Band breaks
- `user.language=en, user.country=US` — บังคับ Gregorian + ENGLISH (กัน Buddhist year)

**ALTER SESSION** (`_SESSION_NLS_STATEMENTS`) ทุก connection:
- `NLS_CALENDAR='GREGORIAN'`
- `NLS_DATE_LANGUAGE='ENGLISH'`
- `NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'`

**Lifecycle:**
- JVM start ครั้งเดียวต่อ process (jpype constraint — classpath freeze หลัง start)
- Connection autocommit=False → ต้อง commit/rollback เอง
- `cursor()` context manager จัดการ commit/rollback อัตโนมัติ

#### `SupabaseConnector` — [`supabases/supabase_connection.py`](db_module/db_conn/supabases/supabase_connection.py)

psycopg2 + SSL (require). Connection params จาก `.env`:
- `SUPABASE_HOST` (e.g. `db.<ref>.supabase.co`) — IPv6 only
- `SUPABASE_PORT=5432`, `SUPABASE_USER=postgres`, `SUPABASE_PASSWORD`
- `SUPABASE_SSLMODE=require`

#### `InfluxConnector` — [`influxdb/influx_connection.py`](db_module/db_conn/influxdb/influx_connection.py)

Wrapper บาง ๆ บน `influxdb_client.InfluxDBClient`:
- `query(flux: str)` → `list[FluxTable]` (ไม่ flatten เป็น DataFrame)
- `client()` context manager สำหรับ raw API access (write_api, delete_api)

**Env:**
- `INFLUX_URL` (e.g. `http://<ec2>:8086`), `INFLUX_TOKEN` (required)
- `INFLUX_ORG=factory`, `INFLUX_BUCKET=iiot_data_raw` (defaults)

### 4.2 Oracle DW provisioning ([`db_module/db_sources/oracle_sql_query/`](db_module/db_sources/oracle_sql_query/))

#### `query/` — 7 SQL files (apply order)

| # | File | Creates |
|---|---|---|
| 01 | `01_schema_dim.sql` | 7 DIM tables + 4 sequences (DIM_DATE/SHIFT/METRIC ใช้ smart key ไม่มี SEQ) |
| 02 | `02_schema_fact.sql` | 5 FACT tables + 5 sequences |
| 03 | `03_schema_staging.sql` | 8 STG tables (5 OLTP staging + 3 DIM source staging) |
| 04 | `04_dim_seed.sql` | DECLARE block (DIM_DATE 1827 rows) + MERGE INTO DIM_SHIFT/METRIC/DEFECT_TYPE |
| 05 | `05_indexes.sql` | 27 indexes (FK + time-range + composite lookup) |
| 06 | `06_procedure_dim_sync.sql` | 3 `SP_SYNC_DIM_*` + 1 `SP_SYNC_ALL_DIMS` master |
| 07 | `07_procedure_fact_load.sql` | `FN_GET_SHIFT_ID` + 5 `SP_LOAD_FACT_*` + `SP_LOAD_ALL_FACTS` master |

**Notable design:**

- **DIM_DATE smart key** = `YYYYMMDD` integer (no surrogate, no SEQ) — JOIN `fact.date_id = dim.date_id` natural
- **`v_curr_date` variable** ใน 04_dim_seed (renamed จาก `current_date` ที่ชนกับ Oracle built-in `CURRENT_DATE` ใน SQL context)
- **MERGE BY src_id** ใน SP_SYNC_DIM_* — preserve surrogate key เสถียรข้าม sync
- **DELETE-by-key + Cursor FOR-LOOP** ใน SP_LOAD_FACT_* — Oracle 10g forbids `SEQ.NEXTVAL ใน INSERT...SELECT`
- **Flag pattern** แทน `CONTINUE` keyword (Oracle 10g ไม่มี)

#### Provisioning scripts

| Script | Purpose |
|---|---|
| [`run_sql_file.py`](db_module/db_sources/oracle_sql_query/run_sql_file.py) | Apply 1 SQL file ทีละ statement; แยก plain SQL (`;`) จาก PL/SQL block (`/`); fail-fast + rollback |
| [`verify_warehouse_schema.py`](db_module/db_sources/oracle_sql_query/verify_warehouse_schema.py) | Smoke test: 20 tables + 9 sequences + 10 procs + 1 fn ครบ |
| [`sync_dimensions_from_supabase.py`](db_module/db_sources/oracle_sql_query/sync_dimensions_from_supabase.py) | One-shot CLI DIM sync (`STG_LINE/MODEL/MACHINE` bulk_insert + `SP_SYNC_ALL_DIMS`); alternative ของ DAG |

### 4.3 Supabase OLTP provisioning ([`db_module/db_sources/supabases_sql_query/`](db_module/db_sources/supabases_sql_query/))

| # | File | Creates |
|---|---|---|
| 01 | `query/01_schema.sql` | 12 tables: production_line, machine, battery_model, defect_type, batch_status, event_reason, production_order, production_batch, batch_status_event, qc_record, qc_defect, downtime_event |
| 02 | `query/02_trigger_functions.sql` | 2 PostgreSQL trigger functions: `fn_sync_batch_status` (อัพเดท status/start_time/end_time จาก batch_status_event) + `fn_compute_downtime_duration` (auto-fill duration_min) + 2 CREATE TRIGGER |
| 03 | `query/03_master_data.sql` | 1 line + 3 machines (M01/M02/M03) + 3 battery models + 7 batch_status + 10 event_reasons + 20 defect_types |
| 04 | `mock/04_mock_data.sql` | Generated 8-day mock (373 batches × ~3.4 events/batch + 373 QC records + 29 defects + 26 downtime) |

**Scripts:**
- [`apply_supabase.py`](db_module/db_sources/supabases_sql_query/apply_supabase.py) — transactional apply 4 files + audit row counts
- [`generate_mock_data.py`](db_module/db_sources/supabases_sql_query/generate_mock_data.py) — generator (2-shift continuous, weighted product mix, beta-distributed yield)

---

## 5. Application Layer (`app/`)

### 5.1 FastAPI Service ([`app/api/`](app/api/))

**Run:**
```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
.venv/bin/uvicorn app.api.main:app --host 0.0.0.0 --port 8000
```

**Layout:**
```
app/api/
├── main.py                     FastAPI() + 2 router includes
├── dw_api/                     Oracle core (Airflow callers use these)
│   ├── deps.py                 4 หน้าที่: auth, connector, JDBC coercion, query helper
│   ├── models.py               Pydantic: SpCallRequest, BulkInsertRequest
│   └── operational.py          /health, /sp/call, /sql/bulk-insert
└── dashboard_api/
    └── dashboard.py            Streamlit-facing endpoints (7 routes)
```

#### `deps.py` — 4 Responsibilities

##### a) Auth — `require_token()`
```python
def require_token(authorization: str | None = Header(default=None)) -> None:
    expected = env_get("ORACLE_API_TOKEN")
    if not expected:                         # dev mode: empty token = skip
        return
    if authorization != f"Bearer {expected}":
        raise HTTPException(401)
```
ใช้เป็น `Depends(require_token)` ที่ router level (gate ครั้งเดียว ทุก endpoint)

##### b) Connection singleton — `get_connector()`
```python
_connector: OracleConnector | None = None
def get_connector():
    global _connector
    if _connector is None:
        _connector = OracleConnector()       # lazy init: starts JVM
    return _connector
```
JVM start ครั้งเดียวต่อ process — เปลี่ยน `ORACLE_JDBC_JAR` ต้อง restart uvicorn

##### c) JDBC type coercion (2 ทิศทาง)

**Response — JDBC value → JSON-safe** (`coerce`):
- date/datetime/time → `isoformat()`
- bool/int/float/str → as-is
- java.lang.String/อื่น → `str(v)`

**Request — ISO string → java.sql.\*** (`parse_iso`):
- `^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}` → `JTimestamp.valueOf(...)` via jpype
- `^\d{4}-\d{2}-\d{2}$` → `JDate.valueOf(...)` via jpype

`OraclePreparedStatement.setObject(int, datetime.date)` ไม่มี overload — ต้องส่ง java.sql.Date object

##### d) Query helper — `query_rows(sql, params=None) -> list[dict]`
- Open-close connection ทุก call (ยังไม่มี pool)
- Lower-case column names (Streamlit อ่าน `row["batch_src_id"]`)
- ใช้ทุก endpoint ใน `dashboard.py`

#### `operational.py` — 3 Generic Endpoints (Airflow uses)

| Method | Path | Used by |
|---|---|---|
| `GET` | `/health` | every DAG (`check_oracle_api` task) |
| `POST` | `/sp/call` `{name, args}` | sp_load_dw, sync_dim_supabase |
| `POST` | `/sql/bulk-insert` `{table, columns, rows, truncate, pipeline_run_id}` | etl_supabase, etl_influxdb, sync_dim_supabase |

#### `dashboard.py` — 7 Domain Endpoints (Streamlit uses)

##### Sensor (Page 2)
- `GET /api/sensor/available-metrics` → DIM_METRIC list (dropdown source)
- `GET /api/sensor/by-machine-15min?date&metric` → 15-min windows for line chart

##### Scheduling (Page 3)
- `GET /api/scheduling/batch-timeline?order_id` → planned vs actual per batch (Gantt drilldown)

##### Analytics (replace Oracle views — AI03 lacks `CREATE VIEW` privilege)

| Endpoint | Replaces | Used by |
|---|---|---|
| `GET /api/analytics/oee-daily?period` | `V_OEE_DAILY` | Page 1 |
| `GET /api/analytics/defect-pareto?period` | `V_DEFECT_PARETO` | Page 1 |
| `GET /api/analytics/schedule-adherence?period` | `V_SCHEDULE_ADHERENCE` | Page 3 |
| `GET /api/analytics/batch-features` | `V_BATCH_FEATURES` (ML feature matrix) | Page 1 + ML pipeline |

`_period_to_start_date_id(period)` แปลง string ("Today" / "This week" / "Last 7 days" / "Last 30 days") → `date_id` (YYYYMMDD)

### 5.2 Streamlit Dashboard ([`app/streamlit/`](app/streamlit/))

**Run** (จาก `app/streamlit/` เพื่อให้ `.streamlit/config.toml` ถูกอ่าน):
```bash
cd app/streamlit && ../../.venv/bin/streamlit run dashboard.py
```

**`dashboard.py`** — Entry point:
- Sidebar: project info + nav + FastAPI status indicator (live `/health` check)
- Landing: render embedded README.md content

**`components/`**

| Module | Role |
|---|---|
| `api_client.py` | `@st.cache_data(ttl=300)` HTTP wrapper; Bearer token จาก `.env` |
| `cards.py` | `kpi_card()`, `kpi_row()`, `status_badge()` — KPI rendering |
| `charts.py` | 7 Plotly builders: oee_trend, defect_pareto, defect_rate_by_model, forecast, slippage_histogram, slippage_trend, batch_gantt |
| `filters.py` | `period_selector()` + 3 filter rows (oee_defect / forecast / schedule) |
| `prophet_trainer.py` | `trigger_training()` (background daemon thread) + `model_status()` + `predict()` — Prophet 1.3.0 |

**`pages/`**

| Page | Layout |
|---|---|
| `1_oee_defect.py` | filters → 4 KPI cards (OEE/Avail/Perf/Quality) → OEE trend chart → Defect Pareto → Defect rate by model → detail table |
| `2_sensor_forecast.py` | filters → status card → forecast chart (history + forecast + confidence band + threshold + "now" line) |
| `3_schedule_adherence.py` | filters → 4 KPI cards (Total/On-time/Minor late/Late) → 2-column (histogram + trend) → Gantt drilldown → batch detail table |

**Notable Plotly workarounds:**
- `pd.to_datetime(..., format="mixed")` — Oracle TIMESTAMP some rows have fractional seconds, some don't
- `add_vline` + `add_annotation` แยก call (Plotly + pandas 2.x bug: `_mean(x)` ทำ `0 + datetime` → TypeError)

---

## 6. Pipeline Layer (Airflow)

**Stack:** Apache Airflow 2.8 in Docker compose ([`db_module/pipeline/docker-compose.yml`](db_module/pipeline/docker-compose.yml))

**UI:** <http://localhost:8088> (login: `airflow / airflow`)

### DAGs

| DAG | Schedule | Tasks | Description |
|---|---|---|---|
| `etl_supabase_to_oracle` | `*/15 * * * *` | 4 parallel after healthcheck | OLTP → STG (4 tables) |
| `etl_influxdb_to_oracle` | `*/15 * * * *` | 1 task after healthcheck | Influx Flux → STG_SENSOR_AGG (with metric/machine validation) |
| `sp_load_dw` | `5,20,35,50 * * * *` | 1 task | Call `SP_LOAD_ALL_FACTS` master orchestrator |
| `sync_dim_supabase` | `0 2 * * *` (nightly UTC) | 3 parallel + 1 convergence | DIM sync (LINE/MODEL/MACHINE) via STG + `SP_SYNC_ALL_DIMS` |

### Helper modules

- [`_oracle_api.py`](db_module/pipeline/airflow/dags/_oracle_api.py) — HTTP helpers: `health()`, `bulk_insert()`, `call_sp()`, `as_iso()`
- [`_supabase.py`](db_module/pipeline/airflow/dags/_supabase.py) — `supabase_cursor()` context manager (psycopg2)

### Schema rename note (2026-04-26)

| Old | New |
|---|---|
| `product_id` | `model_id` (Supabase has `battery_model` ตอนนี้) |
| `qty_sampled` | `qty_inspected` |
| `machine_name` | `machine_code` (DIM_MACHINE + DIM_METRIC) |

### InfluxDB validation guard ([`etl_influxdb_to_oracle.py`](db_module/pipeline/airflow/dags/etl_influxdb_to_oracle.py))

```python
EXPECTED_METRICS = {"temperature_c", "machine_state_num", "cycle_count",
                    "vibration_g", "current_a", "voltage_v"}
EXPECTED_MACHINES = {"M01", "M02", "M03"}
# raises ValueError if Influx schema drifts (กัน silent FACT_SENSOR empty)
```

---

## 7. Oracle DW Schema

### Object inventory

| Type | Count | Names |
|---|---:|---|
| Tables | 20 | 7 DIM + 5 FACT + 8 STG |
| Sequences | 9 | 4 DIM + 5 FACT |
| Procedures | 10 | 4 SP_SYNC + 6 SP_LOAD |
| Functions | 1 | `FN_GET_SHIFT_ID(p_ts TIMESTAMP) RETURN NUMBER` |
| Indexes | 27 | FK + time-range + composite |
| Views | 0 | (replaced by FastAPI analytics endpoints — AI03 lacks CREATE VIEW privilege) |

### DIM (7) — slowly changing master

| Table | Key | Source | Rows |
|---|---|---|---:|
| `DIM_DATE` | smart key `YYYYMMDD` | seed (2024-2028) | 1827 |
| `DIM_LINE` | `SEQ_DIM_LINE` | sync from Supabase production_line | 1 |
| `DIM_SHIFT` | manual id (1=DAY, 2=NIGHT) | seed inline | 2 |
| `DIM_BATTERY_MODEL` | `SEQ_DIM_BATTERY_MODEL` | sync from Supabase battery_model | 3 |
| `DIM_MACHINE` | `SEQ_DIM_MACHINE` | sync from Supabase machine | 3 |
| `DIM_METRIC` | manual id (1-6) | seed inline | 6 |
| `DIM_DEFECT_TYPE` | `SEQ_DIM_DEFECT_TYPE` | seed inline (recursive 5+15) | 20 |

### FACT (5) — measurable events

| Table | Grain | Source | Rows (current) |
|---|---|---|---:|
| `FACT_PRODUCTION` | 1 batch | STG_PRODUCTION_BATCH | 373 |
| `FACT_QUALITY` | 1 QC inspection | STG_QC_RECORD JOIN STG_PRODUCTION_BATCH | 373 |
| `FACT_DEFECT` | 1 defect type per QC (M:N) | STG_QC_DEFECT JOIN STG_QC_RECORD JOIN STG_PRODUCTION_BATCH | 29 |
| `FACT_DOWNTIME` | 1 closed downtime event | STG_DOWNTIME_EVENT | 26 |
| `FACT_SENSOR` | 1 (machine × metric × 15-min window) | STG_SENSOR_AGG (Influx Flux agg) | 6152 |

### STG (8) — buffer (truncate-and-load)

| OLTP staging (5) | DIM source staging (3) |
|---|---|
| STG_PRODUCTION_BATCH | STG_LINE |
| STG_QC_RECORD | STG_BATTERY_MODEL |
| STG_QC_DEFECT | STG_MACHINE |
| STG_DOWNTIME_EVENT | |
| STG_SENSOR_AGG | |

### Key SP signatures

```sql
SP_SYNC_DIM_LINE             -- MERGE STG_LINE → DIM_LINE
SP_SYNC_DIM_BATTERY_MODEL
SP_SYNC_DIM_MACHINE
SP_SYNC_ALL_DIMS             -- master: LINE → MODEL → MACHINE

SP_LOAD_FACT_PRODUCTION      -- DELETE-by-key + cursor FOR-LOOP INSERT
SP_LOAD_FACT_QUALITY
SP_LOAD_FACT_DEFECT          -- flag pattern แทน CONTINUE
SP_LOAD_FACT_DOWNTIME
SP_LOAD_FACT_SENSOR
SP_LOAD_ALL_FACTS            -- master: PROD → QC → DEFECT → DOWNTIME → SENSOR

FN_GET_SHIFT_ID(ts) RETURN NUMBER  -- DAY=1 (07:30-16:30), NIGHT=2 (else)
```

---

## 8. Setup from Scratch

### Prerequisites
- Python 3.12+
- Java 17 (for JDBC) — `brew install openjdk@17`
- Docker (for Airflow)
- KMITL Oracle access — credentials in `.env`
- Supabase project + InfluxDB instance (or use the one defined in `.env`)

### Step 1 — Python env

```bash
python3.12 -m venv .venv
.venv/bin/python -m ensurepip                  # bootstrap pip if missing
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pip install prophet        # ~5-10 min, compiles cmdstanpy
```

### Step 2 — Oracle JDBC driver

ดาวน์โหลด `ojdbc8.jar` (Oracle 19c+ จะ compatible ลง 10g):
```bash
mkdir -p db_module/db_conn/oracle/drivers
# วาง ojdbc8.jar ที่ db_module/db_conn/oracle/drivers/
```

### Step 3 — `.env`

```bash
cp .env.example .env
# แก้ค่าจริง: SUPABASE_*, INFLUX_*, ORACLE_*, ORACLE_API_TOKEN, JAVA_HOME
```

### Step 4 — Verify connectors

```bash
.venv/bin/python -m pytest test/test_connectors.py -v
.venv/bin/python test/test_connection.py        # 3-layer Oracle probe
```

### Step 5 — Apply Supabase OLTP

```bash
.venv/bin/python db_module/db_sources/supabases_sql_query/apply_supabase.py
# applies 01_schema → 02_triggers → 03_master_data → 04_mock_data
```

### Step 6 — Apply Oracle DW

```bash
for f in 01_schema_dim 02_schema_fact 03_schema_staging \
         04_dim_seed 05_indexes 06_procedure_dim_sync \
         07_procedure_fact_load; do
    .venv/bin/python db_module/db_sources/oracle_sql_query/run_sql_file.py \
        "db_module/db_sources/oracle_sql_query/query/${f}.sql"
done

.venv/bin/python db_module/db_sources/oracle_sql_query/verify_warehouse_schema.py
# Expected: OK — all expected objects present
```

### Step 7 — Sync DIMs (one-shot)

```bash
.venv/bin/python db_module/db_sources/oracle_sql_query/sync_dimensions_from_supabase.py
# Expected: DIM_LINE=1, DIM_BATTERY_MODEL=3, DIM_MACHINE=3
```

### Step 8 — Start services

```bash
# Terminal 1: FastAPI
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
.venv/bin/uvicorn app.api.main:app --host 0.0.0.0 --port 8000

# Terminal 2: Airflow
docker compose -f db_module/pipeline/docker-compose.yml up -d

# Terminal 3: Streamlit
cd app/streamlit && ../../.venv/bin/streamlit run dashboard.py
```

### Step 9 — Trigger ETL DAGs

ใน Airflow UI <http://localhost:8088> → unpause + trigger ทั้ง 4 DAGs ตามลำดับ:
1. `sync_dim_supabase` (รอ 30s)
2. `etl_supabase_to_oracle` + `etl_influxdb_to_oracle` (parallel, รอ 2 นาที)
3. `sp_load_dw` (รอ 1 นาที)

### Step 10 — Open dashboard

<http://localhost:8501/>

---

## 9. Daily Operations

### Routine

| Frequency | Task | How |
|---|---|---|
| Every 15 min | ETL ทำงานอัตโนมัติ | Airflow scheduler |
| Every 15 min (5 min after) | FACT load | sp_load_dw scheduled |
| Nightly 02:00 | DIM sync | sync_dim_supabase scheduled |
| Manual | Refresh dashboard | คลิก Refresh button ที่ filter row ของแต่ละ page |
| Manual | Train Prophet model | Page 2 → เลือก machine + metric → คลิก "Train model" |

### Manual ETL trigger (เมื่อต้องการ run นอก schedule)

```bash
COMPOSE=db_module/pipeline/docker-compose.yml
docker compose -f $COMPOSE exec -T airflow-scheduler \
    airflow dags trigger <dag_id>
```

---

## 10. Smoke Tests

### Health checks

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

### Endpoint smoke

```bash
TOKEN=$(grep ^ORACLE_API_TOKEN= .env | cut -d= -f2)
BASE=http://localhost:8000
DATE=2026-04-25

for ep_args in \
    "/api/sensor/available-metrics" \
    "/api/sensor/by-machine-15min?date=$DATE&metric=temperature_c" \
    "/api/scheduling/batch-timeline?order_id=1" \
    "/api/analytics/oee-daily?period=Last+7+days" \
    "/api/analytics/defect-pareto?period=Last+7+days" \
    "/api/analytics/schedule-adherence?period=Last+7+days" \
    "/api/analytics/batch-features"; do
    n=$(curl -s "$BASE${ep_args}" -H "Authorization: Bearer $TOKEN" | jq '(.rows // []) | length')
    echo "  $ep_args -> $n rows"
done
```

### Data consistency

```bash
# Supabase ↔ Oracle FACT counts ต้อง match
.venv/bin/python -c "
from db_module.db_conn import SupabaseConnector
with SupabaseConnector().cursor() as cur:
    for sql in ['SELECT COUNT(*) FROM production_batch WHERE end_time IS NOT NULL',
                'SELECT COUNT(*) FROM qc_record',
                'SELECT COUNT(*) FROM qc_defect',
                'SELECT COUNT(*) FROM downtime_event WHERE end_ts IS NOT NULL']:
        cur.execute(sql); print(cur.fetchone()[0])
"
# Compare to: FACT_PRODUCTION / FACT_QUALITY / FACT_DEFECT / FACT_DOWNTIME counts
```

---

## 11. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `ConfigError: missing env ORACLE_HOST` | `.env` ไม่ได้โหลด หรือ key ไม่มี | check `.env` exists + has all required vars |
| `Unable to locate a Java Runtime` | JAVA_HOME ไม่ตั้ง | `export JAVA_HOME=/opt/homebrew/opt/openjdk@17` หรือใส่ใน `.env` |
| `ORA-01861: literal does not match format string` | passing Python date string ตรง ๆ | ใช้ `prepare_row()` ใน operational.py — แปลงเป็น java.sql.Date |
| `ORA-00911: invalid character` ตอน apply view | `_PLSQL_BLOCK_STARTS` มี match `CREATE OR REPLACE` แบบหลวม | already fixed (specific to PROCEDURE/FUNCTION/TRIGGER/PACKAGE) |
| `PLS-00201: identifier 'CONTINUE' must be declared` | Oracle 10g ไม่มี CONTINUE | ใช้ flag pattern (`v_skip BOOLEAN`) |
| `ORA-01031: insufficient privileges` ตอน CREATE VIEW | AI03 ไม่มี CREATE VIEW privilege | views replaced by FastAPI `/api/analytics/*` endpoints |
| `DIM_DATE.full_date = SYSDATE for all rows` | `current_date` variable name ชนกับ Oracle CURRENT_DATE function | renamed to `v_curr_date` |
| Streamlit `pd.to_datetime` ValueError | mixed format (some rows have `.fff` microseconds) | ใช้ `format="mixed"` |
| Streamlit Plotly add_vline TypeError | `0 + datetime` ใน annotation positioning | split `add_vline` + `add_annotation` แยก call |
| FastAPI cold start ช้า 2-3s | JVM start + ojdbc8 load | normal — singleton หลัง first request |
| FACT empty หลัง trigger DAG | DAG window ไม่ตรง mock data range | manual full ETL: หรือ backfill window ที่มี data |

---

## 12. Constraints & Design Decisions

### Oracle Server (10.2.0.3)

| Constraint | Workaround |
|---|---|
| ไม่รองรับ `CONTINUE` PL/SQL keyword | flag pattern: `v_skip BOOLEAN`, `IF NOT v_skip THEN ... END IF` |
| ไม่รองรับ `SEQ.NEXTVAL ใน INSERT...SELECT` | Cursor FOR-LOOP iterating STG → INSERT one by one |
| Default NLS_CALENDAR = 'THAI BUDDHA' | ALTER SESSION 'GREGORIAN' on every connection |
| python-oracledb thin requires 12c+ | JDBC thin via `jaydebeapi` + `ojdbc8.jar` + `jpype` |
| AI03 lacks `CREATE VIEW` privilege | views replaced by FastAPI analytics endpoints |
| Variable `current_date` ชนกับ built-in `CURRENT_DATE` ใน SQL context (MERGE INSERT VALUES) | rename to `v_curr_date` |

### JDBC + JVM

| Constraint | Detail |
|---|---|
| `thinLogonCapability=o3` | Required for Oracle 10g logon protocol |
| `disableOob=true` | Prevent network out-of-band issues |
| `user.language=en, user.country=US` | Prevent Buddhist year on wire |
| JVM start ครั้งเดียวต่อ process | jpype constraint — classpath freeze; restart uvicorn เพื่อเปลี่ยน jar |

### Schema Design

- **Smart key for DIM_DATE** (`YYYYMMDD` integer) — natural JOIN, no surrogate
- **MERGE BY src_id** in SP_SYNC_DIM_* — preserve surrogate keys across sync
- **Truncate-and-load STG** — buffer last 15-min window only
- **DELETE-by-key + INSERT** in SP_LOAD_FACT_* — idempotent (re-run ก็ไม่ duplicate)
- **Denormalize line_name in DIM_MACHINE** — query join saver

### App Architecture

- **FastAPI HTTP wrapper** — avoid bundling Java in Airflow Docker image
- **OracleConnector singleton** — single uvicorn process; query rate ~1 req/min ไม่ต้องการ pool
- **Streamlit cache TTL 5 min** — DAG cadence 15 min; refresh button clears cache instantly
- **Prophet daemon thread training** — UI ไม่ block; status badge แสดง progress
- **Pages auto-refresh 15 min** (via `streamlit_autorefresh`) — ตรง DAG cadence

---

## License + Credits

KMITL Computer Science — IIoT Data Architecture project (2026)
