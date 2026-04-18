# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> **Progress tracker lives in [claude_track/PLAN.md](claude_track/PLAN.md)** — read it before proposing work. It has per-phase checkboxes, exit criteria, and an authoritative record of which parts of the spec below are built vs still on paper. Update it as you finish tasks.

---

## Working in this repo

### Commands

```bash
# One-time deps
uv pip install --python .venv/bin/python -r requirements.txt

# JAVA_HOME is mandatory for every JDBC (Oracle) call. Either set it in your
# shell or export before each invocation:
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home

# Smoke-test connectors (skips Supabase/Influx cleanly if their env is unset)
.venv/bin/python -m pytest test/test_connectors.py -v

# Run a single connector test
.venv/bin/python -m pytest test/test_connectors.py::test_oracle_connector_roundtrip -v

# Manual Oracle reachability probe (HTTP + TCP + JDBC roundtrip)
.venv/bin/python test/test_connection.py

# Full Oracle DDL/DML lifecycle smoke test
.venv/bin/python test/test_create_table.py

# Apply Oracle DW DDL against AI03 (splits SQL + PL/SQL blocks)
.venv/bin/python datasources/oracle_sql_query/apply_ddl.py [path.sql]

# Seed DIM_MACHINE/PRODUCT/STAGE/MATERIAL from live Supabase master data
.venv/bin/python datasources/oracle_sql_query/seed_dims.py

# Apply full Supabase schema + master + mock (one transaction)
.venv/bin/python datasources/supabases_sql_query/apply_supabase.py

# Start the Oracle API service (what Airflow will call instead of JDBC).
# Use --host 0.0.0.0 so Airflow containers can reach it via host.docker.internal.
.venv/bin/uvicorn app.api.main:app --host 0.0.0.0 --port 8000 --reload
# Endpoints: GET /health, POST /sql/query, /sql/execute, /sp/call, /sql/bulk-insert
# Auth: Bearer token from ORACLE_API_TOKEN env (blank = no auth)

# Start the Airflow stack (requires the Oracle API to be running on host:8000)
cd db_module/pipeline && docker compose up -d --build
# Webserver: http://localhost:8088  (NOT 8080 — taken by label-studio in another project)
# Creds: admin / admin
# Tear down (keep metadata volume): docker compose down
# Full reset: docker compose down -v

# Run a DAG task one-off against a specific execution date (no schedule change)
docker compose exec -T airflow-scheduler airflow tasks test \
    etl_supabase_to_oracle extract_production_batch 2026-04-15

# Launch the Streamlit OEE dashboard (reads from the Oracle API)
DASHBOARD_API_URL=http://localhost:8000 \
ORACLE_API_TOKEN=dev-local-token-please-change \
    .venv/bin/streamlit run app/streamlit/dashboard.py --server.port 8501
# http://localhost:8501
```

### Critical gotchas

- **Oracle 10.2.0.3 forces JDBC.** `python-oracledb` thin mode needs server ≥ 12c; ARM64 Instant Client doesn't exist for 10g. Use `JayDeBeApi` + `db_module/db_conn/oracle/drivers/ojdbc8.jar` — no exceptions.
- **JDBC logon flag is non-negotiable.** Oracle 10g only speaks O3LOGON. `OracleConnector._ensure_jvm` passes `-Doracle.jdbc.thinLogonCapability=o3`; removing it breaks the handshake.
- **JVM starts once per Python process.** Classpath is frozen at first `startJVM()` call. If you change `ORACLE_JDBC_JAR` at runtime, you must restart Python.
- **JayDeBeApi returns `java.lang.String`, not `str`.** Always wrap result columns with `str()` before calling Python string methods — otherwise `AttributeError: 'java.lang.String' object has no attribute 'upper'`.
- **Thai locale + JVM = Buddhist calendar corruption.** On a Thai-locale host, the JVM's default `Locale` is `th_TH` which makes `java.sql.Date.valueOf("2026-01-01")` write year **2569** (2026 + 543). `OracleConnector` forces `-Duser.language=en -Duser.country=US` at JVM start and `NLS_CALENDAR='GREGORIAN'` + `NLS_DATE_LANGUAGE='ENGLISH'` on every session — do not remove these.
- **JayDeBeApi's `Date()` / `Timestamp()` factories return strings, not Java objects.** Oracle rejects string binds for DATE/TIMESTAMP columns (ORA-01861). Use `jpype.JClass("java.sql.Date").valueOf(iso_str)` / `Timestamp.valueOf(...)` — see `app/api/main.py:_parse_iso`.
- **Oracle API service shields Airflow from JDBC.** [app/api/main.py](app/api/main.py) runs locally via `uvicorn` and exposes `/health`, `/sql/query`, `/sql/execute`, `/sp/call`, `/sql/bulk-insert`. DAGs call this service with plain `requests` — no Java or ojdbc8.jar inside the Airflow container.
- **Airflow container cannot use `ORACLE_API_URL=http://localhost:8000`** — inside the container `localhost` is the container, not the host. The compose file overrides it to `http://host.docker.internal:8000` via `environment:`. Start `uvicorn` with `--host 0.0.0.0` (not `127.0.0.1`) or the host gateway alias can't reach it.
- **Supabase `db.<ref>.supabase.co` is IPv6-only.** Docker Desktop's default bridge has no IPv6, so the Airflow compose network sets `enable_ipv6: true` with an ULA subnet. If you see `Network is unreachable` / `No address associated with hostname` from a DAG reaching Supabase, that flag was dropped.
- **psycopg2 returns DECIMAL as Python `decimal.Decimal`** which `json.dumps` rejects. Any helper that posts DB rows over HTTP must convert Decimal → float first (see `dags/_oracle_api.py:as_iso`).
- **Oracle 10g forbids `SEQ.NEXTVAL` inside `INSERT ... SELECT`** (ORA-02287). Fact loaders that need both `GROUP BY` and a sequence (e.g. `SP_LOAD_FACT_PRODUCTION`) must use an explicit cursor FOR loop with per-row INSERT.
- **Scripts under `test/` need a `sys.path` hack to import `db_module`.** `test/test_connection.py` and `test/test_create_table.py` both do `sys.path.insert(0, parent.parent)` at the top; follow that pattern for new scripts. Pytest itself works without this because it auto-discovers the repo root.
- **Secrets live in `.env`** (gitignored). Template: `.env.example`. Connectors raise `ConfigError` with a clear message when a required var is unset or empty — empty string is treated as unset on purpose.

### Architecture decisions that OVERRIDE the spec below

The spec in §§1–14 was written before these four decisions were locked (2026-04-18). When they conflict, these win:

1. **Oracle hosts the Data Warehouse only. Supabase is OLTP.** Ignore any reference to `BATTERY_OLTP` on Oracle. The 17-table ERD in §7 lives in Supabase and is the [A] deliverable.
2. **No `CREATE USER` step.** All STG + DW tables go under the existing `AI03` schema. When writing DDL, strip the `BATTERY_STG.` / `BATTERY_DW.` prefixes from §§8–10.
3. **IIoT stack (NodeRED + Mosquitto + InfluxDB 2.0) runs on AWS — we consume, not provision.** `datasources/iiot_container/` is reference-only; do not scaffold compose services for it. Airflow's Influx DAG reads directly from the AWS endpoint.
4. **Local-only deployment.** No EC2 provisioning. Compose files use `localhost` / container DNS; remote data sources are hit over the public network.

### Connector module shape

Three connectors live in [db_module/db_conn/](db_module/db_conn/), all following the same pattern:

- Construct with no args → reads from `.env` via [_env.py](db_module/db_conn/_env.py).
- `connect()` returns a raw DB-API connection for bulk work.
- `cursor()` is a context manager that commits on success and rolls back on exception — prefer this for single logical operations.
- Heavy imports (`jaydebeapi`, `psycopg2`, `influxdb_client`) are inside methods so `from db_module.db_conn import ...` doesn't crash when a specific driver isn't installed yet.

### Current state snapshot

Phase 1 (foundation + connectivity) is functionally complete — see [claude_track/PLAN.md](claude_track/PLAN.md) for authoritative status. Oracle is live-verified against `AI03`. Supabase and InfluxDB connectors are written but unverified against live endpoints (credentials still blank in `.env`). Everything else — OLTP schema, mock data, Airflow DAGs, stored procedures, FastAPI, Streamlit — is unbuilt.

---

# Battery Manufacturing — OEE Data Pipeline (design spec)

> The sections that follow are the *design target*, not a description of what currently exists. Treat them as a specification to build against, and defer to [claude_track/PLAN.md](claude_track/PLAN.md) for ground truth on what's actually shipped.

> **Project Type:** Data Engineering — ETL Pipeline + OEE Dashboard
> **Domain:** Industrial IoT (IIoT) + Manufacturing Analytics
> **Stack:** Supabase · InfluxDB · Apache Airflow · Oracle · FastAPI · Streamlit

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Manufacturing Process](#2-manufacturing-process)
3. [Technology Stack](#3-technology-stack)
4. [Data Architecture](#4-data-architecture)
5. [Data Sources & IIoT](#5-data-sources--iiot)
6. [ETL Pipeline — Airflow DAGs](#6-etl-pipeline--airflow-dags)
7. [OLTP Schema — Supabase A](#7-oltp-schema--supabase-a)
8. [Oracle Staging Schema](#8-oracle-staging-schema)
9. [Data Warehouse — Oracle B](#9-data-warehouse--oracle-b)
10. [Stored Procedures & Functions — C](#10-stored-procedures--functions--c)
11. [Dashboard — FastAPI + Streamlit](#11-dashboard--fastapi--streamlit)
12. [Deployment — Docker Compose](#12-deployment--docker-compose)
13. [Generated Files](#13-generated-files)
14. [Assignment Deliverable Mapping](#14-assignment-deliverable-mapping)
15. [Changelog](#15-changelog)

---

## 1. Project Overview

ระบบ **Data Pipeline แบบ ETL** สำหรับโรงงานผลิตแบตเตอรี่รถยนต์ จำลอง 3 เครื่องจักรหลัก ส่งข้อมูล IIoT ผ่าน MQTT และรวมกับ OLTP เข้าสู่ Data Warehouse สำหรับคำนวณ OEE

### OEE Formula

```
OEE = A x P x Q

A (Availability) = (T_planned - T_downtime) / T_planned
P (Performance)  = (N_produced x t_ideal)   / T_actual_run
Q (Quality)      = N_good / N_produced
```

**Target OEE:** 65-75% (realistic mid-tier manufacturing)
**Planned time per run:** 480 min (8h) hardcoded — no SHIFT table

### Schema Summary

| Layer | Schema | Tables |
|---|---|---|
| OLTP | `BATTERY_OLTP` / Supabase | 17 tables |
| Staging | `BATTERY_STG` | 5 tables |
| Data Warehouse | `BATTERY_DW` | 5 facts + 5 dims |

---

## 2. Manufacturing Process

10 ขั้นตอน — **3 stages มี IIoT sensor (M01/M02/M03)**

```
Raw Material
     |
[1]  Lead Smelting       FURNACE    M01  ideal_cycle=120s  <-- instrumented
[2]  Cutting             CUTTER
[3]  Milling to Paste    MILL
[4]  Grid Pressing       PRESS
[5]  Plate Assembly      ASSEMBLER  M02  ideal_cycle=45s   <-- instrumented
[6]  Case Boxing         ASSEMBLER
[7]  Acid Filling        (manual)
[8]  Formation Charging  CHARGER    M03  ideal_cycle=300s  <-- instrumented
[9]  QC Final            TESTER
[10] Finished Good -> Warehouse
```

### 3 Instrumented Machines

| ID | Machine | Type | Sensors | OEE Dimension |
|---|---|---|---|---|
| M01 | Smelting Furnace #1 | FURNACE | temperature_c, machine_state | Availability |
| M02 | Plate Assembly Unit #1 | ASSEMBLER | cycle_count, vibration_g | Performance |
| M03 | Formation Charger #1 | CHARGER | current_a, voltage_v | Quality |

> Stages 2,3,4,6,7,9,10 exist in OLTP and mock data but have no IIoT sensor

### Raw Materials

| Material | Symbol | Hazard |
|---|---|---|
| Lead | Pb | Class 8 |
| Sulfuric Acid | H2SO4 | Class 8 |
| Polypropylene | PP | — |
| Copper | Cu | Class 9 |
| Metal Grid | — | — |

---

## 3. Technology Stack

### Layer Mapping

| Layer | Reference | Our Stack | Notes |
|---|---|---|---|
| Data Sources | OLTP, Streaming | Supabase + NodeRED/MQTT | |
| Ingestion | Kafka, Airbyte | Airflow Hooks | PostgresHook + influxdb-client |
| Raw Storage | Cloud Storage | InfluxDB | sensor 1Hz |
| Transformation | Spark, Pandas | Oracle SP [C] | PL/SQL |
| DW Storage | BigQuery | Oracle DW | Star Schema |
| Orchestration | Airflow | Airflow | exact match |
| Visualization | Tableau | Streamlit + FastAPI | |

> No Spark: 3 machines x 6 batches/day is well within Pandas + Airflow capacity

### FastAPI Role — Serving Layer Only

```
WRONG:  Supabase  -> FastAPI -> Airflow   (overhead + failure point)
WRONG:  InfluxDB  -> FastAPI -> Airflow   (same reason)
RIGHT:  Oracle DW -> FastAPI -> Streamlit (read-only serving)
```

### Infrastructure

```
AWS EC2 (Docker)
├── Airflow (webserver + scheduler)
├── NodeRED               mock sensor data
├── Mosquitto             MQTT broker
├── InfluxDB              time-series storage
├── FastAPI               serving layer (reads Oracle DW)
└── Streamlit             OEE dashboard

Oracle Server: 161.246.35.92:1521
├── BATTERY_OLTP          [A] assignment OLTP
├── BATTERY_STG           staging buffer
└── BATTERY_DW            [B] data warehouse

Supabase (Cloud)
└── PostgreSQL            OLTP source of truth
```

> Pre-check: `telnet 161.246.35.92 1521` from AWS before building anything

---

## 4. Data Architecture

```
SOURCES
Supabase (17 tables OLTP)           IIoT: NodeRED -> MQTT -> InfluxDB (1Hz)
        |                                                |
        | PostgresHook (direct)         influxdb-client (direct)
        v                                                v
AIRFLOW DAGS  every 8h at 06:00 / 14:00 / 22:00
etl_supabase_to_oracle              etl_influxdb_to_oracle
        |                                                |
        +------------------------------------------------+
                         | cx_Oracle bulk INSERT
                         v
BATTERY_STG  Staging (TRUNCATE before each load = idempotent)
STG_PRODUCTION_BATCH   STG_QC_INSPECTION   STG_QC_RESULT
STG_MAINTENANCE_LOG    STG_SENSOR_AGG
                         |
                         | Stored Procedures [C]
                         v
BATTERY_DW  Star Schema
FACTS:  OEE  PRODUCTION  QUALITY  INVENTORY  MAINTENANCE
DIMS:   DATE  MACHINE  PRODUCT  STAGE  MATERIAL
                         |
                         | cx_Oracle read-only
                         v
FastAPI  ->  REST endpoints  ->  Streamlit OEE Dashboard
```

### Why Staging Is Required

SP [C] reads only from within Oracle. Without staging, SP has nothing to query and assignment [C] fails.

Bonus: if SP crashes midway, just re-run SP without re-extracting from Supabase.

---

## 5. Data Sources & IIoT

### MQTT Topic Hierarchy (UNS)

```
factory/line1/smelting/M01/temperature_c     Gaussian noise ~480 C
factory/line1/smelting/M01/machine_state     1=RUNNING / 0=FAULT

factory/line1/assembly/M02/cycle_count       increment per unit produced
factory/line1/assembly/M02/vibration_g       Gaussian noise ~0.8g

factory/line1/charging/M03/current_a         Gaussian noise ~145A
factory/line1/charging/M03/voltage_v         ramp 10.5 to 12.8V per 5min cycle
```

**Payload (1Hz):**
```json
{
  "machine_id": "M01",
  "stage": "smelting",
  "temperature_c": 482.3,
  "machine_state": "RUNNING",
  "ts": 1705312200
}
```

### NodeRED Flow Pattern

```
[inject 1s] -> [function: value + Gaussian noise] -> [MQTT out]
```

machine_state -> FAULT: 1% chance/sec (Poisson ~1.6 events/hr)

### InfluxDB Schema

```
Measurement: machine_metrics
Tags:        machine_id={M01|M02|M03}, stage={smelting|assembly|charging}
Fields M01:  temperature_c, machine_state_num
Fields M02:  cycle_count, vibration_g, machine_state_num
Fields M03:  current_a, voltage_v, machine_state_num
```

Downtime detection Flux query:

```flux
from(bucket:"sensors")
  |> range(start: -8h)
  |> filter(fn:(r) => r._field == "machine_state_num" and r._value == 0)
  |> aggregateWindow(every: 1m, fn: count)
```

### Simulation Parameters

| Parameter | Value |
|---|---|
| Downtime frequency | Poisson lambda=1 event/4hr |
| Downtime duration | Exponential mu=20 min |
| Defect rate | 2-5% |
| Performance ratio | 85-95% of ideal cycle time |
| Expected OEE | 65-75% |

### Mock Data Generated (30 days)

| Table | Rows |
|---|---|
| production_batch | 540 |
| finished_good | 6,114 |
| material_consumption | 1,080 |
| qc_inspection | 540 |
| qc_result | 6,300 |
| maintenance_log | 15 |
| **Total** | **14,589** |

---

## 6. ETL Pipeline — Airflow DAGs

### DAG 1: Supabase to Oracle Staging

```python
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import cx_Oracle

default_args = {
    'owner': 'data_engineer',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

def extract_supabase_load_oracle(ds, **kwargs):
    # 1. Extract from Supabase via direct PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id='supabase_conn')
    rows = pg_hook.get_records("""
        SELECT batch_id, order_id, line_id, stage_id,
               started_at, completed_at, qty_produced
        FROM production_batch
        WHERE DATE(completed_at) = %s
    """, parameters=[ds])

    # 2. Load to Oracle Staging (TRUNCATE first = idempotent)
    conn = cx_Oracle.connect(user="stg_user", password="...",
                              dsn="161.246.35.92:1521/orcl")
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE BATTERY_STG.STG_PRODUCTION_BATCH")
    cursor.executemany("""
        INSERT INTO BATTERY_STG.STG_PRODUCTION_BATCH
        (batch_id, order_id, line_id, stage_id, started_at, completed_at,
         qty_produced, src_system, pipeline_run_id, loaded_at)
        VALUES (:1,:2,:3,:4,:5,:6,:7,'SUPABASE',:8,SYSDATE)
    """, [(*row, kwargs['run_id']) for row in rows])
    conn.commit()

with DAG('etl_supabase_to_oracle', default_args=default_args,
         schedule_interval='0 6,14,22 * * *',
         start_date=datetime(2024, 1, 1), catchup=False) as dag:
    PythonOperator(task_id='extract_production_batch',
                   python_callable=extract_supabase_load_oracle)
    # Add: extract_qc_inspection, extract_qc_result, extract_maintenance_log
```

### DAG 2: InfluxDB to Oracle Staging

```python
from influxdb_client import InfluxDBClient
import cx_Oracle

def extract_influx_load_oracle(ds, **kwargs):
    client = InfluxDBClient(url="http://influxdb:8086", token="...", org="factory")
    tables = client.query_api().query(f"""
        from(bucket:"sensors")
        |> range(start: {ds}T00:00:00Z, stop: {ds}T23:59:59Z)
        |> filter(fn:(r) => r._measurement == "machine_metrics")
        |> aggregateWindow(every: 8h, fn: mean, createEmpty: false)
        |> pivot(rowKey:["_time","machine_id"],
                 columnKey:["_field"], valueColumn:"_value")
    """)
    rows = [
        (r["machine_id"], r.get("temperature_c"), r.get("cycle_count"),
         r.get("vibration_g"), r.get("current_a"), r.get("voltage_v"))
        for table in tables for r in table.records
    ]

    conn = cx_Oracle.connect(...)
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE BATTERY_STG.STG_SENSOR_AGG")
    cursor.executemany("""
        INSERT INTO BATTERY_STG.STG_SENSOR_AGG
        (machine_id, avg_temp_c, total_cycles, avg_vibration_g,
         avg_current_a, avg_voltage_v, run_date, loaded_at)
        VALUES (:1,:2,:3,:4,:5,:6,TRUNC(SYSDATE),SYSDATE)
    """, rows)
    conn.commit()
```

### DAG Schedule

| DAG | Cron | Notes |
|---|---|---|
| `etl_supabase_to_oracle` | `0 6,14,22 * * *` | every 8h |
| `etl_influxdb_to_oracle` | `0 6,14,22 * * *` | every 8h |
| `sp_load_dw` | `30 6,14,22 * * *` | 30 min after extract |
| `sp_load_inventory` | `0 0 * * *` | daily midnight |

---

## 7. OLTP Schema — Supabase [A]

**17 tables, 5 domain groups, 3NF normalized**

### Domain 1 — Infrastructure (4 tables)

```sql
CREATE TABLE production_line (
    line_id              SERIAL PRIMARY KEY,
    name                 VARCHAR(50) NOT NULL,
    area                 VARCHAR(50),
    capacity_batches_hr  INTEGER
);

CREATE TABLE machine (
    machine_id       SERIAL PRIMARY KEY,
    name             VARCHAR(50)  NOT NULL,
    type             VARCHAR(20)  NOT NULL,
    -- FURNACE|CUTTER|MILL|PRESS|ASSEMBLER|CHARGER|TESTER
    line_id          INTEGER      REFERENCES production_line(line_id),
    ideal_cycle_sec  INTEGER      NOT NULL,
    status           VARCHAR(20)  DEFAULT 'ACTIVE'
);
-- Master: (1,'Smelting Furnace #1','FURNACE',1,120)
--         (2,'Plate Assembly Unit #1','ASSEMBLER',1,45)
--         (3,'Formation Charger #1','CHARGER',1,300)

CREATE TABLE process_stage (
    stage_id         SERIAL PRIMARY KEY,
    name             VARCHAR(50)  NOT NULL,
    sequence         INTEGER      NOT NULL,
    machine_id       INTEGER      REFERENCES machine(machine_id),
    -- NULL = stage has no IIoT sensor
    ideal_cycle_sec  INTEGER
);
-- Master: (1,'Lead Smelting',1,1,120)
--         (5,'Plate Assembly',5,2,45)
--         (8,'Formation Charging',8,3,300)

CREATE TABLE product (
    product_id   SERIAL PRIMARY KEY,
    sku          VARCHAR(30) UNIQUE NOT NULL,
    name         VARCHAR(100),
    voltage_v    DECIMAL(5,2),
    capacity_ah  DECIMAL(6,2)
);
```

### Domain 2 — Material Master (3 tables)

```sql
CREATE TABLE raw_material (
    material_id   SERIAL PRIMARY KEY,
    name          VARCHAR(100) NOT NULL,
    type          VARCHAR(50),
    unit          VARCHAR(20),
    hazard_class  VARCHAR(20)
);

CREATE TABLE bill_of_material (
    bom_id        SERIAL PRIMARY KEY,
    product_id    INTEGER REFERENCES product(product_id),
    material_id   INTEGER REFERENCES raw_material(material_id),
    qty_per_unit  DECIMAL(10,4) NOT NULL,
    unit          VARCHAR(20),
    UNIQUE (product_id, material_id)
);

CREATE TABLE supplier (
    supplier_id     SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,
    contact         VARCHAR(200),
    lead_time_days  INTEGER
);
```

### Domain 3 — Procurement & Inventory (3 tables)

```sql
CREATE TABLE raw_material_po (
    po_id          SERIAL PRIMARY KEY,
    material_id    INTEGER  REFERENCES raw_material(material_id),
    supplier_id    INTEGER  REFERENCES supplier(supplier_id),
    qty_ordered    DECIMAL(12,3) NOT NULL,
    order_date     DATE     NOT NULL,
    expected_date  DATE,
    status         VARCHAR(20) DEFAULT 'PENDING'
    -- PENDING|CONFIRMED|SHIPPED|RECEIVED|CANCELLED
);

CREATE TABLE raw_material_receipt (
    receipt_id     SERIAL PRIMARY KEY,
    po_id          INTEGER  REFERENCES raw_material_po(po_id),
    qty_received   DECIMAL(12,3) NOT NULL,
    received_date  DATE     NOT NULL
    -- 1 PO can have multiple receipts (partial delivery)
);

CREATE TABLE inventory (
    inventory_id   SERIAL PRIMARY KEY,
    material_id    INTEGER  REFERENCES raw_material(material_id) UNIQUE,
    qty_on_hand    DECIMAL(12,3) NOT NULL DEFAULT 0,
    qty_reserved   DECIMAL(12,3) DEFAULT 0,
    reorder_level  DECIMAL(12,3),
    warehouse_loc  VARCHAR(50),
    updated_at     TIMESTAMP DEFAULT NOW()
    -- available = qty_on_hand - qty_reserved
);
```

### Domain 4 — Production (4 tables)

```sql
CREATE TABLE production_order (
    order_id         SERIAL PRIMARY KEY,
    product_id       INTEGER  REFERENCES product(product_id),
    qty_ordered      INTEGER  NOT NULL,
    priority         VARCHAR(10) DEFAULT 'NORMAL',
    -- HIGH|NORMAL|LOW
    scheduled_start  DATE,
    scheduled_end    DATE,
    status           VARCHAR(20) DEFAULT 'PENDING'
);

CREATE TABLE production_batch (
    batch_id      SERIAL PRIMARY KEY,
    order_id      INTEGER  REFERENCES production_order(order_id),
    line_id       INTEGER  REFERENCES production_line(line_id),
    stage_id      INTEGER  REFERENCES process_stage(stage_id),
    started_at    TIMESTAMP NOT NULL,
    completed_at  TIMESTAMP,
    qty_produced  INTEGER  DEFAULT 0
);

CREATE TABLE finished_good (
    fg_id        SERIAL PRIMARY KEY,
    batch_id     INTEGER  REFERENCES production_batch(batch_id),
    serial_no    VARCHAR(50) UNIQUE NOT NULL,
    produced_at  TIMESTAMP DEFAULT NOW(),
    qc_status    VARCHAR(20) DEFAULT 'PENDING'
    -- PENDING|PASS|FAIL|QUARANTINE
);

CREATE TABLE material_consumption (
    consumption_id  SERIAL PRIMARY KEY,
    batch_id        INTEGER  REFERENCES production_batch(batch_id),
    material_id     INTEGER  REFERENCES raw_material(material_id),
    qty_used        DECIMAL(12,4) NOT NULL,
    consumed_at     TIMESTAMP DEFAULT NOW()
);
```

### Domain 5 — Quality & Maintenance (3 tables)

```sql
CREATE TABLE qc_inspection (
    qc_id        SERIAL PRIMARY KEY,
    batch_id     INTEGER  REFERENCES production_batch(batch_id),
    stage_id     INTEGER  REFERENCES process_stage(stage_id),
    sample_qty   INTEGER  NOT NULL,
    inspected_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE qc_result (
    result_id       SERIAL PRIMARY KEY,
    qc_id           INTEGER  REFERENCES qc_inspection(qc_id),
    parameter       VARCHAR(50),
    -- voltage|capacity|internal_resistance|weight
    measured_value  DECIMAL(12,4),
    spec_min        DECIMAL(12,4),
    spec_max        DECIMAL(12,4),
    pass_fail       VARCHAR(4)
    -- PASS|FAIL
    -- Specs: voltage 12.4-12.8V | capacity 58-62Ah
    --        resistance 0-8 mOhm | weight 14.5-15.5kg
);

CREATE TABLE maintenance_log (
    log_id        SERIAL PRIMARY KEY,
    machine_id    INTEGER  REFERENCES machine(machine_id),
    type          VARCHAR(20) NOT NULL,
    -- BREAKDOWN|PREVENTIVE|CHANGEOVER
    started_at    TIMESTAMP NOT NULL,
    ended_at      TIMESTAMP,
    downtime_min  INTEGER,
    -- key input for Availability OEE
    issue_code    VARCHAR(10)
    -- M01=Mechanical E02=Electrical O03=Operator
);
```

### ERD Relationships

```
PRODUCTION_LINE --< MACHINE --< PROCESS_STAGE
MACHINE --< MAINTENANCE_LOG
PROCESS_STAGE --< PRODUCTION_BATCH
PROCESS_STAGE --< QC_INSPECTION

PRODUCT --< BILL_OF_MATERIAL >-- RAW_MATERIAL
PRODUCT --< PRODUCTION_ORDER
SUPPLIER --< RAW_MATERIAL_PO >-- RAW_MATERIAL
RAW_MATERIAL_PO --< RAW_MATERIAL_RECEIPT
RAW_MATERIAL --< INVENTORY

PRODUCTION_ORDER --< PRODUCTION_BATCH
PRODUCTION_LINE --< PRODUCTION_BATCH
PRODUCTION_BATCH --< FINISHED_GOOD
PRODUCTION_BATCH --< MATERIAL_CONSUMPTION >-- RAW_MATERIAL
PRODUCTION_BATCH --< QC_INSPECTION --< QC_RESULT
```

---

## 8. Oracle Staging Schema

5 tables — raw extract buffer, no transform, TRUNCATE before each load (idempotent)

```sql
CREATE TABLE BATTERY_STG.STG_PRODUCTION_BATCH (
    batch_id         NUMBER, order_id         NUMBER,
    line_id          NUMBER, stage_id         NUMBER,
    started_at       TIMESTAMP, completed_at  TIMESTAMP,
    qty_produced     NUMBER,
    src_system       VARCHAR2(20)  DEFAULT 'SUPABASE',
    pipeline_run_id  VARCHAR2(100),
    loaded_at        TIMESTAMP DEFAULT SYSDATE
);

CREATE TABLE BATTERY_STG.STG_QC_INSPECTION (
    qc_id            NUMBER, batch_id      NUMBER,
    stage_id         NUMBER, sample_qty    NUMBER,
    inspected_at     TIMESTAMP,
    pipeline_run_id  VARCHAR2(100),
    loaded_at        TIMESTAMP DEFAULT SYSDATE
);

CREATE TABLE BATTERY_STG.STG_QC_RESULT (
    result_id       NUMBER, qc_id           NUMBER,
    parameter       VARCHAR2(50),
    measured_value  NUMBER, spec_min        NUMBER,
    spec_max        NUMBER, pass_fail       VARCHAR2(4),
    loaded_at       TIMESTAMP DEFAULT SYSDATE
);

CREATE TABLE BATTERY_STG.STG_MAINTENANCE_LOG (
    log_id       NUMBER, machine_id    NUMBER,
    type         VARCHAR2(20),
    started_at   TIMESTAMP, ended_at   TIMESTAMP,
    downtime_min NUMBER, issue_code    VARCHAR2(10),
    loaded_at    TIMESTAMP DEFAULT SYSDATE
);

-- Source: InfluxDB aggregated per machine (NOT raw 1Hz)
CREATE TABLE BATTERY_STG.STG_SENSOR_AGG (
    machine_id       VARCHAR2(20),
    run_date         DATE,
    avg_temp_c       NUMBER(8,2),
    total_cycles     NUMBER,
    avg_vibration_g  NUMBER(8,4),
    avg_current_a    NUMBER(8,2),
    avg_voltage_v    NUMBER(8,2),
    loaded_at        TIMESTAMP DEFAULT SYSDATE
);
```

### Lineage Columns

| Column | Value | Purpose |
|---|---|---|
| `src_system` | SUPABASE or INFLUXDB | Track data origin |
| `pipeline_run_id` | Airflow run_id | Trace to specific DAG run |
| `loaded_at` | SYSDATE | Audit timestamp |

---

## 9. Data Warehouse — Oracle [B]

**Star Schema: 5 Facts + 5 Dims**

| | OLTP | DW |
|---|---|---|
| Goal | Write fast | Aggregate fast |
| Keys | Natural | Surrogate (NUMBER) |
| Normal form | 3NF | Intentionally de-normalized |
| Update | Per transaction | Every 8h batch |

### Dimension Tables

```sql
-- DIM_DATE: generated once (5 years), NOT ETL'd from OLTP
CREATE TABLE BATTERY_DW.DIM_DATE (
    date_id      NUMBER PRIMARY KEY,  -- YYYYMMDD format
    full_date    DATE   NOT NULL,
    day_of_week  NUMBER, week_number NUMBER,
    month_number NUMBER, month_name  VARCHAR2(20),
    quarter      NUMBER, year        NUMBER,
    is_weekend   CHAR(1) DEFAULT 'N',
    is_holiday   CHAR(1) DEFAULT 'N'
);

-- DIM_MACHINE: includes line_name (denormalized = no JOIN needed at query time)
CREATE TABLE BATTERY_DW.DIM_MACHINE (
    machine_id       NUMBER PRIMARY KEY,
    machine_src_id   NUMBER,      -- OLTP machine_id for ETL lookup
    machine_name     VARCHAR2(50),
    machine_type     VARCHAR2(20),
    line_name        VARCHAR2(50),
    ideal_cycle_sec  NUMBER
);

-- DIM_PRODUCT, DIM_STAGE, DIM_MATERIAL: same pattern
-- Each has {name}_src_id for ETL reverse-lookup to OLTP
```

### Fact Tables

```sql
-- FACT_OEE: grain = 1 machine x 1 day  ->  3 rows/day
CREATE TABLE BATTERY_DW.FACT_OEE (
    oee_id           NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_id          NUMBER REFERENCES BATTERY_DW.DIM_DATE(date_id),
    machine_id       NUMBER REFERENCES BATTERY_DW.DIM_MACHINE(machine_id),
    product_id       NUMBER REFERENCES BATTERY_DW.DIM_PRODUCT(product_id),
    -- Additive measures (safe to SUM across time and machines)
    planned_time_min NUMBER,
    actual_run_min   NUMBER,
    downtime_min     NUMBER,
    units_planned    NUMBER,
    units_produced   NUMBER,
    units_good       NUMBER,
    -- NON-additive (NEVER SUM -- must recalculate from raw)
    availability_pct NUMBER(5,2),
    performance_pct  NUMBER(5,2),
    quality_pct      NUMBER(5,2),
    oee_pct          NUMBER(5,2)
);

-- FACT_PRODUCTION: grain = 1 batch x 1 stage
CREATE TABLE BATTERY_DW.FACT_PRODUCTION (
    production_id      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_id            NUMBER, machine_id         NUMBER,
    stage_id           NUMBER, product_id         NUMBER,
    units_produced     NUMBER, avg_cycle_time_sec NUMBER,
    batch_duration_min NUMBER, yield_rate         NUMBER(5,4)
);

-- FACT_QUALITY: grain = 1 QC inspection
CREATE TABLE BATTERY_DW.FACT_QUALITY (
    quality_id       NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_id          NUMBER, product_id NUMBER, stage_id NUMBER,
    samples_taken    NUMBER, pass_count  NUMBER, fail_count      NUMBER,
    defect_rate_pct  NUMBER(5,2),
    top_defect_param VARCHAR2(50)
);

-- FACT_INVENTORY: grain = 1 material x 1 day (periodic snapshot)
CREATE TABLE BATTERY_DW.FACT_INVENTORY (
    inventory_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_id      NUMBER, material_id  NUMBER,
    qty_opening  NUMBER, qty_received NUMBER,
    qty_consumed NUMBER,
    qty_closing  NUMBER,  -- = opening + received - consumed
    stock_value  NUMBER
);

-- FACT_MAINTENANCE: grain = 1 event
CREATE TABLE BATTERY_DW.FACT_MAINTENANCE (
    maintenance_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_id        NUMBER, machine_id   NUMBER,
    event_type     VARCHAR2(20),  -- BREAKDOWN|PREVENTIVE|CHANGEOVER
    downtime_min   NUMBER,
    mtbf_hrs       NUMBER,  -- Mean Time Between Failures
    mttr_min       NUMBER,  -- Mean Time To Repair
    issue_code     VARCHAR2(10)
);
```

### Non-Additive Measures Rule

```sql
-- WRONG: SUM of percentages has no statistical meaning
SELECT SUM(oee_pct) FROM FACT_OEE;

-- CORRECT: always recalculate from raw additive measures
SELECT m.machine_name,
    ROUND(
        ((SUM(f.planned_time_min)-SUM(f.downtime_min))/SUM(f.planned_time_min)) *
        (SUM(f.units_produced*m.ideal_cycle_sec/60.0)/SUM(f.actual_run_min)) *
        (SUM(f.units_good)/SUM(f.units_produced)) * 100, 2
    ) AS oee_pct
FROM BATTERY_DW.FACT_OEE f
JOIN BATTERY_DW.DIM_MACHINE m ON f.machine_id = m.machine_id
GROUP BY m.machine_name;
```

---

## 10. Stored Procedures & Functions — [C]

### FN_CALC_OEE — Pure Function

```sql
CREATE OR REPLACE FUNCTION BATTERY_DW.FN_CALC_OEE(
    p_planned_min NUMBER, p_downtime_min NUMBER, p_actual_run NUMBER,
    p_units_prod  NUMBER, p_ideal_cycle  NUMBER, p_units_good NUMBER
) RETURN NUMBER IS
    v_a NUMBER; v_p NUMBER; v_q NUMBER;
BEGIN
    v_a := (p_planned_min - p_downtime_min) / NULLIF(p_planned_min, 0);
    v_p := (p_units_prod * p_ideal_cycle / 60.0) / NULLIF(p_actual_run, 0);
    v_q := p_units_good / NULLIF(p_units_prod, 0);
    RETURN ROUND(LEAST(v_a,1) * LEAST(v_p,1) * LEAST(v_q,1) * 100, 2);
EXCEPTION
    WHEN ZERO_DIVIDE THEN RETURN 0;
    WHEN OTHERS      THEN RETURN NULL;
END;
/
```

### SP_LOAD_FACT_OEE — Core ETL (3 rows per call)

```sql
CREATE OR REPLACE PROCEDURE BATTERY_DW.SP_LOAD_FACT_OEE(p_date DATE) IS
    v_date_id    NUMBER;
    v_actual_run NUMBER; v_units_prod  NUMBER;
    v_down_min   NUMBER; v_units_good  NUMBER;
    c_planned    CONSTANT NUMBER := 480;  -- 8h hardcoded
BEGIN
    SELECT date_id INTO v_date_id
    FROM BATTERY_DW.DIM_DATE WHERE full_date = TRUNC(p_date);

    DELETE FROM BATTERY_DW.FACT_OEE WHERE date_id = v_date_id;  -- idempotent

    FOR rec IN (SELECT machine_id, machine_src_id, ideal_cycle_sec
                FROM BATTERY_DW.DIM_MACHINE) LOOP

        SELECT NVL(SUM((EXTRACT(HOUR   FROM (completed_at-started_at))*60)
                      + EXTRACT(MINUTE FROM (completed_at-started_at))), 0),
               NVL(SUM(qty_produced), 0)
        INTO v_actual_run, v_units_prod
        FROM BATTERY_STG.STG_PRODUCTION_BATCH
        WHERE stage_id = rec.machine_src_id AND completed_at IS NOT NULL;

        SELECT NVL(SUM(downtime_min), 0) INTO v_down_min
        FROM BATTERY_STG.STG_MAINTENANCE_LOG
        WHERE machine_id = rec.machine_src_id;

        SELECT NVL(COUNT(*), 0) INTO v_units_good
        FROM BATTERY_STG.STG_QC_RESULT qr
        JOIN BATTERY_STG.STG_QC_INSPECTION qi ON qr.qc_id = qi.qc_id
        WHERE qr.pass_fail = 'PASS' AND qi.stage_id = rec.machine_src_id;

        INSERT INTO BATTERY_DW.FACT_OEE (
            date_id, machine_id, planned_time_min,
            actual_run_min, downtime_min, units_produced, units_good,
            availability_pct, performance_pct, quality_pct, oee_pct
        ) VALUES (
            v_date_id, rec.machine_id, c_planned,
            v_actual_run, v_down_min, v_units_prod, v_units_good,
            ROUND((c_planned-v_down_min)/NULLIF(c_planned,0)*100,2),
            ROUND((v_units_prod*rec.ideal_cycle_sec/60.0)/NULLIF(v_actual_run,0)*100,2),
            ROUND(v_units_good/NULLIF(v_units_prod,0)*100,2),
            FN_CALC_OEE(c_planned, v_down_min, v_actual_run,
                        v_units_prod, rec.ideal_cycle_sec, v_units_good)
        );
    END LOOP;
    COMMIT;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('Date not found in DIM_DATE: ' || TO_CHAR(p_date));
    WHEN OTHERS THEN ROLLBACK; RAISE;
END;
/
```

### Execution Order

```sql
-- One-time setup
EXEC BATTERY_DW.SP_LOAD_DIM_DATE(TO_DATE('2020-01-01','YYYY-MM-DD'), 1826);
EXEC BATTERY_DW.SP_LOAD_DIM_MACHINE();
EXEC BATTERY_DW.SP_LOAD_DIM_PRODUCT();

-- Every 8h (Airflow OracleOperator)
EXEC BATTERY_DW.SP_LOAD_FACT_OEE(SYSDATE);
EXEC BATTERY_DW.SP_LOAD_FACT_PRODUCTION(SYSDATE);
EXEC BATTERY_DW.SP_LOAD_FACT_QUALITY(SYSDATE);
EXEC BATTERY_DW.SP_LOAD_FACT_MAINTENANCE(SYSDATE);

-- Daily at midnight
EXEC BATTERY_DW.SP_LOAD_FACT_INVENTORY(TRUNC(SYSDATE));
```

### Reporting Queries [B]-4

```sql
-- 1. OEE by machine by date
SELECT m.machine_name, d.full_date,
       f.availability_pct, f.performance_pct, f.quality_pct, f.oee_pct
FROM BATTERY_DW.FACT_OEE f
JOIN BATTERY_DW.DIM_MACHINE m ON f.machine_id = m.machine_id
JOIN BATTERY_DW.DIM_DATE    d ON f.date_id    = d.date_id
ORDER BY d.full_date DESC, f.oee_pct DESC;

-- 2. Defect rate by stage
SELECT s.stage_name, s.sequence,
       ROUND(AVG(fq.defect_rate_pct),2) AS avg_defect_pct,
       SUM(fq.fail_count)               AS total_fails,
       MAX(fq.top_defect_param)         AS common_defect
FROM BATTERY_DW.FACT_QUALITY fq
JOIN BATTERY_DW.DIM_STAGE s ON fq.stage_id = s.stage_id
GROUP BY s.stage_name, s.sequence ORDER BY s.sequence;

-- 3. Material consumption
SELECT mat.material_name, mat.unit,
       SUM(fi.qty_consumed) AS consumed,
       SUM(fi.qty_received) AS received,
       MIN(fi.qty_closing)  AS current_stock
FROM BATTERY_DW.FACT_INVENTORY fi
JOIN BATTERY_DW.DIM_MATERIAL mat ON fi.material_id = mat.material_id
GROUP BY mat.material_name, mat.unit;

-- 4. MTBF / MTTR
SELECT m.machine_name,
       COUNT(*)                    AS breakdown_count,
       ROUND(AVG(fm.mtbf_hrs),2)   AS avg_mtbf_hrs,
       ROUND(AVG(fm.mttr_min),2)   AS avg_mttr_min,
       SUM(fm.downtime_min)        AS total_downtime_min
FROM BATTERY_DW.FACT_MAINTENANCE fm
JOIN BATTERY_DW.DIM_MACHINE m ON fm.machine_id = m.machine_id
WHERE fm.event_type = 'BREAKDOWN'
GROUP BY m.machine_name ORDER BY total_downtime_min DESC;

-- 5. Weekly OEE trend (recalculated from raw additive measures)
SELECT d.year, d.week_number, m.machine_name,
    ROUND(
        ((SUM(f.planned_time_min)-SUM(f.downtime_min))/SUM(f.planned_time_min)) *
        (SUM(f.units_produced*m.ideal_cycle_sec/60.0)/SUM(f.actual_run_min)) *
        (SUM(f.units_good)/SUM(f.units_produced)) * 100, 2
    ) AS weekly_oee
FROM BATTERY_DW.FACT_OEE f
JOIN BATTERY_DW.DIM_DATE    d ON f.date_id    = d.date_id
JOIN BATTERY_DW.DIM_MACHINE m ON f.machine_id = m.machine_id
GROUP BY d.year, d.week_number, m.machine_name, m.ideal_cycle_sec
ORDER BY d.year, d.week_number;
```

### Truncate + Reload [B]-5

```sql
-- Truncate facts first (FK references dims)
TRUNCATE TABLE BATTERY_DW.FACT_MAINTENANCE;
TRUNCATE TABLE BATTERY_DW.FACT_INVENTORY;
TRUNCATE TABLE BATTERY_DW.FACT_QUALITY;
TRUNCATE TABLE BATTERY_DW.FACT_PRODUCTION;
TRUNCATE TABLE BATTERY_DW.FACT_OEE;
-- Then dims (skip DIM_DATE: pre-populated, keep it)
TRUNCATE TABLE BATTERY_DW.DIM_STAGE;
TRUNCATE TABLE BATTERY_DW.DIM_MATERIAL;
TRUNCATE TABLE BATTERY_DW.DIM_PRODUCT;
TRUNCATE TABLE BATTERY_DW.DIM_MACHINE;
-- Re-run ETL
EXEC BATTERY_DW.SP_LOAD_FACT_OEE(SYSDATE);
EXEC BATTERY_DW.SP_LOAD_FACT_QUALITY(SYSDATE);
EXEC BATTERY_DW.SP_LOAD_FACT_MAINTENANCE(SYSDATE);
EXEC BATTERY_DW.SP_LOAD_FACT_INVENTORY(TRUNC(SYSDATE));
```

---

## 11. Dashboard — FastAPI + Streamlit

### FastAPI

```python
from fastapi import FastAPI
import cx_Oracle

app = FastAPI(title="OEE API")

def get_conn():
    return cx_Oracle.connect(user="dw_user", password="...",
                              dsn="161.246.35.92:1521/orcl")

@app.get("/api/oee/daily")
def get_oee_daily(date: str):
    cursor = get_conn().cursor()
    cursor.execute("""
        SELECT m.machine_name, m.machine_type,
               f.availability_pct, f.performance_pct,
               f.quality_pct, f.oee_pct
        FROM BATTERY_DW.FACT_OEE f
        JOIN BATTERY_DW.DIM_MACHINE m ON f.machine_id = m.machine_id
        JOIN BATTERY_DW.DIM_DATE    d ON f.date_id    = d.date_id
        WHERE d.full_date = TO_DATE(:1, 'YYYY-MM-DD')
    """, [date])
    cols = [c[0].lower() for c in cursor.description]
    return {"data": [dict(zip(cols, row)) for row in cursor.fetchall()]}

@app.get("/api/quality/defect-by-stage")
def get_defect_by_stage(date_from: str, date_to: str): ...

@app.get("/api/maintenance/mtbf-mttr")
def get_mtbf_mttr(): ...
```

### Streamlit

```python
import streamlit as st
import requests, pandas as pd

st.set_page_config(page_title="OEE Dashboard", layout="wide")
st.title("Battery Manufacturing — OEE Dashboard")

API_BASE = "http://fastapi:8000/api"

with st.sidebar:
    selected_date = st.date_input("Date")

df = pd.DataFrame(
    requests.get(f"{API_BASE}/oee/daily",
                 params={"date": selected_date}).json()["data"]
)

col1, col2, col3, col4 = st.columns(4)
col1.metric("OEE",          f"{df['oee_pct'].mean():.1f}%")
col2.metric("Availability", f"{df['availability_pct'].mean():.1f}%")
col3.metric("Performance",  f"{df['performance_pct'].mean():.1f}%")
col4.metric("Quality",      f"{df['quality_pct'].mean():.1f}%")

st.bar_chart(df.set_index("machine_name")["oee_pct"])
st.dataframe(df, use_container_width=True)
```

---

## 12. Deployment — Docker Compose

```yaml
version: '3.8'
services:
  mosquitto:
    image: eclipse-mosquitto:2
    ports: ["1883:1883"]

  nodered:
    image: nodered/node-red:latest
    ports: ["1880:1880"]
    depends_on: [mosquitto]

  influxdb:
    image: influxdb:2.0
    ports: ["8086:8086"]
    environment:
      DOCKER_INFLUXDB_INIT_MODE:   setup
      DOCKER_INFLUXDB_INIT_ORG:    factory
      DOCKER_INFLUXDB_INIT_BUCKET: sensors

  postgres-af:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow

  airflow-webserver:
    image: apache/airflow:2.8.0
    ports: ["8080:8080"]
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: >-
        postgresql+psycopg2://airflow:airflow@postgres-af/airflow
    volumes: ["./dags:/opt/airflow/dags"]
    depends_on: [postgres-af]

  airflow-scheduler:
    image: apache/airflow:2.8.0
    command: scheduler
    volumes: ["./dags:/opt/airflow/dags"]
    depends_on: [postgres-af]

  fastapi:
    build: ./fastapi
    ports: ["8000:8000"]
    environment:
      ORACLE_DSN: "161.246.35.92:1521/orcl"

  streamlit:
    build: ./streamlit
    ports: ["8501:8501"]
    environment:
      API_BASE: "http://fastapi:8000/api"
    depends_on: [fastapi]
```

---

## 13. Generated Files

| File | Description | Command |
|---|---|---|
| `battery_oracle_ddl.sql` | Oracle DDL all 3 schemas (~1,027 lines) | `@battery_oracle_ddl.sql` |
| `battery_mock_data.sql` | 14,589 INSERT rows / 30 days | `@battery_mock_data.sql` |
| `generate_mock_data.py` | Python generator — edit params and re-run | `python generate_mock_data.py` |

### DDL Script Sections

| Section | Content |
|---|---|
| §0 | CREATE USER + GRANT for 3 schemas |
| §1 | BATTERY_OLTP — 17 tables + CHECK constraints + indexes |
| §2 | BATTERY_STG — 5 staging tables + lineage columns |
| §3 | BATTERY_DW — 5 dims + 5 facts + FK constraints |
| §4 | Master data INSERT (machines, stages, products, materials) |
| §5 | FN_CALC_OEE + SP_LOAD_FACT_* + SP_LOAD_DIM_DATE |
| §6 | Truncate + Reload [B]-5 |
| §7 | Reporting queries [B]-4 (5 queries) |

---

## 14. Assignment Deliverable Mapping

| # | Deliverable | Content | Tool |
|---|---|---|---|
| **[A]-1** | ERD Diagram | 17 entities, 5 domain groups | Supabase Studio |
| **[A]-2** | DBA Script OLTP | Section §1 of battery_oracle_ddl.sql | Oracle iSQL*Plus |
| **[A]-3** | Mock Data | battery_mock_data.sql (14,589 rows) | iSQL*Plus |
| **[B]-1** | DW Schema Diagram | Star schema: 5 facts + 5 dims | draw.io |
| **[B]-2** | DBA Script DW | Section §3 of battery_oracle_ddl.sql | Oracle iSQL*Plus |
| **[B]-3** | ETL Demo | Run SPs, show FACT_OEE before/after | iSQL*Plus |
| **[B]-4** | SQL Reporting | Section §7 of battery_oracle_ddl.sql | Oracle SQL |
| **[B]-5** | Truncate + Reload | Section §6 of battery_oracle_ddl.sql | Oracle SQL |
| **[C]-1** | Stored Procedures | SP_LOAD_FACT_OEE/QUALITY/MAINTENANCE/INVENTORY | Oracle PL/SQL |
| **[C]-2** | Functions | FN_CALC_OEE(A,P,Q) | Oracle PL/SQL |
| **[C]-3** | SP Demo | Call SPs, verify 3 rows/day in FACT_OEE | iSQL*Plus |



*OLTP: 17 tables · STG: 5 tables · DW: 5 facts + 5 dims*
*Files: battery_oracle_ddl.sql · battery_mock_data.sql · generate_mock_data.py*