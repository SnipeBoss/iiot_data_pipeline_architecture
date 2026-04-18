# Battery Process Performance Dashboard

End-to-end data pipeline สำหรับวัด process performance ของสายประกอบ battery
(1 line, 3 เครื่อง — M01/M02/M03). Ingest ข้อมูลจาก OLTP (Supabase) และ
IIoT sensor (AWS InfluxDB) → Oracle Data Warehouse → Streamlit dashboard
รีเฟรชทุก 15 นาที

> Architecture ปัจจุบันตาม [`NEW_ARCHITECTURE.md`](NEW_ARCHITECTURE.md) — ลดรูป
> จาก OEE-focused design เดิมให้เหมาะกับ POC ที่มี scope สั้น

---

## Table of Contents

- [Stack](#stack)
- [Directory Layout](#directory-layout)
- [Data Flow](#data-flow)
- [Prerequisites](#prerequisites)
- [First-Time Setup (Step-by-Step)](#first-time-setup-step-by-step)
  - [1. Python environment](#1-python-environment)
  - [2. Oracle JDBC driver](#2-oracle-jdbc-driver)
  - [3. `.env` file](#3-env-file)
  - [4. Smoke-test connectors](#4-smoke-test-connectors)
  - [5. สร้าง Supabase schema + mock data](#5-สร้าง-supabase-schema--mock-data)
  - [6. สร้าง Oracle DW schema](#6-สร้าง-oracle-dw-schema)
  - [7. Seed DIM tables](#7-seed-dim-tables)
  - [8. Start services](#8-start-services)
  - [9. Trigger first ETL run](#9-trigger-first-etl-run)
  - [10. เปิด dashboard](#10-เปิด-dashboard)
- [Daily Operations](#daily-operations)
- [Service Management](#service-management)
- [Troubleshooting](#troubleshooting)
- [Scripts Reference](#scripts-reference)
- [Key Design Notes](#key-design-notes)
- [Testing](#testing)
- [Progress & Planning](#progress--planning)

---

## Stack

| Layer | Tech | Role |
|---|---|---|
| OLTP | Supabase (PostgreSQL) | Production orders, batches, QC records (6 ตาราง) |
| IIoT | NodeRED → InfluxDB 2.0 (AWS) | sensor 1 Hz, 6 fields × 3 machines |
| Orchestration | Apache Airflow 2.8 (Docker) | 3 DAGs รันทุก 15 นาที |
| Serving | FastAPI (uvicorn) | HTTP wrapper รอบ Oracle JDBC |
| Data Warehouse | Oracle 10.2.0.3 @ KMITL (`AI03`) | 3 STG + 4 DIM + 3 FACT |
| Dashboard | Streamlit | Production / Quality / Sensor / Machine Status |

---

## Directory Layout

```
unified_iiot_data_architecture/
├── app/
│   ├── api/main.py                  # FastAPI: /health, /sql/*, /sp/*, /api/*
│   └── streamlit/dashboard.py       # 4-tab dashboard
│
├── db_module/
│   ├── db_conn/                     # OracleConnector / SupabaseConnector / InfluxConnector
│   ├── db_sources/
│   │   ├── supabases_sql_query/
│   │   │   ├── query/               # 01_schema.sql (6 ตาราง) + 02_master_data.sql
│   │   │   ├── mock/                # generate_mock_data.py + 03_mock_data.sql
│   │   │   └── apply_supabase.py    # apply 3 ไฟล์ + audit counts
│   │   ├── oracle_sql_query/
│   │   │   ├── query/               # 01_schema.sql + 02-05 (procedures, reporting, truncate)
│   │   │   ├── run_sql_file.py      # รัน SQL ไฟล์ต่อ Oracle (รองรับ PL/SQL `/` terminator)
│   │   │   ├── sync_dimensions_from_supabase.py   # seed DIM_MACHINE + DIM_PRODUCT
│   │   │   └── verify_warehouse_schema.py         # smoke-check 10 table + 5 seq + 4 proc
│   │   └── iiot_container/          # NodeRED flow (reference only — runs on AWS)
│   └── pipeline/
│       ├── Dockerfile               # Airflow 2.8 image (no Java in container)
│       ├── docker-compose.yml       # postgres-af + scheduler + webserver:8088
│       └── airflow/dags/
│           ├── etl_supabase_to_oracle.py      # production_batch + qc_record → STG
│           ├── etl_influxdb_to_oracle.py      # 15-min aggregate → STG_SENSOR_AGG
│           └── sp_load_dw.py                  # chain 3 fact loaders (5-min offset)
│
├── test/                            # pytest smoke tests (connector round-trips)
├── claude_track/
│   ├── PLAN.md                      # authoritative progress tracker
│   ├── NEW_PLAN.md                  # migration plan (2026-04-19)
│   └── problems_requirements_erd_mapping.md   # business req + ER diagrams
├── NEW_ARCHITECTURE.md              # spec ของ design ปัจจุบัน
└── CLAUDE.md                        # GS-MAD config
```

---

## Data Flow

```
┌─────────────┐                              ┌──────────────────┐
│  Supabase   │                              │  AWS InfluxDB 2  │
│ (6 OLTP)    │                              │  bucket=iiot_... │
└──────┬──────┘                              └──────────┬───────┘
       │ psycopg2                                       │ influxdb-client
       ▼                                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  Airflow (docker compose) — schedule */15 * * * *               │
│  ─────────────────────────────────────────────                  │
│  etl_supabase_to_oracle     etl_influxdb_to_oracle              │
│   └─ extract_production_batch  └─ aggregate_sensor              │
│   └─ extract_qc_record                                          │
│         ↓ HTTP bulk-insert (Bearer token)                       │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  FastAPI (uvicorn @ localhost:8000)                             │
│   Operational: /sql/query /sql/execute /sp/call /sql/bulk-insert│
│   Dashboard:   /api/production/*  /api/quality/*  /api/sensor/* │
└─────────────────────────────────────────────────────────────────┘
                           │ JDBC thin (O3LOGON)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  Oracle 10g (KMITL AI03)                                        │
│   STG:  STG_PRODUCTION_BATCH · STG_QC_RECORD · STG_SENSOR_AGG   │
│   DIM:  DIM_DATE · DIM_MACHINE · DIM_PRODUCT · DIM_METRIC       │
│   FACT: FACT_PRODUCTION · FACT_QUALITY · FACT_SENSOR            │
│                                                                 │
│   sp_load_dw (5,20,35,50 * * * *)                               │
│    └─ SP_LOAD_FACT_PRODUCTION  (merge by batch_src_id)          │
│    └─ SP_LOAD_FACT_QUALITY     (merge by batch_src_id)          │
│    └─ SP_LOAD_FACT_SENSOR      (merge by machine+metric+window) │
└─────────────────────────────────────────────────────────────────┘
                           │ HTTP (read)
                           ▼
┌──────────────────────────────────┐
│  Streamlit (localhost:8501)      │
│   Tab 1: Production overview     │
│   Tab 2: Quality / defect rate   │
│   Tab 3: Sensor per batch        │
│   Tab 4: Machine status 15-min   │
└──────────────────────────────────┘
```

---

## Prerequisites

ต้องติดตั้งก่อนเริ่ม setup:

| Tool | Version | ใช้ทำ | วิธีติดตั้ง (macOS) |
|---|---|---|---|
| **Python** | 3.12 | รัน connector/script/API/dashboard | `brew install python@3.12` |
| **Java JDK** | 17+ | JVM สำหรับ `jpype` + Oracle JDBC | `brew install openjdk@17` |
| **Docker Desktop** | — | รัน Airflow stack | [docker.com/desktop](https://www.docker.com/products/docker-desktop/) |
| **Git** | — | clone repo | `brew install git` |

**Verify ก่อนเริ่ม:**

```bash
python3.12 --version     # Python 3.12.x
java -version            # openjdk 17.x
docker --version         # Docker 24+
docker compose version   # Compose 2.x
```

**External accounts ต้องเตรียม:**
- KMITL Oracle AI03 credentials (host/port/service/user/password)
- Supabase project (host/password — ดูใน dashboard Supabase)
- AWS InfluxDB endpoint + token (จาก admin IIoT)

---

## First-Time Setup (Step-by-Step)

### 1. Python environment

```bash
git clone <repo-url>
cd unified_iiot_data_architecture

python3.12 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt
```

**Verify:**
```bash
.venv/bin/python --version                       # Python 3.12.x
.venv/bin/python -c "import fastapi, streamlit, jaydebeapi; print('OK')"
```

---

### 2. Oracle JDBC driver

ต้องมี `ojdbc8.jar` เพื่อเชื่อม Oracle 10g

```bash
# Download ojdbc8.jar (version 19.23 ขึ้นไป)
# https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html
mkdir -p db_module/db_conn/oracle/drivers
mv ~/Downloads/ojdbc8.jar db_module/db_conn/oracle/drivers/

ls -la db_module/db_conn/oracle/drivers/ojdbc8.jar
# ต้องเห็นไฟล์ขนาด ~4.5 MB
```

---

### 3. `.env` file

คัดลอก `.env.example` → `.env` แล้วเติมค่า:

```bash
cp .env.example .env
# แก้ไขด้วย editor ที่ถนัด
code .env   # หรือ vim, nano
```

**Fields ทั้งหมด:**

```env
# ========== Oracle AI03 (KMITL) ==========
ORACLE_HOST=161.246.35.92
ORACLE_PORT=1521
ORACLE_SERVICE=orcl
ORACLE_USER=AI03
ORACLE_PASSWORD=<ถามอาจารย์/DBA>
ORACLE_JDBC_JAR=db_module/db_conn/oracle/drivers/ojdbc8.jar

# ORACLE_API_* ใช้โดย Airflow DAG + dashboard
ORACLE_API_URL=http://localhost:8000
ORACLE_API_TOKEN=<random string ยาว ๆ เช่น openssl rand -hex 32>

# ========== JVM (สำหรับ jpype บน macOS) ==========
JAVA_HOME=/opt/homebrew/opt/openjdk@17

# ========== Supabase ==========
SUPABASE_HOST=db.<project-ref>.supabase.co
SUPABASE_PORT=5432
SUPABASE_DB=postgres
SUPABASE_USER=postgres
SUPABASE_PASSWORD=<Supabase dashboard → Settings → Database>
SUPABASE_SSLMODE=require

# ========== InfluxDB 2.0 (AWS) ==========
INFLUX_URL=http://<ec2-public-ip>:8086
INFLUX_ORG=factory
INFLUX_BUCKET=iiot_data_raw
INFLUX_TOKEN=<InfluxDB UI → Data → API Tokens>
```

**สำคัญ:** `.env` ถูก `.gitignore` ไว้ ห้าม commit

---

### 4. Smoke-test connectors

ก่อนสร้าง schema ให้ทดสอบ credentials ทั้ง 3 ระบบก่อน:

```bash
.venv/bin/python -m pytest test/test_connectors.py -v
```

**ควรได้:**
```
test_oracle_connector_roundtrip PASSED
test_supabase_connector_roundtrip PASSED
test_influx_connector_query PASSED
```

ถ้าเจอ error → ดู [Troubleshooting](#troubleshooting)

---

### 5. สร้าง Supabase schema + mock data

Supabase ต้องมี 6 ตาราง + master data + mock batches ก่อน (เพราะ Oracle จะ sync DIM จาก Supabase)

```bash
# 5.1 Generate mock SQL (window ตรงกับ InfluxDB live data)
.venv/bin/python db_module/db_sources/supabases_sql_query/mock/generate_mock_data.py
```

**Output ควรเห็น:**
```
Wrote db_module/db_sources/supabases_sql_query/mock/03_mock_data.sql
  Window: 2026-04-18 04:30:00 → 2026-04-18 20:00:00 UTC
  production_order     3
  production_batch     6   (5 finished, 1 in-progress)
  qc_record            5
```

> หมายเหตุ: window ของ mock ควรทับกับช่วงเวลาที่ InfluxDB มีข้อมูลจริง
> (สคริปต์ hardcode `2026-04-18 04:30-20:00 UTC` ตามข้อมูลที่ NodeRED inject)
> ถ้า InfluxDB เปลี่ยน window ต้องแก้ `generate_mock_data.py`

```bash
# 5.2 Apply 3 ไฟล์ SQL เข้า Supabase (schema + master + mock ใน transaction เดียว)
.venv/bin/python db_module/db_sources/supabases_sql_query/apply_supabase.py
```

**Output:**
```
[apply] query/01_schema.sql (~4 KB)
[apply] query/02_master_data.sql (~2 KB)
[apply] mock/03_mock_data.sql (~2 KB)
OK — all files applied and committed.

Row counts:
  production_line               1
  machine                       3
  product                       3
  production_order              3
  production_batch              6
  qc_record                     5
```

---

### 6. สร้าง Oracle DW schema

```bash
# 6.1 Schema (3 STG + 4 DIM + 3 FACT + 5 sequences + DIM_METRIC seed)
.venv/bin/python db_module/db_sources/oracle_sql_query/run_sql_file.py \
    db_module/db_sources/oracle_sql_query/query/01_schema.sql

# 6.2 Stored procedure สำหรับ DIM_DATE
.venv/bin/python db_module/db_sources/oracle_sql_query/run_sql_file.py \
    db_module/db_sources/oracle_sql_query/query/02_procedure_dim_date.sql

# 6.3 Fact loader procedures (3 ตัว — merge by key)
.venv/bin/python db_module/db_sources/oracle_sql_query/run_sql_file.py \
    db_module/db_sources/oracle_sql_query/query/03_procedure_fact_loaders.sql
```

**Verify:**
```bash
.venv/bin/python db_module/db_sources/oracle_sql_query/verify_warehouse_schema.py
```

**ควรเห็น 10 ✓ tables + 5 ✓ sequences + 4 ✓ procedures:**
```
Tables in AI03: 10
  ✓ STG_PRODUCTION_BATCH
  ✓ STG_QC_RECORD
  ✓ STG_SENSOR_AGG
  ✓ DIM_DATE
  ✓ DIM_MACHINE
  ...

OK — all expected objects present.
```

---

### 7. Seed DIM tables

`DIM_METRIC` ถูก seed ตอนรัน `01_schema.sql` แล้ว (6 rows)

ต้อง populate เพิ่ม:
- **DIM_DATE** — calendar 1 ปี
- **DIM_MACHINE / DIM_PRODUCT** — sync จาก Supabase

```bash
# 7.1 DIM_DATE: 365 วัน เริ่ม 2026-01-01
.venv/bin/python -c "
from db_module.db_conn import OracleConnector
conn = OracleConnector().connect()
cur = conn.cursor()
cur.execute(\"BEGIN SP_LOAD_DIM_DATE(DATE '2026-01-01', 365); END;\")
conn.commit()
cur.execute('SELECT COUNT(*) FROM DIM_DATE')
print('DIM_DATE rows:', int(cur.fetchone()[0]))
cur.close(); conn.close()
"
```

**ควรได้:** `DIM_DATE rows: 365`

```bash
# 7.2 DIM_MACHINE + DIM_PRODUCT: sync จาก Supabase
.venv/bin/python db_module/db_sources/oracle_sql_query/sync_dimensions_from_supabase.py
```

**Output:**
```
  DIM_MACHINE  loaded 3 rows
  DIM_PRODUCT  loaded 3 rows

Oracle dim readback:
  DIM_MACHINE   3
  DIM_PRODUCT   3
  DIM_METRIC    6
  DIM_DATE      365
```

---

### 8. Start services

จำเป็นต้อง start 3 services:

```bash
# 8.1 FastAPI (background)
.venv/bin/uvicorn app.api.main:app --host 0.0.0.0 --port 8000 &

# 8.2 Airflow stack (Docker)
cd db_module/pipeline
docker compose up -d
cd -

# รอ Airflow พร้อม (~30 วินาที)
sleep 30

# 8.3 Streamlit dashboard
.venv/bin/streamlit run app/streamlit/dashboard.py --server.port 8501 &
```

**Verify ทีละ service:**

```bash
# FastAPI: ใช้ TOKEN จาก .env
TOKEN=$(grep ORACLE_API_TOKEN .env | cut -d= -f2 | tr -d '"')
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8000/health
# ต้องได้: {"status":"ok","oracle_user":"AI03",...}

# Airflow: ดู status container
docker compose -f db_module/pipeline/docker-compose.yml ps
# ต้องเห็น scheduler + webserver + postgres-af = Up (healthy)

# Streamlit
curl -s http://localhost:8501/_stcore/health
# ต้องได้: ok
```

---

### 9. Trigger first ETL run

Airflow DAG จะรันอัตโนมัติทุก 15 นาที แต่สำหรับครั้งแรกให้ trigger manual เพื่อทดสอบ

**ใน Airflow UI (http://localhost:8088, admin/admin):**
1. เปิด DAG `etl_supabase_to_oracle` → กด toggle pause off → กด "Trigger DAG"
2. เปิด DAG `etl_influxdb_to_oracle` → เหมือนกัน
3. รอ ~1 นาที → เปิด DAG `sp_load_dw` → trigger

**หรือจาก CLI:**

```bash
COMPOSE=db_module/pipeline/docker-compose.yml

# Run Supabase extract (test single task)
docker compose -f $COMPOSE exec -T airflow-scheduler \
    airflow tasks test etl_supabase_to_oracle extract_production_batch 2026-04-18T12:00:00

docker compose -f $COMPOSE exec -T airflow-scheduler \
    airflow tasks test etl_supabase_to_oracle extract_qc_record 2026-04-18T12:00:00

# Run Influx extract (override window ให้ครอบทั้งวัน)
docker compose -f $COMPOSE exec -T -e INFLUX_RANGE_START=2026-04-18T04:00:00Z \
    airflow-scheduler airflow tasks test etl_influxdb_to_oracle \
    aggregate_sensor 2026-04-18T12:00:00

# Run SPs to populate FACT
for sp in sp_load_fact_production sp_load_fact_quality sp_load_fact_sensor; do
  docker compose -f $COMPOSE exec -T airflow-scheduler \
      airflow tasks test sp_load_dw $sp 2026-04-18
done
```

**Verify row counts:**
```bash
.venv/bin/python -c "
from db_module.db_conn import OracleConnector
c = OracleConnector()
with c.cursor() as cur:
    for t in ['STG_PRODUCTION_BATCH', 'STG_QC_RECORD', 'STG_SENSOR_AGG',
              'FACT_PRODUCTION', 'FACT_QUALITY', 'FACT_SENSOR']:
        cur.execute(f'SELECT COUNT(*) FROM {t}')
        print(f'{t:<22} {int(cur.fetchone()[0])}')
"
```

**ควรเห็น FACT_SENSOR > 100, FACT_PRODUCTION = 5, FACT_QUALITY = 5**

---

### 10. เปิด dashboard

```
http://localhost:8501
```

**ควรเห็น:**
- Sidebar: date picker แสดง `2026-04-18`
- Tab 1 (Production): 4 KPI cards (Total Batches=5, Total Output, Avg Yield, Avg Duration) + ตาราง 5 batch
- Tab 2 (Quality): Defect rate
- Tab 3 (Sensor per Batch): เลือก batch → line chart sensor ราย 15 นาที
- Tab 4 (Machine Status): line chart machine_state_num 15 นาทีต่อเครื่อง

---

## Daily Operations

### Start stack ปกติ (หลัง first-time setup เสร็จแล้ว)

```bash
# FastAPI
.venv/bin/uvicorn app.api.main:app --host 0.0.0.0 --port 8000 &

# Airflow (ปกติ running ค้างใน Docker อยู่แล้ว — restart ถ้าต้องการ)
docker compose -f db_module/pipeline/docker-compose.yml up -d

# Streamlit
.venv/bin/streamlit run app/streamlit/dashboard.py &
```

### Stop stack

```bash
pkill -f "uvicorn app.api"
pkill -f "streamlit run"
docker compose -f db_module/pipeline/docker-compose.yml stop
```

### Refresh mock data (regenerate + reapply)

```bash
.venv/bin/python db_module/db_sources/supabases_sql_query/mock/generate_mock_data.py
.venv/bin/python db_module/db_sources/supabases_sql_query/apply_supabase.py
```

### Truncate และ reload DW ทั้งหมด

```bash
# 1. ล้าง FACT + DIM_MACHINE/PRODUCT (เก็บ DIM_DATE + DIM_METRIC)
.venv/bin/python db_module/db_sources/oracle_sql_query/run_sql_file.py \
    db_module/db_sources/oracle_sql_query/query/05_truncate_and_reload.sql

# 2. Re-seed DIM จาก Supabase
.venv/bin/python db_module/db_sources/oracle_sql_query/sync_dimensions_from_supabase.py

# 3. Trigger DAG หรือ SP ใหม่
# (ดู step 9 ของ first-time setup)
```

### Backfill หลายวัน

ในกรณีต้องการ populate FACT หลายวัน ต้อง run SP ต่อวัน:

```bash
TOKEN=$(grep ORACLE_API_TOKEN .env | cut -d= -f2 | tr -d '"')
for d in 2026-04-18 2026-04-19; do
  for sp in SP_LOAD_FACT_PRODUCTION SP_LOAD_FACT_QUALITY SP_LOAD_FACT_SENSOR; do
    curl -s -X POST http://localhost:8000/sp/call \
      -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d "{\"name\":\"$sp\",\"args\":[\"$d\"]}"
    echo ""
  done
done
```

---

## Service Management

### Check running services

```bash
# Python processes
pgrep -af "uvicorn app.api"          # FastAPI
pgrep -af "streamlit run"            # Streamlit

# Docker
docker compose -f db_module/pipeline/docker-compose.yml ps

# Airflow DAG status
docker compose -f db_module/pipeline/docker-compose.yml exec -T airflow-scheduler \
    airflow dags list | grep -E "etl_|sp_load"
```

### Restart ทุกอย่าง

```bash
pkill -f "uvicorn app.api"; pkill -f "streamlit run"; sleep 2
docker compose -f db_module/pipeline/docker-compose.yml restart
.venv/bin/uvicorn app.api.main:app --host 0.0.0.0 --port 8000 &
.venv/bin/streamlit run app/streamlit/dashboard.py &
```

### Ports

| Port | Service |
|---|---|
| 8000 | FastAPI (uvicorn) |
| 8088 | Airflow webserver |
| 8501 | Streamlit |

---

## Troubleshooting

### ❌ `401 Unauthorized` ที่ dashboard

**Cause:** `ORACLE_API_TOKEN` ไม่ถูกโหลดเข้า Streamlit

**Fix:** dashboard โหลด `.env` อัตโนมัติอยู่แล้ว ตรวจว่า
- `.env` ที่ repo root มีบรรทัด `ORACLE_API_TOKEN=xxx` (ไม่เว้นว่าง)
- Streamlit process start จาก directory ของโปรเจกต์

### ❌ `Cannot reach API` ที่ dashboard

**Cause:** FastAPI ยังไม่ start

**Fix:**
```bash
.venv/bin/uvicorn app.api.main:app --host 0.0.0.0 --port 8000 &
sleep 3
curl http://localhost:8000/health    # ต้อง 401 (ไม่มี token) หรือ 200
```

### ❌ `JVM DLL not found` หรือ `JAVA_HOME` error

**Cause:** jpype หา JVM ไม่เจอ

**Fix:**
```bash
# macOS (Apple Silicon)
brew install openjdk@17
echo 'JAVA_HOME=/opt/homebrew/opt/openjdk@17' >> .env
```

### ❌ `ORA-12543: TNS:destination host unreachable`

**Cause:** KMITL network ไม่ถึง (ไม่ได้อยู่ใน network / VPN / firewall block)

**Fix:**
- ต้องอยู่ใน KMITL network (campus wifi / VPN)
- ตรวจ `ORACLE_HOST` ใน `.env` ตรง

### ❌ Supabase connection timeout (IPv6)

**Cause:** `db.<ref>.supabase.co` เป็น IPv6-only host บาง ISP ไม่รองรับ IPv6

**Fix option 1** — ใช้ Supabase connection pooler (IPv4):
```env
SUPABASE_HOST=aws-0-<region>.pooler.supabase.com
SUPABASE_PORT=6543
```

**Fix option 2** — Airflow container ใช้ IPv6 network (compose ทำให้แล้ว ดู `enable_ipv6: true`)

### ❌ `ValueError: unconverted data remains... ".445810"` ที่ dashboard

**Cause:** Oracle TIMESTAMP บางแถวมี microseconds บางแถวไม่มี → pandas ล้ม

**Fix:** แก้ไปแล้วใน `dashboard.py` — ใช้ `pd.to_datetime(..., format="mixed")`

### ❌ `ORA-02287: sequence number not allowed here`

**Cause:** Oracle 10g ห้าม `SEQ.NEXTVAL` ใน `INSERT...SELECT`

**Fix:** ใช้ cursor loop + row-by-row INSERT (ดูตัวอย่างใน `SP_LOAD_FACT_PRODUCTION`)

### ❌ Airflow DAG `host.docker.internal: Name or service not known`

**Cause:** `ORACLE_API_URL` ใน `.env` ใช้ `localhost` ซึ่งใน container หมายถึง container เอง

**Fix:** `docker-compose.yml` ตั้ง `ORACLE_API_URL=http://host.docker.internal:8000` ใน `environment:` override อยู่แล้ว — ตรวจว่ายัง intact

### ❌ Mock window ไม่ตรงกับ InfluxDB data

**Cause:** InfluxDB live data เปลี่ยน range (NodeRED หยุด หรือ simulate window ใหม่)

**Fix:** ตรวจ InfluxDB:
```bash
.venv/bin/python -c "
from db_module.db_conn import InfluxConnector
c = InfluxConnector()
for label, flux in [('first', 'from(bucket:\"iiot_data_raw\") |> range(start: 0) |> first()'),
                     ('last',  'from(bucket:\"iiot_data_raw\") |> range(start: 0) |> last()')]:
    tables = c.query(flux)
    for table in tables:
        for r in table.records:
            print(label, r.get_time()); break
        break
"
```

แล้วแก้ `INFLUX_EARLIEST` / `INFLUX_LATEST` ใน `generate_mock_data.py` ให้ตรง

---

## Scripts Reference

| Script | Purpose |
|---|---|
| `run_sql_file.py <path>` | รัน SQL file ต่อ Oracle (รองรับ PL/SQL `/` terminator) |
| `sync_dimensions_from_supabase.py` | ดึง master จาก Supabase → DIM_MACHINE + DIM_PRODUCT |
| `verify_warehouse_schema.py` | assertion 10 tables + 5 sequences + 4 procedures |
| `apply_supabase.py` | apply 3 ไฟล์ SQL เข้า Supabase ใน transaction เดียว |
| `generate_mock_data.py` | สร้าง mock batch/qc ตรง window ของ InfluxDB live |

### FastAPI Endpoints (สำหรับ dashboard)

ทุก endpoint ต้องส่ง `Authorization: Bearer $ORACLE_API_TOKEN`

| Endpoint | Params | Purpose |
|---|---|---|
| `GET /api/production/available-dates` | — | วันที่มีข้อมูลใน FACT (สำหรับ date picker) |
| `GET /api/production/by-batch` | `date=YYYY-MM-DD` | list ทุก batch + yield + duration |
| `GET /api/production/summary` | `date=YYYY-MM-DD` | KPI รวม (total batches, avg yield, etc.) |
| `GET /api/production/per-machine-15min` | `date=YYYY-MM-DD` | RUNNING/FAULT ต่อเครื่องต่อ window |
| `GET /api/quality/defect-rate` | `date=YYYY-MM-DD` | Defect overall + per batch |
| `GET /api/sensor/available-metrics` | — | 6 metrics จาก DIM_METRIC |
| `GET /api/sensor/by-batch` | `batch_src_id=N` | Sensor param ของ batch ราย 15 นาที |
| `GET /api/sensor/by-machine-15min` | `date=YYYY-MM-DD&metric=NAME` | Sensor ราย 15 นาที per machine |

### Operational Endpoints

| Endpoint | Method | Body/Params |
|---|---|---|
| `/health` | GET | — |
| `/sql/query` | POST | `{"sql": "SELECT ...", "params": [...]}` |
| `/sql/execute` | POST | `{"sql": "INSERT/UPDATE ...", "params": [...]}` |
| `/sp/call` | POST | `{"name": "SP_NAME", "args": [...]}` |
| `/sql/bulk-insert` | POST | `{"table": "T", "columns": [...], "rows": [[...]], "truncate": true}` |

---

## Key Design Notes

- **Oracle 10g compatibility:** ห้ามใช้ `GENERATED AS IDENTITY` — ทุก surrogate PK ใช้ `SEQUENCE` + `NEXTVAL`
- **15-min merge-by-key SPs:** DELETE ตาม key (batch_src_id หรือ window_start) ไม่ใช่ `date_id` ไม่งั้นจะล้าง FACT ทั้งวัน
- **Machine name = InfluxDB tag:** `machine.name` ใน Supabase + `DIM_MACHINE.machine_name` ต้องเป็น `"M01"/"M02"/"M03"` ตรงกับ `machine_id` tag ใน InfluxDB
- **Degenerate dimensions:** `batch_src_id`, `order_src_id` ใน FACT_PRODUCTION/QUALITY เก็บ business key โดยไม่มี DIM_* ของตัวเอง
- **DIM_METRIC seed-in-schema:** 6 sensor metric hardcode ใน `01_schema.sql` — เพิ่ม sensor ใหม่ = INSERT row ไม่แตะ schema อื่น
- **Locale trap:** Thai-locale JVM จะแปลง Gregorian 2026 → Buddhist 2569 ผ่าน `java.sql.Date.valueOf()` → `OracleConnector._JVM_ARGS` บังคับ `-Duser.language=en -Duser.country=US` และทุก session รัน `ALTER SESSION SET NLS_CALENDAR='GREGORIAN'`

---

## Testing

### Connector smoke tests

```bash
.venv/bin/python -m pytest test/ -v
```

ต้องได้ `3 passed` (Oracle + Supabase + InfluxDB round-trip)

### Oracle write access

```bash
.venv/bin/python test/test_create_table.py
```

### DAG task (end-to-end)

```bash
docker compose -f db_module/pipeline/docker-compose.yml exec -T airflow-scheduler \
    airflow tasks test etl_supabase_to_oracle extract_production_batch 2026-04-18T12:00:00
```

### Check FACT counts after a run

```bash
.venv/bin/python -c "
from db_module.db_conn import OracleConnector
c = OracleConnector()
with c.cursor() as cur:
    for t in ['FACT_PRODUCTION', 'FACT_QUALITY', 'FACT_SENSOR']:
        cur.execute(f'SELECT COUNT(*) FROM {t}')
        print(f'{t}: {int(cur.fetchone()[0])}')
"
```

---

## Progress & Planning

| เอกสาร | เนื้อหา |
|---|---|
| [`claude_track/PLAN.md`](claude_track/PLAN.md) | progress tracker หลัก — phase 0-7 เก่า + migration 2026-04-19 |
| [`claude_track/NEW_PLAN.md`](claude_track/NEW_PLAN.md) | migration plan 10 phase (destructive cleanup → rewrite → test) |
| [`claude_track/problems_requirements_erd_mapping.md`](claude_track/problems_requirements_erd_mapping.md) | business requirement + OLTP ER + DW ER + mapping |
| [`NEW_ARCHITECTURE.md`](NEW_ARCHITECTURE.md) | schema spec ปัจจุบัน |
| [`CLAUDE.md`](CLAUDE.md) | GS-MAD methodology + slash commands |

---

## Business Requirements (POC)

ลูกค้า (SI scenario) ต้องการ dashboard สำหรับ General Manager ที่:

1. Refresh ทุก 15 นาที
2. แสดง production ของ 3 เครื่อง (M01/M02/M03) แยก + รวม
3. แสดง defect rate (QC สุ่ม 5% ของ qty_out ต่อ batch)
4. แสดง sensor parameter ต่อ batch ราย 15 นาที
5. แสดง machine status (RUNNING/FAULT) จาก `machine_state_num`

รายละเอียดเพิ่มเติมใน [claude_track/problems_requirements_erd_mapping.md](claude_track/problems_requirements_erd_mapping.md)

---

## License & Authorship

KMITL coursework project — Burased.B@gsbattery.co.th. Architecture advice + code
ร่วมเขียนกับ Claude Code (GS-MAD framework)
