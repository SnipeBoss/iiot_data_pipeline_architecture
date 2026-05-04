# Unified IIoT Data Architecture — Battery Assembly Line COS

End-to-end data pipeline for a single Battery Assembly line (3 machines: M01/M02/M03)
that ingests from two upstream sources (Supabase OLTP + InfluxDB IIoT sensors), lands
data in an Oracle 10g Kimball star schema, and serves a 3-page Streamlit dashboard
with Prophet forecasting through a FastAPI HTTP layer.

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
│    etl_supabase_to_oracle    */15 * * * *       │
│    etl_influxdb_to_oracle    */15 * * * *       │
│    sp_load_dw                5,20,35,50 * * * * │
│    sync_dim_supabase         0 2 * * * (nightly)│
└──────────────────────┬──────────────────────────┘
                       │ HTTP (Bearer)
                       ▼
┌─────────────────────────────────────────────────┐
│  FastAPI (port 8000, host JVM, JDBC thin)       │
│   dw_api/operational.py — /sp/call, /bulk-insert│
│   dashboard_api/dashboard.py — /api/{sensor,    │
│       scheduling,analytics}/*                   │
└──────────────────────┬──────────────────────────┘
                       │ JDBC thin (ojdbc8.jar)
                       ▼
┌─────────────────────────────────────────────────┐
│  Oracle 10g (KMITL AI03) — 20 tables            │
│    7 DIM · 5 FACT · 8 STG                       │
│    9 SEQ · 10 PROC · 1 FN · 27 IDX              │
└──────────────────────┬──────────────────────────┘
                       │ HTTP (cached 5 min)
                       ▼
┌─────────────────────────────────────────────────┐
│  Streamlit Dashboard (port 8501)                │
│    Page 1: OEE & Defect                         │
│    Page 2: Sensor Forecast (Prophet)            │
│    Page 3: Schedule Adherence                   │
└─────────────────────────────────────────────────┘
```

> **Last refactor:** 2026-04-26 (NEW_ARCHITECTURE) — DW reduced to 6 OLTP-driven STG +
> 4 DIM + 3 FACT + DIM_METRIC; load orchestrator collapsed from three parallel SPs into
> one master `SP_LOAD_ALL_FACTS`.
>
> **Production data (mock):** 373 batches and 6,152 sensor windows over an 8-day window.

---

## Stack

| Layer | Technology | Host |
|---|---|---|
| OLTP | PostgreSQL 15 (Supabase) — 12 tables | Cloud |
| IIoT | InfluxDB 2.x + Telegraf + Mosquitto + Node-RED — 1 Hz × 3 machines × 6 metrics | AWS EC2 |
| Pipeline | Apache Airflow 2.8 — 4 DAGs | Docker compose, port 8088 |
| HTTP | FastAPI + uvicorn — JDBC wrapper + analytics | Host process, port 8000 |
| Driver | `jaydebeapi` + `jpype` + `ojdbc8.jar` | Host JVM (singleton) |
| DW | Oracle 10.2.0.3 (`AI03`) — 20 tables | KMITL `161.246.35.92:1521/orcl` |
| Dashboard | Streamlit + Plotly + Prophet — 3 pages | Host process, port 8501 |

---

## Repository Layout

```
unified_iiot_data_architecture/
├── app/
│   ├── api/
│   │   ├── main.py                 # FastAPI() + 2 routers
│   │   ├── dw_api/                 # /health · /sp/call · /sql/bulk-insert
│   │   └── dashboard_api/          # /api/sensor · /api/scheduling · /api/analytics
│   └── streamlit/
│       ├── dashboard.py            # Entry + sidebar nav
│       ├── components/             # api_client · cards · charts · filters · prophet_trainer
│       └── pages/                  # 1_oee_defect · 2_sensor_forecast · 3_schedule_adherence
├── db_module/
│   ├── db_conn/                    # OracleConnector · SupabaseConnector · InfluxConnector
│   ├── db_sources/
│   │   ├── oracle_sql_query/       # 7 schema files + apply / sync / verify scripts
│   │   └── supabases_sql_query/    # schema · triggers · master · mock data
│   └── pipeline/airflow/dags/      # 4 DAGs + helpers
├── test/
└── requirements.txt
```

---

## Data Flow

| # | Flow | Cadence | Pattern |
|---|---|---|---|
| 1 | Supabase → Oracle STG (4 parallel extracts: production_batch, qc_record, qc_defect, downtime_event) | every 15 min | TRUNCATE-then-INSERT |
| 2 | InfluxDB → `STG_SENSOR_AGG` (Flux `aggregateWindow` mean/min/max/count) | every 15 min | TRUNCATE-then-INSERT |
| 3 | STG → FACT via `SP_LOAD_ALL_FACTS` (production · quality · defect · downtime · sensor) | 5 min after ingest | DELETE-by-key + cursor INSERT |
| 4 | Supabase master → Oracle DIM | nightly 02:00 UTC | MERGE BY `src_id` |
| 5 | Streamlit → FastAPI → Oracle DW | on-demand | `@st.cache_data(ttl=300)` |

---

## Key Design Decisions

- **JDBC thin via `jpype`.** Oracle 10g is unsupported by `python-oracledb` thin
  (requires 12c+); Oracle Instant Client has no ARM64 build. JDBC thin (`ojdbc8.jar`,
  `thinLogonCapability=o3`) is the only workable path.
- **Airflow → FastAPI HTTP, not JDBC direct.** The `apache/airflow:2.8` image ships
  without Java; bundling a JVM into every worker is expensive. A single JVM lives in
  the FastAPI host process, and DAGs call `/sql/bulk-insert` and `/sp/call`.
- **Analytics endpoints, not Oracle views.** Schema `AI03` lacks `CREATE VIEW`
  privilege. `V_OEE_DAILY`, `V_DEFECT_PARETO`, `V_SCHEDULE_ADHERENCE`, and
  `V_BATCH_FEATURES` are served as `/api/analytics/*` instead.
- **STG = TRUNCATE-then-INSERT.** STG is a 15-min buffer, not history — keeps the
  pipeline idempotent and bounds storage.
- **FACT = DELETE-by-key + cursor INSERT.** Oracle 10g forbids `SEQ.NEXTVAL` in
  `INSERT…SELECT` (allowed only since 11g R2). DELETE-by-key keeps reruns idempotent.
- **DIM = MERGE BY `src_id`.** A naive DELETE+INSERT would rewrite surrogate keys and
  orphan FACT FKs. MERGE preserves them.

---

## Setup

### Prerequisites
- Python 3.12, Java 17, Docker
- Access to KMITL Oracle, a Supabase project, and an InfluxDB instance

### One-time

```bash
# 1. Python env
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt   # cmdstanpy compile takes 5–10 min

# 2. JDBC driver
mkdir -p db_module/db_conn/oracle/drivers
# place ojdbc8.jar in that folder

# 3. .env (Oracle / Supabase / Influx / ORACLE_API_TOKEN / JAVA_HOME)
cp .env.example .env

# 4. Verify connectors
.venv/bin/pytest test/test_connectors.py -v

# 5. Apply Supabase OLTP (12 tables + triggers + master + mock)
.venv/bin/python db_module/db_sources/supabases_sql_query/apply_supabase.py

# 6. Apply Oracle DW (in dependency order)
for f in 01_schema_dim 02_schema_fact 03_schema_staging \
         04_dim_seed 05_indexes 06_procedure_dim_sync 07_procedure_fact_load; do
    .venv/bin/python db_module/db_sources/oracle_sql_query/run_sql_file.py \
        "db_module/db_sources/oracle_sql_query/query/${f}.sql"
done
.venv/bin/python db_module/db_sources/oracle_sql_query/verify_warehouse_schema.py

# 7. Initial DIM sync (must precede FACT load)
.venv/bin/python db_module/db_sources/oracle_sql_query/sync_dimensions_from_supabase.py
```

### Run (3 terminals)

```bash
# FastAPI
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
.venv/bin/uvicorn app.api.main:app --host 0.0.0.0 --port 8000

# Airflow → http://localhost:8088 (admin/admin)
docker compose -f db_module/pipeline/docker-compose.yml up -d --build

# Streamlit → http://localhost:8501
cd app/streamlit && ../../.venv/bin/streamlit run dashboard.py --server.port 8501
```

In the Airflow UI, unpause and trigger DAGs in this order:

1. `sync_dim_supabase`
2. `etl_supabase_to_oracle` and `etl_influxdb_to_oracle` (parallel)
3. `sp_load_dw`

---

## Smoke Tests

```bash
TOKEN=$(grep ^ORACLE_API_TOKEN= .env | cut -d= -f2)
BASE=http://localhost:8000

# 1. FastAPI + Oracle reachability
curl -s "$BASE/health" -H "Authorization: Bearer $TOKEN" | jq '{status, oracle_user}'

# 2. Streamlit reachability
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8501/

# 3. Airflow scheduler
docker compose -f db_module/pipeline/docker-compose.yml ps airflow-scheduler

# 4. Schema integrity
.venv/bin/python db_module/db_sources/oracle_sql_query/verify_warehouse_schema.py
```

---

## Performance

| Stage | Target | Observed (mock) |
|---|---|---|
| Supabase extract (4 parallel tasks) | < 30 s | 5–10 s |
| InfluxDB Flux aggregate | < 1 min | 10–30 s |
| `SP_LOAD_ALL_FACTS` | < 30 s | 10–20 s |
| **End-to-end (data → FACT)** | **< 6 min** | **30–60 s** |
| `/api/sensor/by-machine-15min` | < 200 ms | ~280 rows/day |
| `/api/analytics/oee-daily` | < 500 ms | ~50 rows/week |
| `/api/analytics/batch-features` | < 1 s | full FACT_PRODUCTION ⨝ FACT_QUALITY ⨝ FACT_SENSOR |

Streamlit caches via `@st.cache_data(ttl=300)`; effective hit rate ~95 % against a
15-minute DAG cadence. JVM cold start adds ~2–3 s to the first FastAPI request only.

---

## Operations

| Task | Action |
|---|---|
| Manual ETL trigger | Airflow UI → DAG → Trigger |
| Manual SP call | `POST /sp/call` with `{"name":"SP_LOAD_FACT_PRODUCTION"}` |
| Influx backfill | Trigger `etl_influxdb_to_oracle` with custom `data_interval_start` / `_end` |
| Daily check | `/health` + Airflow scheduler container + Streamlit landing page |

---

## Constraints

- Oracle 10.2.0.3: no `CONTINUE`, no `SEQ.NEXTVAL` in `INSERT…SELECT`, no `FETCH FIRST` /
  `OFFSET`, no `JSON_*`, limited window functions. Default `NLS_CALENDAR='THAI BUDDHA'`
  is overridden to `'GREGORIAN'` per session.
- Schema `AI03` cannot `CREATE USER` or `CREATE VIEW`.
- macOS ARM64 host: Oracle Instant Client unavailable; only JDBC thin works.
- Single line, single shop floor — multi-line topology is out of scope.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Unable to locate a Java Runtime` | `JAVA_HOME` unset | `export JAVA_HOME=/opt/homebrew/opt/openjdk@17` (or set in `.env`) |
| `ORA-01861: literal does not match format string` | `datetime.date` passed straight to JDBC | use `prepare_row()` in `dw_api/deps.py` (converts to `java.sql.Date`) |
| `PLS-00201: identifier 'CONTINUE' must be declared` | Oracle 10g lacks `CONTINUE` | use the flag pattern (`v_skip BOOLEAN`) — see `SP_LOAD_FACT_DEFECT` |
| `ORA-01031: insufficient privileges` on `CREATE VIEW` | `AI03` lacks the privilege | views are served by `/api/analytics/*` instead |
| FastAPI cold start ~2–3 s | JVM init + `ojdbc8` load | expected once; subsequent requests ~50 ms |
| Airflow container `connection refused` to `localhost:8000` | inside-container `localhost` is the container | use `host.docker.internal:8000` (set in compose) |
| Supabase `connection timed out` from Docker | direct host is IPv6-only | enable IPv6 on the Docker network |

---

## Glossary

- **MES** — Manufacturing Execution System (Supabase OLTP).
- **DW** — Data Warehouse (Oracle 10g `AI03`).
- **OEE** — Availability × Performance × Quality.
- **Surrogate key** — DW-internal sequence-generated PK; stable across re-syncs.
- **Conformed dim** — same DIM shared across multiple FACTs (e.g. `DIM_DATE`).
- **JDBC thin** — pure-Java Oracle driver; no Instant Client required.
- **Idempotent** — repeated runs converge to the same state (DELETE-by-key + INSERT).
- **15-min window** — standard ETL cadence and Influx aggregation interval.

---

## License & Credits

KMITL Computer Science — IIoT Data Architecture project (2026).

Built by [@SnipeBoss](https://github.com/SnipeBoss).
