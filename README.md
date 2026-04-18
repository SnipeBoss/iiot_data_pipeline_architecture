# Battery Manufacturing — Unified IIoT Data Architecture

End-to-end data pipeline for a 3-machine battery-assembly line. Ingests
operational data (Supabase) and 1 Hz sensor telemetry (InfluxDB via NodeRED)
into an Oracle Data Warehouse, computes OEE, and serves a Streamlit dashboard.

| Layer | Tech | Role |
|---|---|---|
| OLTP | Supabase (PostgreSQL) | Production orders, batches, QC, maintenance, inventory |
| Streaming | NodeRED → Mosquitto → InfluxDB 2.0 (AWS) | 1 Hz sensor simulation |
| Orchestration | Apache Airflow 2.8 (local Docker) | 3 DAGs: Supabase extract, Influx aggregate, SP chain |
| Serving layer | FastAPI (local uvicorn) | HTTP wrapper around Oracle 10g JDBC |
| Data warehouse | Oracle 10.2.0.3 @ KMITL (`AI03` schema) | 5 STG + 5 DIM + 5 FACT tables |
| Dashboard | Streamlit | Date picker, KPIs, per-machine breakdown |

This file explains the architecture and every module. For **what is built vs.
what is still on paper**, see [claude_track/PLAN.md](claude_track/PLAN.md).
For **how to run it**, see [CLAUDE.md](CLAUDE.md).

---

## 1. System Architecture

### 1.1 Data flow at a glance

```
                                ┌───────────────────────────────┐
                                │  AWS  (consumed, not hosted)  │
                                │                               │
                                │  NodeRED ─► Mosquitto ─► ─┐   │
                                │            (MQTT 1883)    │   │
                                │                           ▼   │
                                │                    InfluxDB 2 │
                                │                    :8086      │
                                └───────────────┬───────────────┘
                                                │
 ┌─────────────┐                                │
 │  Supabase   │                                │
 │  (cloud     │          .env (secrets)        │
 │   Postgres) │                                │
 └─────┬───────┘                                │
       │                                        │
       │ psycopg2 (IPv6)         influxdb-client│
       │                                        │
  ┌────▼────────────────────────────────────────▼──────────┐
  │   Airflow (docker compose)    localhost:8088            │
  │   ─────────────────────────                             │
  │   etl_supabase_to_oracle      etl_influxdb_to_oracle    │
  │      ↘                              ↙                   │
  │       HTTP bulk-insert, sp/call, health                 │
  │                     │                                   │
  └─────────────────────┼───────────────────────────────────┘
                        ▼
            ┌────────────────────────┐
            │  FastAPI (uvicorn)     │   localhost:8000
            │  app/api/main.py       │   - operational: /sql/*, /sp/*
            │  OracleConnector       │   - reporting:   /api/oee/*, etc.
            │  (JayDeBeApi + JDBC)   │
            └────────┬───────────────┘
                     │ JDBC thin  (port 1521, O3LOGON)
                     ▼
         ┌────────────────────────────┐
         │  Oracle 10.2.0.3  (KMITL)  │
         │  AI03 schema               │
         │  ┌───────┐ ┌───────┐       │
         │  │  STG  │ │  DW   │       │
         │  │ 7 tbl │ │ 5 dim │       │
         │  │       │ │ 5 fact│       │
         │  └───────┘ └───────┘       │
         └────────────▲───────────────┘
                      │ requests.get (reads only)
                      │
            ┌─────────┴───────────┐
            │  Streamlit          │ localhost:8501
            │  dashboard.py       │ KPIs, bars, tabs
            └─────────────────────┘
```

### 1.2 Why these boundaries exist

- **Airflow doesn't talk to Oracle.** Oracle 10.2.0.3 needs JDBC (thin mode
  unsupported, no ARM64 Instant Client). Baking Java + `ojdbc8.jar` into the
  Airflow image would add ~1 GB and build complexity. Instead, the FastAPI
  service on the host holds the one JVM process and exposes `/sql/*`,
  `/sp/call`, `/sql/bulk-insert` over HTTP. DAGs just use `requests`.
- **Streamlit doesn't talk to Oracle either.** Same FastAPI service serves
  both the "operational" writes (for Airflow) and the "reporting" reads
  (for Streamlit). One JVM, one connection pool, one place for the JDBC
  gotchas. See `app/api/main.py`.
- **Supabase = OLTP, Oracle = DW.** Decided 2026-04-18 (see
  `CLAUDE.md` *Architecture decisions that OVERRIDE the spec*). The 17-table
  normalized schema lives in Supabase; Oracle stores the star-schema DW only.
- **Local-only deployment.** No EC2. All compose files use `localhost` /
  `host.docker.internal`. AWS InfluxDB + NodeRED are consumed as remote
  endpoints, not provisioned.

### 1.3 Data model (three zones)

**Supabase (17 tables, 3NF)** — see
`datasources/supabases_sql_query/query/01_schema.sql`. 5 domains:

1. Infrastructure: `production_line`, `machine`, `process_stage`, `product`
2. Material master: `raw_material`, `bill_of_material`, `supplier`
3. Procurement & inventory: `raw_material_po`, `raw_material_receipt`, `inventory`
4. Production: `production_order`, `production_batch`, `finished_good`, `material_consumption`
5. Quality & maintenance: `qc_inspection`, `qc_result`, `maintenance_log`

**Oracle STG (7 tables)** — raw extract buffer,
`datasources/oracle_sql_query/01_dw_ddl.sql` + `06_inventory_pipeline.sql`:

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
`datasources/oracle_sql_query/01_dw_ddl.sql`:

- **Dims** (surrogate keys via sequences, `*_src_id` preserves source PK):
  `DIM_DATE` (5 years pre-populated), `DIM_MACHINE`, `DIM_PRODUCT`,
  `DIM_STAGE`, `DIM_MATERIAL`.
- **Facts**: `FACT_OEE` (one row per machine per day), `FACT_PRODUCTION`,
  `FACT_QUALITY`, `FACT_INVENTORY`, `FACT_MAINTENANCE`.

---

## 2. Software Architecture

### 2.1 Code layout

```
├── CLAUDE.md                       # Project instructions + command reference
├── README.md                       # This file
├── .env / .env.example             # Secrets (gitignored) + template
├── requirements.txt                # Top-level Python deps
├── claude_track/
│   └── PLAN.md                     # Progress tracker (phases + checkboxes)
│
├── db_module/                      # All DB-touching Python
│   ├── db_conn/                    # Connectors (Oracle / Supabase / InfluxDB)
│   │   ├── _env.py                 # dotenv loader + require()/get() helpers
│   │   ├── oracle/
│   │   │   ├── oracle_connection.py    # OracleConnector (JDBC via JayDeBeApi)
│   │   │   └── drivers/ojdbc8.jar
│   │   ├── supabases/
│   │   │   └── supabase_connection.py  # SupabaseConnector (psycopg2)
│   │   └── influxdb/
│   │       └── influx_connection.py    # InfluxConnector (influxdb-client)
│   │
│   └── pipeline/                   # Airflow stack
│       ├── Dockerfile              # apache/airflow:2.8 + pipeline requirements.txt
│       ├── docker-compose.yml      # postgres-af + init + webserver + scheduler
│       ├── requirements.txt        # requests, psycopg2, influxdb-client
│       └── airflow/dags/
│           ├── _oracle_api.py      # HTTP client helpers (bulk_insert, call_sp, ...)
│           ├── _supabase.py        # psycopg2 context manager
│           ├── etl_supabase_to_oracle.py   # DAG: 6 extract tasks
│           ├── etl_influxdb_to_oracle.py   # DAG: 1 aggregate task
│           └── sp_load_dw.py       # DAG: 5 SP-call tasks
│
├── app/
│   ├── api/main.py                 # FastAPI — operational + reporting endpoints
│   └── streamlit/dashboard.py      # Dashboard
│
├── datasources/
│   ├── supabases_sql_query/
│   │   ├── query/01_schema.sql             # 17 tables + indexes
│   │   ├── query/02_master_data.sql        # machines, stages, products, BOM, etc.
│   │   ├── mock/generate_mock_data.py      # 30-day deterministic generator
│   │   ├── mock/03_mock_data.sql           # generator output (~15 k rows)
│   │   └── apply_supabase.py               # applier (runs all 3 files)
│   │
│   ├── oracle_sql_query/
│   │   ├── 01_dw_ddl.sql           # STG + DIM + FACT tables + sequences
│   │   ├── 02_sp_dim_date.sql      # SP_LOAD_DIM_DATE (MERGE-based, idempotent)
│   │   ├── 03_sp_fact_loaders.sql  # FN_CALC_OEE + 4 SP_LOAD_FACT_*
│   │   ├── 04_reporting_queries.sql# 5 read-only reporting queries
│   │   ├── 05_truncate_and_reload.sql # Full-rebuild driver
│   │   ├── 06_inventory_pipeline.sql   # STG_INVENTORY + SP_LOAD_FACT_INVENTORY
│   │   ├── apply_ddl.py            # SQL/PL-SQL splitter + applier
│   │   ├── seed_dims.py            # Supabase → Oracle dim population
│   │   └── verify_dw.py            # sanity check: tables + sequences exist
│   │
│   └── iiot_container/
│       └── nodered/README.md       # 3 NodeRED function-node codes for M01/M02/M03
│
└── test/
    ├── test_connection.py          # HTTP + TCP + JDBC probe
    ├── test_create_table.py        # DDL/DML roundtrip
    └── test_connectors.py          # pytest, skips when env blank
```

### 2.2 Three layering principles

1. **Connector modules (`db_module/db_conn/`) don't import each other.**
   They're independent. Each one reads its own `SUPABASE_*` / `ORACLE_*` /
   `INFLUX_*` env vars via `_env.py`. Heavy driver imports happen inside
   methods so the top-level `from db_module.db_conn import ...` doesn't
   crash if a specific driver isn't installed.
2. **The Oracle API is the only JDBC consumer.** No script outside
   `app/api/main.py` or `db_module/db_conn/oracle/` talks to Oracle directly
   in the deployed flow. (Local-dev scripts like `seed_dims.py`,
   `apply_ddl.py`, and `verify_dw.py` *do* use JDBC, but they run on the
   developer's host, not inside any container.)
3. **SPs are the only thing that writes to FACT tables.** Airflow writes to
   STG; SPs transform STG → FACT. Nothing else touches facts. The Streamlit
   side is pure read.

### 2.3 Data flow (end to end)

1. **Every 8h (06:00, 14:00, 22:00) — `etl_supabase_to_oracle`**
   - `check_oracle_api` — pings `/health`
   - 6 parallel tasks: each runs a time-windowed `SELECT` on Supabase via
     `psycopg2`, serializes rows with `as_iso()` (handles datetime + Decimal),
     and POSTs to `/sql/bulk-insert` on the Oracle API with `truncate=True`.
2. **Every 8h — `etl_influxdb_to_oracle`**
   - Flux query with `aggregateWindow(every: 8h, fn: mean)` + `pivot` on
     `iiot_data_raw/station_1`. 3 rows per run (one per machine). Posted to
     `STG_SENSOR_AGG` via the same bulk-insert endpoint.
3. **Every 8h + 30 min — `sp_load_dw`**
   - 5 parallel tasks, each calls one `SP_LOAD_FACT_*` via `/sp/call`. SPs
     are idempotent — they DELETE matching `date_id` first, then re-INSERT.
4. **On demand — Streamlit**
   - Reads the 6 `/api/oee/*`, `/api/quality/*`, `/api/maintenance/*`,
     `/api/inventory/*` GET endpoints. 30-second `st.cache_data` TTL.

---

## 3. Modules + functions (all of them)

### 3.1 `db_module/db_conn/_env.py` — env loader

Runs `load_dotenv(REPO_ROOT/'.env')` exactly once on import so downstream
modules get a consistent view of secrets.

| Symbol | Purpose |
|---|---|
| `REPO_ROOT` | Computed once from `__file__` — used to anchor relative paths. |
| `ConfigError` | Raised when a required var is missing or empty. Inherits `RuntimeError`. |
| `get(name, default=None)` | Returns the env var, treating empty string as missing. |
| `require(name)` | Returns the value or raises `ConfigError` with a helpful message. |
| `resolve_path(raw)` | Turns a possibly-relative path into an absolute one anchored at `REPO_ROOT`. Used for `ORACLE_JDBC_JAR`. |

**Why empty-string is treated as missing:** `.env` files often contain
`KEY=` placeholders. If we let them through, we'd connect to an unconfigured
endpoint and silently fail.

### 3.2 `db_module/db_conn/oracle/oracle_connection.py` — `OracleConnector`

Wraps JayDeBeApi + `ojdbc8.jar` for Oracle 10.2.0.3. This is the most
complex connector because of the JVM constraints.

| Symbol | Purpose |
|---|---|
| `_JVM_ARGS` | JVM flags required for this server: `thinLogonCapability=o3` (10g O3LOGON auth), `disableOob=true` (TCP OOB unsupported by old Oracle), `user.language=en / user.country=US` (stops Thai-locale JVM from writing Buddhist-calendar years, which adds +543 to every date). |
| `OracleConnector.__init__` | Reads `ORACLE_*` env, validates JDBC jar exists. |
| `OracleConnector.jdbc_url` | Builds `jdbc:oracle:thin:@//host:port/service` from env. |
| `OracleConnector._ensure_jvm` | Starts the JVM once per process. `jpype.isJVMStarted()` guards re-entry. Classpath frozen at first call. |
| `OracleConnector.connect()` | Returns a raw JayDeBeApi connection with `autocommit=False` and session-level `NLS_CALENDAR='GREGORIAN'` + `NLS_DATE_LANGUAGE='ENGLISH'` + `NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'`. Every new session runs these ALTERs. |
| `OracleConnector.cursor()` | Context manager that commits on clean exit, rolls back on exception, and always closes. Prefer over `connect()` for single logical operations. |

**Thai calendar trap** is documented in `CLAUDE.md`. If it's removed,
`java.sql.Date.valueOf("2026-01-01")` writes `2569-01-01` on the wire.

### 3.3 `db_module/db_conn/supabases/supabase_connection.py` — `SupabaseConnector`

Thin psycopg2 wrapper. Not much here by design.

| Symbol | Purpose |
|---|---|
| `SupabaseConnector.__init__` | Reads `SUPABASE_*` env with sensible defaults (port 5432, db `postgres`, user `postgres`, sslmode `require`). |
| `SupabaseConnector.connect()` | Returns a raw psycopg2 connection. No pooling. |
| `SupabaseConnector.cursor()` | Same commit/rollback context manager as Oracle. |

### 3.4 `db_module/db_conn/influxdb/influx_connection.py` — `InfluxConnector`

Wraps `influxdb-client` 2.x.

| Symbol | Purpose |
|---|---|
| `InfluxConnector.__init__` | Reads `INFLUX_URL / _ORG / _TOKEN / _BUCKET`. `ORG` and `BUCKET` have defaults. |
| `InfluxConnector.client()` | Context-managed `InfluxDBClient` — closes on exit. |
| `InfluxConnector.query(flux)` | Runs a Flux query, returns list of `FluxTable`. |
| `InfluxConnector.query_records(flux)` | Convenience: flattens to `list[dict]`. |
| `InfluxConnector.write(points)` | Synchronous write of `Point` objects. |

### 3.5 `app/api/main.py` — FastAPI service

Single FastAPI app with two halves:

**Operational (used by Airflow DAGs):**

| Endpoint | Purpose |
|---|---|
| `GET /health` | Pings Oracle — returns `sysdate`, user, JDBC URL. |
| `POST /sql/execute` | Runs one DDL/DML statement. Body: `{sql, params?}`. |
| `POST /sql/query` | Runs one SELECT. Returns `{columns, rows, rowcount}`. |
| `POST /sp/call` | Calls a stored procedure: `BEGIN <name>(...); END;`. |
| `POST /sql/bulk-insert` | `TRUNCATE` (optional) + `executemany` INSERT. Used by every DAG extract task. |

**Reporting (used by Streamlit):**

| Endpoint | Purpose |
|---|---|
| `GET /api/oee/available-dates` | Distinct dates present in `FACT_OEE` — drives the date picker. |
| `GET /api/oee/daily?date=YYYY-MM-DD` | Per-machine OEE for one date. |
| `GET /api/oee/weekly-trend` | OEE re-derived from additive measures per year/week. |
| `GET /api/quality/defect-by-stage` | Aggregate defect rate per stage across all dates. |
| `GET /api/maintenance/mtbf-mttr` | Per-machine breakdown stats. |
| `GET /api/inventory/latest` | Most recent snapshot from `FACT_INVENTORY`. |

**Internal helpers:**

| Symbol | Purpose |
|---|---|
| `get_connector()` | Module-level singleton `OracleConnector`. |
| `require_token(Authorization)` | FastAPI `Depends` that checks the `Authorization: Bearer …` header against `ORACLE_API_TOKEN`. If token is blank in env, auth is disabled (dev mode). |
| `_coerce(v)` | Converts Oracle return values to JSON-safe primitives. Handles `datetime.date/time`, `java.lang.String`, etc. Must be called on every column you serialize. |
| `_jdbc_types()` | Lazily imports `jpype`, caches `java.sql.Date` / `Timestamp` classes. |
| `_parse_iso(v)` | **Critical.** If `v` is an ISO date or datetime string, converts it to `java.sql.Date` / `Timestamp` via `jpype.JClass(...).valueOf(...)`. JayDeBeApi's built-in `Date()` / `Timestamp()` factories return strings, which Oracle rejects with ORA-01861. |
| `_prepare_row(row)` | Applies `_parse_iso` to each value in a row. Called before every JDBC bind. |
| `_query_rows(sql, params)` | Shared helper for all reporting endpoints — runs SELECT, returns `list[dict]` keyed by lowercase column names. |

### 3.6 `app/streamlit/dashboard.py`

| Symbol | Purpose |
|---|---|
| `API_URL` / `TOKEN` | Read from `DASHBOARD_API_URL` / `ORACLE_API_TOKEN` env. |
| `_headers()` | Adds bearer header if token present. |
| `fetch(path, params)` | HTTP GET with `st.cache_data(ttl=30, show_spinner=False)`. All data pulls go through this. |
| Sidebar | Date picker (populated from `/api/oee/available-dates`) + refresh button that clears cache. |
| 4 KPI cards | Arithmetic means of OEE/A/P/Q across machines for the selected day. |
| Per-machine bar + table | `st.bar_chart` on `oee_pct`, `st.dataframe` for the full record. |
| 4 tabs | Quality by stage (bar + table), MTBF/MTTR (table), Inventory (table), Weekly OEE trend (multi-line). |

### 3.7 Airflow DAGs

#### `db_module/pipeline/airflow/dags/_oracle_api.py` — HTTP client

| Symbol | Purpose |
|---|---|
| `_BASE_URL`, `_TOKEN`, `_TIMEOUT_S` | Read from env (compose propagates via `env_file`). Defaults to `http://host.docker.internal:8000`. |
| `_headers()` / `_post(path, body)` | Share the bearer header + raise on non-2xx. |
| `health()` | GET `/health`. Used as the first task in each DAG. |
| `as_iso(v)` | JSON-safe serializer for Python `datetime` / `date` / `Decimal`. Must be applied to every value before it goes over the wire (psycopg2's `Decimal` breaks `json.dumps`). |
| `bulk_insert(table, columns, rows, truncate, pipeline_run_id)` | POSTs to `/sql/bulk-insert`. Handles the `as_iso` conversion for every cell. |
| `call_sp(name, args)` | POSTs to `/sp/call`. |
| `run_query(sql, params)` | POSTs to `/sql/query`. |

#### `db_module/pipeline/airflow/dags/_supabase.py`

| Symbol | Purpose |
|---|---|
| `supabase_cursor()` | psycopg2 context manager. Reads `SUPABASE_*` env. |

Note the header comment: inside the Airflow container the bridge network has
IPv6 enabled (via compose `enable_ipv6: true`) — required because Supabase's
`db.<ref>.supabase.co` resolves to IPv6 only.

#### `etl_supabase_to_oracle.py` (6 tasks)

All tasks share this pattern:

```
extract(SQL with DATE(...) = ds filter) → _oracle_api.bulk_insert(truncate=True)
```

| Task | Source table | Target STG |
|---|---|---|
| `check_oracle_api` | — | pings `/health` |
| `extract_production_batch` | Supabase `production_batch` | `STG_PRODUCTION_BATCH` |
| `extract_qc_inspection` | Supabase `qc_inspection` | `STG_QC_INSPECTION` |
| `extract_qc_result` | Supabase `qc_result` (joined to `qc_inspection` for date filter) | `STG_QC_RESULT` |
| `extract_maintenance_log` | Supabase `maintenance_log` | `STG_MAINTENANCE_LOG` |
| `extract_inventory` | Supabase `inventory` (no date filter — full snapshot) | `STG_INVENTORY` |
| `extract_material_consumption` | Supabase `material_consumption` | `STG_MATERIAL_CONSUMPTION` |

Schedule: `0 6,14,22 * * *`. Each STG load is `TRUNCATE + INSERT`, so rerun
for the same `ds` is safe.

#### `etl_influxdb_to_oracle.py` (1 task)

`extract_sensor_agg` runs a Flux query with `aggregateWindow(every: 8h,
fn: mean)` over the DAG's `data_interval_start → data_interval_end`, pivots
fields into columns, converts each row to `STG_SENSOR_AGG` shape, and posts
via `bulk_insert`.

Special env hooks:

- `INFLUX_MEASUREMENT` (default `station_1`) — actual NodeRED measurement name.
- `INFLUX_RANGE_START` (optional, e.g. `-15m`) — overrides the
  schedule-derived window. Useful for ad-hoc smoke tests after a fresh
  NodeRED deployment, when `data_interval_start` would otherwise be earlier
  than any recorded data.

#### `sp_load_dw.py` (5 tasks)

All 5 wrap `call_sp(name, [ds])`:

```
check_oracle_api ─► sp_load_fact_oee
                 ─► sp_load_fact_quality
                 ─► sp_load_fact_maintenance
                 ─► sp_load_fact_production
                 ─► sp_load_fact_inventory
```

Schedule: `30 6,14,22 * * *` — 30 minutes after the extract DAGs.

### 3.8 Applier scripts (local dev only)

| Script | Purpose |
|---|---|
| `datasources/oracle_sql_query/apply_ddl.py` | Reads a `.sql` file, splits it on `;` (SQL) and `/` (PL/SQL) boundaries, executes each statement via `OracleConnector`. Fails fast with rollback on first error. Used to apply `01_dw_ddl.sql`, `02_sp_dim_date.sql`, `03_sp_fact_loaders.sql`, `06_inventory_pipeline.sql`. |
| `datasources/oracle_sql_query/seed_dims.py` | First cross-system ETL. Reads master data from Supabase (`machine`, `product`, `process_stage`, `raw_material`), loads into `DIM_*` tables with `SEQ_DIM_*.NEXTVAL` surrogate keys. Source IDs preserved in `*_src_id` columns. |
| `datasources/oracle_sql_query/verify_dw.py` | Sanity check — asserts all expected tables + sequences exist under `AI03`. |
| `datasources/supabases_sql_query/apply_supabase.py` | Runs `01_schema.sql` + `02_master_data.sql` + `03_mock_data.sql` in one psycopg2 transaction. |
| `datasources/supabases_sql_query/mock/generate_mock_data.py` | Deterministic (seed=42) generator producing ~15 k rows matching CLAUDE.md §5 targets. |

### 3.9 SQL files

| File | Purpose |
|---|---|
| `01_dw_ddl.sql` | 5 STG + 5 DIM + 5 FACT tables, 9 sequences, 5 reporting indexes. Idempotent teardown block at top. |
| `02_sp_dim_date.sql` | `SP_LOAD_DIM_DATE(start, days)` — MERGE-based, ISO-week anchoring for locale-safe weekend detection, English month names. |
| `03_sp_fact_loaders.sql` | `FN_CALC_OEE` + 4 `SP_LOAD_FACT_*` procedures. All use `DELETE date_id = v_date_id` then INSERT → idempotent per date. |
| `04_reporting_queries.sql` | 5 read-only reports (OEE by date, defect by stage, material consumption, MTBF/MTTR, weekly OEE trend). |
| `05_truncate_and_reload.sql` | Full DW rebuild — truncates facts → dims (preserves DIM_DATE), drops + recreates sequences. Driver comment explains the Python steps that follow. |
| `06_inventory_pipeline.sql` | Adds `STG_INVENTORY` + `STG_MATERIAL_CONSUMPTION` + `SP_LOAD_FACT_INVENTORY`. |

### 3.10 Test scripts

| Script | Purpose |
|---|---|
| `test/test_connection.py` | Three-layer probe: HTTP iSQL*Plus, TCP 1521, JDBC handshake. Run directly (`python test/test_connection.py`). |
| `test/test_create_table.py` | DDL/DML roundtrip — CREATE, INSERT, SELECT, DROP of a temp table. Proves `AI03` has the needed grants. |
| `test/test_connectors.py` | Pytest smoke tests for all 3 connectors. Each skips if its required env vars are blank, so the suite passes in a half-configured environment. |

---

## 4. What to improve (focus areas for a human)

Ordered by impact / effort ratio. The first three are genuine holes; the
middle group is tech debt; the bottom is deferred scope.

### 4.1 Genuine holes (would ship broken at scale)

1. **No connection pooling or retry in the Oracle API.** Every HTTP request
   opens a fresh JDBC connection (`connect()` → `cursor()` → `close()`).
   On the KMITL server that's ~300 ms per call. Under burst load (multiple
   DAG tasks in parallel) this saturates Oracle session slots. Fix: add a
   thread-safe pool (`java.util.concurrent` in JDBC or a Python-side queue
   of `OracleConnector` instances) and cap concurrency. Also no retry on
   transient errors — one flaky network blip fails the whole bulk-insert.
2. **Idempotency is not transactional across the DAG.** If `extract_qc_result`
   succeeds but `extract_qc_inspection` fails, STG is half-written. The
   SP-loader that runs 30 min later will happily derive facts from the
   inconsistent state. Fix: extract tasks should write to `STG_xxx_PENDING`
   tables, and a final `swap_pending_to_live` task renames them atomically
   when all 6 extracts succeed.
3. **Secrets in plain `.env`.** The Oracle password, Supabase password, and
   InfluxDB token are sitting in `.env` gitignored on the dev machine and
   propagated verbatim to every Airflow container. Fine for coursework, bad
   for anything shared. Fix: Airflow Connections (with Fernet encryption) +
   a real secrets manager (AWS SSM, HashiCorp Vault, etc.) for the host
   uvicorn service.

### 4.2 Architectural tech debt

4. **`.env` has 20+ keys; the project has 3 domains.** You're one typo away
   from ugly runtime errors. Would benefit from a `pydantic-settings`
   Config class that loads + validates the whole environment once, then the
   connectors accept a typed config object instead of rebuilding from env
   each time.
5. **Master data is hardcoded in three places.** Adding a new stage or
   machine requires editing `02_master_data.sql` (Supabase seed), re-running
   `seed_dims.py`, and possibly updating the stage-id join logic in
   `SP_LOAD_FACT_OEE` (which assumes stages 1, 5, 8 map to the 3 machines
   via `sequence_no` matching). Fix: make the SP look up the stage via
   `DIM_STAGE.machine_name = DIM_MACHINE.machine_name` and drop the
   `sequence_no` coupling entirely.
6. **FACT_INVENTORY is a simplification, not a real periodic snapshot.**
   `qty_received = 0` in every row because the extract DAG doesn't pull
   `raw_material_receipt`. `qty_opening` is backward-derived from closing,
   not a true end-of-previous-day carryover. Fix: add a proper opening-
   balance column in `STG_INVENTORY` (`qty_on_hand_previous_day`) and a
   `raw_material_receipt` extract task.
7. **OEE planned-time is hardcoded (`c_planned NUMBER := 480`).** Spec says
   8-hour single-shift, which is fine until someone asks "what about
   weekends / holidays / second shift?" There's no `DIM_SHIFT` table to
   lean on. Easy to add later.

### 4.3 Overengineering (things that are more machinery than they warrant)

8. **`_parse_iso` is a magical coercion layer.** It sniffs ISO date strings
   and converts them to `java.sql.Date` via `jpype.JClass`. It works, but
   every caller has to trust it. If someone adds a VARCHAR column that
   happens to contain an ISO-looking string, it'll get silently converted.
   *Less-magic alternative*: a `DATE:` prefix convention, or a typed
   parameter object. Keeping the magic is defensible because we control
   both sides of the wire — but worth flagging if the API ever becomes
   external.
9. **The `apply_ddl.py` SQL splitter re-implements what `sqlplus` already
   does.** ~40 lines of string scanning. `sqlplus -s` or Oracle's
   `DBMS_UTILITY` could do this, but then you'd need sqlplus installed.
   Also fine because `ojdbc8.jar` is already the assumption.
10. **Airflow for a 3-DAG project.** We're running a Postgres metadata DB,
    a scheduler, a webserver, and a custom image… to run 12 tasks on an
    8-hour cron. A bash script in `cron` would do. That said, Airflow
    gives you the UI, retries, and lineage for free, so it's not egregious
    — just acknowledge the weight.
11. **Docker compose with an init container for a local dev stack.** The
    `airflow-init` service exists only to run `airflow db migrate` and
    create the admin user once. For real coursework use you could just
    run those manually the first time. `airflow-init` makes
    `docker compose up -d` a one-shot but adds cognitive load.
12. **Bearer-token auth on `localhost`.** We check `ORACLE_API_TOKEN` on
    every endpoint. The whole system is running on your laptop. Dev mode
    disables auth when the token is blank — use that, and save the auth
    layer for when we actually deploy somewhere.

### 4.4 Not built, not blocking

13. **Streamlit UI is unverified by automation.** I only confirmed the
    process starts and health endpoints return 200. The actual rendered
    chart / KPI tiles need a browser. No Playwright / Selenium tests.
14. **No unit tests for SPs.** The 4 fact-loaders are only verified by
    running them against a populated DW and checking row counts. A proper
    approach: pre-seed STG with known rows via Oracle API `/sql/execute`,
    call the SP, assert FACT rows match expected.
15. **No monitoring.** A failed DAG at 06:30 is invisible until someone
    refreshes the Airflow UI. Fix: Airflow callbacks that POST to Slack /
    email, or hook into Supabase's realtime for pipeline status.
16. **Observability of the Oracle API is zero.** No structured logs,
    no Prometheus metrics, no request ID. A 500 at 03:00 just shows as a
    DAG retry with a stack trace in Airflow logs. Add `structlog` +
    FastAPI middleware if this ever matters.
17. **NodeRED state lost on restart.** Cycle counts and fault-state
    context don't persist across NodeRED deploys. For a demo this is fine;
    for a real simulator you'd want `context.set('...', ..., 'file')`.

### 4.5 A honest self-assessment

What this project **does well**:

- Cleanly separates the three environments (OLTP, DW, streaming) with
  dedicated connectors and explicit data flow.
- Idempotency by design — every STG load is `TRUNCATE + INSERT`; every SP
  is `DELETE date + INSERT`. Re-runs are safe.
- Catches the Oracle-10g land mines once, in the connector (`NLS_CALENDAR`,
  O3LOGON, JVM locale, `java.sql.Date` factory) — callers don't have to
  think about them.
- Uses the FastAPI service as a boundary: Airflow image stays Python-only,
  Streamlit stays SQL-free.

What it **does less well**:

- Not production-ready. One VM dies, one network hiccups, and you're
  down. Fine for a 7-day coursework project, not fine for real operations.
- Tests are sparse. We verified by running, not by asserting.
- Several joins in the SPs rely on `stage_src_id == sequence_no ==
  STG.stage_id` all being `1..10` — implicit coupling that will break the
  day someone inserts stage 11.

If you had another week, spend it on **(1) connection pooling in the Oracle
API** and **(2) atomic STG swap-over**. Everything else can wait.
