# Battery Manufacturing OEE Pipeline — Plan & Implementation Tracker

> Companion to [CLAUDE.md](../CLAUDE.md). This file is the single source of truth for *what is done, what is next, and what is blocked*.
>
> **How to use:** tick `[x]` the moment a task is verified. If you hit a blocker, do not tick — append a one-liner under **Blockers** in that phase and move on. Update [Changelog](#changelog) when a phase closes.
>
> **Last updated:** 2026-04-18

---

## 0. Current State Snapshot

Scaffolding only — no connector, DAG, DDL, or dashboard code yet. The one working piece is the Oracle JDBC smoke test.

| Area | Path | State |
|---|---|---|
| Oracle JDBC connection probe | [test/test_connection.py](../test/test_connection.py) | ✅ working (JayDeBeApi + ojdbc8.jar, O3LOGON flag) |
| Oracle DDL smoke test | [test/test_create_table.py](../test/test_create_table.py) | ✅ working (AI03 has CREATE/INSERT/SELECT/DROP) |
| Oracle JDBC driver | [db_module/db_conn/oracle/drivers/ojdbc8.jar](../db_module/db_conn/oracle/drivers/ojdbc8.jar) | ✅ present (4.5 MB, v19.23) — moved next to connector module |
| Oracle connector module | [db_module/db_conn/oracle/oracle_connection.py](../db_module/db_conn/oracle/oracle_connection.py) | ✅ `OracleConnector` — live-verified against AI03 |
| Supabase connector module | [db_module/db_conn/supabases/supabase_connection.py](../db_module/db_conn/supabases/supabase_connection.py) | ✅ `SupabaseConnector` — live-verified |
| InfluxDB connector module | [db_module/db_conn/influxdb/influx_connection.py](../db_module/db_conn/influxdb/influx_connection.py) | ✅ `InfluxConnector` written; live-unverified |
| Airflow DAGs | [db_module/pipeline/airflow/dags/](../db_module/pipeline/airflow/dags/) | ✅ `etl_supabase_to_oracle` (4 tasks, live-verified), `etl_influxdb_to_oracle` (skeleton) |
| Pipeline Dockerfile + compose | [db_module/pipeline/](../db_module/pipeline/) | ✅ Airflow 2.8 + postgres-af + webserver(:8088) + scheduler, live |
| Oracle API service | [app/api/main.py](../app/api/main.py) | ✅ FastAPI wrapper — `/health`, `/sql/query`, `/sql/execute`, `/sp/call`, `/sql/bulk-insert`. Live-tested end-to-end. |
| IIoT container (NodeRED/MQTT/Influx) | [datasources/iiot_container/](../datasources/iiot_container/) | 🟥 empty Dockerfile + compose, empty nodered/ |
| Oracle SQL queries | [datasources/oracle_sql_query/](../datasources/oracle_sql_query/) | ✅ `01_dw_ddl.sql` + `02_sp_dim_date.sql` + `apply_ddl.py` + `verify_dw.py` — **live-applied to AI03** |
| Supabase SQL | [datasources/supabases_sql_query/query/](../datasources/supabases_sql_query/query/) + [mock/](../datasources/supabases_sql_query/mock/) | ✅ `01_schema.sql` + `02_master_data.sql` + `03_mock_data.sql` + `generate_mock_data.py` — unverified against live Supabase |
| FastAPI app | [app/api/](../app/api/) | 🟥 empty |
| Streamlit app | [app/streamlit/](../app/streamlit/) | 🟥 empty |
| Python deps | [requirements.txt](../requirements.txt) | ✅ expanded + installed into `.venv` |
| `.env` / `.env.example` / `.gitignore` | repo root | ✅ created; `.env` holds AI03 creds, Supabase + Influx fields blank |

**Verified environment facts (memory / live test):**

- Oracle 10.2.0.3 at `161.246.35.92:1521/orcl`, user `AI03`. Password lives in `.env` (gitignored) — see `.env.example` for keys.
- Thin mode / python-oracledb cannot reach this server — **must** use JDBC (JayDeBeApi + ojdbc8.jar with `-Doracle.jdbc.thinLogonCapability=o3`).
- `AI03` can DDL/DML in its own schema — unverified whether it has `CREATE USER` / tablespace grants needed for `BATTERY_OLTP` / `BATTERY_STG` / `BATTERY_DW` schemas (see [Open Questions](#open-questions)).

---

## 1. Resolved Decisions (2026-04-18)

All five open questions answered. These override CLAUDE.md §3 / §7 / §13 where they conflict.

- [x] **Q1. Drop the `CREATE USER` step entirely.** No new Oracle schemas will be created. Everything lives under `AI03`. Since Supabase owns OLTP (Q2), Oracle only needs STG + DW tables — they don't collide with OLTP names, so no prefixing gymnastics needed. Table names stay as written (`STG_PRODUCTION_BATCH`, `FACT_OEE`, etc.) but live in the `AI03` schema.
- [x] **Q2. Supabase = OLTP source of truth. Oracle = Data Warehouse only.** Drop `BATTERY_OLTP` on Oracle entirely. The 17-table ERD in CLAUDE.md §7 is the Supabase schema and is the [A] deliverable. Oracle AI03 hosts only the 5 STG + 10 DW tables.
- [x] **Q3. InfluxDB 2.0.** Already running on AWS with NodeRED — we consume from it, we don't provision it. Flux query + `influxdb-client` are the right choice.
- [x] **Q4. Local-only deployment.** No EC2 provisioning work. Compose hostnames use `localhost` from host and service-name DNS between containers. Remote services (Supabase cloud, KMITL Oracle, AWS InfluxDB) are hit directly over the public network.
- [x] **Q5. Secrets move to `.env`.** Ship a `.env.example` with placeholder values; real `.env` is git-ignored. Approved to refactor [test_connection.py:29](../test/test_connection.py#L29) as part of Phase 1.

### Follow-up information still needed (block Phase 1 / 3)

Please provide when available — plug straight into `.env`:

- [ ] **AWS InfluxDB**: URL (e.g. `http://<ec2-ip>:8086`), org name, API token, bucket name.
- [ ] **AWS NodeRED**: URL (only needed if we have to inspect or tweak existing flows).
- [ ] **Supabase**: project URL + either service-role key (REST) or direct Postgres connection string (preferred for `PostgresHook`).

---

## 2. Phased Plan

Seven phases mapped to the 7-day schedule in CLAUDE.md §14. Each phase has an exit criterion — do not advance until it's green.

### Phase 1 — Foundation & Connectivity (Day 1)

**Exit criterion:** all three connector classes can `connect() → execute("SELECT 1") → close()` from a local `pytest` run.

- [x] Answer all five [Open Questions](#open-questions). ✅ 2026-04-18
- [x] Move Oracle credentials into [.env](../.env) and add `.env` to [.gitignore](../.gitignore) (create [.env.example](../.env.example)).
- [x] Expand [requirements.txt](../requirements.txt) with `python-dotenv`, `influxdb-client`, `psycopg2-binary`, `pandas`, `fastapi`, `uvicorn[standard]`, `pytest`. (`paho-mqtt` dropped — Phase 3 reads from AWS Influx directly, no MQTT on local side.)
- [x] Implement [db_module/db_conn/oracle/oracle_connection.py](../db_module/db_conn/oracle/oracle_connection.py) — `OracleConnector` class, JDBC logic, `cursor()` context manager with commit/rollback.
- [x] Rename `superbases_connection.py` → [supabase_connection.py](../db_module/db_conn/supabases/supabase_connection.py), implement `SupabaseConnector` with psycopg2.
- [x] Implement [db_module/db_conn/influxdb/influx_connection.py](../db_module/db_conn/influxdb/influx_connection.py) — `InfluxConnector` wrapping `influxdb_client`, `query()` / `query_records()` / `write()`.
- [x] Add shared [_env.py](../db_module/db_conn/_env.py) helper (load_dotenv, `require()`, `get()`, `resolve_path()`).
- [x] Add [db_module/db_conn/__init__.py](../db_module/db_conn/__init__.py) + sub-package `__init__.py` re-exports.
- [x] Install deps into `.venv` via uv.
- [x] Write pytest smoke tests at [test/test_connectors.py](../test/test_connectors.py) that skip when env missing.
- [x] **Live Oracle verified** — `test_connection.py` + `test_create_table.py` + `pytest test_connectors.py::test_oracle_connector_roundtrip` all pass against `161.246.35.92:1521/orcl` as `AI03`. Banner: `Oracle Database 10g Enterprise Edition Release 10.2.0.3.0`.
- [ ] **Live Supabase verification** — blocked, waiting on connection credentials. Test skips cleanly meanwhile.
- [ ] **Live InfluxDB verification** — blocked, waiting on AWS URL/token. Test skips cleanly meanwhile.

**Phase 1 status:** ✅ functionally complete. Only outstanding items are live verifications pending credentials.

### Phase 2 — Supabase OLTP Schema & Mock Data (Day 2)  →  deliverables [A]-1, [A]-2, [A]-3

**Exit criterion:** Supabase contains all 17 tables with 14,589 mock rows; ERD exported. Oracle is untouched in this phase.

- [x] Write [datasources/supabases_sql_query/query/01_schema.sql](../datasources/supabases_sql_query/query/01_schema.sql) — 17 tables from CLAUDE.md §7 (PostgreSQL: `SERIAL`, `DECIMAL`, `TIMESTAMP`, CHECK constraints, 7 indexes for ETL).
- [x] Write [datasources/supabases_sql_query/query/02_master_data.sql](../datasources/supabases_sql_query/query/02_master_data.sql) — 1 line, 3 machines, 10 stages, 3 products, 15 BOM rows, 4 suppliers, 5 raw materials, 5 opening inventory rows. Sequence values synced with `setval()` after inserts.
- [x] Author [datasources/supabases_sql_query/mock/generate_mock_data.py](../datasources/supabases_sql_query/mock/generate_mock_data.py) — deterministic (seed=42), 30 days starting 2026-03-19, bias toward instrumented stages 1/5/8.
- [x] Run generator → [03_mock_data.sql](../datasources/supabases_sql_query/mock/03_mock_data.sql) (~900 KB, 15 K lines). **Row counts within 2-4 % of CLAUDE.md §5 targets:**

  | Table | Actual | Target | Delta |
  |---|---:|---:|---:|
  | production_batch | 540 | 540 | ✓ |
  | finished_good | 6,237 | 6,114 | +2.0 % |
  | material_consumption | 1,080 | 1,080 | ✓ |
  | qc_inspection | 540 | 540 | ✓ |
  | qc_result | 6,524 | 6,300 | +3.5 % |
  | maintenance_log | 15 | 15 | ✓ |
  | **core total** | **14,936** | **14,589** | **+2.4 %** |

- [x] **Live-applied against Supabase** via [apply_supabase.py](../datasources/supabases_sql_query/apply_supabase.py). All 3 files in one transaction (916 KB total). Row counts verified from live DB match generator output exactly.
- [ ] Export ERD (Supabase Studio screenshot) → `claude_track/erd_oltp.png` — manual task.

**Phase 2 status:** ✅ live and verified. Only outstanding item is the ERD screenshot for [A]-1 deliverable.

### Phase 3 — Verify AWS IIoT Data Flow (Day 3)

The NodeRED + Mosquitto + InfluxDB 2.0 stack already runs on AWS. We **consume** it — we do not rebuild it locally. The local [datasources/iiot_container/](../datasources/iiot_container/) directory is now reference-only (keep empty compose as documentation of the remote layout, or delete).

**Exit criterion:** local Python script using `InfluxConnector` runs a Flux query against the AWS bucket and returns recent rows tagged with all three `machine_id`s.

- [ ] Collect AWS endpoint details into `.env`: `INFLUX_URL`, `INFLUX_ORG`, `INFLUX_TOKEN`, `INFLUX_BUCKET`.
- [ ] Verify the three expected tag values (`M01`/`M02`/`M03`) and field names (`temperature_c`, `machine_state_num`, `cycle_count`, `vibration_g`, `current_a`, `voltage_v`) exist — if not, log a follow-up to fix the NodeRED flow.
- [ ] Write `test/test_influx_read.py` — connects, queries last 1 h, asserts ≥ 1 row per machine_id, asserts `machine_state_num` field present.
- [ ] Decide whether `datasources/iiot_container/` is kept as documentation or removed (recommend keeping a README + a copy of the NodeRED flow JSON for reproducibility).

**Blockers:** _needs AWS InfluxDB credentials (see Follow-ups)_

### Phase 4 — Oracle DDL: Staging + Data Warehouse (Day 4)  →  deliverables [B]-1, [B]-2

All tables under `AI03` schema — no `CREATE USER`, no `BATTERY_OLTP`. Only STG + DW.

**Exit criterion:** DDL applied cleanly against `AI03`; 5 STG + 10 DW tables exist; DIM_DATE populated for 5 years; star-schema diagram exported.

- [x] Write [datasources/oracle_sql_query/01_dw_ddl.sql](../datasources/oracle_sql_query/01_dw_ddl.sql) — 5 STG + 5 DIM + 5 FACT tables, 9 sequences for surrogate keys, 5 reporting indexes, idempotent teardown block at top. Oracle 10g compatible (no `GENERATED AS IDENTITY`).
- [x] Write [datasources/oracle_sql_query/02_sp_dim_date.sql](../datasources/oracle_sql_query/02_sp_dim_date.sql) — `SP_LOAD_DIM_DATE(start, days)` using ISO-week anchoring for locale-safe weekend detection, English month names via `NLS_DATE_LANGUAGE=ENGLISH`, `MERGE` for idempotency.
- [x] Write [datasources/oracle_sql_query/apply_ddl.py](../datasources/oracle_sql_query/apply_ddl.py) — splits on `;` for SQL and `/` on own line for PL/SQL blocks, fails fast with rollback.
- [x] Write [datasources/oracle_sql_query/verify_dw.py](../datasources/oracle_sql_query/verify_dw.py) — asserts all 15 tables + 9 sequences exist.
- [x] **Live-applied 01_dw_ddl.sql against AI03.** All 30 statements committed; verify script reports all objects ✓.
- [x] **Live-applied 02_sp_dim_date.sql.** SP compiled VALID.
- [x] **DIM_DATE populated for 5 years.** `SP_LOAD_DIM_DATE(DATE '2024-01-01', 1826)` → 1,826 rows, 2024-01-01 → 2028-12-30. Sample: Saturday 2025-06-07 correctly tagged `day_of_week=6`, `is_weekend='Y'`, `month_name='June'`.
- [x] **DIM_MACHINE / PRODUCT / STAGE / MATERIAL seeded from Supabase** via [seed_dims.py](../datasources/oracle_sql_query/seed_dims.py) — first working cross-system ETL (Supabase → Oracle). Loaded 3 / 3 / 10 / 5 rows respectively. Source IDs preserved in `*_src_id` columns for fact-loader lookup.
- [ ] Export star-schema diagram → `claude_track/erd_dw.png` — manual draw.io task.

**Phase 4 status:** ✅ fully live-verified. Only remaining item is the ERD screenshot for [B]-1 deliverable.

### Phase 5 — ETL DAGs (Day 5)

**Architecture change (2026-04-18):** Airflow does not talk to Oracle directly. It calls the FastAPI Oracle service ([app/api/main.py](../app/api/main.py)) via HTTP. This keeps Java + `ojdbc8.jar` + JayDeBeApi out of the Airflow image — Airflow only needs Python + `requests` + `psycopg2` + `influxdb-client`.

```
Airflow container                            Local host (uvicorn)             Remote
─────────────────                            ────────────────────             ──────
etl_supabase_to_oracle → requests.post → app/api/main.py → JDBC → Oracle AI03
etl_influxdb_to_oracle → influxdb-client ────────────────────────────→ AWS InfluxDB
                                             │
                                             └─ PostgresHook (psycopg2)  → Supabase
```

**Exit criterion:** both DAGs run green for yesterday's date; STG row counts > 0; lineage columns correctly filled.

- [x] Oracle API service [app/api/main.py](../app/api/main.py) — `/health`, `/sql/query`, `/sql/execute`, `/sp/call`, `/sql/bulk-insert`. Bearer-token auth via `ORACLE_API_TOKEN`. Live-tested end-to-end against AI03.
- [x] [db_module/pipeline/Dockerfile](../db_module/pipeline/Dockerfile) — Airflow 2.8 + `requests` / `psycopg2-binary` / `influxdb-client`. **Zero Java in image.**
- [x] [db_module/pipeline/docker-compose.yml](../db_module/pipeline/docker-compose.yml) — postgres-af + airflow-init + webserver + scheduler. Webserver on `localhost:8088` (8080/8081 taken by existing `data_layer` stack). Network has `enable_ipv6: true` + ULA subnet so containers can reach Supabase's IPv6-only direct-connection host. `host.docker.internal` + `ORACLE_API_URL` override point DAGs at host-side uvicorn.
- [x] Shared helpers in [dags/](../db_module/pipeline/airflow/dags/): `_oracle_api.py` (bulk_insert / call_sp / run_query / health, all via bearer token), `_supabase.py` (psycopg2 context manager).
- [x] DAG [etl_supabase_to_oracle.py](../db_module/pipeline/airflow/dags/etl_supabase_to_oracle.py) — healthcheck gate + 4 parallel extract tasks (production_batch, qc_inspection, qc_result, maintenance_log). `TRUNCATE + INSERT` per STG table with lineage (src_system, pipeline_run_id). Cron `0 6,14,22 * * *`.
- [x] DAG [etl_influxdb_to_oracle.py](../db_module/pipeline/airflow/dags/etl_influxdb_to_oracle.py) — live. Consumes measurement `station_1` from bucket `iiot_data_raw` (NodeRED's actual names, not the `machine_metrics`/`sensors` placeholders from CLAUDE.md §5). Supports `INFLUX_RANGE_START` env override for ad-hoc smoke tests when `data_interval` hasn't caught up to fresh data.
- [x] **Live end-to-end verified for 2026-04-15.** `airflow tasks test etl_supabase_to_oracle {task} 2026-04-15` for all 4 tasks → `bulk_insert` succeeds → Oracle STG tables populated:

  | STG Table | Rows | Source |
  |---|---:|---|
  | STG_PRODUCTION_BATCH | 20 | Supabase `production_batch` |
  | STG_QC_INSPECTION | 20 | Supabase `qc_inspection` |
  | STG_QC_RESULT | 216 | Supabase `qc_result` |
  | STG_MAINTENANCE_LOG | 2 | Supabase `maintenance_log` |
  | STG_SENSOR_AGG | 3 | InfluxDB `iiot_data_raw` / `station_1` (live NodeRED) |

  Every row tagged with `src_system` (`SUPABASE` or `INFLUXDB`) + Airflow's native `pipeline_run_id`. Sensor aggregates show M01=478.2 °C / M02 vibration 0.80 g / M03 143.6 A @ 10.5 V — all in their expected normal ranges.

- [ ] Author `sp_load_dw.py` DAG — chains `/sp/call` for the FACT loaders. **Waits on Phase 6** (SPs not yet written).
- [ ] Configure Airflow **Variables / Connections** UI — optional; the current env-var flow works but lacks UI manageability.
- [ ] Unpause DAGs for scheduled runs — on hold until we have confidence / backfill plan.

**Phase 5 status:** ✅ end-to-end data flow (Supabase → Airflow → Oracle API → AI03 STG) is live and verified for a backfill date. Remaining Phase-5 items depend on Phase 6 SPs.

### Phase 6 — Stored Procedures / Functions + Reporting (Day 6)  →  deliverables [B]-3, [B]-4, [B]-5, [C]-1, [C]-2, [C]-3

**Exit criterion:** `SP_LOAD_FACT_OEE(date)` produces exactly 3 rows in FACT_OEE (one per machine); all 4 fact tables populate cleanly; DAG chains SPs after extract.

- [x] [datasources/oracle_sql_query/03_sp_fact_loaders.sql](../datasources/oracle_sql_query/03_sp_fact_loaders.sql) — `FN_CALC_OEE` + `SP_LOAD_FACT_OEE` / `_QUALITY` / `_MAINTENANCE` / `_PRODUCTION`. All idempotent (DELETE-by-date then INSERT). Oracle 10g compatibility: SP_LOAD_FACT_PRODUCTION uses cursor-loop + sequence NEXTVAL (not INSERT…SELECT, which throws ORA-02287 on 10g).
- [x] Applied live against AI03 via [apply_ddl.py](../datasources/oracle_sql_query/apply_ddl.py). All 5 objects (1 function + 4 procedures) compile `VALID`.
- [x] **FACT tables populated for 2026-04-17** (the date with both production + qc in STG):

  | Fact | Rows | Notes |
  |---|---:|---|
  | FACT_OEE | 3 | one per machine, OEE% math verified |
  | FACT_QUALITY | 7 | one per stage that has inspections |
  | FACT_MAINTENANCE | 2 | from STG_MAINTENANCE_LOG |
  | FACT_PRODUCTION | 7 | one per (machine, stage) with batches |

  OEE math sanity-check (M03 Formation Charger):
  A = (480−0)/480 = 100 % · P = (72×300/60)/648 = 55.56 % · Q = 66/72 = 91.67 %
  → OEE = 1.0 × 0.5556 × 0.9167 × 100 = **50.93 %** ✓ matches `FN_CALC_OEE`.

- [x] DAG [sp_load_dw.py](../db_module/pipeline/airflow/dags/sp_load_dw.py) — healthcheck + 4 parallel SP tasks, cron `30 6,14,22 * * *` (30 min after extract DAGs). Live-verified: `airflow tasks test sp_load_dw sp_load_fact_oee 2026-04-17` → `SP_LOAD_FACT_OEE(2026-04-17) → {'ok': True}`.
- [x] **[B]-4 Reporting queries** — [04_reporting_queries.sql](../datasources/oracle_sql_query/04_reporting_queries.sql) with all 5 queries from CLAUDE.md §10. Q1/Q2/Q4 live-verified against populated DW (Q3 needs FACT_INVENTORY; Q5 needs multi-week data).
- [x] **[B]-5 Truncate + Reload script** — [05_truncate_and_reload.sql](../datasources/oracle_sql_query/05_truncate_and_reload.sql). TRUNCATEs facts → dims (preserves DIM_DATE), drops + recreates sequences (Oracle 10g can't `ALTER SEQUENCE RESTART`), then driver comment points at `seed_dims.py` + SP re-run loop.
- [x] **FACT_INVENTORY pipeline** — [06_inventory_pipeline.sql](../datasources/oracle_sql_query/06_inventory_pipeline.sql) adds `STG_INVENTORY` (5-row snapshot) + `STG_MATERIAL_CONSUMPTION` (per-date aggregates) + `SP_LOAD_FACT_INVENTORY`. Two new extract tasks added to [etl_supabase_to_oracle](../db_module/pipeline/airflow/dags/etl_supabase_to_oracle.py). `sp_load_fact_inventory` wired into [sp_load_dw](../db_module/pipeline/airflow/dags/sp_load_dw.py) as a 5th parallel task. Live-verified for 2026-04-17 → 5 rows (one per material) with opening/consumed/closing math working (e.g. Lead closing 5000 kg, consumed 67.6 kg, opening 5067.6 kg).

**Phase 6 status:** ✅ **all deliverables live.** [B]-3, [B]-4, [B]-5, [C]-1, [C]-2, [C]-3 complete.

### Phase 7 — Serving & Dashboard (Day 7)

**Exit criterion:** `http://localhost:8501` shows OEE/Availability/Performance/Quality KPIs for a selected date across M01/M02/M03, bar chart, and raw table — all sourced from Oracle DW through FastAPI.

- [x] Added **6 dashboard endpoints** to [app/api/main.py](../app/api/main.py) on top of the existing operational ones:
  - `GET /api/oee/available-dates` — drives the date picker
  - `GET /api/oee/daily?date=YYYY-MM-DD`
  - `GET /api/quality/defect-by-stage`
  - `GET /api/maintenance/mtbf-mttr`
  - `GET /api/inventory/latest`
  - `GET /api/oee/weekly-trend`

  All 6 live-tested via curl against the populated DW.
- [x] [app/streamlit/dashboard.py](../app/streamlit/dashboard.py) — Streamlit UI with:
  - Sidebar date picker (populated from `/api/oee/available-dates`) + refresh button.
  - 4 KPI cards (OEE / A / P / Q, averaged across machines).
  - Per-machine bar chart + detailed table.
  - 4 tabs: Quality by stage, MTBF/MTTR, Inventory snapshot, Weekly OEE trend (line chart).
  - 30-second `st.cache_data` on every HTTP fetch so re-renders don't hammer Oracle.
- [x] Launched via `.venv/bin/streamlit run` — process up, `http://localhost:8501` returns 200, `/_stcore/health` = `ok`, Python startup log clean. **UI rendering unverified by automation** (Streamlit requires a browser/websocket session); you need to open the URL and confirm the layout.
- [ ] `app/api/Dockerfile` + `app/streamlit/Dockerfile` + unified top-level compose — deferred (local-only deployment per Q4 decision; add later if deployment target changes).

**Phase 7 status:** ✅ backend + frontend wired up and reachable. Browser smoke-test pending (manual step).

---

## 3. Cross-Cutting Concerns

- **Secrets.** Single `.env` at repo root, loaded by `python-dotenv`. Never commit. Airflow reads via `.env` + `env_file:` in compose.
- **Idempotency.** Every STG load is `TRUNCATE` + `INSERT`. Every DW load is `DELETE WHERE date_id = :d` + `INSERT`. SPs are safe to re-run.
- **Timezone.** Supabase stores UTC; Oracle uses server time. Convert at extract time, not in SPs.
- **Testing discipline.** Phase N cannot be ticked complete if its exit criterion is un-verified. "Code written" ≠ "phase done".

---

## 4. Risk Register

| # | Risk | Mitigation |
|---|---|---|
| R1 | ~~AI03 lacks `CREATE USER`~~ | **Resolved (Q1)** — no schema creation; all tables under `AI03` |
| R2 | Shared Oracle 10g session slots exhausted | Every connector enforces `close()` in `finally`; add `sessions.max_per_user` note |
| R3 | Supabase free-tier connection cap during bulk load | Batch inserts in chunks of 1000, single long-lived connection |
| R4 | NodeRED simulator drifting from production reality (OEE lands outside 65-75 %) | Tune `defect_rate` / `downtime_freq` in simulator config once Phase 6 renders real OEE |
| R5 | InfluxDB Flux `aggregateWindow(every: 8h)` misaligns with DAG `6,14,22` cron | Align Flux window start to 06:00 explicitly with `timeSrc: "_start"` |

---

## 5. Changelog

- **2026-04-18 — Phase 0.** Plan drafted. Current state surveyed. 5 open questions raised, schema strategy (Q1) flagged as hardest blocker.
- **2026-04-18 — Decisions locked.** All 5 questions resolved: Supabase-only OLTP, Oracle-only DW under `AI03` (no new schemas), IIoT stack consumed from AWS (not rebuilt locally), local-only deployment, `.env`-based secrets. Phase 2/3/4 rewritten to match.
- **2026-04-18 — Phase 1 functionally complete.** All three connectors implemented + exported. `.env` / `.env.example` / `.gitignore` in place. Deps installed into `.venv`. Oracle round-trip verified live (sysdate, banner, full DDL/DML lifecycle). Supabase + Influx smoke tests skip cleanly pending credentials.
- **2026-04-18 — Phase 2 authored (live-blocked).** 17-table PostgreSQL schema + master data + deterministic mock generator. Generator produces 14,936 core rows (vs 14,589 target, +2.4 %). Execution against Supabase waits on credentials.
- **2026-04-18 — Phase 4 live and verified.** Oracle DDL applied to AI03: 5 STG + 5 DIM + 5 FACT tables + 9 sequences + 5 indexes. `SP_LOAD_DIM_DATE` compiled VALID, populated 1,826 rows (2024-01-01 → 2028-12-30). Only remaining Phase 4 items (dim population for machine/product/stage/material, ERD diagram export) wait on Phase 2 live apply.
- **2026-04-18 — Phase 2 & 4 complete.** Supabase credentials arrived. All 3 SQL files applied live (17 tables, 14,936 mock rows). `seed_dims.py` ran first working Supabase→Oracle ETL: DIM_MACHINE/PRODUCT/STAGE/MATERIAL populated from live Supabase master data. Remaining in Phase 2/4: ERD diagram screenshots (manual draw.io work).
- **2026-04-18 — Oracle API service shipped.** Moved `ojdbc8.jar` to `db_module/db_conn/oracle/drivers/`. Built FastAPI wrapper at [app/api/main.py](../app/api/main.py) so Airflow can avoid bundling Java + JDBC. All 5 endpoints (`/health`, `/sql/query`, `/sql/execute`, `/sp/call`, `/sql/bulk-insert`) live-tested. Two new JDBC gotchas discovered and fixed in [OracleConnector](../db_module/db_conn/oracle/oracle_connection.py): (1) JVM locale defaulted to Thai on host, converting Gregorian 2026 → Buddhist 2569 via `java.sql.Date.valueOf()` — fixed with `-Duser.language=en -Duser.country=US` JVM args + `NLS_CALENDAR=GREGORIAN` session alter. (2) JayDeBeApi's `Date()`/`Timestamp()` PEP-249 factories return strings (not Java objects) so ISO date strings must be converted via `jpype.JClass("java.sql.Date").valueOf(...)`. Phase 5 architecture revised: DAGs call the API over HTTP; Airflow image stays Java-free.
- **2026-04-18 — Phase 5 end-to-end live.** Airflow stack up on `localhost:8088` (8080/8081 both taken). Full extract pipeline verified for 2026-04-15: 20 / 20 / 216 / 2 rows into the four Supabase-sourced STG tables. Three new-to-project gotchas hit and captured: (1) supabase's `db.<ref>.supabase.co` is IPv6-only — enabled `enable_ipv6: true` on the compose network with an ULA subnet. (2) `ORACLE_API_URL` from repo `.env` uses `localhost` which inside a container is the container itself — overridden to `http://host.docker.internal:8000` in compose `environment:`. (3) psycopg2 returns DECIMAL as Python `Decimal` which `requests`'s JSON encoder rejects — `_oracle_api.as_iso` now converts Decimal → float. Remaining Phase-5 items (SP-loading DAG, scheduled unpause) depend on Phase 6.
- **2026-04-18 — Phase 5 Influx DAG live too.** NodeRED on AWS (`13.213.1.152:8086`) is writing into bucket `iiot_data_raw` / measurement `station_1` — different names from CLAUDE.md §5 placeholders. Code updated to match actual deployment (not the spec). Aggregation DAG pulled 3 rows into `STG_SENSOR_AGG` (one per machine) with sensor values in expected normal ranges. Gotcha: `airflow tasks test` with a past execution_date computes `data_interval` 8 h earlier — before NodeRED existed — so added `INFLUX_RANGE_START` env override to pull `-15m` for ad-hoc smoke tests.
- **2026-04-18 — Phase 6 core live.** `FN_CALC_OEE` + 4 fact-loader SPs written, applied, and called end-to-end for 2026-04-17: FACT_OEE (3 rows with correct A/P/Q/OEE math), FACT_QUALITY (7), FACT_MAINTENANCE (2), FACT_PRODUCTION (7). `sp_load_dw` DAG wires all four as parallel tasks after a healthcheck gate, schedule `30 6,14,22 * * *`. New Oracle 10g gotcha: `SEQ.NEXTVAL` is forbidden inside `INSERT ... SELECT` (ORA-02287) — fact-loader for production uses a cursor-loop + row-by-row INSERT instead. Outstanding: [B]-4 reporting queries, [B]-5 truncate+reload, FACT_INVENTORY (needs inventory/material_consumption extract DAG).
- **2026-04-18 — Phase 6 complete.** Shipped the remaining three items: [B]-4 five reporting queries (3 of 5 live-verified, 2 need richer data), [B]-5 truncate+reload script (with sequence drop+recreate because Oracle 10g lacks `ALTER SEQUENCE RESTART`), and full FACT_INVENTORY pipeline (2 new STG tables, 2 new Airflow extract tasks, `SP_LOAD_FACT_INVENTORY`, wired into `sp_load_dw`). Live-verified 5 inventory rows for 2026-04-17 with opening/consumed/closing derivations.
- **2026-04-18 — Phase 7 up.** Added 6 dashboard GET endpoints to the existing FastAPI service (no new process), all live-tested. Streamlit dashboard ([app/streamlit/dashboard.py](../app/streamlit/dashboard.py)) launched on `:8501`: date picker, 4 KPI cards (OEE/A/P/Q averages), per-machine bar + table, and 4 tabs (quality / maintenance / inventory / weekly trend). Dashboard code parses clean, `/_stcore/health` = ok, but rendered UI is browser-only and not automation-verified.
