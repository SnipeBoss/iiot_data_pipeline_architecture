# NEW_PLAN — Process Performance Dashboard (ตาม NEW_ARCHITECTURE.md)

> Plan การ migrate จาก architecture เก่า (17 OLTP + 7 STG + 5 DIM + 5 FACT) →
> architecture ใหม่ที่เรียบง่ายกว่าเดิม (6 OLTP + 3 STG + 4 DIM + 3 FACT)
>
> **หลักการ:** ลบของเก่าให้หมดก่อน แล้วค่อยสร้างใหม่ — ไม่ migrate ไม่ alter
>
> **Last updated:** 2026-04-19

---

## 0. สรุปการเปลี่ยนแปลง

### 0.1 สิ่งที่หายไป (Drop ทั้งหมด)

**OLTP (Supabase) — ลบ 11 ตาราง:**
- `process_stage`, `raw_material`, `bill_of_material`, `supplier`
- `raw_material_po`, `raw_material_receipt`, `inventory`
- `finished_good`, `material_consumption`
- `qc_inspection`, `qc_result`, `maintenance_log`

**Oracle DW — ลบ 9 ตาราง:**
- STG: `STG_QC_INSPECTION`, `STG_QC_RESULT`, `STG_MAINTENANCE_LOG`, `STG_INVENTORY`, `STG_MATERIAL_CONSUMPTION`
- DIM: `DIM_STAGE`, `DIM_MATERIAL`
- FACT: `FACT_OEE`, `FACT_INVENTORY`, `FACT_MAINTENANCE`

### 0.2 สิ่งที่เปลี่ยนรูป (Rewrite)

| Entity | เก่า | ใหม่ |
|---|---|---|
| `production_batch` | มี stage_id | ไม่มี stage_id (1 batch = full line run) |
| `STG_PRODUCTION_BATCH` | per-stage | per-batch |
| `STG_SENSOR_AGG` | 8h window | 15-min window, 6 metrics × 3 machines |
| `FACT_PRODUCTION` | machine-level | batch-level (no machine_id) |
| `FACT_QUALITY` | stage-level | batch-level (end-of-line QC) |

### 0.3 สิ่งที่เพิ่มใหม่ (New)

- OLTP: `qc_record` (replace qc_inspection + qc_result)
- DW: `DIM_METRIC` (catalog ของ sensor metrics)
- DW: `FACT_SENSOR` (15-min aggregate per machine × metric)

### 0.4 Key Design Decisions

- **Oracle 10g compatibility:** ห้ามใช้ `GENERATED ALWAYS AS IDENTITY` (10g ไม่รองรับ) → ใช้ SEQUENCE
- **Schedule:** 15-min update (เปลี่ยนจาก 8h)
- **No OEE formula:** เปลี่ยนเป็น Production + Quality + Sensor Parameters (ตาม dashboard ใหม่)
- **Spaghetti cleanup:** rename ไฟล์ + ย้าย subdir + ลบ dead code

---

## 1. Execution Phases

แต่ละเฟสมี **Exit Criterion (✅ ทดสอบผ่าน) → มี** ถึงจะขยับเฟสถัดไป

### Phase 0 — Cleanup เก่า (destructive, ทำก่อนอื่นทั้งหมด)

**เป้าหมาย:** ลบ object และ data เก่าทิ้งทั้งหมดทั้งฝั่ง Supabase และ Oracle AI03 เพื่อให้ schema เก่าไม่ชน/ไม่ confuse schema ใหม่

**Tasks:**
- [ ] 0.1 Drop ตาราง OLTP 17 ตัว (+ SEQUENCE ที่ผูก SERIAL) บน Supabase → สะอาดเปล่า
- [ ] 0.2 Drop ตาราง DW 17 ตัว (5 STG + 5 DIM + 5 FACT + 2 inventory STG) + 9 SEQUENCE + 5 PROCEDURE + 1 FUNCTION บน Oracle AI03
- [ ] 0.3 Verify: `user_tables` / `user_sequences` / `user_procedures` ของ AI03 ว่าง (หรือเหลือแค่ object ที่ไม่เกี่ยวกับโปรเจกต์นี้)

**Test:**
```bash
# Oracle
.venv/bin/python -c "from db_module.db_conn import OracleConnector; \
    c = OracleConnector(); \
    with c.cursor() as cur: \
        cur.execute(\"SELECT object_name, object_type FROM user_objects WHERE object_name LIKE 'STG_%' OR object_name LIKE 'DIM_%' OR object_name LIKE 'FACT_%' OR object_name LIKE 'SEQ_%' OR object_name LIKE 'SP_%' OR object_name LIKE 'FN_%'\"); \
        rows = cur.fetchall(); \
        assert len(rows) == 0, f'stragglers: {rows}'; \
        print('AI03 clean')"

# Supabase — ควรเหลือ 0 ตารางจากโปรเจกต์ (ยกเว้น supabase internal tables)
```

**Deliverable:** สคริปต์ `db_module/db_sources/cleanup_legacy.py` (ทำครั้งเดียว ไม่ commit ถาวร)

---

### Phase 1 — OLTP Supabase (6 ตาราง)

**Tasks:**
- [ ] 1.1 Rewrite `db_module/db_sources/supabases_sql_query/query/01_schema.sql` — 6 ตาราง (production_line, machine, product, production_order, production_batch, qc_record) + 7 index สำหรับ ETL extract
- [ ] 1.2 Rewrite `02_master_data.sql` — 1 line, 3 machines (M01/M02/M03), 3 products
- [ ] 1.3 Rewrite `mock/generate_mock_data.py` — ลบ logic สำหรับ material/stage/maintenance ออก; generate แค่ order + batch + qc
- [ ] 1.4 Run generator → `03_mock_data.sql` ใหม่
- [ ] 1.5 Apply ทั้ง 3 ไฟล์ไป Supabase ผ่าน `apply_supabase.py`

**Test:**
```bash
.venv/bin/python db_module/db_sources/supabases_sql_query/apply_supabase.py
# Expect:
#   [apply] query/01_schema.sql (~2 KB)
#   [apply] query/02_master_data.sql
#   [apply] mock/03_mock_data.sql
#   Row counts:
#     production_line   1
#     machine           3
#     product           3
#     production_order  N (mock-dependent)
#     production_batch  N
#     qc_record         N (= batch count ที่ end_time IS NOT NULL)
```

**Exit Criterion:**
- ✅ Supabase มีตารางใหม่ 6 ตัว
- ✅ Row count ของ machine = 3 (M01, M02, M03)
- ✅ `production_batch.end_time` มีค่าในบาง row (เพื่อ ETL extract ได้)

---

### Phase 2 — Oracle DW (3 STG + 4 DIM + 3 FACT)

**Tasks:**
- [ ] 2.1 Rewrite `db_module/db_sources/oracle_sql_query/query/01_schema.sql` — ตาม NEW_ARCHITECTURE.md **แต่ใช้ SEQUENCE ไม่ใช่ IDENTITY** (10g compatibility)
  - 4 DIM: DIM_DATE, DIM_MACHINE, DIM_PRODUCT, DIM_METRIC (มี master data 6 rows)
  - 3 FACT: FACT_PRODUCTION, FACT_QUALITY, FACT_SENSOR
  - 3 STG: STG_PRODUCTION_BATCH, STG_QC_RECORD, STG_SENSOR_AGG (มี src_system/pipeline_run_id/loaded_at)
  - Sequences: SEQ_FACT_PRODUCTION, SEQ_FACT_QUALITY, SEQ_FACT_SENSOR (DIM_* ไม่ต้อง ถ้า seed มี explicit id), SEQ_DIM_MACHINE, SEQ_DIM_PRODUCT
- [ ] 2.2 Keep/rewrite `02_procedure_dim_date.sql` — logic เดิมใช้ได้
- [ ] 2.3 Rewrite `03_procedure_fact_loaders.sql` — 3 SP ใหม่:
  - `SP_LOAD_FACT_PRODUCTION(p_date)` — อ่าน STG_PRODUCTION_BATCH → คำนวณ yield_rate, duration_min
  - `SP_LOAD_FACT_QUALITY(p_date)` — อ่าน STG_QC_RECORD → คำนวณ defect_rate_pct
  - `SP_LOAD_FACT_SENSOR(p_date)` — อ่าน STG_SENSOR_AGG → map machine_name/metric_name → machine_id/metric_id
- [ ] 2.4 Rewrite `04_reporting_queries.sql` — 4 query ใหม่ ตรงกับ dashboard RQ:
  - Q1: Production ราย 15 นาที (batch level + overall)
  - Q2: Defect rate
  - Q3: Sensor parameter ต่อ batch ราย 15 นาที (JOIN FACT_SENSOR + FACT_PRODUCTION by time window)
  - Q4: Production ต่อ machine (group by machine + 15-min window)
- [ ] 2.5 Rewrite `05_truncate_and_reload.sql`
- [ ] 2.6 Delete `06_inventory_pipeline.sql` (file removal)
- [ ] 2.7 Apply ทั้ง 5 ไฟล์ไป Oracle AI03 ด้วย `run_sql_file.py` (ไฟล์ที่จะถูก rename จาก `apply_ddl.py` ใน Phase 3)
- [ ] 2.8 Seed DIM_DATE: `SP_LOAD_DIM_DATE(DATE '2026-01-01', 730)` (2 ปี พอสำหรับ POC)

**Test:**
```bash
# Apply
.venv/bin/python db_module/db_sources/oracle_sql_query/run_sql_file.py db_module/db_sources/oracle_sql_query/query/01_schema.sql
.venv/bin/python db_module/db_sources/oracle_sql_query/run_sql_file.py db_module/db_sources/oracle_sql_query/query/02_procedure_dim_date.sql
.venv/bin/python db_module/db_sources/oracle_sql_query/run_sql_file.py db_module/db_sources/oracle_sql_query/query/03_procedure_fact_loaders.sql

# Verify
.venv/bin/python db_module/db_sources/oracle_sql_query/verify_warehouse_schema.py
# Expect:
#   10 tables ✓ (3 STG + 4 DIM + 3 FACT)
#   5 sequences ✓
#   4 procedures ✓ (SP_LOAD_DIM_DATE + 3 fact loaders)

# Populate DIM_DATE
.venv/bin/python -c "from db_module.db_conn import OracleConnector; \
    c = OracleConnector(); \
    with c.cursor() as cur: \
        cur.execute(\"BEGIN SP_LOAD_DIM_DATE(DATE '2026-01-01', 730); END;\"); \
        cur.execute('SELECT COUNT(*) FROM DIM_DATE'); \
        n = cur.fetchone()[0]; \
        print(f'DIM_DATE rows: {n}'); \
        assert n == 730"

# DIM_METRIC seed check
.venv/bin/python -c "from db_module.db_conn import OracleConnector; \
    c = OracleConnector(); \
    with c.cursor() as cur: \
        cur.execute('SELECT COUNT(*) FROM DIM_METRIC'); \
        n = cur.fetchone()[0]; \
        assert n == 6, f'expected 6 metrics, got {n}'; \
        print(f'DIM_METRIC rows: {n}')"
```

**Exit Criterion:**
- ✅ 10 ตาราง + 5 sequence + 4 procedure อยู่ใน AI03
- ✅ DIM_DATE มี 730 row (2 ปี)
- ✅ DIM_METRIC มี 6 row (temperature_c / machine_state_num / cycle_count / vibration_g / current_a / voltage_v)

---

### Phase 3 — Restructure + Rename `oracle_sql_query/`

**Tasks:**
- [ ] 3.1 Move 5 SQL ไป `oracle_sql_query/query/` (`01_schema.sql`, `02_procedure_dim_date.sql`, `03_procedure_fact_loaders.sql`, `04_reporting_queries.sql`, `05_truncate_and_reload.sql`)
- [ ] 3.2 Rename `apply_ddl.py` → `run_sql_file.py`
- [ ] 3.3 Rename `seed_dims.py` → `sync_dimensions_from_supabase.py` (และอัพเดต logic: DIM_MACHINE, DIM_PRODUCT เท่านั้น — ลบ DIM_STAGE, DIM_MATERIAL)
- [ ] 3.4 Rename `verify_dw.py` → `verify_warehouse_schema.py` (update EXPECTED_TABLES/SEQUENCES/PROCEDURES list)
- [ ] 3.5 Create `oracle_sql_query/mock/seed_stg_mock.py` — insert mock ตรงเข้า STG (ไม่ต้องรัน ETL):
  - 10 row `STG_PRODUCTION_BATCH` (mock batch ต่อวัน 2 วัน)
  - 10 row `STG_QC_RECORD` (1:1 กับ batch)
  - 288 row `STG_SENSOR_AGG` (96 windows × 3 machines — แต่จำกัดเฉพาะ metric ที่ตรงเครื่อง รวม ~96 × (1+1+1+2+1+1) = ขึ้นอยู่กับ metric mapping; target ~576 rows สำหรับ 2 วัน)
- [ ] 3.6 Delete old files: `apply_ddl.py`, `seed_dims.py`, `verify_dw.py`, `06_inventory_pipeline.sql`

**Test:**
```bash
# Scripts still work under new names
.venv/bin/python db_module/db_sources/oracle_sql_query/verify_warehouse_schema.py
.venv/bin/python db_module/db_sources/oracle_sql_query/sync_dimensions_from_supabase.py
# Expect:
#   DIM_MACHINE   loaded 3 rows
#   DIM_PRODUCT   loaded 3 rows

# Mock STG seed
.venv/bin/python db_module/db_sources/oracle_sql_query/mock/seed_stg_mock.py
.venv/bin/python -c "from db_module.db_conn import OracleConnector; \
    c = OracleConnector(); \
    with c.cursor() as cur: \
        for t in ['STG_PRODUCTION_BATCH', 'STG_QC_RECORD', 'STG_SENSOR_AGG']: \
            cur.execute(f'SELECT COUNT(*) FROM {t}'); \
            print(f'{t}: {cur.fetchone()[0]}')"

# Directory structure
ls db_module/db_sources/oracle_sql_query/
# Expect:
#   query/  (5 SQL files)
#   mock/   (1 Python file)
#   run_sql_file.py
#   sync_dimensions_from_supabase.py
#   verify_warehouse_schema.py
# NOT expect:
#   apply_ddl.py, seed_dims.py, verify_dw.py, 06_inventory_pipeline.sql
```

**Exit Criterion:**
- ✅ ไฟล์เก่าถูกลบหมด
- ✅ โครงสร้างใหม่ตรงกับ Supabase pattern (query/, mock/)
- ✅ `verify_warehouse_schema.py` pass
- ✅ `sync_dimensions_from_supabase.py` pass (DIM_MACHINE/DIM_PRODUCT มีข้อมูล)

---

### Phase 4 — Cleanup Dead Code ใน Connector

**Tasks:**
- [ ] 4.1 ลบ `InfluxConnector.query_records()` — ไม่มีใครเรียก
- [ ] 4.2 ลบ `InfluxConnector.write()` — ไม่มีใครเรียก
- [ ] 4.3 ลบ `SYNCHRONOUS` import ที่ไม่ใช้แล้ว
- [ ] 4.4 ลบ `Sequence` typing import ที่ไม่ใช้แล้ว

**Test:**
```bash
.venv/bin/python -m pytest test/test_connectors.py -v
# Expect: all tests pass (or skip cleanly if env missing)

.venv/bin/python -c "from db_module.db_conn import InfluxConnector; \
    methods = [m for m in dir(InfluxConnector) if not m.startswith('_')]; \
    assert sorted(methods) == ['client', 'query'], f'got {methods}'; \
    print('InfluxConnector API:', methods)"
```

**Exit Criterion:**
- ✅ InfluxConnector เหลือแค่ `client()` + `query()` + constructor
- ✅ pytest pass

---

### Phase 5 — Airflow DAGs (3 DAGs)

**Tasks:**
- [ ] 5.1 Rewrite `etl_supabase_to_oracle.py` — extract 2 ตารางแค่ `production_batch` + `qc_record` (ลบ 4 tasks เก่า: qc_inspection / qc_result / maintenance_log / inventory / material_consumption), schedule `*/15 * * * *`
- [ ] 5.2 Rewrite `etl_influxdb_to_oracle.py` — schedule `*/15 * * * *`, aggregate 15-min window, emit 18 rows (6 metrics × 3 machines) ต่อรอบ
- [ ] 5.3 Rewrite `sp_load_dw.py` — chain 3 SPs (production, quality, sensor), schedule `5/15 * * * *` (offset 5 นาทีหลัง extract)
- [ ] 5.4 Update `_oracle_api.py` helper ถ้าจำเป็น (น่าจะใช้ร่วมได้)
- [ ] 5.5 Update `_supabase.py` helper ถ้าจำเป็น

**Test:**
```bash
# DAG parse (ไม่รันจริง)
cd db_module/pipeline
docker compose up -d
docker compose exec scheduler airflow dags list | grep -E "etl_supabase|etl_influx|sp_load"
docker compose exec scheduler airflow dags list-import-errors
# Expect: 3 DAGs, 0 import errors

# Test tasks (single run)
docker compose exec scheduler airflow tasks test etl_supabase_to_oracle extract_production_batch 2026-04-19
docker compose exec scheduler airflow tasks test etl_supabase_to_oracle extract_qc_record 2026-04-19
docker compose exec scheduler airflow tasks test etl_influxdb_to_oracle aggregate_sensor 2026-04-19
docker compose exec scheduler airflow tasks test sp_load_dw sp_load_fact_production 2026-04-19
docker compose exec scheduler airflow tasks test sp_load_dw sp_load_fact_quality 2026-04-19
docker compose exec scheduler airflow tasks test sp_load_dw sp_load_fact_sensor 2026-04-19

# Verify counts
.venv/bin/python -c "from db_module.db_conn import OracleConnector; \
    c = OracleConnector(); \
    with c.cursor() as cur: \
        for t in ['STG_PRODUCTION_BATCH', 'STG_QC_RECORD', 'STG_SENSOR_AGG', \
                  'FACT_PRODUCTION', 'FACT_QUALITY', 'FACT_SENSOR']: \
            cur.execute(f'SELECT COUNT(*) FROM {t}'); \
            print(f'{t:<25} {cur.fetchone()[0]}')"
```

**Exit Criterion:**
- ✅ 3 DAGs parse ไม่มี error
- ✅ รัน `airflow tasks test` แต่ละ task ผ่าน end-to-end
- ✅ STG + FACT มีข้อมูลจริงจาก live extract

---

### Phase 6 — FastAPI Endpoints

**Tasks:**
- [ ] 6.1 Keep operational endpoints: `/health`, `/sql/query`, `/sql/execute`, `/sp/call`, `/sql/bulk-insert`
- [ ] 6.2 ลบ endpoint เก่า: `/api/oee/*`, `/api/maintenance/mtbf-mttr`, `/api/inventory/latest` (architecture ใหม่ไม่ใช้)
- [ ] 6.3 เพิ่ม endpoint ใหม่ตาม dashboard RQ:
  - `GET /api/production/by-batch?date=YYYY-MM-DD` — list ทุก batch + yield_rate
  - `GET /api/production/per-machine-15min?date=YYYY-MM-DD` — aggregate ต่อเครื่อง ต่อ 15-min window
  - `GET /api/quality/defect-rate?date=YYYY-MM-DD` — aggregate defect rate
  - `GET /api/sensor/by-batch?batch_src_id=N` — sensor param ของ batch นั้นราย 15 นาที
  - `GET /api/sensor/available-metrics` — list DIM_METRIC
  - `GET /api/production/available-dates`

**Test:**
```bash
# Start API
.venv/bin/uvicorn app.api.main:app --host 0.0.0.0 --port 8000 &

# Test each endpoint
TOKEN=$(grep ORACLE_API_TOKEN .env | cut -d= -f2)
curl -s http://localhost:8000/health | jq
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/production/available-dates" | jq
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/production/by-batch?date=2026-04-19" | jq '.rows | length'
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/quality/defect-rate?date=2026-04-19" | jq
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/sensor/available-metrics" | jq '.rows | length'   # expect 6
```

**Exit Criterion:**
- ✅ ทุก endpoint return 200 + JSON ที่ shape ถูกต้อง
- ✅ ไม่มี 500 error
- ✅ `/api/sensor/available-metrics` คืน 6 metrics

---

### Phase 7 — Streamlit Dashboard

**Tasks:**
- [ ] 7.1 Rewrite `app/streamlit/dashboard.py` — layout ใหม่ตาม GM requirement:
  - Sidebar: date picker + batch selector + auto-refresh (15 นาที)
  - Tab 1: **Production Overview** — KPI (total qty_out, total batches, avg yield rate) + bar chart ต่อ machine (15-min window) + table batch-level
  - Tab 2: **Quality** — defect rate overall + per-batch list + fail reason (ถ้ามี)
  - Tab 3: **Machine Parameters** — เลือก batch → line chart ของ sensor metric ต่อ 15-min window
  - Tab 4: **Raw 15-min Sensor** — DataFrame ของ FACT_SENSOR สำหรับวันที่เลือก
- [ ] 7.2 ใช้ `st.cache_data(ttl=300)` (5 นาที) บน fetch function

**Test:**
```bash
# Launch
.venv/bin/streamlit run app/streamlit/dashboard.py --server.headless true &
sleep 3

# Health check
curl -s http://localhost:8501/_stcore/health
# Expect: ok

# Manual browser test — open http://localhost:8501
```

**Exit Criterion:**
- ✅ `/_stcore/health` = ok
- ✅ Python parse clean (no import errors)
- ✅ Manual: ทุก tab render ได้ไม่ error

---

### Phase 8 — Tests

**Tasks:**
- [ ] 8.1 Update `test/test_connectors.py` — ยังเหมือนเดิม (connector API ไม่เปลี่ยน)
- [ ] 8.2 Update `test/test_connection.py` — (probe-style, น่าจะไม่ต้องแก้)
- [ ] 8.3 Update `test/test_create_table.py` — เปลี่ยน test table name ไม่ชน object ใหม่

**Test:**
```bash
.venv/bin/python -m pytest test/ -v
# Expect: all pass
```

**Exit Criterion:**
- ✅ pytest green

---

### Phase 9 — Update Documents

**Tasks:**
- [ ] 9.1 Update `claude_track/PLAN.md` — พิมพ์ section Phase 8 (new arch migration complete) + update link ไฟล์
- [ ] 9.2 Update `claude_track/problems_requirements_erd_mapping.md` — แก้ OLTP ER (6 ตาราง) + DW ER (4 DIM + 3 FACT + 3 STG) + ตัด section material/OEE ออก
- [ ] 9.3 Update `README.md` — refresh diagram + table counts
- [ ] 9.4 Update `CLAUDE.md` ถ้ามีส่วนที่ reference schema เก่า

**Test:** manual review

**Exit Criterion:**
- ✅ link ทุกไฟล์ใน doc ชี้ path ใหม่
- ✅ ไม่มีอ้างอิง material/OEE/maintenance ใน doc ใหม่

---

### Phase 10 — Memory + Final Cleanup

**Tasks:**
- [ ] 10.1 เขียน memory file `project_new_architecture.md` สรุป key decisions ของ architecture ใหม่
- [ ] 10.2 Update/remove memory เก่าที่ขัด (เช่น ถ้ามี memory อ้าง DIM_MATERIAL)
- [ ] 10.3 `git status` ตรวจไฟล์ที่ต้องลบ/commit
- [ ] 10.4 ลบ `db_module/db_sources/cleanup_legacy.py` (ใช้ครั้งเดียว)

**Exit Criterion:**
- ✅ Memory update แล้ว
- ✅ Git clean (ยกเว้นการ commit ที่ user ต้องการ)

---

## 2. Risk & Rollback

### 2.1 ความเสี่ยง

| # | Risk | Mitigation |
|---|---|---|
| R1 | AI03 มี object ค้างจาก schema เก่า ทำให้ apply ใหม่ fail | Phase 0 cleanup script ทำ drop แบบ safe (ใช้ EXECUTE IMMEDIATE + IGNORE ORA-00942) |
| R2 | Supabase cascade FK ของเก่าไม่ยอม drop | ต้อง drop FK ก่อนเสมอ (CASCADE) |
| R3 | 10g ไม่รองรับ `IDENTITY` ตาม NEW_ARCHITECTURE.md | แปลงเป็น SEQUENCE ทุกตัว |
| R4 | Airflow DAG schedule 15 นาที ทำให้ load Supabase หนัก | LIMIT extract ที่ data_interval + index ที่ `end_time` |
| R5 | Mock data ไม่ครอบคลุม 3 เครื่อง × 6 metric ทำให้ FACT_SENSOR ไม่สมบูรณ์ | Mock generator ใน Phase 3.5 (`seed_stg_mock.py`) ต้อง seed ครบทุก combo |

### 2.2 Rollback Strategy

- **ก่อน Phase 0:** commit git (tag `pre-new-arch`) — สะดวก revert
- **หลัง Phase 0 (schema เก่าถูกลบ):** ไม่มี rollback ง่าย ๆ เพราะ data เก่าหาย
  - ถ้าเฟสกลางทางล้ม ต้อง run `db_module/db_sources/cleanup_legacy.py` อีกรอบแล้วเริ่ม Phase 1 ใหม่
- **Live Oracle:** KMITL AI03 ไม่มี backup → ยอมรับว่า drop = หายถาวร

---

## 3. Deliverables

เมื่อครบ 10 phase จะได้:

**ไฟล์ใหม่ / เปลี่ยน:**
- `db_module/db_sources/supabases_sql_query/query/01_schema.sql` (6 ตาราง)
- `db_module/db_sources/supabases_sql_query/query/02_master_data.sql`
- `db_module/db_sources/supabases_sql_query/mock/generate_mock_data.py`
- `db_module/db_sources/supabases_sql_query/mock/03_mock_data.sql`
- `db_module/db_sources/oracle_sql_query/query/01_schema.sql` (10 object)
- `db_module/db_sources/oracle_sql_query/query/02_procedure_dim_date.sql`
- `db_module/db_sources/oracle_sql_query/query/03_procedure_fact_loaders.sql`
- `db_module/db_sources/oracle_sql_query/query/04_reporting_queries.sql`
- `db_module/db_sources/oracle_sql_query/query/05_truncate_and_reload.sql`
- `db_module/db_sources/oracle_sql_query/mock/seed_stg_mock.py` (NEW)
- `db_module/db_sources/oracle_sql_query/run_sql_file.py` (renamed)
- `db_module/db_sources/oracle_sql_query/sync_dimensions_from_supabase.py` (renamed)
- `db_module/db_sources/oracle_sql_query/verify_warehouse_schema.py` (renamed)
- `db_module/db_conn/influxdb/influx_connection.py` (slim)
- `db_module/pipeline/airflow/dags/etl_supabase_to_oracle.py`
- `db_module/pipeline/airflow/dags/etl_influxdb_to_oracle.py`
- `db_module/pipeline/airflow/dags/sp_load_dw.py`
- `app/api/main.py`
- `app/streamlit/dashboard.py`

**ไฟล์ที่ลบ:**
- `db_module/db_sources/oracle_sql_query/apply_ddl.py`
- `db_module/db_sources/oracle_sql_query/seed_dims.py`
- `db_module/db_sources/oracle_sql_query/verify_dw.py`
- `db_module/db_sources/oracle_sql_query/01_dw_ddl.sql`
- `db_module/db_sources/oracle_sql_query/02_sp_dim_date.sql`
- `db_module/db_sources/oracle_sql_query/03_sp_fact_loaders.sql`
- `db_module/db_sources/oracle_sql_query/04_reporting_queries.sql`
- `db_module/db_sources/oracle_sql_query/05_truncate_and_reload.sql`
- `db_module/db_sources/oracle_sql_query/06_inventory_pipeline.sql`

**Doc update:**
- `claude_track/PLAN.md`
- `claude_track/problems_requirements_erd_mapping.md`
- `README.md`

---

## 4. Execution Checklist

ทำตามลำดับนี้เป๊ะ ๆ:

1. [ ] Phase 0 cleanup (destructive)
2. [ ] Phase 1 OLTP (Supabase rewrite + apply)
3. [ ] Phase 2 Oracle DW (rewrite + apply + verify)
4. [ ] Phase 3 Restructure oracle_sql_query/ (rename + move + mock populator)
5. [ ] Phase 4 InfluxConnector dead code
6. [ ] Phase 5 Airflow DAGs (3 DAGs end-to-end verify)
7. [ ] Phase 6 FastAPI endpoints (curl test all)
8. [ ] Phase 7 Streamlit dashboard (health check + parse)
9. [ ] Phase 8 Tests (pytest green)
10. [ ] Phase 9 Docs update
11. [ ] Phase 10 Memory + git cleanup

ถ้าติดเฟสใด **หยุด** และรายงาน blocker ก่อนจะข้ามไปเฟสถัดไป
