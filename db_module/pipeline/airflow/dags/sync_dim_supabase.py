from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from _oracle_api import bulk_insert, call_sp, health
from _supabase import supabase_cursor


"""Nightly DIM sync DAG (NEW 2026-04-26)

Sync master data จาก Supabase OLTP → Oracle DIM_* (LINE / BATTERY_MODEL / MACHINE)

Pattern:
    Supabase ──psycopg2──▶ STG_LINE/BATTERY_MODEL/MACHINE (truncate-and-load) ──▶ SP_SYNC_ALL_DIMS (MERGE BY src_id → DIM_*)

ทำไม MERGE pattern: เก็บ surrogate key เสถียรข้าม sync — FACT FK ที่ชี้
DIM surrogate ไม่ orphan เมื่อ sync ครั้งหน้า

Schedule: 02:00 ทุกวัน (low traffic) — DIM ไม่เปลี่ยนบ่อย, sync 1 ครั้งต่อวันพอ
ถ้าต้องการ urgent sync (เช่น เพิ่มเครื่องใหม่) → trigger manual ใน Airflow UI

Pre-condition สำหรับ FACT load:
    DIM_LINE / DIM_BATTERY_MODEL / DIM_MACHINE ต้องมีข้อมูลก่อน FACT load
    ไม่งั้น SP_LOAD_FACT_PRODUCTION จะ throw NO_DATA_FOUND ตอน lookup
    → ต้อง trigger DAG นี้ manually 1 ครั้งหลัง deploy ใหม่
"""


log = logging.getLogger(__name__)


def check_oracle_api(**_) -> None:
    info = health()
    log.info("oracle-api up: user=%s", info.get("oracle_user"))


def _extract(sql: str) -> list[list]:
    """
    ดึงทุก row จาก Supabase (ไม่ filter time — DIM = master data)
    """
    with supabase_cursor() as cur:
        cur.execute(sql)
        return [list(row) for row in cur.fetchall()]


def load_stg_line(**ctx) -> None:
    """
    Extract production_line → STG_LINE
    """
    run_id = ctx["run_id"]
    rows = _extract("""
        SELECT line_id, name, area FROM production_line ORDER BY line_id
    """)
    payload = [r + ["SUPABASE", run_id] for r in rows]
    log.info("extracted %d production_line rows", len(rows))
    bulk_insert(
        "STG_LINE",
        columns=["line_id",
                 "name",
                 "area",
                 "src_system",
                 "pipeline_run_id"],
        rows=payload,
        truncate=True,
    )


def load_stg_battery_model(**ctx) -> None:
    """Extract battery_model → STG_BATTERY_MODEL

    is_active เป็น CHAR(1) Y/N ทั้ง 2 ฝั่ง — ไม่ต้อง convert
    """
    run_id = ctx["run_id"]
    rows = _extract("""
        SELECT model_id, model_code, name,
               spec_plate_count, spec_weight_kg, spec_terminal_type,
               dim_length_mm, dim_width_mm, dim_height_mm,
               is_active
          FROM battery_model
         ORDER BY model_id
    """)
    payload = [r + ["SUPABASE", run_id] for r in rows]
    log.info("extracted %d battery_model rows", len(rows))
    bulk_insert(
        "STG_BATTERY_MODEL",
        columns=["model_id",
                 "model_code",
                 "name",
                 "spec_plate_count",
                 "spec_weight_kg",
                 "spec_terminal_type",
                 "dim_length_mm",
                 "dim_width_mm",
                 "dim_height_mm",
                 "is_active",
                 "src_system",
                 "pipeline_run_id"],
        rows=payload,
        truncate=True,
    )


def load_stg_machine(**ctx) -> None:
    """Extract machine → STG_MACHINE

    Supabase machine ไม่มี is_active column → set 'Y' default
    """
    run_id = ctx["run_id"]
    rows = _extract("""
        SELECT machine_id, line_id, machine_code, machine_type,
               sequence_position, install_date
          FROM machine
         ORDER BY machine_id
    """)
    # เพิ่ม is_active='Y' (default) + lineage
    payload = [r + ["Y", "SUPABASE", run_id] for r in rows]
    log.info("extracted %d machine rows", len(rows))
    bulk_insert(
        "STG_MACHINE",
        columns=["machine_id",
                 "line_id",
                 "machine_code",
                 "machine_type",
                 "sequence_position",
                 "install_date",
                 "is_active",
                 "src_system",
                 "pipeline_run_id"],
        rows=payload,
        truncate=True,
    )


def run_sp_sync_all_dims(**_) -> None:
    """เรียก SP_SYNC_ALL_DIMS — orchestrator: LINE → BATTERY_MODEL → MACHINE

    ลำดับสำคัญ: DIM_MACHINE.line_id ชี้ DIM_LINE (FK) — LINE ต้อง sync ก่อน
    SP เองทำ MERGE BY src_id → preserve surrogate key
    """
    result = call_sp("SP_SYNC_ALL_DIMS")
    log.info("SP_SYNC_ALL_DIMS → %s", result)


default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="sync_dim_supabase",
    description="Nightly DIM sync: Supabase master data -> Oracle DIM_LINE/BATTERY_MODEL/MACHINE",
    default_args=default_args,
    schedule="0 2 * * *",      # 02:00 UTC ทุกวัน
    start_date=datetime(2026, 4, 18),
    catchup=False,
    max_active_runs=1,
    tags=["sync", "dim", "supabase", "oracle"],
) as dag:

    healthcheck = PythonOperator(
        task_id="check_oracle_api",
        python_callable=check_oracle_api,
    )

    extract_line = PythonOperator(
        task_id="extract_production_line",
        python_callable=load_stg_line,
    )

    extract_model = PythonOperator(
        task_id="extract_battery_model",
        python_callable=load_stg_battery_model,
    )

    extract_machine = PythonOperator(
        task_id="extract_machine",
        python_callable=load_stg_machine,
    )

    sync_dims = PythonOperator(
        task_id="sp_sync_all_dims",
        python_callable=run_sp_sync_all_dims,
    )

    # 3 extracts รัน parallel; ทุกตัวต้องเสร็จก่อน sync_dims
    healthcheck >> [extract_line, extract_model, extract_machine] >> sync_dims
