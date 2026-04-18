from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from _oracle_api import bulk_insert, health
from _supabase import supabase_cursor


"""Extract Supabase OLTP → Oracle STG ตาม schema ใหม่ (NEW_ARCHITECTURE)

Architecture:
  Supabase ──psycopg2──▶ [DAG] ──HTTP bulk-insert──▶ Oracle API ──JDBC──▶ AI03 STG

Tasks:
  - extract_production_batch — ดึง batch ที่เสร็จ (end_time IS NOT NULL) ใน 15-min window
  - extract_qc_record       — ดึง qc_record ที่ inspected ใน window

แต่ละ task เป็น TRUNCATE + INSERT (idempotent) แต่ truncate แค่ rows ของ
pipeline_run_id เดิม? → ไม่ใช่ truncate ทั้งตาราง แค่ delete ตาม window

Schedule: ทุก 15 นาที (*/15 * * * *) ตาม NEW_ARCHITECTURE RQ
"""


log = logging.getLogger(__name__)


def _extract(sql: str, params: tuple) -> list[list]:
    with supabase_cursor() as cur:
        cur.execute(sql, params)
        return [list(row) for row in cur.fetchall()]


def check_oracle_api(**_) -> None:
    """Short-circuit DAG ถ้า Oracle API ไม่ตอบ"""
    info = health()
    log.info("oracle-api up: user=%s sysdate=%s",
             info.get("oracle_user"), info.get("oracle_sysdate"))


def load_production_batch(**ctx) -> None:
    """ดึง batch ที่ end_time อยู่ใน 15-min window ของ execution interval"""
    run_id = ctx["run_id"]
    start = ctx["data_interval_start"]
    end   = ctx["data_interval_end"]

    rows = _extract(
        """
        SELECT batch_id, order_id,
               (SELECT product_id FROM production_order po WHERE po.order_id = pb.order_id) AS product_id,
               qty_planned, qty_out, start_time, end_time
        FROM production_batch pb
        WHERE end_time IS NOT NULL
          AND end_time >= %s
          AND end_time <  %s
        """,
        (start, end),
    )
    payload = [r + ["SUPABASE", run_id] for r in rows]
    log.info("extracted %d production_batch rows for %s → %s", len(rows), start, end)
    bulk_insert(
        "STG_PRODUCTION_BATCH",
        columns=["batch_id", "order_id", "product_id",
                 "qty_planned", "qty_out", "start_time", "end_time",
                 "src_system", "pipeline_run_id"],
        rows=payload,
        truncate=True,
    )


def load_qc_record(**ctx) -> None:
    """ดึง qc_record ที่ inspected_at อยู่ใน window"""
    run_id = ctx["run_id"]
    start = ctx["data_interval_start"]
    end   = ctx["data_interval_end"]

    rows = _extract(
        """
        SELECT qc_id, batch_id, qty_sampled, qty_passed, qty_failed, inspected_at
        FROM qc_record
        WHERE inspected_at >= %s
          AND inspected_at <  %s
        """,
        (start, end),
    )
    payload = [r + ["SUPABASE", run_id] for r in rows]
    log.info("extracted %d qc_record rows for %s → %s", len(rows), start, end)
    bulk_insert(
        "STG_QC_RECORD",
        columns=["qc_id", "batch_id", "qty_sampled", "qty_passed", "qty_failed",
                 "inspected_at", "src_system", "pipeline_run_id"],
        rows=payload,
        truncate=True,
    )


default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="etl_supabase_to_oracle",
    description="Supabase OLTP -> Oracle AI03 STG, every 15 min.",
    default_args=default_args,
    schedule="*/15 * * * *",
    start_date=datetime(2026, 4, 18),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "supabase", "oracle"],
) as dag:

    healthcheck = PythonOperator(
        task_id="check_oracle_api",
        python_callable=check_oracle_api,
    )

    extract_batch = PythonOperator(
        task_id="extract_production_batch",
        python_callable=load_production_batch,
    )

    extract_qc = PythonOperator(
        task_id="extract_qc_record",
        python_callable=load_qc_record,
    )

    healthcheck >> [extract_batch, extract_qc]
