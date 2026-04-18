"""Extract from Supabase OLTP (cloud Postgres) into Oracle AI03 staging.

Architecture (see claude_track/PLAN.md Phase 5):
  Supabase ──psycopg2─▶  [this DAG]  ──HTTP bulk-insert─▶  Oracle API  ─JDBC─▶ Oracle AI03

Each task is `TRUNCATE + INSERT` on its STG table (idempotent). Task scope is
a single calendar day (`{{ ds }}`) so a rerun for the same execution date
overwrites cleanly.

Schedule: every 8h at 06:00 / 14:00 / 22:00 (CLAUDE.md §6).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Callable

from airflow import DAG
from airflow.operators.python import PythonOperator

from _oracle_api import bulk_insert, health
from _supabase import supabase_cursor

log = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Extract/load helpers. Each returns rows for the given `ds` date.
# -----------------------------------------------------------------------------


def _extract(sql: str, params: tuple) -> list[list]:
    with supabase_cursor() as cur:
        cur.execute(sql, params)
        return [list(row) for row in cur.fetchall()]


def load_production_batch(**ctx) -> None:
    ds = ctx["ds"]
    run_id = ctx["run_id"]
    rows = _extract(
        """
        SELECT batch_id, order_id, line_id, stage_id,
               started_at, completed_at, qty_produced
        FROM production_batch
        WHERE DATE(completed_at) = %s
        """,
        (ds,),
    )
    # Append src_system + pipeline_run_id to match STG schema
    payload = [r + ["SUPABASE", run_id] for r in rows]
    bulk_insert(
        "STG_PRODUCTION_BATCH",
        columns=[
            "batch_id", "order_id", "line_id", "stage_id",
            "started_at", "completed_at", "qty_produced",
            "src_system", "pipeline_run_id",
        ],
        rows=payload,
        truncate=True,
    )


def load_qc_inspection(**ctx) -> None:
    ds = ctx["ds"]
    run_id = ctx["run_id"]
    rows = _extract(
        """
        SELECT qc_id, batch_id, stage_id, sample_qty, inspected_at
        FROM qc_inspection
        WHERE DATE(inspected_at) = %s
        """,
        (ds,),
    )
    payload = [r + ["SUPABASE", run_id] for r in rows]
    bulk_insert(
        "STG_QC_INSPECTION",
        columns=["qc_id", "batch_id", "stage_id", "sample_qty", "inspected_at",
                 "src_system", "pipeline_run_id"],
        rows=payload,
        truncate=True,
    )


def load_qc_result(**ctx) -> None:
    ds = ctx["ds"]
    run_id = ctx["run_id"]
    # Filter by inspected_at of the parent inspection so the grain matches the
    # production day.
    rows = _extract(
        """
        SELECT qr.result_id, qr.qc_id, qr.parameter,
               qr.measured_value, qr.spec_min, qr.spec_max, qr.pass_fail
        FROM qc_result qr
        JOIN qc_inspection qi ON qr.qc_id = qi.qc_id
        WHERE DATE(qi.inspected_at) = %s
        """,
        (ds,),
    )
    payload = [r + ["SUPABASE", run_id] for r in rows]
    bulk_insert(
        "STG_QC_RESULT",
        columns=["result_id", "qc_id", "parameter", "measured_value",
                 "spec_min", "spec_max", "pass_fail",
                 "src_system", "pipeline_run_id"],
        rows=payload,
        truncate=True,
    )


def load_maintenance_log(**ctx) -> None:
    ds = ctx["ds"]
    run_id = ctx["run_id"]
    rows = _extract(
        """
        SELECT log_id, machine_id, type, started_at, ended_at,
               downtime_min, issue_code
        FROM maintenance_log
        WHERE DATE(started_at) = %s
        """,
        (ds,),
    )
    payload = [r + ["SUPABASE", run_id] for r in rows]
    bulk_insert(
        "STG_MAINTENANCE_LOG",
        columns=["log_id", "machine_id", "type", "started_at", "ended_at",
                 "downtime_min", "issue_code",
                 "src_system", "pipeline_run_id"],
        rows=payload,
        truncate=True,
    )


def load_inventory(**ctx) -> None:
    """Full current-snapshot reload — inventory is not time-partitioned."""
    run_id = ctx["run_id"]
    rows = _extract(
        """
        SELECT material_id, qty_on_hand, qty_reserved, reorder_level,
               warehouse_loc, updated_at
        FROM inventory
        """,
        (),
    )
    payload = [r + ["SUPABASE", run_id] for r in rows]
    bulk_insert(
        "STG_INVENTORY",
        columns=["material_id", "qty_on_hand", "qty_reserved", "reorder_level",
                 "warehouse_loc", "updated_at", "src_system", "pipeline_run_id"],
        rows=payload,
        truncate=True,
    )


def load_material_consumption(**ctx) -> None:
    ds = ctx["ds"]
    run_id = ctx["run_id"]
    rows = _extract(
        """
        SELECT consumption_id, batch_id, material_id, qty_used, consumed_at
        FROM material_consumption
        WHERE DATE(consumed_at) = %s
        """,
        (ds,),
    )
    payload = [r + ["SUPABASE", run_id] for r in rows]
    bulk_insert(
        "STG_MATERIAL_CONSUMPTION",
        columns=["consumption_id", "batch_id", "material_id", "qty_used",
                 "consumed_at", "src_system", "pipeline_run_id"],
        rows=payload,
        truncate=True,
    )


def check_oracle_api(**_) -> None:
    """Short-circuit the DAG if the Oracle API isn't reachable."""
    info = health()
    log.info("oracle-api up: user=%s sysdate=%s", info.get("oracle_user"), info.get("oracle_sysdate"))


# -----------------------------------------------------------------------------
# DAG definition
# -----------------------------------------------------------------------------

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_supabase_to_oracle",
    description="Supabase OLTP -> Oracle AI03 STG, every 8h, one day per run.",
    default_args=default_args,
    schedule="0 6,14,22 * * *",
    start_date=datetime(2026, 3, 19),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "supabase", "oracle"],
) as dag:

    healthcheck = PythonOperator(
        task_id="check_oracle_api",
        python_callable=check_oracle_api,
    )

    tasks: list[tuple[str, Callable]] = [
        ("extract_production_batch",     load_production_batch),
        ("extract_qc_inspection",        load_qc_inspection),
        ("extract_qc_result",            load_qc_result),
        ("extract_maintenance_log",      load_maintenance_log),
        ("extract_inventory",            load_inventory),
        ("extract_material_consumption", load_material_consumption),
    ]

    for task_id, fn in tasks:
        t = PythonOperator(task_id=task_id, python_callable=fn)
        healthcheck >> t
