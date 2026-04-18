"""Chain the FACT-loader stored procedures after extract DAGs complete.

Each task calls one SP via the Oracle API's `/sp/call` endpoint. The SPs
themselves do the heavy lifting (DELETE-by-date + INSERT-from-STG) — this
DAG just schedules + orders them.

Scheduled 30 minutes after the extract DAGs so STG is guaranteed fresh.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from _oracle_api import call_sp, health

log = logging.getLogger(__name__)


def check_oracle_api(**_) -> None:
    info = health()
    log.info("oracle-api up: user=%s", info.get("oracle_user"))


def _make_sp_task(sp_name: str):
    def _run(**ctx):
        ds = ctx["ds"]
        result = call_sp(sp_name, [ds])
        log.info("%s(%s) → %s", sp_name, ds, result)
    _run.__name__ = f"run_{sp_name.lower()}"
    return _run


default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sp_load_dw",
    description="Chain FACT-loader SPs after STG is populated.",
    default_args=default_args,
    schedule="30 6,14,22 * * *",
    start_date=datetime(2026, 3, 19),
    catchup=False,
    max_active_runs=1,
    tags=["oracle", "dw", "facts"],
) as dag:

    healthcheck = PythonOperator(
        task_id="check_oracle_api",
        python_callable=check_oracle_api,
    )

    # SP_LOAD_FACT_OEE is the primary deliverable; others in parallel after.
    load_oee = PythonOperator(
        task_id="sp_load_fact_oee",
        python_callable=_make_sp_task("SP_LOAD_FACT_OEE"),
    )
    load_quality = PythonOperator(
        task_id="sp_load_fact_quality",
        python_callable=_make_sp_task("SP_LOAD_FACT_QUALITY"),
    )
    load_maintenance = PythonOperator(
        task_id="sp_load_fact_maintenance",
        python_callable=_make_sp_task("SP_LOAD_FACT_MAINTENANCE"),
    )
    load_production = PythonOperator(
        task_id="sp_load_fact_production",
        python_callable=_make_sp_task("SP_LOAD_FACT_PRODUCTION"),
    )
    load_inventory = PythonOperator(
        task_id="sp_load_fact_inventory",
        python_callable=_make_sp_task("SP_LOAD_FACT_INVENTORY"),
    )

    healthcheck >> [load_oee, load_quality, load_maintenance, load_production, load_inventory]
