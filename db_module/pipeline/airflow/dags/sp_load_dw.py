from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from _oracle_api import call_sp, health


"""Chain FACT-loader SPs หลังจาก STG populate เสร็จ

Tasks (parallel):
  - sp_load_fact_production  — FACT_PRODUCTION จาก STG_PRODUCTION_BATCH
  - sp_load_fact_quality     — FACT_QUALITY จาก STG_QC_RECORD
  - sp_load_fact_sensor      — FACT_SENSOR จาก STG_SENSOR_AGG

SP เองทำ DELETE-by-date + INSERT-from-STG (idempotent) — DAG แค่ schedule + call
Schedule: 5 นาทีหลัง extract DAG (ให้ STG พร้อมก่อน)
"""


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
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="sp_load_dw",
    description="Chain FACT-loader SPs (15-min offset from extract DAGs).",
    default_args=default_args,
    schedule="5,20,35,50 * * * *",   # 5 นาทีหลังทุก */15
    start_date=datetime(2026, 4, 18),
    catchup=False,
    max_active_runs=1,
    tags=["oracle", "dw", "facts"],
) as dag:

    healthcheck = PythonOperator(
        task_id="check_oracle_api",
        python_callable=check_oracle_api,
    )

    load_production = PythonOperator(
        task_id="sp_load_fact_production",
        python_callable=_make_sp_task("SP_LOAD_FACT_PRODUCTION"),
    )
    load_quality = PythonOperator(
        task_id="sp_load_fact_quality",
        python_callable=_make_sp_task("SP_LOAD_FACT_QUALITY"),
    )
    load_sensor = PythonOperator(
        task_id="sp_load_fact_sensor",
        python_callable=_make_sp_task("SP_LOAD_FACT_SENSOR"),
    )

    healthcheck >> [load_production, load_quality, load_sensor]
