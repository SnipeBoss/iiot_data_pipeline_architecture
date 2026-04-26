from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from _oracle_api import call_sp, health


"""Trigger master FACT loader หลัง STG populate เสร็จ (2026-04-26)

Pattern เปลี่ยนจาก 3 parallel SPs → single master orchestrator:
    SP_LOAD_ALL_FACTS = PRODUCTION → QUALITY → DEFECT → DOWNTIME → SENSOR
    (ลำดับสำคัญ — DEFECT depend on QUALITY+PRODUCTION; DOWNTIME independent;
     SENSOR independent ของ OLTP)

Schedule: 5,20,35,50 — รัน 5 นาทีหลังทุก */15 (ให้ STG พร้อมก่อน)

SP เองทำ DELETE-by-key + INSERT-from-STG (idempotent) — DAG แค่ schedule + call
"""


log = logging.getLogger(__name__)


def check_oracle_api(**_) -> None:
    info = health()
    log.info("oracle-api up: user=%s", info.get("oracle_user"))


def run_sp_load_all_facts(**ctx) -> None:
    """เรียก SP_LOAD_ALL_FACTS — ภายในรัน 5 SPs ตามลำดับ dependency

    ใช้เวลา ~10-30 วินาที ขึ้นกับ STG row count
    ถ้า SP ตัวใดตัวหนึ่ง raise → master rollback (PL/SQL atomic per call)
    """
    result = call_sp("SP_LOAD_ALL_FACTS")
    log.info("SP_LOAD_ALL_FACTS → %s", result)


default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="sp_load_dw",
    description="Trigger SP_LOAD_ALL_FACTS (5-min offset from extract DAGs)",
    default_args=default_args,
    schedule="5,20,35,50 * * * *",
    start_date=datetime(2026, 4, 18),
    catchup=False,
    max_active_runs=1,
    tags=["oracle", "dw", "facts"],
) as dag:

    healthcheck = PythonOperator(
        task_id="check_oracle_api",
        python_callable=check_oracle_api,
    )

    load_all = PythonOperator(
        task_id="sp_load_all_facts",
        python_callable=run_sp_load_all_facts,
    )

    healthcheck >> load_all
