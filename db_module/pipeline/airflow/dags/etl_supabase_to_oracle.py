from __future__ import annotations
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from _oracle_api import bulk_insert, health
from _supabase import supabase_cursor


log = logging.getLogger(__name__)


"""
Extract Supabase OLTP → Oracle STG ตาม schema ใหม่ (2026-04-26)

Architecture:
    Supabase ──psycopg2──▶ [DAG] ──HTTP bulk-insert──▶ Oracle API ──JDBC──▶ AI03 STG

Tasks (parallel ทั้ง 4 ตัว):
    extract_production_batch  → STG_PRODUCTION_BATCH
    extract_qc_record         → STG_QC_RECORD
    extract_qc_defect         → STG_QC_DEFECT     (NEW)
    extract_downtime_event    → STG_DOWNTIME_EVENT (NEW)

แต่ละ task เป็น TRUNCATE + INSERT (idempotent) — STG เป็น buffer ของ window ล่าสุด
SP_LOAD_FACT_* (รัน 5 นาทีหลัง) จะ MERGE STG → FACT แล้ว STG ถูก truncate รอบหน้า

Schema changes vs version 2026-04-19:
- product_id  → model_id            (Supabase battery_model แทน product)
- qty_sampled → qty_inspected       (rename ตาม schema ใหม่)
- เพิ่ม STG_QC_DEFECT + STG_DOWNTIME_EVENT (เดิมไม่มี)
"""





def _extract(sql: str, params: tuple) -> list[list]:
    with supabase_cursor() as cur:
        cur.execute(sql, params)
        return [list(row) for row in cur.fetchall()]




def check_oracle_api(**_) -> None:
    """
    Short-circuit DAG ถ้า Oracle API ไม่ตอบ
    """
    info = health()
    
    log.info("oracle-api up: user=%s sysdate=%s",
             info.get("oracle_user"), 
             info.get("oracle_sysdate"))








def load_production_batch(**ctx) -> None:
    """
    ดึง batch ที่ end_time อยู่ใน 15-min window

    JOIN production_order เพื่อ carry planning data (model_id, planned_*)
    + sub-query order_total_qty (SUM ต่อ order) ส่งให้ SP คำนวณ batch share
    """
    
    # Get Configuration
    run_id = ctx["run_id"]
    start = ctx["data_interval_start"]
    end   = ctx["data_interval_end"]


    # Query Method
    rows = _extract(
        """
        SELECT b.batch_id,
               b.order_id,
               b.line_id,
               o.model_id,
               b.qty_planned,
               b.qty_out,
               b.start_time,
               b.end_time,
               o.planned_start AS order_planned_start,
               o.planned_end   AS order_planned_end,
               (SELECT SUM(qty_planned) FROM production_batch
                 WHERE order_id = b.order_id) AS order_total_qty
          FROM production_batch b
          JOIN production_order o ON b.order_id = o.order_id
         WHERE b.end_time IS NOT NULL
           AND b.end_time >= %s
           AND b.end_time <  %s
        """,
        (start, end),
    )



    payload = [r + ["SUPABASE", run_id] for r in rows]
    log.info("extracted %d production_batch rows for %s → %s", len(rows), start, end)
    
    
    bulk_insert(
        "STG_PRODUCTION_BATCH",
        columns=["batch_id", 
                 "order_id", 
                 "line_id", 
                 "model_id",
                 "qty_planned", 
                 "qty_out", 
                 "start_time", 
                 "end_time",
                 "order_planned_start", 
                 "order_planned_end", 
                 "order_total_qty",
                 "src_system", 
                 "pipeline_run_id"],
        rows=payload,
        truncate=True,
    )




def load_qc_record(**ctx) -> None:
    """
    ดึง qc_record ที่ inspected_at อยู่ใน window
    """
    
    run_id = ctx["run_id"]
    start = ctx["data_interval_start"]
    end   = ctx["data_interval_end"]

    rows = _extract(
        """
        SELECT qc_id, batch_id, qty_inspected, qty_passed, qty_failed, inspected_at
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
        columns=["qc_id",
                 "batch_id",
                 "qty_inspected",
                 "qty_passed",
                 "qty_failed",
                 "inspected_at",
                 "src_system",
                 "pipeline_run_id"],
        rows=payload,
        truncate=True,
    )




def load_qc_defect(**ctx) -> None:
    """
    ดึง qc_defect (M:N junction) ของ qc ที่ inspected ใน window

    JOIN qc_record เพื่อ filter by inspected_at — defect บริสุทธิ์ไม่มี timestamp
    """

    run_id = ctx["run_id"]
    start = ctx["data_interval_start"]
    end   = ctx["data_interval_end"]

    rows = _extract(
        """
        SELECT qd.qc_id, qd.defect_code, qd.qty_affected
          FROM qc_defect qd
          JOIN qc_record qc ON qc.qc_id = qd.qc_id
         WHERE qc.inspected_at >= %s
           AND qc.inspected_at <  %s
        """,
        (start, end),
    )
    
    payload = [r + ["SUPABASE", run_id] for r in rows]
    log.info("extracted %d qc_defect rows for %s → %s", len(rows), start, end)
    bulk_insert(
        "STG_QC_DEFECT",
        
        columns=["qc_id", 
                 "defect_code", 
                 "qty_affected",
                 "src_system", 
                 "pipeline_run_id"],
        
        rows=payload,
        truncate=True,
    )






def load_downtime_event(**ctx) -> None:
    """ดึง downtime ที่ end_ts ปิดแล้วใน window

    JOIN machine + event_reason เพื่อ:
        - machine_code (M01/M02/M03) สำหรับ DIM_MACHINE lookup
        - line_id (จาก machine, ไม่ใช่ downtime_event)
        - is_planned (จาก event_reason)
    Filter end_ts IS NOT NULL — ไม่ load open events
    """
    run_id = ctx["run_id"]
    start = ctx["data_interval_start"]
    end   = ctx["data_interval_end"]

    rows = _extract(
        """
        SELECT de.event_id,
               de.machine_id,
               m.machine_code,
               m.line_id,
               de.batch_id,
               de.reason_code,
               er.is_planned,
               de.start_ts,
               de.end_ts,
               de.duration_min
          FROM downtime_event de
          JOIN machine m       ON m.machine_id = de.machine_id
          JOIN event_reason er ON er.reason_code = de.reason_code
         WHERE de.end_ts IS NOT NULL
           AND de.end_ts >= %s
           AND de.end_ts <  %s
        """,
        (start, end),
    )
    payload = [r + ["SUPABASE", run_id] for r in rows]
    log.info("extracted %d downtime_event rows for %s → %s", len(rows), start, end)
    bulk_insert(
        "STG_DOWNTIME_EVENT",
        columns=["event_id",
                 "machine_id",
                 "machine_code",
                 "line_id",
                 "batch_id",
                 "reason_code",
                 "is_planned",
                 "start_ts",
                 "end_ts",
                 "duration_min",
                 "src_system",
                 "pipeline_run_id"],
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
    description="Supabase OLTP -> Oracle AI03 STG (4 tables, every 15 min)",
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

    extract_qc_defect = PythonOperator(
        task_id="extract_qc_defect",
        python_callable=load_qc_defect,
    )

    extract_downtime = PythonOperator(
        task_id="extract_downtime_event",
        python_callable=load_downtime_event,
    )

    # 4 extracts รัน parallel หลัง healthcheck ผ่าน
    healthcheck >> [extract_batch, extract_qc, extract_qc_defect, extract_downtime]
