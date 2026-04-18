from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from _oracle_api import bulk_insert, health


"""Aggregate InfluxDB sensor data → Oracle STG_SENSOR_AGG ทุก 15 นาที

ออกแบบตาม NEW_ARCHITECTURE.md:
- Window = 15 นาที (ตรงกับ Airflow data_interval)
- Aggregation = mean/min/max + count ต่อ (machine_id × field)
- Output: 1 row ต่อ (machine × metric × window)
  * 3 machines × 6 metrics = สูงสุด 18 row ต่อรอบ
  * filter ตาม DIM_METRIC.machine_name ฝั่ง SP_LOAD_FACT_SENSOR
  * ตรงนี้ extract ทุก metric ทุกเครื่องที่มีข้อมูล (transform ตอน load fact)

Flux schema:
  measurement: station_1
  tag: machine_id (M01/M02/M03)
  fields: temperature_c, machine_state_num, cycle_count,
          vibration_g, current_a, voltage_v

INFLUX_RANGE_START env override = สำหรับ ad-hoc test (เช่น "-6h")
"""


log = logging.getLogger(__name__)


def check_oracle_api(**_) -> None:
    info = health()
    log.info("oracle-api up: user=%s", info.get("oracle_user"))


def extract_sensor_agg(**ctx) -> None:
    url = os.environ.get("INFLUX_URL")
    token = os.environ.get("INFLUX_TOKEN")
    if not url or not token:
        log.warning("INFLUX_URL / INFLUX_TOKEN unset — skipping")
        return

    from influxdb_client import InfluxDBClient

    org = os.environ.get("INFLUX_ORG", "factory")
    bucket = os.environ.get("INFLUX_BUCKET", "iiot_data_raw")
    measurement = os.environ.get("INFLUX_MEASUREMENT", "station_1")

    start = ctx["data_interval_start"]
    end   = ctx["data_interval_end"]

    # ad-hoc override window (เช่น backfill หรือ ทดสอบ)
    range_override = os.environ.get("INFLUX_RANGE_START")
    if range_override:
        range_clause = f"range(start: {range_override})"
        log.warning("Using INFLUX_RANGE_START override: %s", range_override)
    else:
        range_clause = (
            f"range(start: {start.isoformat()}, stop: {end.isoformat()})"
        )

    # Flux: aggregate ราย 15-min window, group by machine_id + field
    # ใช้ 3 query แยก สำหรับ mean / min / max แล้ว join ใน Python
    def _flux(agg_fn: str) -> str:
        return f"""
        from(bucket:"{bucket}")
          |> {range_clause}
          |> filter(fn:(r) => r._measurement == "{measurement}")
          |> aggregateWindow(every: 15m, fn: {agg_fn}, createEmpty: false)
        """

    with InfluxDBClient(url=url, token=token, org=org) as client:
        mean_tables  = client.query_api().query(_flux("mean"), org=org)
        min_tables   = client.query_api().query(_flux("min"), org=org)
        max_tables   = client.query_api().query(_flux("max"), org=org)
        count_tables = client.query_api().query(_flux("count"), org=org)

    def _collect(tables):
        """{(machine, field, window_end): value}"""
        out = {}
        for tbl in tables:
            for rec in tbl.records:
                key = (
                    rec.values.get("machine_id"),
                    rec.get_field(),
                    rec.get_time(),
                )
                out[key] = rec.get_value()
        return out

    means  = _collect(mean_tables)
    mins   = _collect(min_tables)
    maxs   = _collect(max_tables)
    counts = _collect(count_tables)

    # Build rows — ใช้ key จาก means เป็น source of truth
    rows: list[list] = []
    run_id = ctx["run_id"]
    for (machine, field, win_end), avg_val in means.items():
        if machine is None or field is None:
            continue
        # window_start = win_end - 15 นาที (aggregateWindow ใส่ timestamp ที่ end)
        window_start = (win_end - timedelta(minutes=15)).replace(tzinfo=None)
        window_end_naive = win_end.replace(tzinfo=None)
        rows.append([
            str(machine),
            str(field),
            window_start,
            window_end_naive,
            float(avg_val) if avg_val is not None else None,
            float(mins.get((machine, field, win_end), avg_val) or avg_val),
            float(maxs.get((machine, field, win_end), avg_val) or avg_val),
            int(counts.get((machine, field, win_end), 0) or 0),
            "INFLUXDB",
            run_id,
        ])

    if not rows:
        log.warning("Flux returned 0 rows for %s → %s", start, end)
        return

    log.info("writing %d sensor aggregate rows", len(rows))
    bulk_insert(
        "STG_SENSOR_AGG",
        columns=["machine_name", "metric_name", "window_start", "window_end",
                 "avg_value", "min_value", "max_value", "sample_count",
                 "src_system", "pipeline_run_id"],
        rows=rows,
        truncate=True,
    )


default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="etl_influxdb_to_oracle",
    description="InfluxDB (AWS) -> Oracle STG_SENSOR_AGG, every 15 min.",
    default_args=default_args,
    schedule="*/15 * * * *",
    start_date=datetime(2026, 4, 18),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "influx", "oracle"],
) as dag:
    hc = PythonOperator(task_id="check_oracle_api", python_callable=check_oracle_api)
    extract = PythonOperator(task_id="aggregate_sensor", python_callable=extract_sensor_agg)
    hc >> extract
