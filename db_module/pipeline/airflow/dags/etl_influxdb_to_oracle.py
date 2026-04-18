"""Aggregate 1 Hz sensor rows from InfluxDB into STG_SENSOR_AGG.

**Blocked on credentials** (INFLUX_URL + INFLUX_TOKEN still blank as of
2026-04-18). If the URL is unset at runtime the task no-ops with a log
message so the DAG stays parseable and schedulable.

Flux window matches the 8-hour Airflow schedule so each run covers exactly
one execution slot. Aggregation: mean of numeric fields + last state flag
per machine_id.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from _oracle_api import bulk_insert, health

log = logging.getLogger(__name__)


def check_oracle_api(**_) -> None:
    info = health()
    log.info("oracle-api up: user=%s", info.get("oracle_user"))


def extract_sensor_agg(**ctx) -> None:
    url = os.environ.get("INFLUX_URL")
    token = os.environ.get("INFLUX_TOKEN")
    if not url or not token:
        log.warning("INFLUX_URL / INFLUX_TOKEN unset — skipping sensor extraction")
        return

    from influxdb_client import InfluxDBClient

    org = os.environ.get("INFLUX_ORG", "factory")
    bucket = os.environ.get("INFLUX_BUCKET", "sensors")
    ds = ctx["ds"]
    data_interval_start = ctx.get("data_interval_start")
    data_interval_end = ctx.get("data_interval_end")

    # NodeRED on AWS writes to measurement `station_1` (per live deployment
    # as of 2026-04-18). Fields/tag names match CLAUDE.md §5.
    measurement = os.environ.get("INFLUX_MEASUREMENT", "station_1")

    # For ad-hoc verification we allow `INFLUX_RANGE_START` (e.g. "-15m") to
    # override the schedule-derived window — handy when NodeRED was just
    # deployed and historical `data_interval`s have no data yet.
    range_override = os.environ.get("INFLUX_RANGE_START")
    if range_override:
        range_clause = f"range(start: {range_override})"
        log.warning("Using INFLUX_RANGE_START override: %s", range_override)
    else:
        range_clause = (
            f"range(start: {data_interval_start.isoformat()}, "
            f"stop: {data_interval_end.isoformat()})"
        )

    flux = f"""
    from(bucket:"{bucket}")
      |> {range_clause}
      |> filter(fn:(r) => r._measurement == "{measurement}")
      |> aggregateWindow(every: 8h, fn: mean, createEmpty: false)
      |> pivot(rowKey:["_time","machine_id"], columnKey:["_field"], valueColumn:"_value")
    """

    with InfluxDBClient(url=url, token=token, org=org) as client:
        tables = client.query_api().query(flux)

    run_id = ctx["run_id"]
    rows = []
    for table in tables:
        for rec in table.records:
            vals = rec.values
            rows.append([
                str(vals.get("machine_id", "")),
                ds,
                vals.get("temperature_c"),
                vals.get("cycle_count"),
                vals.get("vibration_g"),
                vals.get("current_a"),
                vals.get("voltage_v"),
                "INFLUXDB",
                run_id,
            ])

    if not rows:
        log.warning("Flux query returned 0 rows for %s", ds)
        return

    bulk_insert(
        "STG_SENSOR_AGG",
        columns=["machine_id", "run_date", "avg_temp_c", "total_cycles",
                 "avg_vibration_g", "avg_current_a", "avg_voltage_v",
                 "src_system", "pipeline_run_id"],
        rows=rows,
        truncate=True,
    )


default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_influxdb_to_oracle",
    description="InfluxDB (AWS) -> Oracle STG_SENSOR_AGG, every 8h.",
    default_args=default_args,
    schedule="0 6,14,22 * * *",
    start_date=datetime(2026, 3, 19),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "influx", "oracle"],
) as dag:
    hc = PythonOperator(task_id="check_oracle_api", python_callable=check_oracle_api)
    extract = PythonOperator(task_id="extract_sensor_agg", python_callable=extract_sensor_agg)
    hc >> extract
