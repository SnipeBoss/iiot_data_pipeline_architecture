from __future__ import annotations
import logging
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from _oracle_api import bulk_insert, health
from influxdb_client import InfluxDBClient


"""
Aggregate InfluxDB sensor data → Oracle STG_SENSOR_AGG ทุก 15 นาที

- Window = 15 นาที (ตรงกับ Airflow data_interval)
- Aggregation = mean/min/max + count ต่อ (machine_id × field)
- Output: 1 row ต่อ (machine × metric × window)
  * 3 machines × 6 metrics = สูงสุด 18 row ต่อรอบ
  * filter ตาม DIM_METRIC.machine_code ฝั่ง SP_LOAD_FACT_SENSOR
  * ตรงนี้ extract ทุก metric ทุกเครื่องที่มีข้อมูล (transform ตอน load fact)

Flux schema:
  measurement: station_1
  tag: machine_id (M01/M02/M03)
  fields: temperature_c, machine_state_num, cycle_count,
          vibration_g, current_a, voltage_v

INFLUX_RANGE_START env override = สำหรับ ad-hoc test (เช่น "-6h")
"""


log = logging.getLogger(__name__)


EXPECTED_METRICS = {
    "temperature_c", 
    "machine_state_num", 
    "cycle_count",
    "vibration_g", 
    "current_a", 
    "voltage_v",
}

EXPECTED_MACHINES = {
    "M01", 
    "M02", 
    "M03"
}






def check_oracle_api(**_) -> None:
    info = health()
    log.info("oracle-api up: user=%s", info.get("oracle_user"))









def extract_sensor_agg(**ctx) -> None:


    # Get Influx Env
    url = os.environ.get("INFLUX_URL")
    token = os.environ.get("INFLUX_TOKEN")
    if not url or not token:
        log.warning("INFLUX_URL / INFLUX_TOKEN unset — skipping")
        return

    # Get Configure
    org = os.environ.get("INFLUX_ORG", "factory")
    bucket = os.environ.get("INFLUX_BUCKET", "iiot_data_raw")
    measurement = os.environ.get("INFLUX_MEASUREMENT", "station_1")

    # Get Configuration Start and End Time
    start = ctx["data_interval_start"]
    end   = ctx["data_interval_end"]


    # ad-hoc override window -> For Backfill
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
        """
        {(machine, field, window_end): value}
        """
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

        # ถ้า min/max key ไม่ match mean → ใช้ None ไม่ fallback avg 
        min_raw = mins.get((machine, field, win_end))
        max_raw = maxs.get((machine, field, win_end))

        rows.append([
            str(machine),
            str(field),
            window_start,
            window_end_naive,
            float(avg_val) if avg_val is not None else None,
            float(min_raw) if min_raw is not None else None,
            float(max_raw) if max_raw is not None else None,
            int(counts.get((machine, field, win_end), 0) or 0),
            "INFLUXDB",
            run_id,
        ])


    if not rows:
        log.warning("Flux returned 0 rows for %s → %s", start, end)
        return


    # Validate ก่อน bulk_insert — ถ้า InfluxDB schema drift จะ fail loud
    # (ไม่ silent FACT_SENSOR empty เพราะ JOIN ไม่เจอ)
    unknown_metrics = {row[1] for row in rows if row[1] not in EXPECTED_METRICS}
    unknown_machines = {row[0] for row in rows if row[0] not in EXPECTED_MACHINES}

    if unknown_metrics:
        raise ValueError(
            f"unknown metric_name(s) ใน Influx ไม่ตรง DIM_METRIC: {sorted(unknown_metrics)}"
        )
    
    if unknown_machines:
        raise ValueError(
            f"unknown machine_code(s) ใน Influx ไม่ตรง DIM_MACHINE: {sorted(unknown_machines)}"
        )

    log.info("writing %d sensor aggregate rows", len(rows))

    bulk_insert(
        "STG_SENSOR_AGG",
    
        # match กับ DIM_MACHINE.machine_code (M01/M02/M03)
        columns=["machine_code",
                 "metric_name",
                 "window_start",
                 "window_end",
                 "avg_value",
                 "min_value",
                 "max_value",
                 "sample_count",
                 "src_system",
                 "pipeline_run_id"],
    
        rows=rows,
        truncate=True,
    )










default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}



with DAG(

    # Defined ID Name
    dag_id="etl_influxdb_to_oracle",

    # Description
    description="InfluxDB (AWS) -> Oracle STG_SENSOR_AGG, every 15 min.",

    # Arguments 
    default_args=default_args,

    # 15 min
    schedule="*/15 * * * *",
    start_date=datetime(2026, 4, 18),
    
    catchup=False,
    
    max_active_runs=1,
    
    tags=["etl", "influx", "oracle"],

) as dag:
    
    # Checking Oracle API 
    hc = PythonOperator(
        task_id="check_oracle_api", 
        python_callable=check_oracle_api
    )

    # Set InfluxDB Aggregate Sensor
    extract = PythonOperator(
        task_id="aggregate_sensor", 
        python_callable=extract_sensor_agg
    )

    # Send to extract task
    hc >> extract
