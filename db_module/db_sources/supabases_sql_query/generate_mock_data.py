"""สร้าง mock OLTP data ตาม schema ใหม่ (12 ตาราง, event-sourced)

ใช้งาน:
    python db_module/db_sources/supabases_sql_query/mock/generate_mock_data.py

Output: 04_mock_data.sql ในโฟลเดอร์เดียวกัน

หลักการ:
- Mock window aligned กับช่วง InfluxDB จริง — clamp 7 วันล่าสุด, แต่ปรับขอบเขตให้
  ตรงกับ shift boundary เพื่อหลีกเลี่ยง partial shift
- 2-shift continuous: DAY 07:30-16:30 (9 hr), NIGHT 17:30-06:30 next day (13 hr)
- Trigger-aware: insert batch ด้วย status='CREATED' + times=NULL แล้วยิง events
  CREATED→STARTED→[PAUSED/RESUMED]→COMPLETED, trigger จัดการ sync ให้
- ห้าม set duration_min ของ downtime_event — trigger compute เอง
- Reason code category match strictly:
    batch_status_event.reason_code  → BATCH_HOLD only (PAUSED reasons)
    downtime_event.reason_code      → MACHINE_DOWNTIME only

Deterministic ด้วย SEED คงที่ — reproducible
"""
from __future__ import annotations

import math
import random
import sys
from datetime import datetime, time, timedelta
from pathlib import Path

import numpy as np

# repo root อยู่ที่ parents[4] (mock → supabases_sql_query → db_sources → db_module → root)
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from db_module.db_conn import InfluxConnector  # noqa: E402


# ---------------------------------------------------------------------------
# Parameters (LOCK ตาม RECREATE_CODE.md Phase 2)
# ---------------------------------------------------------------------------
SEED = 42
MAX_WINDOW_DAYS = 7
INFLUX_BUCKET = "iiot_data_raw"

OUT_PATH = Path(__file__).parent / "04_mock_data.sql"

DAY_SHIFT_START = time(7, 30)
DAY_SHIFT_END = time(16, 30)
NIGHT_SHIFT_START = time(17, 30)
NIGHT_SHIFT_END = time(6, 30)  # ของวันถัดไป

LINE_ID = 1
MODELS = [1, 2, 3]
MODEL_WEIGHTS = [0.4, 0.4, 0.2]
MACHINES = [1, 2, 3]

# leaf defects (ตรงกับ category='LEAF' ใน 03_master_data.sql)
LEAF_DEFECTS = [
    "TERMINAL_LOOSE", "TERMINAL_MISALIGNED", "TERMINAL_BURN_MARK",
    "COVER_GAP", "COVER_CRACK", "COVER_MISFIT",
    "WELD_INCOMPLETE", "WELD_OVER", "WELD_POROSITY",
    "PLATE_REVERSED", "PLATE_MISSING", "PLATE_DOUBLE",
    "CASING_SCRATCH", "CASING_CRACK", "CASING_DEFORM",
]
# pause reasons ต้องเป็น category='BATCH_HOLD' เท่านั้น (FK sanity check 4.7)
PAUSE_REASONS = ["LUNCH_BREAK", "MATERIAL_SHORTAGE"]
# downtime reasons ต้องเป็น category='MACHINE_DOWNTIME'
DOWNTIME_UNPLANNED = ["EQUIPMENT_FAULT", "SENSOR_ALARM", "POWER_OUTAGE"]
DOWNTIME_UNPLANNED_WEIGHTS = [0.5, 0.3, 0.2]


# ---------------------------------------------------------------------------
# InfluxDB range discovery
# ---------------------------------------------------------------------------
def discover_influx_range() -> tuple[datetime, datetime]:
    """Query first/last timestamp ของ bucket แล้วคืนเป็น naive UTC datetime"""
    c = InfluxConnector()
    flux_first = (
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: -90d) '
        f'|> first() '
        f'|> keep(columns: ["_time"])'
    )
    flux_last = (
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: -90d) '
        f'|> last() '
        f'|> keep(columns: ["_time"])'
    )

    ts_first: datetime | None = None
    ts_last: datetime | None = None

    for table in c.query(flux_first):
        for rec in table.records:
            ts = rec.get_time().replace(tzinfo=None)
            if ts_first is None or ts < ts_first:
                ts_first = ts
    for table in c.query(flux_last):
        for rec in table.records:
            ts = rec.get_time().replace(tzinfo=None)
            if ts_last is None or ts > ts_last:
                ts_last = ts

    if ts_first is None or ts_last is None:
        raise RuntimeError("InfluxDB bucket ว่าง — ไม่พบ first/last timestamp")
    return ts_first, ts_last


def compute_mock_window(earliest: datetime, latest: datetime,
                        max_days: int = MAX_WINDOW_DAYS) -> tuple:
    """หา (start_date, night_end_date, day_end_date) ที่ shift ทั้งหมด aligned ภายใน [earliest, latest]

    night_end_date = D ล่าสุดที่ NIGHT-D ends (= (D+1) 06:30) ก่อน latest
    day_end_date   = D ล่าสุดที่ DAY-D ends (= D 16:30) ก่อน latest
                     (อาจมากกว่า night_end_date 1 วัน เมื่อ latest ตกในช่วง morning)

    การ split ทั้งสองค่านี้ทำให้สามารถเพิ่ม trailing DAY shift ได้
    เพื่อให้ทุกวันใน window มี DAY orders (ผ่าน shift coverage check 4.5.1)
    """
    night_end_date = (latest - timedelta(days=1)).date()
    while datetime.combine(night_end_date + timedelta(days=1), NIGHT_SHIFT_END) > latest:
        night_end_date -= timedelta(days=1)

    day_end_date = latest.date()
    while datetime.combine(day_end_date, DAY_SHIFT_END) > latest:
        day_end_date -= timedelta(days=1)

    # principal cap = max_days from night_end_date
    start_date = night_end_date - timedelta(days=max_days - 1)
    while datetime.combine(start_date, DAY_SHIFT_START) < earliest:
        start_date += timedelta(days=1)

    if start_date > night_end_date:
        raise RuntimeError(
            f"Mock window degenerate: start={start_date} > night_end={night_end_date}"
        )
    return start_date, night_end_date, day_end_date


def iter_shifts(start_date, night_end_date, day_end_date):
    """Yield (shift_start_dt, shift_end_dt, label) — DAY ก่อน NIGHT ทุกวัน
    + extra DAY shift ที่ day_end_date ถ้ามากกว่า night_end_date
    (เพื่อให้ calendar date สุดท้ายมี DAY orders ครบ — ผ่าน 4.5.1 shift coverage)
    """
    cur = start_date
    while cur <= night_end_date:
        yield (datetime.combine(cur, DAY_SHIFT_START),
               datetime.combine(cur, DAY_SHIFT_END), "DAY")
        yield (datetime.combine(cur, NIGHT_SHIFT_START),
               datetime.combine(cur + timedelta(days=1), NIGHT_SHIFT_END), "NIGHT")
        cur += timedelta(days=1)
    if day_end_date > night_end_date:
        extra = night_end_date + timedelta(days=1)
        yield (datetime.combine(extra, DAY_SHIFT_START),
               datetime.combine(extra, DAY_SHIFT_END), "DAY")


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------
def gen_orders(shifts, rng):
    """Stagger order ทุก ~75-90 min ภายในแต่ละ shift; ตัด last order ถ้าเกิน shift_end"""
    orders = []
    order_id = 0
    for shift_start, shift_end, label in shifts:
        n_target = rng.randint(7, 9) if label == "DAY" else rng.randint(10, 13)
        # initial offset เล็กน้อยจาก shift start (1-5 min) เพื่อไม่ให้ทุก shift เริ่ม 07:30 เป๊ะ
        cur = shift_start + timedelta(minutes=rng.randint(1, 5))

        for _ in range(n_target):
            planned_start = cur
            planned_end = planned_start + timedelta(minutes=60)
            if planned_end > shift_end:
                break  # last order เกิน shift → ตัดทิ้ง
            order_id += 1
            orders.append({
                "order_id": order_id,
                "model_id": rng.choices(MODELS, weights=MODEL_WEIGHTS, k=1)[0],
                "qty_ordered": rng.randint(100, 300),
                "planned_start": planned_start,
                "planned_end": planned_end,
                "shift_label": label,
            })
            # next order: stagger ~70-85 min (= 60 min execution + 10-25 min idle)
            cur = planned_start + timedelta(minutes=rng.randint(70, 85))
    return orders


def split_qty(total: int, n: int, rng) -> list[int]:
    """แบ่ง total ออกเป็น n ส่วน โดยแต่ละส่วน ≥ 30% ของค่าเฉลี่ย และ sum=total"""
    avg = total / n
    min_each = max(1, int(avg * 0.30))
    if min_each * n > total:
        min_each = max(1, total // n)
    splits = [min_each] * n
    remaining = total - min_each * n
    for _ in range(remaining):
        splits[rng.randint(0, n - 1)] += 1
    return splits


def gen_batches_and_events(orders, rng, np_rng):
    """สร้าง batch + lifecycle events + QC + defects ครบ flow

    ค่า status_code/start_time/end_time ของ production_batch ถูก set ผ่าน trigger
    เมื่อ batch_status_event ถูก insert — เราใส่ status='CREATED' + NULL times ตอนแรก
    """
    batches = []
    events = []
    qc_records = []
    qc_defects = []
    batch_id = 0
    qc_id = 0

    for o in orders:
        n_batches = rng.randint(2, 4)
        qty_splits = split_qty(o["qty_ordered"], n_batches, rng)
        # batch duration (60 min / n_batches) — total batches+gaps ≈ 60-75 min
        batch_dur = timedelta(minutes=60.0 / n_batches)
        cur_start = o["planned_start"]

        for qty_planned in qty_splits:
            batch_id += 1
            yield_rate = float(np.clip(np_rng.beta(95, 5), 0.85, 1.00))
            qty_out = round(qty_planned * yield_rate)
            # round() อาจปัดลงจน ratio < 0.85 — clamp เพื่อผ่าน 4.3 yield distribution
            if qty_planned > 0 and qty_out / qty_planned < 0.85:
                qty_out = math.ceil(qty_planned * 0.85)

            actual_start = cur_start
            actual_end = actual_start + batch_dur
            has_pause = rng.random() < 0.15

            batches.append({
                "batch_id": batch_id,
                "order_id": o["order_id"],
                "line_id": LINE_ID,
                "qty_planned": qty_planned,
                "qty_out": qty_out,
                # เก็บไว้ใช้ detect downtime overlap
                "_actual_start": actual_start,
                "_actual_end": actual_end,
            })

            # event timeline
            evt_list = [
                ("CREATED", None, actual_start - timedelta(minutes=5)),
                ("STARTED", None, actual_start),
            ]
            if has_pause:
                pause_ts = actual_start + batch_dur / 2
                resume_ts = pause_ts + timedelta(minutes=rng.randint(1, 3))
                evt_list.append(("PAUSED", rng.choice(PAUSE_REASONS), pause_ts))
                evt_list.append(("RESUMED", None, resume_ts))
            evt_list.append(("COMPLETED", None, actual_end))

            # microsecond uniqueness ภายใน batch (UNIQUE uq_batch_event_ts)
            seen_ts: set[datetime] = set()
            for status, reason, ts in evt_list:
                while ts in seen_ts:
                    ts = ts + timedelta(microseconds=1)
                seen_ts.add(ts)
                events.append({
                    "batch_id": batch_id,
                    "status_code": status,
                    "reason_code": reason,
                    "event_ts": ts,
                })

            # QC record (ทุก batch ที่ qty_out > 0)
            if qty_out > 0:
                qty_inspected = max(1, round(qty_out * 0.05))
                qty_failed = int(np_rng.binomial(qty_inspected, 0.03))
                qty_passed = qty_inspected - qty_failed
                inspected_at = actual_end + timedelta(minutes=rng.uniform(2, 8))
                qc_id += 1
                qc_records.append({
                    "qc_id": qc_id,
                    "batch_id": batch_id,
                    "qty_inspected": qty_inspected,
                    "qty_passed": qty_passed,
                    "qty_failed": qty_failed,
                    "inspected_at": inspected_at,
                })

                if qty_failed > 0:
                    n_def = rng.randint(1, min(3, qty_failed))
                    chosen = rng.sample(LEAF_DEFECTS, n_def)
                    qty_splits_d = split_qty(qty_failed, n_def, rng)
                    for code, qa in zip(chosen, qty_splits_d):
                        qc_defects.append({
                            "qc_id": qc_id,
                            "defect_code": code,
                            "qty_affected": qa,
                            "notes": None,
                        })

            # next batch start = current end + 2-5 min gap
            cur_start = actual_end + timedelta(minutes=rng.randint(2, 5))

    return batches, events, qc_records, qc_defects


def find_active_batch(batches, ts: datetime):
    """หา batch ที่ active ณ ts (actual_start ≤ ts ≤ actual_end)"""
    for b in batches:
        if b["_actual_start"] <= ts <= b["_actual_end"]:
            return b["batch_id"]
    return None


def gen_downtime_events(batches, start_date, end_date, rng):
    """2-3 unplanned events ใน production hours + 0-1 PLANNED_PM ใน handover gap, ต่อวัน"""
    events = []
    cur = start_date
    while cur <= end_date:
        # 2-3 unplanned ใน DAY หรือ NIGHT shift
        for _ in range(rng.randint(2, 3)):
            if rng.random() < 0.5:
                shift_start = datetime.combine(cur, DAY_SHIFT_START)
                shift_end = datetime.combine(cur, DAY_SHIFT_END)
            else:
                shift_start = datetime.combine(cur, NIGHT_SHIFT_START)
                shift_end = datetime.combine(cur + timedelta(days=1), NIGHT_SHIFT_END)
            duration = rng.randint(5, 30)
            window_sec = max(0, (shift_end - shift_start).total_seconds() - duration * 60)
            offset_sec = rng.uniform(0, window_sec)
            start_ts = shift_start + timedelta(seconds=offset_sec)
            end_ts = start_ts + timedelta(minutes=duration)
            events.append({
                "machine_id": rng.choice(MACHINES),
                "batch_id": find_active_batch(batches, start_ts),
                "reason_code": rng.choices(
                    DOWNTIME_UNPLANNED, weights=DOWNTIME_UNPLANNED_WEIGHTS, k=1)[0],
                "start_ts": start_ts,
                "end_ts": end_ts,
            })

        # 0-1 PLANNED_PM ใน handover gap (16:30-17:30 หรือ 06:30-07:30 ของ cur)
        if rng.random() < 0.5:
            if rng.random() < 0.5:
                gap_start = datetime.combine(cur, time(16, 30))
                gap_end = datetime.combine(cur, time(17, 30))
            else:
                gap_start = datetime.combine(cur, time(6, 30))
                gap_end = datetime.combine(cur, time(7, 30))
            duration = rng.randint(5, 30)
            window_sec = max(0, (gap_end - gap_start).total_seconds() - duration * 60)
            offset_sec = rng.uniform(0, window_sec)
            start_ts = gap_start + timedelta(seconds=offset_sec)
            end_ts = start_ts + timedelta(minutes=duration)
            events.append({
                "machine_id": rng.choice(MACHINES),
                "batch_id": None,  # handover gap → ไม่มี batch active
                "reason_code": "PLANNED_PM",
                "start_ts": start_ts,
                "end_ts": end_ts,
            })

        cur += timedelta(days=1)
    return events


# ---------------------------------------------------------------------------
# SQL emission
# ---------------------------------------------------------------------------
def _sql_str(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, datetime):
        # microseconds precision สำคัญ (UNIQUE uq_batch_event_ts)
        return f"'{v.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
    return "'" + str(v).replace("'", "''") + "'"


def _bulk_insert(table: str, cols: list, rows: list, out, comment: str = "") -> None:
    if not rows:
        return
    note = f" — {comment}" if comment else ""
    out.write(f"-- {len(rows)} rows -> {table}{note}\n")
    out.write(f"INSERT INTO {table} ({', '.join(cols)}) VALUES\n")
    chunks = [
        "    (" + ", ".join(_sql_str(r.get(c)) for c in cols) + ")"
        for r in rows
    ]
    out.write(",\n".join(chunks))
    out.write(";\n\n")


def emit_sql(orders, batches, events, qc_records, qc_defects, downtimes,
             window_start, window_end, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w") as out:
        out.write(
            f"-- Auto-generated by generate_mock_data.py (seed={SEED})\n"
            f"-- Mock window: {window_start} → {window_end} UTC "
            f"(aligned with InfluxDB range)\n"
            f"-- Apply AFTER 03_master_data.sql ใน transaction เดียว\n\n"
            f"BEGIN;\n\n"
        )

        _bulk_insert(
            "production_order",
            ["order_id", "model_id", "qty_ordered", "planned_start", "planned_end"],
            orders, out,
        )
        # batch ใส่ status='CREATED' + start/end=NULL — trigger จะ sync ตอน events insert
        batch_rows = [
            {
                "batch_id": b["batch_id"],
                "order_id": b["order_id"],
                "line_id": b["line_id"],
                "status_code": "CREATED",
                "qty_planned": b["qty_planned"],
                "qty_out": b["qty_out"],
                "start_time": None,
                "end_time": None,
            }
            for b in batches
        ]
        _bulk_insert(
            "production_batch",
            ["batch_id", "order_id", "line_id", "status_code",
             "qty_planned", "qty_out", "start_time", "end_time"],
            batch_rows, out,
            comment="status=CREATED, times=NULL — trigger sync ตอน events insert",
        )

        # events ต้อง insert ตามลำดับเวลาเพื่อ trigger update batch ถูก step
        events_sorted = sorted(events, key=lambda e: (e["batch_id"], e["event_ts"]))
        _bulk_insert(
            "batch_status_event",
            ["batch_id", "status_code", "reason_code", "event_ts"],
            events_sorted, out,
            comment="trigger trg_sync_batch_status sync status/start_time/end_time",
        )

        _bulk_insert(
            "qc_record",
            ["qc_id", "batch_id", "qty_inspected", "qty_passed", "qty_failed",
             "inspected_at"],
            qc_records, out,
        )
        _bulk_insert(
            "qc_defect",
            ["qc_id", "defect_code", "qty_affected", "notes"],
            qc_defects, out,
            comment="leaf defects only (FK ตาม category='LEAF')",
        )

        # downtime: ห้ามใส่ duration_min — trigger compute เอง
        _bulk_insert(
            "downtime_event",
            ["machine_id", "batch_id", "reason_code", "start_ts", "end_ts"],
            downtimes, out,
            comment="trigger trg_compute_downtime_duration คำนวณ duration_min",
        )

        out.write(
            "-- Sync sequences past explicit IDs\n"
            "SELECT setval('production_order_order_id_seq',"
            " (SELECT MAX(order_id) FROM production_order));\n"
            "SELECT setval('production_batch_batch_id_seq',"
            " (SELECT MAX(batch_id) FROM production_batch));\n"
            "SELECT setval('qc_record_qc_id_seq',"
            " (SELECT MAX(qc_id) FROM qc_record));\n"
            "SELECT setval('batch_status_event_event_id_seq',"
            " (SELECT MAX(event_id) FROM batch_status_event));\n"
            "SELECT setval('downtime_event_event_id_seq',"
            " (SELECT MAX(event_id) FROM downtime_event));\n\n"
            "COMMIT;\n"
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    rng = random.Random(SEED)
    np_rng = np.random.default_rng(SEED)

    earliest, latest = discover_influx_range()
    start_date, night_end_date, day_end_date = compute_mock_window(
        earliest, latest, MAX_WINDOW_DAYS)
    window_start = datetime.combine(start_date, DAY_SHIFT_START)
    # window end = ใหญ่สุดระหว่าง night-end (next-day 06:30) กับ day-end (16:30)
    window_end = max(
        datetime.combine(night_end_date + timedelta(days=1), NIGHT_SHIFT_END),
        datetime.combine(day_end_date, DAY_SHIFT_END),
    )

    print(f"InfluxDB range: {earliest} → {latest} UTC ({(latest-earliest).total_seconds()/3600:.1f} hr)")
    print(f"Mock window:    {window_start} → {window_end} UTC")
    print(f"Days covered:   {(day_end_date - start_date).days + 1}")

    shifts = list(iter_shifts(start_date, night_end_date, day_end_date))
    print(f"Shifts:         {len(shifts)} ({sum(1 for s in shifts if s[2]=='DAY')} DAY + "
          f"{sum(1 for s in shifts if s[2]=='NIGHT')} NIGHT)")

    orders = gen_orders(shifts, rng)
    batches, events, qc_records, qc_defects = gen_batches_and_events(orders, rng, np_rng)
    downtimes = gen_downtime_events(batches, start_date, day_end_date, rng)

    emit_sql(orders, batches, events, qc_records, qc_defects, downtimes,
             window_start, window_end, OUT_PATH)

    print(f"\nWrote {OUT_PATH}")
    print(f"  production_order    {len(orders):>5}")
    print(f"  production_batch    {len(batches):>5}")
    print(f"  batch_status_event  {len(events):>5}")
    print(f"  qc_record           {len(qc_records):>5}")
    print(f"  qc_defect           {len(qc_defects):>5}")
    print(f"  downtime_event      {len(downtimes):>5}")


if __name__ == "__main__":
    main()
