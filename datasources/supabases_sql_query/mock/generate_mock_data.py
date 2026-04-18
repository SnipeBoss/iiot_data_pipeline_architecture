"""Generate 30 days of mock OLTP rows matching CLAUDE.md §5 targets.

Deterministic via a fixed seed so counts and values reproduce exactly.
Writes SQL INSERT statements to 03_mock_data.sql alongside this script.

Row-count targets (CLAUDE.md §5):
    production_batch       540
    finished_good        6,114
    material_consumption 1,080
    qc_inspection          540
    qc_result            6,300
    maintenance_log         15

These six tables sum to 14,589 — the headline total. Orders / POs / receipts
are generated at small counts so FK references resolve but aren't called out
in the spec.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from pathlib import Path

SEED = 42
START_DATE = datetime(2026, 3, 19)
DAYS = 30
END_DATE = START_DATE + timedelta(days=DAYS)

# Master-data references (must match 02_master_data.sql)
PRODUCTS = [1, 2, 3]
MATERIALS = [1, 2, 3, 4, 5]
SUPPLIERS = [1, 2, 3, 4]
MACHINES = [1, 2, 3]
STAGES = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
INSTRUMENTED_STAGES = [1, 5, 8]
STAGE_WEIGHTS = [3, 1, 1, 1, 3, 1, 1, 3, 1, 1]  # bias toward instrumented
LINE_ID = 1

# Tuning knobs for per-batch generation
BATCHES_PER_DAY = 18
QTY_PER_BATCH_MIN = 9
QTY_PER_BATCH_MAX = 14  # avg 11.5 → ~6210 FG over 540 batches
MATERIALS_PER_BATCH = 2  # 540 × 2 = 1080 material_consumption rows
QC_SAMPLES_PER_INSPECTION_MIN = 2
QC_SAMPLES_PER_INSPECTION_MAX = 4  # 4 params × avg 2.9 samples ≈ 6300 results
QC_PARAMETERS = ["voltage", "capacity", "internal_resistance", "weight"]
QC_SPECS = {
    "voltage":             (12.4, 12.8),
    "capacity":            (58.0, 62.0),
    "internal_resistance": (0.0, 8.0),
    "weight":              (14.5, 15.5),
}
DEFECT_RATE = 0.035   # 3.5 % — middle of CLAUDE.md's 2-5 % band
DOWNTIME_COUNT = 15
MAINT_TYPES = ["BREAKDOWN", "PREVENTIVE", "CHANGEOVER"]
ISSUE_CODES = ["M01", "E02", "O03"]

OUT_PATH = Path(__file__).parent / "03_mock_data.sql"


# -----------------------------------------------------------------------------
# SQL helpers
# -----------------------------------------------------------------------------


def _sql_str(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, datetime):
        return f"'{v.strftime('%Y-%m-%d %H:%M:%S')}'"
    return "'" + str(v).replace("'", "''") + "'"


def _bulk_insert(table: str, cols: list[str], rows: list[tuple], out) -> None:
    if not rows:
        return
    out.write(f"-- {len(rows)} rows -> {table}\n")
    out.write(f"INSERT INTO {table} ({', '.join(cols)}) VALUES\n")
    chunks = []
    for row in rows:
        chunks.append("    (" + ", ".join(_sql_str(v) for v in row) + ")")
    out.write(",\n".join(chunks))
    out.write(";\n\n")


# -----------------------------------------------------------------------------
# Generators
# -----------------------------------------------------------------------------


def gen_production_orders():
    orders = []
    total_qty_remaining = BATCHES_PER_DAY * DAYS * (QTY_PER_BATCH_MIN + QTY_PER_BATCH_MAX) // 2
    order_id = 1
    d = START_DATE
    while d < END_DATE and total_qty_remaining > 0:
        product_id = random.choice(PRODUCTS)
        qty = min(random.randint(150, 400), total_qty_remaining)
        orders.append((
            order_id,
            product_id,
            qty,
            random.choice(["HIGH", "NORMAL", "NORMAL", "LOW"]),
            d.date(),
            (d + timedelta(days=random.randint(3, 10))).date(),
            "IN_PROGRESS",
        ))
        total_qty_remaining -= qty
        order_id += 1
        d += timedelta(days=random.uniform(0.6, 1.5))
    return orders


def gen_production_batches(order_ids):
    batches = []
    finished_goods = []
    material_consumption = []
    qc_inspections = []
    qc_results = []

    batch_id = 1
    fg_id = 1
    mc_id = 1
    qc_id = 1
    qr_id = 1

    for day in range(DAYS):
        day_start = START_DATE + timedelta(days=day)
        for _ in range(BATCHES_PER_DAY):
            stage_id = random.choices(STAGES, weights=STAGE_WEIGHTS, k=1)[0]
            started = day_start + timedelta(
                hours=random.uniform(0, 22),
                minutes=random.uniform(0, 59),
            )
            duration = timedelta(minutes=random.uniform(30, 180))
            completed = started + duration
            qty = random.randint(QTY_PER_BATCH_MIN, QTY_PER_BATCH_MAX)
            order_id = random.choice(order_ids)

            batches.append((
                batch_id, order_id, LINE_ID, stage_id,
                started, completed, qty,
            ))

            # finished_good rows — one per produced unit
            for unit in range(qty):
                is_defect = random.random() < DEFECT_RATE
                status = "FAIL" if is_defect else "PASS"
                serial = f"SN-{batch_id:05d}-{unit:03d}"
                produced_at = completed - timedelta(seconds=random.randint(0, int(duration.total_seconds())))
                finished_goods.append((fg_id, batch_id, serial, produced_at, status))
                fg_id += 1

            # material_consumption — pick N distinct materials per batch
            chosen = random.sample(MATERIALS, k=MATERIALS_PER_BATCH)
            for material_id in chosen:
                qty_used = round(random.uniform(0.5, 15.0), 4)
                material_consumption.append((
                    mc_id, batch_id, material_id, qty_used, completed,
                ))
                mc_id += 1

            # qc_inspection — one per batch
            sample_qty = random.randint(QC_SAMPLES_PER_INSPECTION_MIN, QC_SAMPLES_PER_INSPECTION_MAX)
            inspected_at = completed + timedelta(minutes=random.randint(5, 60))
            qc_inspections.append((
                qc_id, batch_id, stage_id, sample_qty, inspected_at,
            ))

            # qc_result — sample_qty × 4 params
            for _sample in range(sample_qty):
                for param in QC_PARAMETERS:
                    lo, hi = QC_SPECS[param]
                    # Most values in-spec; small chance of out-of-spec.
                    if random.random() < DEFECT_RATE:
                        # Out-of-spec: pick outside the band.
                        if random.random() < 0.5:
                            measured = round(lo - random.uniform(0.01, (hi - lo) * 0.1), 4)
                        else:
                            measured = round(hi + random.uniform(0.01, (hi - lo) * 0.1), 4)
                        pass_fail = "FAIL"
                    else:
                        measured = round(random.uniform(lo, hi), 4)
                        pass_fail = "PASS"
                    qc_results.append((
                        qr_id, qc_id, param, measured, lo, hi, pass_fail,
                    ))
                    qr_id += 1

            qc_id += 1
            batch_id += 1

    return batches, finished_goods, material_consumption, qc_inspections, qc_results


def gen_maintenance():
    logs = []
    for log_id in range(1, DOWNTIME_COUNT + 1):
        machine_id = random.choice(MACHINES)
        event_type = random.choice(MAINT_TYPES)
        started = START_DATE + timedelta(
            days=random.uniform(0, DAYS - 0.5),
            hours=random.uniform(0, 23),
        )
        downtime = random.randint(10, 120)
        ended = started + timedelta(minutes=downtime)
        logs.append((
            log_id, machine_id, event_type,
            started, ended, downtime, random.choice(ISSUE_CODES),
        ))
    return logs


def gen_procurement():
    pos = []
    receipts = []
    po_id = 1
    receipt_id = 1
    for _ in range(20):
        material_id = random.choice(MATERIALS)
        supplier_id = random.choice(SUPPLIERS)
        qty = round(random.uniform(200, 2000), 3)
        order_date = START_DATE.date() + timedelta(days=random.randint(0, DAYS - 1))
        expected_date = order_date + timedelta(days=random.randint(3, 14))
        status = random.choice(["PENDING", "CONFIRMED", "SHIPPED", "RECEIVED"])
        pos.append((po_id, material_id, supplier_id, qty, order_date, expected_date, status))

        if status == "RECEIVED":
            # Possibly two partial receipts
            received = round(qty * random.uniform(0.9, 1.0), 3)
            receipts.append((receipt_id, po_id, received, expected_date))
            receipt_id += 1
            if random.random() < 0.3:
                receipts.append((receipt_id, po_id, round(qty - received, 3), expected_date + timedelta(days=2)))
                receipt_id += 1
        po_id += 1
    return pos, receipts


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


def main() -> None:
    random.seed(SEED)

    orders = gen_production_orders()
    order_ids = [o[0] for o in orders]
    batches, fgs, mcs, qcis, qcrs = gen_production_batches(order_ids)
    maintenance = gen_maintenance()
    pos, receipts = gen_procurement()

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w") as out:
        out.write(
            f"-- Auto-generated by generate_mock_data.py (seed={SEED}).\n"
            f"-- Window: {START_DATE.date()} to {END_DATE.date()} ({DAYS} days).\n"
            f"-- Apply AFTER 02_master_data.sql. Wrapped in a single transaction.\n\n"
            f"BEGIN;\n\n"
        )

        _bulk_insert(
            "production_order",
            ["order_id", "product_id", "qty_ordered", "priority",
             "scheduled_start", "scheduled_end", "status"],
            orders, out,
        )
        _bulk_insert(
            "production_batch",
            ["batch_id", "order_id", "line_id", "stage_id",
             "started_at", "completed_at", "qty_produced"],
            batches, out,
        )
        _bulk_insert(
            "finished_good",
            ["fg_id", "batch_id", "serial_no", "produced_at", "qc_status"],
            fgs, out,
        )
        _bulk_insert(
            "material_consumption",
            ["consumption_id", "batch_id", "material_id", "qty_used", "consumed_at"],
            mcs, out,
        )
        _bulk_insert(
            "qc_inspection",
            ["qc_id", "batch_id", "stage_id", "sample_qty", "inspected_at"],
            qcis, out,
        )
        _bulk_insert(
            "qc_result",
            ["result_id", "qc_id", "parameter", "measured_value",
             "spec_min", "spec_max", "pass_fail"],
            qcrs, out,
        )
        _bulk_insert(
            "maintenance_log",
            ["log_id", "machine_id", "type", "started_at", "ended_at",
             "downtime_min", "issue_code"],
            maintenance, out,
        )
        _bulk_insert(
            "raw_material_po",
            ["po_id", "material_id", "supplier_id", "qty_ordered",
             "order_date", "expected_date", "status"],
            pos, out,
        )
        _bulk_insert(
            "raw_material_receipt",
            ["receipt_id", "po_id", "qty_received", "received_date"],
            receipts, out,
        )

        # Bump SERIAL sequences past the hardcoded IDs.
        out.write(
            "-- Sync sequences past explicit IDs.\n"
            "SELECT setval('production_order_order_id_seq',            (SELECT MAX(order_id)        FROM production_order));\n"
            "SELECT setval('production_batch_batch_id_seq',            (SELECT MAX(batch_id)        FROM production_batch));\n"
            "SELECT setval('finished_good_fg_id_seq',                  (SELECT MAX(fg_id)           FROM finished_good));\n"
            "SELECT setval('material_consumption_consumption_id_seq',  (SELECT MAX(consumption_id)  FROM material_consumption));\n"
            "SELECT setval('qc_inspection_qc_id_seq',                  (SELECT MAX(qc_id)           FROM qc_inspection));\n"
            "SELECT setval('qc_result_result_id_seq',                  (SELECT MAX(result_id)       FROM qc_result));\n"
            "SELECT setval('maintenance_log_log_id_seq',               (SELECT MAX(log_id)          FROM maintenance_log));\n"
            "SELECT setval('raw_material_po_po_id_seq',                (SELECT MAX(po_id)           FROM raw_material_po));\n"
            "SELECT setval('raw_material_receipt_receipt_id_seq',      (SELECT MAX(receipt_id)      FROM raw_material_receipt));\n\n"
            "COMMIT;\n"
        )

    core = len(batches) + len(fgs) + len(mcs) + len(qcis) + len(qcrs) + len(maintenance)
    print(f"Wrote {OUT_PATH}")
    print(f"  production_order        {len(orders):>6}")
    print(f"  production_batch        {len(batches):>6}   (target 540)")
    print(f"  finished_good           {len(fgs):>6}   (target 6,114)")
    print(f"  material_consumption    {len(mcs):>6}   (target 1,080)")
    print(f"  qc_inspection           {len(qcis):>6}   (target 540)")
    print(f"  qc_result               {len(qcrs):>6}   (target 6,300)")
    print(f"  maintenance_log         {len(maintenance):>6}   (target 15)")
    print(f"  raw_material_po         {len(pos):>6}")
    print(f"  raw_material_receipt    {len(receipts):>6}")
    print(f"  -- core total (6 tables): {core:>6}   (target 14,589)")


if __name__ == "__main__":
    main()
