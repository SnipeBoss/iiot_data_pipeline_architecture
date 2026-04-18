"""Sanity-check that all expected DW objects exist under AI03 after DDL apply."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from db_module.db_conn import OracleConnector  # noqa: E402

EXPECTED_TABLES = [
    "STG_PRODUCTION_BATCH", "STG_QC_INSPECTION", "STG_QC_RESULT",
    "STG_MAINTENANCE_LOG", "STG_SENSOR_AGG",
    "DIM_DATE", "DIM_MACHINE", "DIM_PRODUCT", "DIM_STAGE", "DIM_MATERIAL",
    "FACT_OEE", "FACT_PRODUCTION", "FACT_QUALITY",
    "FACT_INVENTORY", "FACT_MAINTENANCE",
]
EXPECTED_SEQUENCES = [
    "SEQ_DIM_MACHINE", "SEQ_DIM_PRODUCT", "SEQ_DIM_STAGE", "SEQ_DIM_MATERIAL",
    "SEQ_FACT_OEE", "SEQ_FACT_PRODUCTION", "SEQ_FACT_QUALITY",
    "SEQ_FACT_INVENTORY", "SEQ_FACT_MAINTENANCE",
]


def main() -> int:
    with OracleConnector().cursor() as cur:
        cur.execute("SELECT table_name FROM user_tables ORDER BY table_name")
        tables = {str(r[0]) for r in cur.fetchall()}
        cur.execute("SELECT sequence_name FROM user_sequences ORDER BY sequence_name")
        sequences = {str(r[0]) for r in cur.fetchall()}

    missing_t = [t for t in EXPECTED_TABLES if t not in tables]
    missing_s = [s for s in EXPECTED_SEQUENCES if s not in sequences]

    print(f"tables in AI03: {len(tables)}")
    for t in EXPECTED_TABLES:
        flag = "✓" if t in tables else "✗"
        print(f"  {flag} {t}")

    print(f"\nsequences in AI03: {len(sequences)}")
    for s in EXPECTED_SEQUENCES:
        flag = "✓" if s in sequences else "✗"
        print(f"  {flag} {s}")

    if missing_t or missing_s:
        print(f"\nFAIL — missing tables: {missing_t}, missing sequences: {missing_s}")
        return 1
    print("\nOK — all expected objects present.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
