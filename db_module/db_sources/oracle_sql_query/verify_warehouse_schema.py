from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from db_module.db_conn import OracleConnector


"""ตรวจสอบว่า DW objects ครบถ้วนใน AI03 หลังรัน DDL ใหม่ (7 ไฟล์ใน query/)

EXPECTED ตรงกับโครงสร้างใหม่หลัง recreate (2026-04-26):
- 20 tables (7 DIM + 5 FACT + 8 STG)
- 9 sequences (4 DIM + 5 FACT) — DIM_DATE/DIM_SHIFT/DIM_METRIC ใช้ smart key ไม่มี SEQ
- 10 procedures + 1 function (3 sync DIM + 1 sync_all + 5 fact load + 1 load_all + FN_GET_SHIFT_ID)

Views ถูก skip ถาวร (AI03 ไม่มี CREATE VIEW privilege ใน Oracle 10gR2):
  V_BATCH_FEATURES → ใช้ inline SQL ใน app.ai.features
  V_OEE_DAILY / V_DEFECT_PARETO / V_SCHEDULE_ADHERENCE
    → ใช้ FastAPI endpoints ใน app.api.dashboard_api.dashboard

คืน exit 0 ถ้าครบ; 1 ถ้าขาด
"""


# 20 ตาราง — 7 DIM + 5 FACT + 8 STG
EXPECTED_TABLES = [
    # DIM (7)
    "DIM_DATE", "DIM_LINE", "DIM_SHIFT", "DIM_BATTERY_MODEL",
    "DIM_MACHINE", "DIM_METRIC", "DIM_DEFECT_TYPE",
    # FACT (5)
    "FACT_PRODUCTION", "FACT_QUALITY", "FACT_DEFECT",
    "FACT_DOWNTIME", "FACT_SENSOR",
    # STG (8) — 5 OLTP staging + 3 DIM source staging
    "STG_PRODUCTION_BATCH", "STG_QC_RECORD", "STG_QC_DEFECT",
    "STG_DOWNTIME_EVENT", "STG_SENSOR_AGG",
    "STG_LINE", "STG_BATTERY_MODEL", "STG_MACHINE",
]

# 9 sequences — 4 DIM + 5 FACT
# DIM_DATE/DIM_SHIFT/DIM_METRIC ไม่มี SEQ (smart key / seeded inline)
EXPECTED_SEQUENCES = [
    "SEQ_DIM_LINE", "SEQ_DIM_BATTERY_MODEL",
    "SEQ_DIM_MACHINE", "SEQ_DIM_DEFECT_TYPE",
    "SEQ_FACT_PRODUCTION", "SEQ_FACT_QUALITY",
    "SEQ_FACT_DEFECT", "SEQ_FACT_DOWNTIME", "SEQ_FACT_SENSOR",
]

# 10 procedures
EXPECTED_PROCEDURES = [
    # DIM sync (3 + 1 master)
    "SP_SYNC_DIM_LINE", "SP_SYNC_DIM_BATTERY_MODEL",
    "SP_SYNC_DIM_MACHINE", "SP_SYNC_ALL_DIMS",
    # FACT load (5 + 1 master)
    "SP_LOAD_FACT_PRODUCTION", "SP_LOAD_FACT_QUALITY",
    "SP_LOAD_FACT_DEFECT", "SP_LOAD_FACT_DOWNTIME",
    "SP_LOAD_FACT_SENSOR", "SP_LOAD_ALL_FACTS",
]

# 1 function
EXPECTED_FUNCTIONS = [
    "FN_GET_SHIFT_ID",
]


def main() -> int:
    """Query AI03 schema + เทียบกับ EXPECTED_*"""
    with OracleConnector().cursor() as cur:
        cur.execute("SELECT table_name FROM user_tables ORDER BY table_name")
        tables = {str(r[0]) for r in cur.fetchall()}

        cur.execute("SELECT sequence_name FROM user_sequences ORDER BY sequence_name")
        sequences = {str(r[0]) for r in cur.fetchall()}

        cur.execute(
            "SELECT object_name FROM user_procedures WHERE object_type = 'PROCEDURE'"
        )
        procedures = {str(r[0]) for r in cur.fetchall()}

        cur.execute(
            "SELECT object_name FROM user_procedures WHERE object_type = 'FUNCTION'"
        )
        functions = {str(r[0]) for r in cur.fetchall()}

    missing_t = [t for t in EXPECTED_TABLES if t not in tables]
    missing_s = [s for s in EXPECTED_SEQUENCES if s not in sequences]
    missing_p = [p for p in EXPECTED_PROCEDURES if p not in procedures]
    missing_f = [f for f in EXPECTED_FUNCTIONS if f not in functions]

    print(f"Tables in AI03: {len(tables)} (expected {len(EXPECTED_TABLES)})")
    for t in EXPECTED_TABLES:
        print(f"  {'✓' if t in tables else '✗'} {t}")

    print(f"\nSequences in AI03: {len(sequences)} (expected {len(EXPECTED_SEQUENCES)})")
    for s in EXPECTED_SEQUENCES:
        print(f"  {'✓' if s in sequences else '✗'} {s}")

    print(f"\nProcedures in AI03: {len(procedures)} (expected {len(EXPECTED_PROCEDURES)})")
    for p in EXPECTED_PROCEDURES:
        print(f"  {'✓' if p in procedures else '✗'} {p}")

    print(f"\nFunctions in AI03: {len(functions)} (expected {len(EXPECTED_FUNCTIONS)})")
    for f in EXPECTED_FUNCTIONS:
        print(f"  {'✓' if f in functions else '✗'} {f}")

    print("\nViews: SKIPPED (AI03 lacks CREATE VIEW privilege; views replaced by FastAPI endpoints)")

    if missing_t or missing_s or missing_p or missing_f:
        print(
            f"\nFAIL — missing: tables={missing_t}, sequences={missing_s}, "
            f"procedures={missing_p}, functions={missing_f}"
        )
        return 1
    print("\nOK — all expected objects present.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
