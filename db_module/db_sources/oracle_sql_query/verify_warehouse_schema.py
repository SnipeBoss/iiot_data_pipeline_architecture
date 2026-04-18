from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from db_module.db_conn import OracleConnector


"""ตรวจสอบว่า DW objects ครบถ้วนใน AI03 หลังรัน DDL

รันหลัง run_sql_file.py ต่อ 01_schema.sql + 02_procedure_dim_date.sql +
03_procedure_fact_loaders.sql

คืน exit 0 ถ้าครบ; 1 ถ้าขาด
"""


# 10 ตาราง (3 STG + 4 DIM + 3 FACT)
EXPECTED_TABLES = [
    "STG_PRODUCTION_BATCH", "STG_QC_RECORD", "STG_SENSOR_AGG",
    "DIM_DATE", "DIM_MACHINE", "DIM_PRODUCT", "DIM_METRIC",
    "FACT_PRODUCTION", "FACT_QUALITY", "FACT_SENSOR",
]

# 5 sequence (DIM_DATE ใช้ YYYYMMDD, DIM_METRIC seed explicit ID → ไม่ต้องมี sequence)
EXPECTED_SEQUENCES = [
    "SEQ_DIM_MACHINE", "SEQ_DIM_PRODUCT",
    "SEQ_FACT_PRODUCTION", "SEQ_FACT_QUALITY", "SEQ_FACT_SENSOR",
]

# 4 procedure
EXPECTED_PROCEDURES = [
    "SP_LOAD_DIM_DATE",
    "SP_LOAD_FACT_PRODUCTION", "SP_LOAD_FACT_QUALITY", "SP_LOAD_FACT_SENSOR",
]


def main() -> int:
    """Query AI03 schema + เทียบกับ EXPECTED_*"""
    with OracleConnector().cursor() as cur:
        cur.execute("SELECT table_name FROM user_tables ORDER BY table_name")
        tables = {str(r[0]) for r in cur.fetchall()}
        cur.execute("SELECT sequence_name FROM user_sequences ORDER BY sequence_name")
        sequences = {str(r[0]) for r in cur.fetchall()}
        cur.execute("SELECT object_name FROM user_procedures WHERE object_type = 'PROCEDURE'")
        procedures = {str(r[0]) for r in cur.fetchall()}

    missing_t = [t for t in EXPECTED_TABLES if t not in tables]
    missing_s = [s for s in EXPECTED_SEQUENCES if s not in sequences]
    missing_p = [p for p in EXPECTED_PROCEDURES if p not in procedures]

    print(f"Tables in AI03: {len(tables)}")
    for t in EXPECTED_TABLES:
        print(f"  {'✓' if t in tables else '✗'} {t}")

    print(f"\nSequences in AI03: {len(sequences)}")
    for s in EXPECTED_SEQUENCES:
        print(f"  {'✓' if s in sequences else '✗'} {s}")

    print(f"\nProcedures in AI03: {len(procedures)}")
    for p in EXPECTED_PROCEDURES:
        print(f"  {'✓' if p in procedures else '✗'} {p}")

    if missing_t or missing_s or missing_p:
        print(f"\nFAIL — missing: tables={missing_t}, sequences={missing_s}, procedures={missing_p}")
        return 1
    print("\nOK — all expected objects present.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
