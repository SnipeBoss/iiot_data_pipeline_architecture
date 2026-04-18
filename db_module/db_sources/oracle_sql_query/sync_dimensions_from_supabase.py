from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from db_module.db_conn import OracleConnector, SupabaseConnector


"""One-shot ETL: sync master data จาก Supabase → Oracle DIM_*

DIM ที่ sync:
  DIM_MACHINE  ← machine + production_line (denormalize line_name)
  DIM_PRODUCT  ← product

DIM_DATE sync ครั้งเดียวผ่าน SP_LOAD_DIM_DATE (ไม่ใช้สคริปต์นี้)
DIM_METRIC seed ที่ 01_schema.sql (master data fixed ตาม NodeRED)

Idempotent: DELETE ทั้งตารางก่อน INSERT ใหม่
(FACT ต้อง empty หรือ truncate ก่อน เพราะ FK ชี้ DIM)

การใช้งาน:
    python db_module/db_sources/oracle_sql_query/sync_dimensions_from_supabase.py
"""


def seed() -> int:
    sb = SupabaseConnector()
    ora = OracleConnector()

    sb_conn = sb.connect()
    sb_cur = sb_conn.cursor()
    ora_conn = ora.connect()
    ora_cur = ora_conn.cursor()

    try:
        # ---- DIM_MACHINE ----
        # JOIN กับ production_line เพื่อ denormalize line_name
        sb_cur.execute("""
            SELECT m.machine_id, m.name, l.name
            FROM machine m
            LEFT JOIN production_line l ON m.line_id = l.line_id
            ORDER BY m.machine_id
        """)
        machines = sb_cur.fetchall()
        ora_cur.execute("DELETE FROM DIM_MACHINE")
        for src_id, name, line_name in machines:
            ora_cur.execute("""
                INSERT INTO DIM_MACHINE
                    (machine_id, machine_src_id, machine_name, line_name)
                VALUES (SEQ_DIM_MACHINE.NEXTVAL, ?, ?, ?)
            """, [src_id, name, line_name])
        print(f"  DIM_MACHINE  loaded {len(machines)} rows")

        # ---- DIM_PRODUCT ----
        sb_cur.execute("""
            SELECT product_id, name FROM product ORDER BY product_id
        """)
        products = sb_cur.fetchall()
        ora_cur.execute("DELETE FROM DIM_PRODUCT")
        for src_id, name in products:
            ora_cur.execute("""
                INSERT INTO DIM_PRODUCT
                    (product_id, product_src_id, product_name)
                VALUES (SEQ_DIM_PRODUCT.NEXTVAL, ?, ?)
            """, [src_id, name])
        print(f"  DIM_PRODUCT  loaded {len(products)} rows")

        ora_conn.commit()

        # Readback
        print("\nOracle dim readback:")
        for t in ("DIM_MACHINE", "DIM_PRODUCT", "DIM_METRIC", "DIM_DATE"):
            ora_cur.execute(f"SELECT COUNT(*) FROM {t}")
            print(f"  {t:<13} {int(ora_cur.fetchone()[0])}")
        return 0
    except Exception as exc:
        ora_conn.rollback()
        print(f"\nFAIL: {exc}")
        return 1
    finally:
        ora_cur.close()
        ora_conn.close()
        sb_cur.close()
        sb_conn.close()


if __name__ == "__main__":
    sys.exit(seed())
