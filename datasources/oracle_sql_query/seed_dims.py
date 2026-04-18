"""Seed DIM_MACHINE / DIM_PRODUCT / DIM_STAGE / DIM_MATERIAL from Supabase.

One-shot ETL. Reads master data from Supabase (`machine`, `product`,
`process_stage`, `raw_material`), inserts into Oracle AI03 dims with
surrogate keys from each `SEQ_DIM_*` sequence. Source IDs stored in the
`*_src_id` column so the Phase-5 fact loaders can reverse-lookup.

Idempotent: DELETEs the target dim before loading (FKs from fact tables are
fine here because facts are empty at this phase).

Run:
    python datasources/oracle_sql_query/seed_dims.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from db_module.db_conn import OracleConnector, SupabaseConnector  # noqa: E402


def fetch_supabase(sb_cur, sql: str) -> list[tuple]:
    sb_cur.execute(sql)
    return sb_cur.fetchall()


def seed() -> int:
    sb = SupabaseConnector()
    ora = OracleConnector()

    sb_conn = sb.connect()
    sb_cur = sb_conn.cursor()
    ora_conn = ora.connect()
    ora_cur = ora_conn.cursor()

    try:
        # ---- DIM_MACHINE ----
        machines = fetch_supabase(sb_cur, """
            SELECT m.machine_id, m.name, m.type, l.name, m.ideal_cycle_sec
            FROM machine m
            LEFT JOIN production_line l ON m.line_id = l.line_id
            ORDER BY m.machine_id
        """)
        ora_cur.execute("DELETE FROM DIM_MACHINE")
        for src_id, name, mtype, line_name, ideal in machines:
            ora_cur.execute("""
                INSERT INTO DIM_MACHINE
                    (machine_id, machine_src_id, machine_name, machine_type,
                     line_name, ideal_cycle_sec)
                VALUES (SEQ_DIM_MACHINE.NEXTVAL, ?, ?, ?, ?, ?)
            """, [src_id, name, mtype, line_name, ideal])
        print(f"  DIM_MACHINE   loaded {len(machines)} rows")

        # ---- DIM_PRODUCT ----
        products = fetch_supabase(sb_cur, """
            SELECT product_id, sku, name, voltage_v, capacity_ah
            FROM product ORDER BY product_id
        """)
        ora_cur.execute("DELETE FROM DIM_PRODUCT")
        for src_id, sku, name, voltage, cap in products:
            ora_cur.execute("""
                INSERT INTO DIM_PRODUCT
                    (product_id, product_src_id, sku, product_name,
                     voltage_v, capacity_ah)
                VALUES (SEQ_DIM_PRODUCT.NEXTVAL, ?, ?, ?, ?, ?)
            """, [src_id, sku, name, voltage, cap])
        print(f"  DIM_PRODUCT   loaded {len(products)} rows")

        # ---- DIM_STAGE ----
        stages = fetch_supabase(sb_cur, """
            SELECT s.stage_id, s.name, s.sequence, m.name, s.ideal_cycle_sec
            FROM process_stage s
            LEFT JOIN machine m ON s.machine_id = m.machine_id
            ORDER BY s.sequence
        """)
        ora_cur.execute("DELETE FROM DIM_STAGE")
        for src_id, name, seq, machine_name, ideal in stages:
            ora_cur.execute("""
                INSERT INTO DIM_STAGE
                    (stage_id, stage_src_id, stage_name, sequence_no,
                     machine_name, ideal_cycle_sec)
                VALUES (SEQ_DIM_STAGE.NEXTVAL, ?, ?, ?, ?, ?)
            """, [src_id, name, seq, machine_name, ideal])
        print(f"  DIM_STAGE     loaded {len(stages)} rows")

        # ---- DIM_MATERIAL ----
        materials = fetch_supabase(sb_cur, """
            SELECT material_id, name, type, unit, hazard_class
            FROM raw_material ORDER BY material_id
        """)
        ora_cur.execute("DELETE FROM DIM_MATERIAL")
        for src_id, name, mtype, unit, hazard in materials:
            ora_cur.execute("""
                INSERT INTO DIM_MATERIAL
                    (material_id, material_src_id, material_name, material_type,
                     unit, hazard_class)
                VALUES (SEQ_DIM_MATERIAL.NEXTVAL, ?, ?, ?, ?, ?)
            """, [src_id, name, mtype, unit, hazard])
        print(f"  DIM_MATERIAL  loaded {len(materials)} rows")

        ora_conn.commit()

        # Readback
        print("\nOracle dim readback:")
        for t in ("DIM_MACHINE", "DIM_PRODUCT", "DIM_STAGE", "DIM_MATERIAL"):
            ora_cur.execute(f"SELECT COUNT(*) FROM {t}")
            print(f"  {t:<14} {int(ora_cur.fetchone()[0])}")
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
