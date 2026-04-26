from __future__ import annotations
import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from db_module.db_conn import OracleConnector, SupabaseConnector


"""One-shot ETL: sync master data จาก Supabase → Oracle DIM_* (รุ่นใหม่ 2026-04-26)

Pattern (สอดคล้องกับ Airflow DAG):
    Supabase OLTP → Oracle STG_* (transient buffer, truncate-and-load)
                  → SP_SYNC_DIM_* (MERGE BY src_id เข้า DIM)

DIM ที่ sync (3 ตัว):
    DIM_LINE           ← STG_LINE           ← Supabase production_line
    DIM_BATTERY_MODEL  ← STG_BATTERY_MODEL  ← Supabase battery_model
    DIM_MACHINE        ← STG_MACHINE        ← Supabase machine

ไม่ sync ผ่านสคริปต์นี้:
    DIM_DATE         seeded ใน 04_dim_seed.sql (5-yr calendar 2024-2028)
    DIM_SHIFT        seeded ใน 04_dim_seed.sql (DAY/NIGHT)
    DIM_METRIC       seeded ใน 04_dim_seed.sql (6 metrics)
    DIM_DEFECT_TYPE  seeded ใน 04_dim_seed.sql (20 defect types)

Idempotent: STG truncate-and-load ทุกครั้ง, SP_SYNC_DIM_* ใช้ MERGE BY src_id
ทำให้ surrogate key เสถียรข้าม sync (FACT FK ไม่หาย)

การใช้งาน:
    python db_module/db_sources/oracle_sql_query/sync_dimensions_from_supabase.py
"""


SRC_SYSTEM = "SUPABASE"
PIPELINE_RUN_ID = "manual_sync_dim"


def _coerce(v):
    """แปลง Python type → JDBC-friendly equivalent

    - datetime.date / datetime → java.sql.Date / Timestamp ผ่าน jpype
      (JDBC OraclePreparedStatement ไม่มี overload สำหรับ Python datetime ตรง ๆ)
    - decimal.Decimal → float (Oracle 10g NUMERIC ไม่ต้องการ exact precision)
    Reuse pattern จาก app.api.dw_api.deps.parse_iso (แต่อ่าน datetime ตรง ไม่ใช่ ISO string)
    """
    import decimal
    import jpype
    if isinstance(v, dt.datetime):
        JTimestamp = jpype.JClass("java.sql.Timestamp")
        return JTimestamp.valueOf(v.strftime("%Y-%m-%d %H:%M:%S"))
    if isinstance(v, dt.date):
        JDate = jpype.JClass("java.sql.Date")
        return JDate.valueOf(v.isoformat())
    if isinstance(v, decimal.Decimal):
        return float(v)
    return v


def _bulk_insert(ora_cur, table: str, columns: list[str], rows: list[list]) -> int:
    """TRUNCATE + bulk INSERT เข้า STG table ผ่าน JDBC"""
    if not rows:
        return 0
    ora_cur.execute(f"TRUNCATE TABLE {table}")
    placeholders = ", ".join("?" for _ in columns)
    col_list = ", ".join(columns)
    sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
    coerced = [[_coerce(v) for v in row] for row in rows]
    ora_cur.executemany(sql, coerced)
    return len(rows)


def seed() -> int:
    sb = SupabaseConnector()
    ora = OracleConnector()

    sb_conn = sb.connect()
    sb_cur = sb_conn.cursor()
    ora_conn = ora.connect()
    ora_cur = ora_conn.cursor()

    try:
        # ============================================================
        # Step 1: Extract from Supabase + Load STG tables
        # ============================================================
        print("Step 1/2: Load STG tables from Supabase")

        # ---- STG_LINE ----
        sb_cur.execute("""
            SELECT line_id, name, area
            FROM production_line
            ORDER BY line_id
        """)
        line_rows = [
            [src_id, name, area, SRC_SYSTEM, PIPELINE_RUN_ID]
            for src_id, name, area in sb_cur.fetchall()
        ]
        n_line = _bulk_insert(
            ora_cur,
            "STG_LINE",
            ["line_id", "name", "area", "src_system", "pipeline_run_id"],
            line_rows,
        )
        print(f"  STG_LINE          loaded {n_line} rows")

        # ---- STG_BATTERY_MODEL ----
        sb_cur.execute("""
            SELECT model_id, model_code, name,
                   spec_plate_count, spec_weight_kg, spec_terminal_type,
                   dim_length_mm, dim_width_mm, dim_height_mm, is_active
            FROM battery_model
            ORDER BY model_id
        """)
        bm_rows = [
            [
                src_id, code, name,
                plate_count, weight_kg, terminal,
                length_mm, width_mm, height_mm,
                "Y" if is_active else "N",
                SRC_SYSTEM, PIPELINE_RUN_ID,
            ]
            for (
                src_id, code, name,
                plate_count, weight_kg, terminal,
                length_mm, width_mm, height_mm, is_active,
            ) in sb_cur.fetchall()
        ]
        n_bm = _bulk_insert(
            ora_cur,
            "STG_BATTERY_MODEL",
            [
                "model_id", "model_code", "name",
                "spec_plate_count", "spec_weight_kg", "spec_terminal_type",
                "dim_length_mm", "dim_width_mm", "dim_height_mm", "is_active",
                "src_system", "pipeline_run_id",
            ],
            bm_rows,
        )
        print(f"  STG_BATTERY_MODEL loaded {n_bm} rows")

        # ---- STG_MACHINE ----
        # Supabase machine ไม่มี is_active column → set 'Y' default
        sb_cur.execute("""
            SELECT machine_id, line_id, machine_code, machine_type,
                   sequence_position, install_date
            FROM machine
            ORDER BY machine_id
        """)
        m_rows = [
            [
                src_id, line_id, code, mtype,
                seq_pos, install_dt,
                "Y",
                SRC_SYSTEM, PIPELINE_RUN_ID,
            ]
            for (
                src_id, line_id, code, mtype,
                seq_pos, install_dt,
            ) in sb_cur.fetchall()
        ]
        n_m = _bulk_insert(
            ora_cur,
            "STG_MACHINE",
            [
                "machine_id", "line_id", "machine_code", "machine_type",
                "sequence_position", "install_date", "is_active",
                "src_system", "pipeline_run_id",
            ],
            m_rows,
        )
        print(f"  STG_MACHINE       loaded {n_m} rows")

        ora_conn.commit()

        # ============================================================
        # Step 2: Run SP_SYNC_ALL_DIMS (orchestrator: LINE → MODEL → MACHINE)
        # ============================================================
        print("\nStep 2/2: Run SP_SYNC_ALL_DIMS (MERGE STG → DIM)")
        ora_cur.execute("BEGIN SP_SYNC_ALL_DIMS; END;")
        ora_conn.commit()
        print("  SP_SYNC_ALL_DIMS  ok")

        # ============================================================
        # Readback
        # ============================================================
        print("\nOracle DIM readback:")
        for t in (
            "DIM_LINE", "DIM_BATTERY_MODEL", "DIM_MACHINE",
            "DIM_DATE", "DIM_SHIFT", "DIM_METRIC", "DIM_DEFECT_TYPE",
        ):
            ora_cur.execute(f"SELECT COUNT(*) FROM {t}")
            print(f"  {t:<18} {int(ora_cur.fetchone()[0])}")
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
