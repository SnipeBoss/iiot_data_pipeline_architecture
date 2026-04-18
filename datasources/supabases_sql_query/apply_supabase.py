"""Apply Supabase SQL files in order against the configured SUPABASE_* env.

Executes each file as a single multi-statement query — psycopg2 forwards the
whole string to the server, which parses and runs sequentially inside one
transaction. Fails fast on first error with full rollback.

Usage:
    python datasources/supabases_sql_query/apply_supabase.py

Default order:
    query/01_schema.sql
    query/02_master_data.sql
    mock/03_mock_data.sql

Override with CLI args: any number of SQL paths applied in listed order.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from db_module.db_conn import SupabaseConnector  # noqa: E402

HERE = Path(__file__).parent
DEFAULT_FILES = [
    HERE / "query" / "01_schema.sql",
    HERE / "query" / "02_master_data.sql",
    HERE / "mock" / "03_mock_data.sql",
]


def apply_all(paths: list[Path]) -> int:
    for p in paths:
        if not p.exists():
            print(f"not found: {p}")
            return 2

    connector = SupabaseConnector()
    conn = connector.connect()
    cur = conn.cursor()
    try:
        for path in paths:
            sql = path.read_text()
            print(f"[apply] {path.relative_to(HERE.parent.parent)} ({len(sql):,} bytes)")
            cur.execute(sql)
        conn.commit()
        print("\nOK — all files applied and committed.")

        # Quick row-count check
        print("\nRow counts:")
        for table in (
            "production_line", "machine", "process_stage", "product",
            "raw_material", "bill_of_material", "supplier",
            "inventory", "production_order", "production_batch",
            "finished_good", "material_consumption",
            "qc_inspection", "qc_result", "maintenance_log",
            "raw_material_po", "raw_material_receipt",
        ):
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            print(f"  {table:<24} {cur.fetchone()[0]:>6}")
        return 0
    except Exception as exc:
        conn.rollback()
        print(f"\nFAIL: {exc}")
        return 1
    finally:
        cur.close()
        conn.close()


def main() -> int:
    args = [Path(a) for a in sys.argv[1:]]
    paths = args if args else DEFAULT_FILES
    return apply_all(paths)


if __name__ == "__main__":
    sys.exit(main())
