"""Apply an Oracle DDL script against AI03 via OracleConnector.

Splits on top-level `;` separators, plus `/` on its own line for PL/SQL
blocks (anonymous BEGIN/END or CREATE PROCEDURE bodies). Prints each
statement's first line so progress is readable. Fails fast on first error.

Usage:
    python datasources/oracle_sql_query/apply_ddl.py [path_to.sql]

Defaults to 01_dw_ddl.sql next to this script.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from db_module.db_conn import OracleConnector  # noqa: E402


def split_statements(sql: str) -> list[str]:
    """Split a DDL script into statements.

    Rule: a line containing only `/` terminates a PL/SQL block (everything
    since the previous terminator becomes one statement). Outside of blocks,
    statements end at `;` that is the last non-whitespace char on a line.
    """
    lines = sql.splitlines()
    out: list[str] = []
    buf: list[str] = []
    in_block = False

    for raw in lines:
        stripped = raw.strip()

        # Line-only comment -> drop
        if stripped.startswith("--") and not in_block:
            continue

        # PL/SQL block start heuristic
        if not in_block and stripped.upper().startswith(("BEGIN", "DECLARE", "CREATE OR REPLACE ", "CREATE PROCEDURE", "CREATE FUNCTION", "CREATE TRIGGER")):
            in_block = True

        if in_block:
            if stripped == "/":
                stmt = "\n".join(buf).strip()
                if stmt:
                    out.append(stmt)
                buf = []
                in_block = False
                continue
            buf.append(raw)
            continue

        # Plain SQL: accumulate until a line ends with ';'
        buf.append(raw)
        if stripped.endswith(";"):
            stmt = "\n".join(buf).strip().rstrip(";").strip()
            if stmt:
                out.append(stmt)
            buf = []

    tail = "\n".join(buf).strip()
    if tail:
        out.append(tail.rstrip(";").strip())
    return out


def apply(sql_path: Path) -> int:
    sql_text = sql_path.read_text()
    statements = split_statements(sql_text)
    print(f"Loaded {len(statements)} statements from {sql_path.name}")

    connector = OracleConnector()
    conn = connector.connect()
    cur = conn.cursor()
    try:
        for i, stmt in enumerate(statements, start=1):
            first_line = stmt.splitlines()[0][:80]
            print(f"  [{i:>3}/{len(statements)}] {first_line}")
            try:
                cur.execute(stmt)
            except Exception as exc:
                print(f"\nFAIL on statement {i}:\n{stmt}\n---\n{exc}")
                conn.rollback()
                return 1
        conn.commit()
        print("\nOK — all statements applied and committed.")
        return 0
    finally:
        cur.close()
        conn.close()


def main() -> int:
    script = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / "01_dw_ddl.sql"
    if not script.exists():
        print(f"not found: {script}")
        return 2
    return apply(script)


if __name__ == "__main__":
    sys.exit(main())
