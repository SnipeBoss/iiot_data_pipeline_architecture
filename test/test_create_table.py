"""Smoke-test write access: CREATE / INSERT / SELECT / DROP a temp table.

Uses the project's OracleConnector (JDBC). The table name is prefixed with
the connected user so it doesn't collide with other workspaces on the shared
DB. Cleanup always runs in `finally` so a failed run leaves no stale objects.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db_module.db_conn import OracleConnector  # noqa: E402
from db_module.db_conn._env import ConfigError  # noqa: E402


def drop_if_exists(cur, table: str) -> None:
    try:
        cur.execute(f"DROP TABLE {table} PURGE")
        print(f"    dropped existing {table}")
    except Exception as exc:
        if "ORA-00942" in str(exc):
            return
        raise


def main() -> int:
    try:
        connector = OracleConnector()
    except ConfigError as exc:
        print(f"config error: {exc}")
        return 2

    table = f"{connector.user}_CONN_TEST"
    print(f"connecting to {connector.jdbc_url} as {connector.user}")

    conn = connector.connect()
    cur = conn.cursor()
    try:
        print(f"[1] cleanup stale {table} (if any)")
        drop_if_exists(cur, table)

        print(f"[2] CREATE TABLE {table}")
        cur.execute(
            f"CREATE TABLE {table} ("
            "  id NUMBER(10) PRIMARY KEY,"
            "  note VARCHAR2(200),"
            "  created_at DATE DEFAULT SYSDATE"
            ")"
        )

        print("[3] INSERT rows")
        cur.execute(
            f"INSERT INTO {table} (id, note) VALUES (?, ?)",
            [1, "hello from test_create_table.py"],
        )
        cur.execute(
            f"INSERT INTO {table} (id, note) VALUES (?, ?)",
            [2, "row two"],
        )
        conn.commit()

        print("[4] SELECT back")
        cur.execute(f"SELECT id, note, created_at FROM {table} ORDER BY id")
        for row in cur.fetchall():
            print(f"    {row}")

        print(f"[5] DROP TABLE {table}")
        cur.execute(f"DROP TABLE {table} PURGE")

        print("\nOK — write access confirmed")
        return 0
    except Exception as exc:
        print(f"\nFAIL: {exc}")
        print(f"attempting cleanup of {table}")
        try:
            drop_if_exists(cur, table)
        except Exception as cleanup_exc:
            print(f"cleanup also failed: {cleanup_exc}")
        return 1
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
