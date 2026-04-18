"""Probe the Oracle endpoint.

Three layers tested:
1. HTTP reachability of iSQL*Plus.
2. TCP listener reachability on 1521.
3. Direct DB connection via the project's OracleConnector (JDBC thin driver).

Credentials come from `.env` at the repo root — see `.env.example`. The
server is Oracle 10.2.0.3, which is too old for python-oracledb thin mode
and most ARM64 Instant Clients, so JDBC is the portable path.
"""

from __future__ import annotations

import socket
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests  # noqa: E402

from db_module.db_conn import OracleConnector  # noqa: E402
from db_module.db_conn._env import ConfigError, require  # noqa: E402

ISQLPLUS_URL_TEMPLATE = "http://{host}:5560/isqlplus/workspace.uix"


def check_tcp(host: str, port: int, timeout: float = 3.0) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        try:
            sock.connect((host, port))
            return True
        except OSError:
            return False


def check_isqlplus(host: str) -> None:
    url = ISQLPLUS_URL_TEMPLATE.format(host=host)
    print(f"[1] HTTP probe: {url}")
    try:
        resp = requests.get(url, timeout=10)
        print(
            f"    status={resp.status_code} "
            f"content-type={resp.headers.get('Content-Type')}"
        )
    except requests.RequestException as exc:
        print(f"    GET failed: {exc}")


def check_tcp_layer(host: str, port: int) -> bool:
    print(f"\n[2] TCP probe {host}:{port}")
    ok = check_tcp(host, port)
    print(f"    {'OPEN' if ok else 'closed/filtered'}")
    return ok


def check_jdbc(connector: OracleConnector) -> None:
    print(f"\n[3] JDBC connect (driver={connector.jdbc_jar.name})")
    print(f"    url: {connector.jdbc_url}")
    try:
        with connector.cursor() as cur:
            cur.execute(
                "SELECT SYSDATE, USER, "
                "(SELECT banner FROM v$version WHERE ROWNUM=1) "
                "FROM DUAL"
            )
            row = cur.fetchone()
        print(f"    OK sysdate={row[0]} user={row[1]}")
        print(f"    banner={row[2]}")
    except Exception as exc:
        msg = str(exc).splitlines()[0][:300]
        print(f"    FAIL: {msg}")


def main() -> int:
    try:
        host = require("ORACLE_HOST")
        port = int(require("ORACLE_PORT"))
        connector = OracleConnector()
    except ConfigError as exc:
        print(f"config error: {exc}")
        return 2

    check_isqlplus(host)
    if check_tcp_layer(host, port):
        check_jdbc(connector)
    return 0


if __name__ == "__main__":
    sys.exit(main())
