"""Helper for DAGs: call the FastAPI Oracle service instead of JDBC.

Reads `ORACLE_API_URL` and `ORACLE_API_TOKEN` from env (compose pipes them in
via env_file). All functions raise on non-2xx so the task fails loudly.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
from typing import Any, Sequence

import requests

log = logging.getLogger(__name__)

_BASE_URL = os.environ.get("ORACLE_API_URL", "http://host.docker.internal:8000")
_TOKEN = os.environ.get("ORACLE_API_TOKEN", "")
_TIMEOUT_S = int(os.environ.get("ORACLE_API_TIMEOUT", "120"))


def _headers() -> dict[str, str]:
    h = {"Content-Type": "application/json"}
    if _TOKEN:
        h["Authorization"] = f"Bearer {_TOKEN}"
    return h


def _post(path: str, body: dict) -> dict:
    url = f"{_BASE_URL}{path}"
    r = requests.post(url, json=body, headers=_headers(), timeout=_TIMEOUT_S)
    if not r.ok:
        log.error("oracle-api %s -> %s: %s", path, r.status_code, r.text[:500])
        r.raise_for_status()
    return r.json()


def health() -> dict:
    r = requests.get(f"{_BASE_URL}/health", headers=_headers(), timeout=10)
    r.raise_for_status()
    return r.json()


def as_iso(v: Any) -> Any:
    """Serialize Python datetime/date/Decimal to JSON-safe primitives."""
    if v is None:
        return None
    if isinstance(v, dt.datetime):
        return v.isoformat(sep=" ")
    if isinstance(v, dt.date):
        return v.isoformat()
    # psycopg2 returns DECIMAL as `decimal.Decimal` which `json` rejects —
    # convert to float. OK for DW loads where exact precision isn't required.
    import decimal
    if isinstance(v, decimal.Decimal):
        return float(v)
    return v


def bulk_insert(
    table: str,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    truncate: bool = True,
    pipeline_run_id: str | None = None,
) -> int:
    """Bulk-load a staging table. Returns number of rows inserted."""
    # Serialize all values
    payload_rows = [[as_iso(v) for v in row] for row in rows]
    body = {
        "table": table,
        "columns": list(columns),
        "rows": payload_rows,
        "truncate": truncate,
    }
    if pipeline_run_id is not None:
        body["pipeline_run_id"] = pipeline_run_id
    result = _post("/sql/bulk-insert", body)
    log.info("bulk_insert %s: rows=%d truncated=%s", table, result["rowcount"], result.get("truncated"))
    return int(result["rowcount"])


def call_sp(name: str, args: list[Any] | None = None) -> dict:
    body = {"name": name, "args": [as_iso(v) for v in (args or [])]}
    return _post("/sp/call", body)
