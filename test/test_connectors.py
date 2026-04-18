"""Pytest smoke tests for the three connectors.

Each test skips cleanly if its required env vars are missing/empty, so this
suite always passes in a half-configured environment. Once credentials are
filled into `.env`, tests light up automatically.
"""

from __future__ import annotations

import os

import pytest

from db_module.db_conn import InfluxConnector, OracleConnector, SupabaseConnector


def _skip_if_missing(*names: str) -> None:
    missing = [n for n in names if not os.getenv(n)]
    if missing:
        pytest.skip(f"env vars not set: {', '.join(missing)}")


def test_oracle_connector_roundtrip():
    _skip_if_missing("ORACLE_HOST", "ORACLE_USER", "ORACLE_PASSWORD")
    conn = OracleConnector()
    with conn.cursor() as cur:
        cur.execute("SELECT SYSDATE, USER FROM DUAL")
        row = cur.fetchone()
    assert str(row[1]).upper() == conn.user.upper()


def test_supabase_connector_roundtrip():
    _skip_if_missing("SUPABASE_HOST", "SUPABASE_PASSWORD")
    conn = SupabaseConnector()
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone()[0] == 1


def test_influx_connector_query():
    _skip_if_missing("INFLUX_URL", "INFLUX_TOKEN")
    conn = InfluxConnector()
    flux = f'from(bucket:"{conn.bucket}") |> range(start: -5m) |> limit(n:1)'
    tables = conn.query(flux)
    # Just asserting the query shape executes; empty bucket is still a pass.
    assert isinstance(tables, list)
