"""Supabase (cloud PostgreSQL) connector via psycopg2.

Supabase exposes standard Postgres on port 5432 — we hit it directly rather
than going through PostgREST, so the same connector is reusable from Airflow
`PostgresHook` later without rewriting.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from .._env import get, require


class SupabaseConnector:
    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        dbname: str | None = None,
        user: str | None = None,
        password: str | None = None,
        sslmode: str | None = None,
    ) -> None:
        self.host = host or require("SUPABASE_HOST")
        self.port = int(port) if port else int(get("SUPABASE_PORT", "5432"))
        self.dbname = dbname or get("SUPABASE_DB", "postgres")
        self.user = user or get("SUPABASE_USER", "postgres")
        self.password = password or require("SUPABASE_PASSWORD")
        self.sslmode = sslmode or get("SUPABASE_SSLMODE", "require")

    def connect(self):
        import psycopg2

        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            sslmode=self.sslmode,
        )

    @contextmanager
    def cursor(self) -> Iterator:
        conn = self.connect()
        try:
            cur = conn.cursor()
            try:
                yield cur
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()
        finally:
            conn.close()
