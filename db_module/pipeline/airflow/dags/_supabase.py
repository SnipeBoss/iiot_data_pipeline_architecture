"""Thin psycopg2 wrapper for DAGs. Reads SUPABASE_* env configured via compose.

Supabase's direct-connection hostname (`db.<ref>.supabase.co`) is IPv6-only.
The compose network has `enable_ipv6: true` so psycopg2's default
hostname-to-IP resolution works.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

import psycopg2


@contextmanager
def supabase_cursor() -> Iterator:
    conn = psycopg2.connect(
        host=os.environ["SUPABASE_HOST"],
        port=int(os.environ.get("SUPABASE_PORT", "5432")),
        dbname=os.environ.get("SUPABASE_DB", "postgres"),
        user=os.environ.get("SUPABASE_USER", "postgres"),
        password=os.environ["SUPABASE_PASSWORD"],
        sslmode=os.environ.get("SUPABASE_SSLMODE", "require"),
    )
    try:
        cur = conn.cursor()
        try:
            yield cur
        finally:
            cur.close()
    finally:
        conn.close()
