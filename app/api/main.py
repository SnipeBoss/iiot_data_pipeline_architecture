"""Oracle API service — thin HTTP wrapper around OracleConnector (JDBC).

Purpose: keep JDBC / Java out of the Airflow container. Airflow DAGs call
this service over HTTP with plain `requests`; this service talks to the
KMITL Oracle 10g box via JayDeBeApi + ojdbc8.jar.

Auth: shared-secret bearer token (`ORACLE_API_TOKEN`). Leave blank to
disable auth in local dev.

Run locally:
    export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
    .venv/bin/uvicorn app.api.main:app --reload --port 8000
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel, Field

from db_module.db_conn import OracleConnector
from db_module.db_conn._env import get as env_get

app = FastAPI(
    title="Oracle Service API",
    description="HTTP wrapper around the KMITL Oracle 10g DW, so Airflow "
                "and other consumers don't need a JDBC driver in-image.",
    version="0.1.0",
)

_connector: OracleConnector | None = None


def get_connector() -> OracleConnector:
    global _connector
    if _connector is None:
        _connector = OracleConnector()
    return _connector


# -----------------------------------------------------------------------------
# Auth
# -----------------------------------------------------------------------------


def require_token(authorization: str | None = Header(default=None)) -> None:
    expected = env_get("ORACLE_API_TOKEN")
    if not expected:
        # Dev mode — no token configured, skip auth.
        return
    if authorization != f"Bearer {expected}":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid or missing bearer token",
        )


# -----------------------------------------------------------------------------
# Request / response models
# -----------------------------------------------------------------------------


class ExecuteRequest(BaseModel):
    sql: str
    params: list[Any] | None = None


class QueryRequest(BaseModel):
    sql: str
    params: list[Any] | None = None


class SpCallRequest(BaseModel):
    name: str
    args: list[Any] = Field(default_factory=list)


class BulkInsertRequest(BaseModel):
    table: str
    columns: list[str]
    rows: list[list[Any]]
    truncate: bool = False
    pipeline_run_id: str | None = None


class QueryResponse(BaseModel):
    columns: list[str]
    rows: list[list[Any]]
    rowcount: int


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _coerce(v: Any) -> Any:
    """JSON-safe conversion of JDBC/Oracle return values."""
    if v is None:
        return None
    if isinstance(v, (dt.date, dt.datetime, dt.time)):
        return v.isoformat()
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float, str)):
        return v
    # java.lang.String and friends — stringify
    return str(v)


_ISO_DATE = r"^\d{4}-\d{2}-\d{2}$"
_ISO_DATETIME = r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}"


_JDATE = None
_JTIMESTAMP = None


def _jdbc_types():
    """Lazily resolve java.sql.Date / Timestamp after JVM is up."""
    global _JDATE, _JTIMESTAMP
    if _JDATE is None:
        import jpype  # type: ignore[import-untyped]

        _JDATE = jpype.JClass("java.sql.Date")
        _JTIMESTAMP = jpype.JClass("java.sql.Timestamp")
    return _JDATE, _JTIMESTAMP


def _parse_iso(v: Any) -> Any:
    """Convert ISO date/datetime strings to JDBC-bindable java.sql types.

    Python `datetime.date` is rejected by the JDBC overload resolver, and
    JayDeBeApi's PEP-249 `Date()` / `Timestamp()` factories just return
    strings — which Oracle refuses against DATE/TIMESTAMP columns
    (ORA-01861). Constructing `java.sql.Date/Timestamp` via jpype works.
    """
    if not isinstance(v, str):
        return v
    import re

    if re.match(_ISO_DATETIME, v):
        JDate, JTimestamp = _jdbc_types()
        try:
            return JTimestamp.valueOf(v.replace("T", " ").rstrip("Z"))
        except Exception:
            return v
    if re.match(_ISO_DATE, v):
        JDate, _ = _jdbc_types()
        try:
            return JDate.valueOf(v)
        except Exception:
            return v
    return v


def _prepare_row(row: list[Any]) -> list[Any]:
    return [_parse_iso(v) for v in row]


# -----------------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------------


@app.get("/health")
def health(_: None = Depends(require_token)) -> dict:
    connector = get_connector()
    try:
        with connector.cursor() as cur:
            cur.execute("SELECT SYSDATE, USER FROM DUAL")
            sysdate, user = cur.fetchone()
        return {
            "status": "ok",
            "oracle_user": str(user),
            "oracle_sysdate": _coerce(sysdate),
            "jdbc_url": connector.jdbc_url,
        }
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"oracle unreachable: {exc}")


@app.post("/sql/execute")
def sql_execute(
    req: ExecuteRequest,
    _: None = Depends(require_token),
) -> dict:
    """Run a single DDL/DML statement. Commits on success."""
    connector = get_connector()
    with connector.cursor() as cur:
        if req.params:
            cur.execute(req.sql, _prepare_row(req.params))
        else:
            cur.execute(req.sql)
        rowcount = getattr(cur, "rowcount", -1)
    return {"rowcount": rowcount}


@app.post("/sql/query", response_model=QueryResponse)
def sql_query(
    req: QueryRequest,
    _: None = Depends(require_token),
) -> QueryResponse:
    """Run a SELECT and return all rows as JSON."""
    connector = get_connector()
    conn = connector.connect()
    try:
        cur = conn.cursor()
        try:
            if req.params:
                cur.execute(req.sql, _prepare_row(req.params))
            else:
                cur.execute(req.sql)
            columns = [str(c[0]) for c in cur.description] if cur.description else []
            rows = [[_coerce(v) for v in row] for row in cur.fetchall()]
            return QueryResponse(columns=columns, rows=rows, rowcount=len(rows))
        finally:
            cur.close()
    finally:
        conn.close()


@app.post("/sp/call")
def sp_call(
    req: SpCallRequest,
    _: None = Depends(require_token),
) -> dict:
    """Call a stored procedure: `BEGIN <name>(:1, :2, ...); END;`."""
    placeholders = ", ".join(f"?" for _ in req.args)
    body = f"BEGIN {req.name}({placeholders}); END;"
    connector = get_connector()
    with connector.cursor() as cur:
        if req.args:
            cur.execute(body, _prepare_row(req.args))
        else:
            cur.execute(f"BEGIN {req.name}; END;")
    return {"ok": True, "procedure": req.name}


@app.post("/sql/bulk-insert")
def bulk_insert(
    req: BulkInsertRequest,
    _: None = Depends(require_token),
) -> dict:
    """Idempotent staging loader.

    If `truncate=True`, truncate the target before inserting.
    `pipeline_run_id`, if provided, is appended as an extra column value
    after the caller-supplied values — caller must include its column name
    in `columns` at the same position.
    """
    if not req.rows:
        return {"rowcount": 0, "truncated": False}

    placeholders = ", ".join("?" for _ in req.columns)
    col_list = ", ".join(req.columns)
    sql = f"INSERT INTO {req.table} ({col_list}) VALUES ({placeholders})"

    connector = get_connector()
    conn = connector.connect()
    try:
        cur = conn.cursor()
        truncated = False
        try:
            if req.truncate:
                cur.execute(f"TRUNCATE TABLE {req.table}")
                truncated = True
            prepared = [_prepare_row(r) for r in req.rows]
            cur.executemany(sql, prepared)
            conn.commit()
            return {"rowcount": len(req.rows), "truncated": truncated}
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
    finally:
        conn.close()


# =============================================================================
# Dashboard / reporting endpoints (Phase 7).
# Thin views on top of the DW facts, purpose-built for the Streamlit UI.
# Each endpoint runs one SQL, converts Oracle types via `_coerce`, and returns
# either `{rows: [...]}` or a single object, depending on the shape.
# =============================================================================


def _query_rows(sql: str, params: list[Any] | None = None) -> list[dict]:
    """Shared helper — run a SELECT and return rows as list-of-dicts."""
    connector = get_connector()
    conn = connector.connect()
    try:
        cur = conn.cursor()
        try:
            if params:
                cur.execute(sql, _prepare_row(params))
            else:
                cur.execute(sql)
            cols = [str(c[0]).lower() for c in cur.description] if cur.description else []
            return [dict(zip(cols, [_coerce(v) for v in row])) for row in cur.fetchall()]
        finally:
            cur.close()
    finally:
        conn.close()


@app.get("/api/oee/available-dates")
def oee_available_dates(_: None = Depends(require_token)) -> dict:
    """List dates that have FACT_OEE rows — for the dashboard date picker."""
    rows = _query_rows("""
        SELECT DISTINCT d.full_date AS the_date
          FROM FACT_OEE f
          JOIN DIM_DATE d ON f.date_id = d.date_id
         ORDER BY d.full_date DESC
    """)
    # Strip to YYYY-MM-DD
    return {"dates": [r["the_date"][:10] if r["the_date"] else None for r in rows]}


@app.get("/api/oee/daily")
def oee_daily(date: str, _: None = Depends(require_token)) -> dict:
    """OEE per machine for a specific date (YYYY-MM-DD)."""
    rows = _query_rows("""
        SELECT m.machine_name,
               m.machine_type,
               f.availability_pct,
               f.performance_pct,
               f.quality_pct,
               f.oee_pct,
               f.planned_time_min,
               f.actual_run_min,
               f.downtime_min,
               f.units_produced,
               f.units_good
          FROM FACT_OEE    f
          JOIN DIM_MACHINE m ON f.machine_id = m.machine_id
          JOIN DIM_DATE    d ON f.date_id    = d.date_id
         WHERE d.full_date = ?
         ORDER BY m.machine_id
    """, [date])
    return {"date": date, "rows": rows}


@app.get("/api/quality/defect-by-stage")
def quality_defect_by_stage(_: None = Depends(require_token)) -> dict:
    """Aggregate defect rate per stage across all dates in FACT_QUALITY."""
    rows = _query_rows("""
        SELECT s.stage_name,
               s.sequence_no,
               ROUND(AVG(fq.defect_rate_pct), 2) AS avg_defect_pct,
               SUM(fq.samples_taken)             AS total_samples,
               SUM(fq.pass_count)                AS total_passes,
               SUM(fq.fail_count)                AS total_fails
          FROM FACT_QUALITY fq
          JOIN DIM_STAGE    s ON fq.stage_id = s.stage_id
         GROUP BY s.stage_name, s.sequence_no
         ORDER BY s.sequence_no
    """)
    return {"rows": rows}


@app.get("/api/maintenance/mtbf-mttr")
def maintenance_mtbf_mttr(_: None = Depends(require_token)) -> dict:
    """Per-machine breakdown stats across all dates."""
    rows = _query_rows("""
        SELECT m.machine_name,
               COUNT(*)                        AS breakdown_count,
               ROUND(AVG(fm.downtime_min), 2)  AS avg_downtime_min,
               SUM(fm.downtime_min)            AS total_downtime_min,
               MAX(fm.issue_code)              AS most_recent_issue
          FROM FACT_MAINTENANCE fm
          JOIN DIM_MACHINE      m ON fm.machine_id = m.machine_id
         WHERE fm.event_type = 'BREAKDOWN'
         GROUP BY m.machine_name
         ORDER BY total_downtime_min DESC
    """)
    return {"rows": rows}


@app.get("/api/inventory/latest")
def inventory_latest(_: None = Depends(require_token)) -> dict:
    """Most recent day's inventory snapshot across all materials."""
    rows = _query_rows("""
        SELECT mat.material_name,
               mat.unit,
               mat.hazard_class,
               fi.qty_opening,
               fi.qty_received,
               fi.qty_consumed,
               fi.qty_closing,
               d.full_date AS as_of_date
          FROM FACT_INVENTORY fi
          JOIN DIM_MATERIAL   mat ON fi.material_id = mat.material_id
          JOIN DIM_DATE       d   ON fi.date_id     = d.date_id
         WHERE d.date_id = (
             SELECT MAX(f2.date_id) FROM FACT_INVENTORY f2
         )
         ORDER BY mat.material_name
    """)
    return {"rows": rows}


@app.get("/api/oee/weekly-trend")
def oee_weekly_trend(_: None = Depends(require_token)) -> dict:
    """Weekly OEE trend — recalculated from additive measures, not averaged."""
    rows = _query_rows("""
        SELECT d.year,
               d.week_number,
               m.machine_name,
               ROUND(
                   (  (SUM(f.planned_time_min) - SUM(f.downtime_min)) /
                           NULLIF(SUM(f.planned_time_min), 0)
                    * (SUM(f.units_produced * m.ideal_cycle_sec / 60.0) /
                           NULLIF(SUM(f.actual_run_min), 0))
                    * (SUM(f.units_good) /
                           NULLIF(SUM(f.units_produced), 0))
                   ) * 100
               , 2) AS weekly_oee_pct
          FROM FACT_OEE    f
          JOIN DIM_DATE    d ON f.date_id    = d.date_id
          JOIN DIM_MACHINE m ON f.machine_id = m.machine_id
         GROUP BY d.year, d.week_number, m.machine_name, m.ideal_cycle_sec
         ORDER BY d.year, d.week_number, m.machine_name
    """)
    return {"rows": rows}
