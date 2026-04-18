from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from app.api.deps import coerce, get_connector, prepare_row, require_token
from app.api.models import (
    BulkInsertRequest,
    ExecuteRequest,
    QueryRequest,
    QueryResponse,
    SpCallRequest,
)


"""Operational endpoints — generic DB access ที่ Airflow DAG ใช้

- /health                — smoke test + Oracle session info
- /sql/execute           — รัน 1 DDL/DML statement
- /sql/query             — รัน SELECT
- /sp/call               — เรียก stored procedure
- /sql/bulk-insert       — insert หลาย row เข้า STG (optional truncate ก่อน)

Auth: ทุก route ต้องผ่าน require_token
"""


router = APIRouter(
    dependencies=[Depends(require_token)],
    tags=["operational"],
)


@router.get("/health")
def health() -> dict:
    """Smoke test ต่อ Oracle — คืน SYSDATE + current USER"""
    connector = get_connector()
    try:
        with connector.cursor() as cur:
            cur.execute("SELECT SYSDATE, USER FROM DUAL")
            sysdate, user = cur.fetchone()
        return {
            "status": "ok",
            "oracle_user": str(user),
            "oracle_sysdate": coerce(sysdate),
            "jdbc_url": connector.jdbc_url,
        }
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"oracle unreachable: {exc}")


@router.post("/sql/execute")
def sql_execute(req: ExecuteRequest) -> dict:
    """DDL/DML statement เดียว — commit เมื่อสำเร็จ"""
    connector = get_connector()
    with connector.cursor() as cur:
        if req.params:
            cur.execute(req.sql, prepare_row(req.params))
        else:
            cur.execute(req.sql)
        rowcount = getattr(cur, "rowcount", -1)
    return {"rowcount": rowcount}


@router.post("/sql/query", response_model=QueryResponse)
def sql_query(req: QueryRequest) -> QueryResponse:
    """SELECT → columns + rows + rowcount"""
    connector = get_connector()
    conn = connector.connect()
    try:
        cur = conn.cursor()
        try:
            if req.params:
                cur.execute(req.sql, prepare_row(req.params))
            else:
                cur.execute(req.sql)
            columns = [str(c[0]) for c in cur.description] if cur.description else []
            rows = [[coerce(v) for v in row] for row in cur.fetchall()]
            return QueryResponse(columns=columns, rows=rows, rowcount=len(rows))
        finally:
            cur.close()
    finally:
        conn.close()


@router.post("/sp/call")
def sp_call(req: SpCallRequest) -> dict:
    """เรียก stored procedure: BEGIN <name>(:1, :2, ...); END;"""
    placeholders = ", ".join("?" for _ in req.args)
    body = f"BEGIN {req.name}({placeholders}); END;"
    connector = get_connector()
    with connector.cursor() as cur:
        if req.args:
            cur.execute(body, prepare_row(req.args))
        else:
            cur.execute(f"BEGIN {req.name}; END;")
    return {"ok": True, "procedure": req.name}


@router.post("/sql/bulk-insert")
def bulk_insert(req: BulkInsertRequest) -> dict:
    """Bulk insert เข้า STG table

    ถ้า truncate=True → TRUNCATE ก่อน (ใช้ทำ idempotent reload)
    คืน {"rowcount": N, "truncated": bool}
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
            prepared = [prepare_row(r) for r in req.rows]
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
