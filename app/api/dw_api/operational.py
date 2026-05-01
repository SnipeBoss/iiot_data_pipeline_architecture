from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException
from app.api.dw_api.deps import coerce, get_connector, prepare_row, require_token
from app.api.dw_api.models import BulkInsertRequest, SpCallRequest


"""
Operational endpoints — generic DB access ที่ Airflow DAG ใช้

- /health                — smoke test + Oracle session info
- /sp/call               — เรียก stored procedure
- /sql/bulk-insert       — insert หลาย row เข้า STG (optional truncate ก่อน)

Auth: ทุก route ต้องผ่าน require_token
"""


# Set the API Specification
router = APIRouter(
    dependencies=[Depends(require_token)],
    tags=["operational"],
)


# Health API Checking
@router.get("/health")
def health() -> dict:
    """
    Smoke test ต่อ Oracle — คืน SYSDATE + current USER
    """

    # Define Connection to Oracle
    connector = get_connector()

    try:

        # Set Connection by query the Select user
        with connector.cursor() as cur:
            cur.execute("SELECT SYSDATE, USER FROM DUAL")
            sysdate, user = cur.fetchone()

        # Return Status
        return {
            "status": "ok",
            "oracle_user": str(user),
            "oracle_sysdate": coerce(sysdate),
            "jdbc_url": connector.jdbc_url,
        }

    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"oracle unreachable: {exc}")





@router.post("/sp/call")
def sp_call(req: SpCallRequest) -> dict:
    """
    เรียก stored procedure: BEGIN <name>(:1, :2, ...); END;
    """
    
    # Modified the Query Structured
    placeholders = ", ".join("?" for _ in req.args)
    body = f"BEGIN {req.name}({placeholders}); END;"
    
    # Connection to Oracle Databases
    connector = get_connector()

    # Connection and Send Query
    with connector.cursor() as cur:
        if req.args:
            cur.execute(body, prepare_row(req.args))
        else:
            cur.execute(f"BEGIN {req.name}; END;")

    return {"ok": True, "procedure": req.name}






@router.post("/sql/bulk-insert")
def bulk_insert(req: BulkInsertRequest) -> dict:
    """
    Bulk insert เข้า STG table

    ถ้า truncate=True → TRUNCATE ก่อน (ใช้ทำ idempotent reload)
    คืน {"rowcount": N, "truncated": bool}
    """

    
    if not req.rows:
        return {"rowcount": 0, "truncated": False}

    # Modified Input 
    placeholders = ", ".join("?" for _ in req.columns)
    col_list = ", ".join(req.columns)

    # Set Query for Insert Data
    sql = f"INSERT INTO {req.table} ({col_list}) VALUES ({placeholders})"


    # Set Connection
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
