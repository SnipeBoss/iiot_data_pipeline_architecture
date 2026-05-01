from __future__ import annotations
import datetime as dt
import re
import jpype  
from typing import Any
from fastapi import Header, HTTPException, status
from db_module.db_conn import OracleConnector
from db_module.db_conn._env import get as env_get


"""
Shared FastAPI dependencies + low-level JDBC helpers

ประกอบด้วย 4 หน้าที่:

    1. Auth — `require_token` ตรวจ bearer token

    2. Connection — `get_connector()` singleton ของ OracleConnector

    3. JDBC coercion — แปลงค่า Python ↔ java.sql.* (ฝั่ง request)
                        แปลงค่าจาก JDBC → JSON-safe (ฝั่ง response)

    4. Query helper — `query_rows()` shortcut สำหรับ SELECT → list[dict]

Routes import จากที่นี่แทนที่จะเขียน logic ซ้ำในทุก handler
"""


# =============================================================================
# Auth
# =============================================================================


def require_token(authorization: str | None = Header(default=None)) -> None:
    """
    Bearer token gate
    """

    # Get Oracle API Token 
    expected = env_get("ORACLE_API_TOKEN")
    if not expected:
        return
    
    if authorization != f"Bearer {expected}":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid or missing bearer token",
        )



_connector: OracleConnector | None = None


def get_connector() -> OracleConnector:
    """
    Connection management (singleton)
    OracleConnector singleton — lazy init ครั้งแรกที่เรียก
    """
    global _connector
    if _connector is None:
        _connector = OracleConnector()
    return _connector




def coerce(v: Any) -> Any:
    """
    JDBC type coercion (response: JDBC → JSON-safe)
    แปลงค่าจาก JDBC/Oracle ให้ JSON-serializable
    """
    if v is None:
        return None
    if isinstance(v, (dt.date, dt.datetime, dt.time)):
        return v.isoformat()
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float, str)):
        return v
    # java.lang.String หรือประเภท Java อื่น → stringify
    return str(v)



# JDBC type coercion (request: ISO string → java.sql.*)
_ISO_DATE = r"^\d{4}-\d{2}-\d{2}$"
_ISO_DATETIME = r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}"
_JDATE = None
_JTIMESTAMP = None


def _jdbc_types():
    """
    Lazy-resolve java.sql.Date / Timestamp หลังจาก JVM พร้อม
    """
    global _JDATE, _JTIMESTAMP
    if _JDATE is None:
        _JDATE = jpype.JClass("java.sql.Date")
        _JTIMESTAMP = jpype.JClass("java.sql.Timestamp")

    return _JDATE, _JTIMESTAMP



def parse_iso(v: Any) -> Any:
    """
    แปลง ISO date/datetime string → java.sql.Date/Timestamp ให้ JDBC bind ได้

    ทำไมต้องแปลง:
    - Python `datetime.date` โดน JDBC overload resolver reject
    - JayDeBeApi's `Date()` / `Timestamp()` factories คืน string ซึ่ง Oracle
      refuse กับ DATE/TIMESTAMP column (ORA-01861)
    → สร้าง java.sql.Date/Timestamp object ผ่าน jpype แทน
    """
    
    # Check ISO Format
    if not isinstance(v, str):
        return v
    
    # Check ISO Datetime
    if re.match(_ISO_DATETIME, v):
        _, JTimestamp = _jdbc_types()
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



def prepare_row(row: list[Any]) -> list[Any]:
    """
    แปลงทุก element ใน row ให้พร้อม bind กับ JDBC
    """
    return [parse_iso(v) for v in row]


# =============================================================================
# Query helper (ใช้ซ้ำในทุก dashboard endpoint)
# =============================================================================


def query_rows(sql: str, params: list[Any] | None = None) -> list[dict]:
    """
    รัน SELECT → list[dict] (column name lower-cased เพื่อ JSON friendly)
    """

    # Set Connector
    connector = get_connector()
    conn = connector.connect()

    
    try:
        cur = conn.cursor()
        try:
            if params:
                cur.execute(sql, prepare_row(params))
            else:
                cur.execute(sql)
            cols = [str(c[0]).lower() for c in cur.description] if cur.description else []
            return [dict(zip(cols, [coerce(v) for v in row])) for row in cur.fetchall()]
        finally:
            cur.close()
    finally:
        conn.close()
