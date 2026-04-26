from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field


"""
Pydantic request models สำหรับ operational endpoints
"""


class SpCallRequest(BaseModel):
    """เรียก stored procedure: BEGIN name(args...); END;"""
    name: str
    args: list[Any] = Field(default_factory=list)


class BulkInsertRequest(BaseModel):
    """Bulk insert เข้า STG table (รองรับ truncate-then-insert)"""
    table: str
    columns: list[str]
    rows: list[list[Any]]
    truncate: bool = False
    pipeline_run_id: str | None = None
