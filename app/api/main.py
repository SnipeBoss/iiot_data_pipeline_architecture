from __future__ import annotations
from fastapi import FastAPI


"""
Oracle Service API — thin HTTP wrapper รอบ OracleConnector (JDBC)

**จุดประสงค์:** แยก Java/JDBC ออกจาก Airflow container → DAG เรียกผ่าน HTTP
plain `requests` แทน bundle Java ใน image

Sub-packages :



- `dw_api/`         — Oracle DW core
    - `deps.py`         auth, connector singleton, JDBC type coercion, query helper
    - `models.py`       Pydantic request/response
    - `operational.py`  /health + /sql/* + /sp/call + /sql/bulk-insert (generic DB ที่ DAG ใช้)

- `dashboard_api/`  — domain endpoints
    - `dashboard.py`    /api/production/* + /quality/* + /sensor/* + /scheduling/* + /analytics/*
                        (สำหรับ Streamlit + ML pipeline)

                        

**Run:**
    export JAVA_HOME=/opt/homebrew/opt/openjdk@17
    .venv/bin/uvicorn app.api.main:app --reload --port 8000
    Auth: Bearer token จาก ORACLE_API_TOKEN ใน .env (เว้นว่าง = disable auth)
"""


app = FastAPI(

    # API Connect to Oracle Data Warehouses
    title="Oracle Service API",

    # Description for API
    description="HTTP wrapper around KMITL Oracle 10g DW — no JDBC in client.",

    # Versioning Updated
    version="1.0.0",
)


# Connecting Operational Router -> /health + /sql/* + /sp/call + /sql/bulk-insert (generic DB)
from app.api.dw_api import operational
app.include_router(operational.router)


# Connecting -> /api/production/* + /api/quality/* + /api/sensor/* (Streamlit)
from app.api.dashboard_api import dashboard
app.include_router(dashboard.router)
