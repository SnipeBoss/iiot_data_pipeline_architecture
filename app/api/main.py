from __future__ import annotations

from fastapi import FastAPI

from app.api import dashboard, operational


"""Oracle Service API — thin HTTP wrapper รอบ OracleConnector (JDBC)

**จุดประสงค์:** แยก Java/JDBC ออกจาก Airflow container → DAG เรียกผ่าน HTTP
plain `requests` แทน bundle Java ใน image

**Modules:**
- `deps.py`        — auth, connector singleton, JDBC type coercion, query helper
- `models.py`      — Pydantic request/response
- `operational.py` — /health + /sql/* + /sp/call + /sql/bulk-insert (generic DB)
- `dashboard.py`   — /api/production/* + /api/quality/* + /api/sensor/* (Streamlit)

**Run:**
    export JAVA_HOME=/opt/homebrew/opt/openjdk@17
    .venv/bin/uvicorn app.api.main:app --reload --port 8000

Auth: Bearer token จาก ORACLE_API_TOKEN ใน .env (เว้นว่าง = disable auth)
"""


app = FastAPI(
    title="Oracle Service API",
    description="HTTP wrapper around KMITL Oracle 10g DW — no JDBC in client.",
    version="1.0.0",
)

app.include_router(operational.router)
app.include_router(dashboard.router)
