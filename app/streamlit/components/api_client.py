from __future__ import annotations
import os
from pathlib import Path
import requests
import streamlit as st
from dotenv import load_dotenv

"""
Shared API client 
cached wrapper รอบ FastAPI /api/* endpoints
"""

_REPO_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(_REPO_ROOT / ".env", override=False)

_BASE = os.getenv("DASHBOARD_API_URL", os.getenv("ORACLE_API_URL", "http://localhost:8000"))
_TOKEN = os.getenv("ORACLE_API_TOKEN", "")


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_TOKEN}"
    } if _TOKEN else {}


@st.cache_data(ttl=300)
def get(endpoint: str, 
        params: dict | None = None) -> dict | list:
    
    """
    GET + Bearer — cache 5 นาที (300s) ต่อ (endpoint, params)
    คลิก Refresh ใน filter row จะ clear cache ทันทีก่อน TTL หมด
    """

    # Using Request for get data 
    r = requests.get(
        f"{_BASE}{endpoint}",
        headers=_headers(),
        params=params or {},
        timeout=30,
    )

    # Set Status
    r.raise_for_status()
    return r.json()
