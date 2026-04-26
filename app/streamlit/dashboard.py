"""
Streamlit entry point — sidebar nav + landing page (renders README.md)

Pages auto-discovered โดย Streamlit จาก `pages/` folder
Filters อยู่ใน page (ไม่ใช่ sidebar) — sidebar = nav + status เท่านั้น

Run (จาก app/streamlit/):
    cd app/streamlit && ../../.venv/bin/streamlit run dashboard.py

หรือ จาก repo root:
    STREAMLIT_CONFIG_DIR=app/streamlit/.streamlit \\
        .venv/bin/streamlit run app/streamlit/dashboard.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Add repo root + streamlit dir ลง sys.path เพื่อให้ pages import "from components" ได้
_REPO_ROOT = Path(__file__).resolve().parents[2]
_STREAMLIT_DIR = Path(__file__).resolve().parent
for p in (_REPO_ROOT, _STREAMLIT_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import requests
import streamlit as st


st.set_page_config(
    page_title="Battery MES Analytics",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ─── Sidebar ──────────────────────────────────────────────
st.sidebar.title("Battery MES")
st.sidebar.caption("Line COS Analytics")

st.sidebar.markdown(
    """
    ### Navigation
    - **OEE & Defect** (page 1)
    - **Sensor Forecast** (page 2)
    - **Schedule Adherence** (page 3)
    """
)

st.sidebar.divider()


# ─── FastAPI status indicator ─────────────────────────────
api_url = (
    os.getenv("DASHBOARD_API_URL")
    or os.getenv("ORACLE_API_URL")
    or "http://localhost:8000"
)
token = os.getenv("ORACLE_API_TOKEN", "")
headers = {"Authorization": f"Bearer {token}"} if token else {}

try:
    r = requests.get(f"{api_url}/health", headers=headers, timeout=3)
    if r.status_code == 200:
        st.sidebar.success(f"FastAPI OK   {api_url}")
    else:
        st.sidebar.error(f"FastAPI FAIL HTTP {r.status_code}")
except Exception as exc:
    st.sidebar.error(f"FastAPI FAIL {type(exc).__name__}")


st.sidebar.caption("DAG cadence: 15 min")
st.sidebar.caption("Cache TTL: 5 min")


# ─── Landing page — render README.md ──────────────────────
@st.cache_data(ttl=600)
def _load_readme() -> str:
    """อ่าน README.md จาก repo root (cache 10 นาที)"""
    readme_path = _REPO_ROOT / "README.md"
    if not readme_path.exists():
        return "README.md not found at repo root"
    return readme_path.read_text(encoding="utf-8")


readme_text = _load_readme()
st.markdown(readme_text, unsafe_allow_html=False)
