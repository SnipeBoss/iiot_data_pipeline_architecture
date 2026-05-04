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
from dotenv import load_dotenv

load_dotenv(_REPO_ROOT / ".env", override=False)



# Set Page Template
st.set_page_config(

    # Title Browser
    page_title="Battery MES Analytics",

    # Full Page
    layout="wide",

    # Show Sidebar
    initial_sidebar_state="expanded",
)


# Add Sidebar Information
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



# FastAPI status indicator
api_url = os.getenv("DASHBOARD_API_URL", os.getenv("ORACLE_API_URL", "http://localhost:8000"))
token = os.getenv("ORACLE_API_TOKEN", "")
headers = {
    "Authorization": f"Bearer {token}"
} if token else {}


try:

    # Send Request 
    r = requests.get(
            f"{api_url}/health", 
            headers=headers, 
            timeout=3
        )
    
    # Check Status
    if r.status_code == 200:
        st.sidebar.success(f"FastAPI OK   {api_url}")

    else:
        st.sidebar.error(f"FastAPI FAIL HTTP {r.status_code}")

except Exception as exc:
    st.sidebar.error(f"FastAPI FAIL {type(exc).__name__}")


# Set Caption
st.sidebar.caption("DAG cadence: 15 min")
st.sidebar.caption("Cache TTL: 5 min")


# Landing page Content
@st.cache_data(ttl=600)
def _load_readme() -> str:
    """
    อ่าน README.md จาก repo root (cache 10 นาที)
    """

    readme_path = _REPO_ROOT / "README.md"
    
    if not readme_path.exists():
        return "README.md not found at repo root"
    
    return readme_path.read_text(encoding="utf-8")


readme_text = _load_readme()

# Show Markdown to landing page
st.markdown(readme_text, unsafe_allow_html=False)
