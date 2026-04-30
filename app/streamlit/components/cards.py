from __future__ import annotations
import streamlit as st


def kpi_card(label: str, 
             value: str | float, 
             delta: str | None = None,
             help: str | None = None) -> None:
    """
    ห่อ st.metric เพื่อ formatting consistent

    Args:
        label: card title (e.g. "OEE")
        value: main number (formatted string หรือ numeric)
        delta: optional delta text (e.g. "+2.3%") — ใช้สำหรับ trend indicator
        help: optional tooltip
    """
    st.metric(label=label, value=value, delta=delta, help=help)



def kpi_row(items: list[dict]) -> None:
    """Render row ของ KPI cards ใน equal columns

    Args:
        items: list ของ dict ที่มี keys: label, value, delta?, help?

    Example:
        kpi_row([
            {"label": "OEE",          "value": "78%", "delta": "+2%"},
            {"label": "Availability", "value": "92%"},
            {"label": "Performance",  "value": "88%"},
            {"label": "Quality",      "value": "96%"},
        ])
    """
    cols = st.columns(len(items))
    for col, item in zip(cols, items):
        with col:
            kpi_card(**item)



def status_badge(text: str, status: str = "info") -> str:
    """Markdown badge (HTML) สำหรับใส่ใน st.markdown(... unsafe_allow_html=True)

    Args:
        status: 'success' (เขียว) / 'warning' (เหลือง) / 'danger' (แดง) / 'info' (น้ำเงิน)
    """
    colors = {
        "success": ("#1D9E75", "white"),
        "warning": ("#BA7517", "white"),
        "danger":  ("#E24B4A", "white"),
        "info":    ("#185FA5", "white"),
    }
    bg, fg = colors.get(status, colors["info"])
    return (f'<span style="background:{bg};color:{fg};padding:2px 10px;'
            f'border-radius:11px;font-size:12px;font-weight:500;">'
            f'{text}</span>')
