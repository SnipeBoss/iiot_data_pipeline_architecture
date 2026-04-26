from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go


# ============================================================
# Page 1 — OEE + Defect
# ============================================================

def oee_trend_chart(df: pd.DataFrame) -> go.Figure:
    """OEE trend พร้อม sub-components (availability/performance/quality)

    Expected columns: full_date, oee_pct, availability_pct, performance_pct, quality_pct
    """
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["full_date"], y=df["oee_pct"],
                             name="OEE", mode="lines+markers",
                             line=dict(color="#0F6E56", width=3)))
    fig.add_trace(go.Scatter(x=df["full_date"], y=df["availability_pct"],
                             name="Availability", mode="lines",
                             line=dict(color="#534AB7", dash="dash")))
    fig.add_trace(go.Scatter(x=df["full_date"], y=df["performance_pct"],
                             name="Performance", mode="lines",
                             line=dict(color="#185FA5", dash="dot")))
    fig.add_trace(go.Scatter(x=df["full_date"], y=df["quality_pct"],
                             name="Quality", mode="lines",
                             line=dict(color="#BA7517", dash="dashdot")))
    fig.update_layout(
        height=350, hovermode="x unified",
        yaxis=dict(title="%", range=[0, 105]),
        xaxis=dict(title=None),
        margin=dict(t=20, b=40, l=40, r=20),
    )
    return fig


def defect_pareto_chart(df: pd.DataFrame) -> go.Figure:
    """Pareto chart: bars เรียงจากมาก→น้อย + cumulative % line (top 10)

    Expected columns: defect_code (or defect_type), qty_affected, pct_of_total
    """
    qty_col = "qty_affected" if "qty_affected" in df.columns else "total_qty_affected"
    code_col = "defect_code" if "defect_code" in df.columns else "defect_type"

    df = df.sort_values(qty_col, ascending=False).head(10).copy()
    df["cum_pct"] = df["pct_of_total"].cumsum()

    fig = go.Figure()
    fig.add_trace(go.Bar(x=df[code_col], y=df[qty_col],
                         name="Qty", marker_color="#D85A30"))
    fig.add_trace(go.Scatter(x=df[code_col], y=df["cum_pct"],
                             name="Cumulative %", mode="lines+markers",
                             yaxis="y2", line=dict(color="#534AB7")))
    fig.update_layout(
        height=350,
        yaxis=dict(title="Quantity"),
        yaxis2=dict(title="Cumulative %", overlaying="y", side="right",
                    range=[0, 105]),
        margin=dict(t=20, b=80, l=40, r=40),
    )
    return fig


def defect_rate_by_model_chart(df: pd.DataFrame) -> go.Figure:
    """Bar chart — defect rate per battery model

    Expected columns: model_name, defect_rate_pct
    """
    df = df.sort_values("defect_rate_pct", ascending=False)
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df["model_name"], y=df["defect_rate_pct"],
                         marker_color="#993C1D",
                         text=df["defect_rate_pct"].round(2),
                         textposition="outside"))
    fig.update_layout(
        height=300,
        yaxis=dict(title="Defect rate (%)"),
        xaxis=dict(title="Battery model"),
        margin=dict(t=20, b=40, l=40, r=20),
    )
    return fig


# ============================================================
# Page 2 — Sensor Forecast
# ============================================================

def forecast_chart(historical: pd.DataFrame, forecast: pd.DataFrame,
                   threshold: float | None = None) -> go.Figure:
    """Prophet forecast พร้อม confidence band + threshold line

    historical columns: ds, y
    forecast columns:   ds, yhat, yhat_lower, yhat_upper
    """
    fig = go.Figure()

    # Historical (blue solid)
    fig.add_trace(go.Scatter(x=historical["ds"], y=historical["y"],
                             name="Historical", mode="lines",
                             line=dict(color="#185FA5", width=2)))

    # Forecast (orange dashed)
    fig.add_trace(go.Scatter(x=forecast["ds"], y=forecast["yhat"],
                             name="Forecast", mode="lines",
                             line=dict(color="#BA7517", width=2, dash="dash")))

    # Confidence band — ทึบกว่าเดิมเพื่อให้เห็น forecast ชัด
    fig.add_trace(go.Scatter(
        x=list(forecast["ds"]) + list(forecast["ds"][::-1]),
        y=list(forecast["yhat_upper"]) + list(forecast["yhat_lower"][::-1]),
        fill="toself", fillcolor="rgba(250, 199, 117, 0.3)",
        line=dict(color="rgba(255,255,255,0)"),
        name="Confidence band", hoverinfo="skip",
    ))

    # Threshold (red dashed) — เส้น critical limit ของ metric
    if threshold is not None:
        fig.add_hline(y=threshold, line_dash="dash", line_color="#E24B4A",
                      annotation_text=f"threshold {threshold}",
                      annotation_position="top right")

    # "Now" vertical line ที่ขอบ historical/forecast
    # Plotly bug: add_vline(x=datetime, annotation_text=...) ทำให้ internal
    # _mean ลอง sum([x,x]) ที่ start=0 → 0 + datetime = TypeError
    # Workaround: split add_vline (line) + add_annotation (label) แยกกัน
    if not historical.empty:
        now = historical["ds"].max()
        if hasattr(now, "isoformat"):
            now = now.isoformat()
        fig.add_vline(x=now, line_dash="dash", line_color="#888780")
        fig.add_annotation(x=now, y=1, yref="paper",
                           text="now", showarrow=False,
                           xanchor="left", yanchor="top",
                           font=dict(color="#888780", size=11))

    fig.update_layout(
        height=400, hovermode="x unified",
        xaxis=dict(title=None),
        yaxis=dict(title="Value"),
        margin=dict(t=20, b=40, l=40, r=20),
    )
    return fig


# ============================================================
# Page 3 — Schedule Adherence
# ============================================================

def slippage_histogram(df: pd.DataFrame) -> go.Figure:
    """Histogram ของ slippage_min พร้อม threshold lines (5/15 นาที)"""
    fig = go.Figure()
    fig.add_trace(go.Histogram(
        x=df["slippage_min"], nbinsx=20,
        marker=dict(color="#1D9E75"),
    ))
    fig.add_vline(x=5, line_dash="dash", line_color="#888780",
                  annotation_text="on-time threshold")
    fig.add_vline(x=15, line_dash="dash", line_color="#E24B4A",
                  annotation_text="late threshold")
    fig.update_layout(
        height=300,
        xaxis=dict(title="Slippage (minutes)"),
        yaxis=dict(title="Batch count"),
        margin=dict(t=20, b=40, l=40, r=20),
    )
    return fig


def slippage_trend(df: pd.DataFrame) -> go.Figure:
    """Average slippage per date (จาก full_date หรือ derive จาก order_planned_start)"""
    if "full_date" in df.columns:
        date_col = "full_date"
        work = df
    elif "order_planned_start" in df.columns:
        work = df.copy()
        # format='mixed' — Oracle TIMESTAMP บางแถวมี fractional seconds บางแถวไม่มี
        work["__date"] = pd.to_datetime(work["order_planned_start"], format="mixed").dt.date
        date_col = "__date"
    elif "batch_planned_start" in df.columns:
        work = df.copy()
        work["__date"] = pd.to_datetime(work["batch_planned_start"], format="mixed").dt.date
        date_col = "__date"
    else:
        # ไม่มี date column → คืน empty figure แทนการ crash
        fig = go.Figure()
        fig.update_layout(height=300, title="No date column to plot trend")
        return fig

    daily = work.groupby(date_col)["slippage_min"].mean().reset_index()
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=daily[date_col], y=daily["slippage_min"],
                             mode="lines+markers",
                             line=dict(color="#0F6E56", width=2)))
    fig.add_hline(y=0, line_dash="dot", line_color="#888780")
    fig.update_layout(
        height=300,
        xaxis=dict(title=None),
        yaxis=dict(title="Avg slippage (min)"),
        margin=dict(t=20, b=40, l=40, r=20),
    )
    return fig


def batch_gantt(df: pd.DataFrame) -> go.Figure:
    """Gantt chart ของ batches ใน order ที่เลือก (planned vs actual)

    Expected columns: batch_src_id, batch_planned_start, batch_planned_end,
                      start_time/actual_start, end_time/actual_end
    """
    fig = go.Figure()

    # Schema flexible — บาง endpoint return start_time, บางตัว actual_start
    actual_start_col = "actual_start" if "actual_start" in df.columns else "start_time"
    actual_end_col = "actual_end" if "actual_end" in df.columns else "end_time"

    for _, row in df.iterrows():
        bid = f"batch {row['batch_src_id']}"

        # Planned phase (gray bar)
        if row.get("batch_planned_start") and row.get("batch_planned_end"):
            fig.add_trace(go.Scatter(
                x=[row["batch_planned_start"], row["batch_planned_end"]],
                y=[bid, bid], mode="lines",
                line=dict(color="#B4B2A9", width=12),
                showlegend=False, hovertext=f"planned: {bid}",
            ))

        # Actual phase (green if on-time, red if late)
        slip = row.get("slippage_min", 0) or 0
        color = "#E24B4A" if slip > 15 else "#1D9E75"
        if row.get(actual_start_col) and row.get(actual_end_col):
            fig.add_trace(go.Scatter(
                x=[row[actual_start_col], row[actual_end_col]],
                y=[bid, bid], mode="lines",
                line=dict(color=color, width=8),
                showlegend=False,
                hovertext=f"actual: {bid} (slip {slip:.1f} min)",
            ))

    fig.update_layout(
        height=max(300, 30 * len(df) + 100),
        xaxis=dict(title=None, type="date"),
        yaxis=dict(title=None, autorange="reversed"),
        margin=dict(t=20, b=40, l=80, r=20),
    )
    return fig
