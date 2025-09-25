# views/tables.py
from __future__ import annotations
import uuid
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

def _to_dataframe(obj):
    if isinstance(obj, pd.io.formats.style.Styler):
        return obj.data
    if isinstance(obj, pd.DataFrame):
        return obj
    if isinstance(obj, (list, tuple)) and obj and isinstance(obj[0], dict):
        return pd.DataFrame(obj)
    return pd.DataFrame(obj)

def _auto_height(n_rows: int) -> int:
    return int(36 + n_rows * 32 + 12)

def render_table(
    df_like,
    height: int | None = None,
    key: str | None = None,
    highlight_col: str | int | None = None,
    highlight_color: str = "#17c9c3",
    **_ignored
):
    df = _to_dataframe(df_like)
    if df is None or len(df) == 0:
        st.info("Sem dados para mostrar.")
        return

    headers = list(df.columns)
    values = [df[c].tolist() for c in headers]

    n_rows = len(df.index)
    fills = [[None] * n_rows for _ in headers]
    if highlight_col is not None:
        try:
            idx = headers.index(highlight_col) if isinstance(highlight_col, str) else int(highlight_col)
            fills[idx] = [highlight_color] * n_rows
        except Exception:
            pass

    fig_tbl = go.Figure(data=[go.Table(
        header=dict(values=headers, align="center"),
        cells=dict(values=values, align="center", fill_color=fills),
    )])
    fig_tbl.update_layout(margin=dict(l=0, r=0, t=8, b=0), height=height or _auto_height(n_rows))
    st.plotly_chart(fig_tbl, use_container_width=True, key=key or f"tbl_{uuid.uuid4().hex}")
