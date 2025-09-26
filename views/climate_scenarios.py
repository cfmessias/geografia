# views/climate_scenarios.py — GLOBAL OFFLINE (lendo data/)
from __future__ import annotations
from pathlib import Path
import io
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"

_SCENARIO_COLORS = {
    "historical": "#6c757d",
    "ssp126":     "#2ca02c",
    "ssp245":     "#ff7f0e",
    "ssp370":     "#9467bd",
    "ssp585":     "#d62728",
}
print(DATA_DIR)
def _hex_to_rgba(hex_color: str, a: float) -> str:
    h = hex_color.lstrip("#"); r=int(h[0:2],16); g=int(h[2:4],16); b=int(h[4:6],16)
    return f"rgba({r},{g},{b},{a})"

def _pretty(s: str) -> str:
    return {"historical":"Histórico","ssp126":"SSP1-2.6","ssp245":"SSP2-4.5","ssp370":"SSP3-7.0","ssp585":"SSP5-8.5"}.get(s,s)

def _warming_tail_value(df: pd.DataFrame, scn: str) -> float | None:
    g = df[df["scenario"] == scn].sort_values("time")
    if g.empty: return None
    end = g[(g["year"]>=2091)&(g["year"]<=2100)]
    if end.empty: end = g.tail(10)
    if end.empty or end["mean"].isna().all(): return None
    return float(end["mean"].mean())

def render_climate_tab():
    st.subheader("🌍 Projeções globais de temperatura (CMIP6) — média e incerteza por cenário")

    f_stat = DATA_DIR / "cmip6_global_ensemble_stats.csv"
    f_all  = DATA_DIR / "cmip6_global_all_models.csv"
    if not f_stat.exists() or not f_all.exists():
        st.warning("Faltam CSVs.\nExecute: `python -u scripts/meteo/fetch_cmip6_global.py`")
        return

    stats = pd.read_csv(f_stat, parse_dates=["time"])
    stats["year"] = stats["time"].dt.year.astype(int)

    scenarios = st.multiselect(
        "Cenários a mostrar",
        options=["historical","ssp126","ssp245","ssp370","ssp585"],
        default=["historical","ssp126","ssp245","ssp370","ssp585"],
    )
    win = st.number_input("Suavização (média móvel, anos)", 1, 11, 5, 2)

    stat = stats.copy()
    mask = (stat["scenario"] != "historical") | (stat["time"].dt.year >= 1950)
    stat = stat.loc[mask].reset_index(drop=True)
    stat["mean"] = (
        stat.sort_values("time")
            .groupby("scenario")["mean"]
            .transform(lambda s: s.rolling(win, center=True, min_periods=1).mean())
    )

    fig = go.Figure()
    for scn in [s for s in scenarios if s in set(stat["scenario"])]:
        g = stat[stat["scenario"] == scn].sort_values("time")
        color = _SCENARIO_COLORS.get(scn, "#1f77b4")
        fig.add_trace(go.Scatter(
            x=pd.concat([g["time"], g["time"][::-1]]),
            y=pd.concat([g["max"],  g["min"][::-1]]),
            fill="toself", fillcolor=_hex_to_rgba(color, 0.18),
            line=dict(width=0), hoverinfo="skip", showlegend=False,
            name=f"{_pretty(scn)} (incerteza)",
        ))
        fig.add_trace(go.Scatter(
            x=g["time"], y=g["mean"], mode="lines+markers",
            line=dict(color=color, width=2), marker=dict(size=4),
            name=_pretty(scn), showlegend=False,
        ))
        g_valid = g.dropna(subset=["mean"])
        if not g_valid.empty:
            x_last = g_valid["time"].iloc[-1]; y_last = g_valid["mean"].iloc[-1]
            w = _warming_tail_value(stat, scn)
            txt = f"{_pretty(scn)}" + (f" · ~+{w:.1f} °C" if w is not None else "")
            fig.add_annotation(x=x_last, y=y_last, text=txt, xanchor="left", yanchor="middle",
                               xshift=8, showarrow=False, font=dict(size=12), bgcolor="rgba(0,0,0,0)")

    fig.update_layout(height=380, margin=dict(l=6,r=6,t=40,b=0),
                      xaxis_title="Ano", yaxis_title="Δ°C (anomalia vs 1991–2020, calculada offline)",
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0))
    fig.update_yaxes(gridcolor="rgba(160,160,160,0.35)", gridwidth=1.2)
    fig.update_xaxes(gridcolor="rgba(160,160,160,0.18)", gridwidth=0.8)
    st.plotly_chart(fig, use_container_width=True)

    # resumo por década
    stat["decada"] = (stat["year"]//10)*10
    dec = (stat[stat["year"]>=1950]
           .groupby(["scenario","decada"])["mean"].mean().reset_index()
           .pivot(index="decada", columns="scenario", values="mean").sort_index().reset_index())
    order = ["decada","historical","ssp126","ssp245","ssp370","ssp585"]
    dec = dec[[c for c in order if c in dec.columns]]
    headers_map = {"decada":"Década","historical":"Histórico","ssp126":"SSP1-2.6","ssp245":"SSP2-4.5","ssp370":"SSP3-7.0","ssp585":"SSP5-8.5"}
    headers = [headers_map.get(c,c) for c in dec.columns]
    dec["decada"] = dec["decada"].astype(int).astype(str)
    for c in dec.columns:
        if c != "decada":
            dec[c] = dec[c].apply(lambda v: "" if pd.isna(v) else f"{float(v):.4f}")
    cell_vals = [dec[c].tolist() for c in dec.columns]
    fig_tbl = go.Figure(data=[go.Table(header=dict(values=headers, align="center"),
                                       cells=dict(values=cell_vals, align="center"))])
    fig_tbl.update_layout(margin=dict(l=0,r=0,t=8,b=0), height=420)
    st.plotly_chart(fig_tbl, use_container_width=True)

    # download
    buf = io.StringIO(); stat.to_csv(buf, index=False)
    st.download_button("💾 Download CSV (ensemble — mean/min/max por cenário/ano)",
                       data=buf.getvalue(), file_name="cmip6_global_ensemble_anom.csv",
                       mime="text/csv", key="dl_cmip6_global_ensemble")
