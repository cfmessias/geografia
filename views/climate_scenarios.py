# -*- coding: utf-8 -*-
# views/climate_scenarios.py — GLOBAL OFFLINE (lendo data/)
from __future__ import annotations
from pathlib import Path
import io
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from services.i18n import t as tr
try:
    from services.i18n_boot import _ensure_lang_state
except ImportError:
    from services.i18n_boot import init_i18n_state as _ensure_lang_state

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"

_SCENARIO_COLORS = {
    "historical": "#6c757d",
    "ssp126":     "#2ca02c",
    "ssp245":     "#ff7f0e",
    "ssp370":     "#9467bd",
    "ssp585":     "#d62728",
}

def _hex_to_rgba(hex_color: str, a: float) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{a})"

def _scn_label(code: str) -> str:
    # labels i18n dos cenários
    mapping = {
        "historical": tr("climate_scenarios.scn.historical"),
        "ssp126":     tr("climate_scenarios.scn.ssp126"),
        "ssp245":     tr("climate_scenarios.scn.ssp245"),
        "ssp370":     tr("climate_scenarios.scn.ssp370"),
        "ssp585":     tr("climate_scenarios.scn.ssp585"),
    }
    return mapping.get(code, code)

def _warming_tail_value(df: pd.DataFrame, scn: str) -> float | None:
    g = df[df["scenario"] == scn].sort_values("time")
    if g.empty:
        return None
    end = g[(g["year"] >= 2091) & (g["year"] <= 2100)]
    if end.empty:
        end = g.tail(10)
    if end.empty or end["mean"].isna().all():
        return None
    return float(end["mean"].mean())

def render_climate_tab():
    _ensure_lang_state()
    st.subheader(tr("climate_scenarios.projecoes_globais_de_temperatura_cmip6_media_e_incerteza_por_cenario"))

    f_stat = DATA_DIR / "cmip6_global_ensemble_stats.csv"
    f_all  = DATA_DIR / "cmip6_global_all_models.csv"
    if not f_stat.exists() or not f_all.exists():
        st.warning(tr("climate_scenarios.faltam_csvs_cmd", cmd="python -u scripts/meteo/fetch_cmip6_global.py"))
        return

    # — Dados e smoothing —
    stats = pd.read_csv(f_stat, parse_dates=["time"])
    stats["year"] = stats["time"].dt.year.astype(int)

    scenarios = st.multiselect(
        tr("climate_scenarios.cenarios_a_mostrar"),
        options=["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        default=["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        format_func=_scn_label,
    )
    win = st.number_input(tr("climate_scenarios.suavizacao_media_movel_anos"), min_value=1, max_value=11, value=5, step=2)

    stat = stats.copy()
    # corta 'historical' pré-1950 só para limpeza visual
    mask = (stat["scenario"] != "historical") | (stat["time"].dt.year >= 1950)
    stat = stat.loc[mask].reset_index(drop=True)
    stat["mean"] = (
        stat.sort_values("time")
            .groupby("scenario", observed=False)["mean"]
            .transform(lambda s: s.rolling(win, center=True, min_periods=1).mean())
    )

    # — Figura principal —
    fig = go.Figure()
    present_scenarios = [s for s in scenarios if s in set(stat["scenario"])]

    for scn in present_scenarios:
        g = stat[stat["scenario"] == scn].sort_values("time")
        color = _SCENARIO_COLORS.get(scn, "#1f77b4")

        # envelope min–max (incerteza)
        fig.add_trace(go.Scatter(
            x=pd.concat([g["time"], g["time"][::-1]], ignore_index=True),
            y=pd.concat([g["max"],  g["min"][::-1]], ignore_index=True),
            fill="toself", fillcolor=_hex_to_rgba(color, 0.18),
            line=dict(width=0), hoverinfo="skip", showlegend=False,
            name=f"{_scn_label(scn)}",
        ))
        # média com marcadores
        fig.add_trace(go.Scatter(
            x=g["time"], y=g["mean"], mode="lines+markers",
            line=dict(color=color, width=2), marker=dict(size=4),
            name=_scn_label(scn), showlegend=False,
        ))

        # anotação no fim da série com ~+Δ°C (2091–2100)
        g_valid = g.dropna(subset=["mean"])
        if not g_valid.empty:
            x_last = g_valid["time"].iloc[-1]
            y_last = g_valid["mean"].iloc[-1]
            w = _warming_tail_value(stat, scn)
            txt = _scn_label(scn) + (f" · ~+{w:.1f} °C" if w is not None else "")
            fig.add_annotation(
                x=x_last, y=y_last, text=txt, xanchor="left", yanchor="middle",
                xshift=8, showarrow=False, font=dict(size=12), bgcolor="rgba(0,0,0,0)"
            )

    fig.update_layout(
        height=380, margin=dict(l=6, r=6, t=40, b=0),
        xaxis_title=tr("climate_indicators.ano"),
        yaxis_title=tr("climate_scenarios.δc_anomalia_vs_1991_2020_calculada_offline"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    fig.update_yaxes(gridcolor="rgba(160,160,160,0.35)", gridwidth=1.2)
    fig.update_xaxes(gridcolor="rgba(160,160,160,0.18)", gridwidth=0.8)
    st.plotly_chart(fig, use_container_width=True)

    # — Resumo por década (tabela) —
    stat["decada"] = (stat["year"] // 10) * 10
    dec = (
        stat[stat["year"] >= 1950]
        .groupby(["scenario", "decada"], observed=False)["mean"].mean().reset_index()
        .pivot(index="decada", columns="scenario", values="mean").sort_index().reset_index()
    )
    order = ["decada", "historical", "ssp126", "ssp245", "ssp370", "ssp585"]
    dec = dec[[c for c in order if c in dec.columns]]

    headers_map = {
        "decada":    tr("climate_scenarios.decada"),
        "historical": _scn_label("historical"),
        "ssp126":     _scn_label("ssp126"),
        "ssp245":     _scn_label("ssp245"),
        "ssp370":     _scn_label("ssp370"),
        "ssp585":     _scn_label("ssp585"),
    }
    headers = [headers_map.get(c, c) for c in dec.columns]

    # formatação para strings (quatro casas)
    dec_fmt = dec.copy()
    dec_fmt["decada"] = dec_fmt["decada"].astype(int).astype(str)
    for c in dec_fmt.columns:
        if c != "decada":
            dec_fmt[c] = dec_fmt[c].apply(lambda v: "" if pd.isna(v) else f"{float(v):.4f}")

    cell_vals = [dec_fmt[c].tolist() for c in dec_fmt.columns]
    fig_tbl = go.Figure(data=[go.Table(
        header=dict(values=headers, align="center"),
        cells=dict(values=cell_vals, align="center"),
    )])
    fig_tbl.update_layout(margin=dict(l=0, r=0, t=8, b=0), height=420)
    st.plotly_chart(fig_tbl, use_container_width=True)

    # — Download do CSV (ensemble mean/min/max) —
    buf = io.StringIO()
    stat.to_csv(buf, index=False)
    st.download_button(
        tr("climate_scenarios.download_csv_ensemble_mean_min_max_por_cenario_ano"),
        data=buf.getvalue(),
        file_name="cmip6_global_ensemble_anom.csv",
        mime="text/csv",
        key="dl_cmip6_global_ensemble",
    )
