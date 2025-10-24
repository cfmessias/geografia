# -*- coding: utf-8 -*-
from __future__ import annotations

import numpy as np
import pandas as pd
import streamlit as st

from utils import charts
from services.seismic import fetch_usgs_quakes
from services.i18n import t as tr
try:
    from services.i18n_boot import _ensure_lang_state
except ImportError:
    from services.i18n_boot import init_i18n_state as _ensure_lang_state


# ---------------- Cache de dados ----------------
@st.cache_data(ttl=15 * 60, show_spinner=False)
def _cached_quakes(lat, lon, start, end, radius_km, minmag, limit):
    return fetch_usgs_quakes(lat, lon, start, end, radius_km, minmag, limit)


# ---------------- Helpers ----------------
def _fmt_date(d):
    try:
        return d.strftime("%Y-%m-%d")
    except Exception:
        return str(d)

def _haversine_vec(lat1, lon1, lat2_series, lon2_series):
    R = 6371.0
    lat1r = np.radians(lat1); lon1r = np.radians(lon1)
    lat2r = np.radians(lat2_series.values.astype(float))
    lon2r = np.radians(lon2_series.values.astype(float))
    dlat = lat2r - lat1r; dlon = lon2r - lon1r
    a = np.sin(dlat/2.0)**2 + np.cos(lat1r)*np.cos(lat2r)*np.sin(dlon/2.0)**2
    return R * 2.0 * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a))

def _mag_bins(minmag: float):
    """
    Constrói bins legíveis com base na magnitude mínima atual.
    Ex.: [minmag→3.5), [3.5→4.5), [4.5→5.5), [5.5→∞)
    """
    lo = float(minmag)
    # limites base
    edges = [max(2.5, lo), 3.5, 4.5, 5.5, 10.0]
    edges = sorted(set(edges))
    if lo < 2.5:
        edges = [lo] + edges
    edges = sorted(edges)

    labels = []
    for i in range(len(edges) - 1):
        labels.append(f"{edges[i]:.1f}–{edges[i+1]:.1f}")
    labels.append(f"≥{edges[-1]:.1f}")

    cut_edges = edges + [np.inf]
    return cut_edges, labels


# ---------------- Render ----------------
def render_seismicity_tab(lat, lon, start, end, key_prefix: str | None = None, **_):
    _ensure_lang_state()
    st.subheader(tr("seismicity.sismicidade_usgs"))

    # ── Filtros da aba ───────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        raio_km = st.number_input(tr("seismicity.raio_km"),
                                  min_value=10.0, max_value=2000.0, value=500.0, step=10.0)
    with c2:
        minmag = st.number_input(tr("seismicity.magnitude_minima"),
                                 min_value=0.0, max_value=9.9, value=2.5, step=0.1, format="%.1f")
    with c3:
        limit = st.number_input(tr("seismicity.max_eventos"),
                                min_value=100, max_value=20000, value=5000, step=100)
    with c4:
        codes = ["D", "M", "Y"]  # daily, monthly, yearly
        labels = {
            "D": tr("seismicity.diario"),
            "M": tr("seismicity.mensal"),
            "Y": tr("seismicity.anual"),
        }
        agg_code = st.selectbox(
            tr("seismicity.agregacao_para_o_histograma"),
            options=codes,
            index=codes.index("Y"),   # default: Anual
            format_func=lambda c: labels[c],
            key=f"{key_prefix or 'eq'}_agg",
        )

    # legenda "Período e parâmetros"
    st.caption(tr(
        "seismicity.periodo_e_parametros",
        start=_fmt_date(start),
        end=_fmt_date(end),
        radius_km=int(raio_km),
        mag_min=f"{float(minmag):g}",
    ))

    # ── Dados ────────────────────────────────────────────────────────────────
    with st.spinner("A obter eventos sísmicos da USGS…"):
        df = _cached_quakes(lat, lon, start, end, float(raio_km), float(minmag), int(limit))
    if df is None or df.empty:
        st.info("Sem eventos para os critérios selecionados.")
        return

    # Distância ao centro (km)
    df["distance_km"] = _haversine_vec(lat, lon,
                                       df["latitude"].astype(float),
                                       df["longitude"].astype(float))

    st.subheader(tr("seismicity.evolucao_temporal_e_distribuicao"))

    left, right = st.columns(2)

    # ── Esquerda: distribuição anual por bins de magnitude (empilhado) ──────
    with left:
        dfa = df.copy()
        dfa["year"] = pd.to_datetime(dfa["time_utc"]).dt.year

        bins, lbls = _mag_bins(minmag)
        dfa["mag_bin"] = pd.cut(
            pd.to_numeric(dfa["mag"], errors="coerce"),
            bins=bins, labels=lbls, right=False, include_lowest=True
        )

        dist = (dfa.groupby(["year", "mag_bin"], as_index=False, observed=False)
                  .size().rename(columns={"size": "events"}))

        years = sorted(dfa["year"].dropna().unique().tolist())
        grid = pd.MultiIndex.from_product([years, lbls], names=["year", "mag_bin"])
        dist = (dist.set_index(["year", "mag_bin"])
                    .reindex(grid, fill_value=0)
                    .reset_index())

        fig_stack = charts.bar(
            dist, x="year", y="events", color="mag_bin",
            title=tr("seismicity.distribuicao_anual_por_intervalos_de_magnitude"),
            x_title=tr("climate_indicators.ano"),
            y_title=tr("seismicity.eventos"),
        )
        fig_stack.update_layout(barmode="stack")
        st.plotly_chart(fig_stack, use_container_width=True)

    # ── Direita: histograma temporal (contagem) com agregação selecionada ────
    with right:
        dfts = df.copy()
        dt = pd.to_datetime(dfts["time_utc"], errors="coerce")

        if agg_code == "D":
            freq, x_title, tickfmt = "D", tr("seismicity.dia"), "%Y-%m-%d"
            dfts["period"] = dt.dt.floor("D")
        elif agg_code == "M":
            freq, x_title, tickfmt = "MS", tr("seismicity.mes"), "%Y-%m"
            dfts["period"] = dt.dt.to_period("M").dt.to_timestamp()
        else:  # "Y"
            freq, x_title, tickfmt = "YS", tr("seismicity.ano"), "%Y"
            dfts["period"] = dt.dt.to_period("Y").dt.to_timestamp()

        counts = (dfts.dropna(subset=["period"])
                       .groupby("period", as_index=False, observed=False)
                       .size().rename(columns={"size": "events"}))

        full_idx = pd.date_range(start=pd.to_datetime(start), end=pd.to_datetime(end), freq=freq)
        counts = (counts.set_index("period")
                         .reindex(full_idx, fill_value=0)
                         .rename_axis("period")
                         .reset_index())

        fig_hist = charts.bar(
            counts, x="period", y="events",
            title=tr("seismicity.eventos_por_periodo"),
            x_title=x_title, y_title=tr("seismicity.eventos"),
        )
        # cor distinta
        fig_hist.update_traces(marker_color="#9b59b6", marker_line_color="#9b59b6")
        fig_hist.update_xaxes(tickformat=tickfmt)
        st.plotly_chart(fig_hist, use_container_width=True)

    # ── Tabela + CSV ─────────────────────────────────────────────────────────
    st.subheader(tr("seismicity.eventos_tabela"))
    show_cols = ["time_utc", "mag", "depth_km", "distance_km", "place", "latitude", "longitude", "id"]
    st.dataframe(df[show_cols], use_container_width=True, hide_index=True)
    # st.download_button(
    #     tr("seismicity.download_csv_sismos"),
    #     data=df[show_cols].to_csv(index=False),
    #     file_name="sismos_usgs.csv",
    #     mime="text/csv",
    #     key="dl_csv_quakes",
    # )
