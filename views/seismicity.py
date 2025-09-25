# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import streamlit as st
from utils import charts
from services.seismic import fetch_usgs_quakes

@st.cache_data(ttl=15*60)
def _cached_quakes(lat, lon, start, end, radius_km, minmag, limit):
    return fetch_usgs_quakes(lat, lon, start, end, radius_km, minmag, limit)

def render_seismicity_tab(lat: float, lon: float, start, end):
    st.subheader("🌍 Sismicidade (USGS)")

    # ---------------- Filtros da aba ----------------
    top1, top2, top3, top4 = st.columns([1,1,1,1])
    with top1:
        radius_km = st.number_input("Raio (km)", min_value=10.0, max_value=2000.0, value=500.0, step=10.0)
    with top2:
        minmag = st.number_input("Magnitude mínima", min_value=0.0, max_value=9.9, value=2.5, step=0.1, format="%.1f")
    with top3:
        limit = st.number_input("Máx. eventos", min_value=100, max_value=20000, value=5000, step=100)
    with top4:
        agg = st.selectbox("Agregação (para o histograma)", ["Diário", "Mensal", "Anual"], index=1)

    period_txt = f"{str(start)} → {str(end)} • raio {int(radius_km)} km • M≥{minmag:g}"
    st.caption(f"Período e parâmetros: **{period_txt}**")

    # ---------------- Dados ----------------
    with st.spinner("A obter eventos sísmicos da USGS…"):
        df = _cached_quakes(lat, lon, start, end, radius_km, minmag, int(limit))
    if df.empty:
        st.info("Sem eventos para os critérios selecionados."); return

    # Distância ao centro (haversine simplificado)
    df["distance_km"] = _haversine_vec(lat, lon, df["latitude"].astype(float), df["longitude"].astype(float))

   
    # ---------------- Distribuição por ano (à esquerda) + Histograma (à direita) ----------------
    st.subheader("Evolução temporal e distribuição")

    left, right = st.columns(2)

    # ====== NOVO: Distribuição anual por intervalos de magnitude (empilhado) ======
    with left:
        dfa = df.copy()
        dfa["year"] = pd.to_datetime(dfa["time_utc"]).dt.year

        # Define intervalos de magnitude (bins) legíveis
        bins, labels = _mag_bins(minmag)  # usa a minmag atual para o 1º limite
        dfa["mag_bin"] = pd.cut(dfa["mag"], bins=bins, labels=labels, right=False, include_lowest=True)

        dist = dfa.groupby(["year", "mag_bin"], as_index=False, observed=False).size().rename(columns={"size": "events"})

        # Garantir todas as combinações (anos x bins) para barras vazias = 0
        years = sorted(dfa["year"].unique())
        grid = pd.MultiIndex.from_product([years, labels], names=["year", "mag_bin"])
        dist = dist.set_index(["year", "mag_bin"]).reindex(grid, fill_value=0).reset_index()

        fig_stack = charts.bar(
            dist, x="year", y="events", color="mag_bin",
            title="Distribuição anual por intervalos de magnitude",
            x_title="Ano", y_title="Eventos"
        )
        fig_stack.update_layout(barmode="stack")
        st.plotly_chart(fig_stack, width="stretch")

    # ====== Histograma (contagens) com agregação selecionada ======
    with right:
        dfts = df.copy()
        dt = pd.to_datetime(dfts["time_utc"])
        if agg == "Diário":
            dfts["period"] = dt.dt.date; x_title = "Dia"; fmt = None
        elif agg == "Mensal":
            dfts["period"] = dt.dt.to_period("M").dt.to_timestamp(); x_title = "Mês"; fmt = "%Y-%m"
        else:
            dfts["period"] = dt.dt.to_period("Y").dt.to_timestamp(); x_title = "Ano"; fmt = "%Y"

        nb = int(np.clip(np.sqrt(len(df)) * 1.5, 10, 40))
        fig_hist = charts.hist(dfts, x="mag", nbins=nb,
                               title=f"Histograma de magnitudes — {period_txt}",
                               x_title="Magnitude (Mw)")
        st.plotly_chart(fig_hist, width="stretch")

    # ---------------- Tabela + CSV ----------------
    st.subheader("Eventos (tabela)")
    show_cols = ["time_utc","mag","depth_km","distance_km","place","latitude","longitude","id"]
    st.dataframe(df[show_cols], use_container_width=True, hide_index=True)

    st.download_button("💾 Download CSV (sismos)",
                       data=df[show_cols].to_csv(index=False),
                       file_name="sismos_usgs.csv", mime="text/csv",
                       key="dl_csv_quakes")

# ---------- helpers ----------
def _haversine_vec(lat1, lon1, lat2_series, lon2_series):
    import numpy as np
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
    Intervalos resultantes (labels):
      [minmag/2.5→2.5), [2.5→3.5), [3.5→4.5), [4.5→5.5), [5.5→∞)
    """
    lo = min(2.5, float(minmag))  # se Mmin>2.5, o 1º bin começa em Mmin
    edges = [lo, 3.5, 4.5, 5.5, 10.0]  # 10 como teto prático
    edges = sorted(set(edges))  # segura duplicados quando lo==3.5, etc.

    # Se minmag < 2.5, acrescenta um bin inicial [minmag,2.5)
    if float(minmag) < 2.5:
        edges = [float(minmag)] + edges
    # Garante estritamente crescente
    edges = sorted(edges)

    # Labels
    labels = []
    for i in range(len(edges)-1):
        labels.append(f"{edges[i]:.1f}–{edges[i+1]:.1f}")
    labels.append(f"≥{edges[-1]:.1f}")

    # Para o cut: precisamos incluir o topo do penúltimo como início do último bin aberto
    cut_edges = edges + [np.inf]
    return cut_edges, labels

