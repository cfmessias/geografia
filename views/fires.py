# -*- coding: utf-8 -*-
import io
import numpy as np
import pandas as pd
import streamlit as st
from utils import charts

_SEASONS_PT = ["Inverno", "Primavera", "Verão", "Outono"]

@st.cache_data(ttl=60 * 10)
def _load_icnf_csv(path: str) -> pd.DataFrame:
    """
    Espera um CSV com colunas: year;season;occurrences;burned_area_ha
    - year: int
    - season: Inverno / Primavera / Verão / Outono
    - occurrences: int
    - burned_area_ha: float (hectares)
    """
    # tenta separar por ';' (como no exemplo), cai para ',' se necessário
    try:
        df = pd.read_csv(path, sep=";")
    except Exception:
        df = pd.read_csv(path)

    # normalizações
    df.columns = [c.strip().lower() for c in df.columns]
    expected = {"year", "season", "occurrences", "burned_area_ha"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"Colunas em falta no CSV: {sorted(missing)} "
                         f"(esperado: {sorted(expected)})")

    # tipos
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["occurrences"] = pd.to_numeric(df["occurrences"], errors="coerce")
    df["burned_area_ha"] = pd.to_numeric(df["burned_area_ha"], errors="coerce")

    # estação em PT normalizada (title-case + ordem consistente)
    df["season"] = df["season"].astype(str).str.strip().str.lower().str.replace("+", "ã", regex=False)
    map_pt = {
        "inverno": "Inverno",
        "primavera": "Primavera",
        "verão": "Verão",
        "verao": "Verão",
        "outono": "Outono",
    }
    df["season"] = df["season"].map(map_pt).fillna(df["season"].str.title())

    # limpa linhas inválidas
    df = df.dropna(subset=["year", "season"]).copy()
    df["year"] = df["year"].astype(int)

    return df

def _agg_by_year(df: pd.DataFrame, season: str | None) -> pd.DataFrame:
    """Aggregação por ano (total Portugal), opcionalmente filtrada a uma estação."""
    d = df.copy()
    if season and season in _SEASONS_PT:
        d = d[d["season"] == season]
    return d.groupby("year", as_index=False).agg(
        occurrences=("occurrences", "sum"),
        burned_area_ha=("burned_area_ha", "sum")
    ).sort_values("year")

def _fmt_delta(v):
    if v is None or (isinstance(v, float) and (np.isnan(v))):
        return None
    s = f"{v:+,.0f}"
    return s.replace(",", " ")  # espaço fino para legibilidade

def render_fires_tab(csv_path: str = "dados/fogos_icnf.csv"):
    st.subheader("🔥 Fogos florestais (ICNF) — Portugal")

    # Permitir alterar o caminho do CSV (útil para testes)
    cpath1, cpath2 = st.columns([0.7, 0.3])
    with cpath1:
        path = st.text_input("Ficheiro CSV do ICNF", value=csv_path, help="Formato: year;season;occurrences;burned_area_ha")
    with cpath2:
        st.caption("Alimenta este CSV manualmente a partir dos Excel anuais do ICNF.")

    # Carregar dados
    try:
        df_raw = _load_icnf_csv(path)
    except Exception as e:
        st.error(f"Não foi possível ler o CSV em **{path}**.\n\nDetalhe: {e}")
        st.stop()

    years_avail = sorted(df_raw["year"].unique())
    if not years_avail:
        st.info("CSV sem anos válidos.")
        st.stop()

    # ---------- Filtros da aba ----------
    top1, top2, top3 = st.columns([1, 1, 1])
    with top1:
        season = st.selectbox("Estação", ["Todas"] + _SEASONS_PT, index=1)  # default Primavera (ajusta se preferires)
        season_sel = None if season == "Todas" else season
    with top2:
        default_b = years_avail[-1]
        default_a = years_avail[-2] if len(years_avail) >= 2 else years_avail[0]
        year_a = st.selectbox("Ano A", years_avail, index=years_avail.index(default_a))
    with top3:
        year_b = st.selectbox("Ano B", years_avail, index=years_avail.index(default_b))

    if year_a == year_b:
        st.warning("Escolha dois anos diferentes para comparar.")

    # ---------- Séries por ano (total ou por estação) ----------
    ser = _agg_by_year(df_raw, season_sel)

    c1, c2 = st.columns(2)
    with c1:
        fig_occ = charts.bar(
            ser, x="year", y="occurrences",
            title=f"Nº de ocorrências por ano{'' if not season_sel else f' — {season_sel}'}",
            x_title="Ano", y_title="Ocorrências"
        )
        st.plotly_chart(fig_occ, use_container_width=True)
    with c2:
        fig_area = charts.bar(
            ser, x="year", y="burned_area_ha",
            title=f"Área ardida (ha) por ano{'' if not season_sel else f' — {season_sel}'}",
            x_title="Ano", y_title="ha"
        )
        st.plotly_chart(fig_area, use_container_width=True)

    # ---------- Comparação direta Ano A vs Ano B ----------
    st.subheader("Comparação entre anos")

    # dados dos anos selecionados (agregados na mesma regra das séries)
    rowA = ser[ser["year"] == year_a].iloc[0] if (ser["year"] == year_a).any() else None
    rowB = ser[ser["year"] == year_b].iloc[0] if (ser["year"] == year_b).any() else None

    occA = None if rowA is None else float(rowA["occurrences"])
    occB = None if rowB is None else float(rowB["occurrences"])
    areaA = None if rowA is None else float(rowA["burned_area_ha"])
    areaB = None if rowB is None else float(rowB["burned_area_ha"])

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric(f"Ocorrências — {year_a}", f"{(0 if occA is None else occA):,.0f}".replace(",", " "))
    with m2:
        st.metric(f"Ocorrências — {year_b}",
                  f"{(0 if occB is None else occB):,.0f}".replace(",", " "),
                  delta=_fmt_delta(None if (occA is None or occB is None) else occB - occA))
    with m3:
        st.metric(f"Área ardida — {year_a}", f"{(0 if areaA is None else areaA):,.0f} ha".replace(",", " "))
    with m4:
        st.metric(f"Área ardida — {year_b}",
                  f"{(0 if areaB is None else areaB):,.0f} ha".replace(",", " "),
                  delta=_fmt_delta(None if (areaA is None or areaB is None) else areaB - areaA))

    # ---------- Distribuição por estação (empilhado), para os 2 anos (opcional e útil) ----------
    st.subheader("Composição por estação (apenas anos escolhidos)")
    picked = df_raw[df_raw["year"].isin([year_a, year_b])].copy()
    comp = picked.groupby(["year", "season"], as_index=False).agg(
        occurrences=("occurrences", "sum"),
        burned_area_ha=("burned_area_ha", "sum")
    )
    # garantir ordem consistente de estações
    comp["season"] = pd.Categorical(comp["season"], categories=_SEASONS_PT, ordered=True)
    comp = comp.sort_values(["year", "season"])

    cc1, cc2 = st.columns(2)
    with cc1:
        fig_st_occ = charts.bar(comp, x="year", y="occurrences", color="season",
                                title="Ocorrências por estação (anos selecionados)",
                                x_title="Ano", y_title="Ocorrências")
        fig_st_occ.update_layout(barmode="stack")
        st.plotly_chart(fig_st_occ, use_container_width=True)
    with cc2:
        fig_st_area = charts.bar(comp, x="year", y="burned_area_ha", color="season",
                                 title="Área ardida por estação (anos selecionados)",
                                 x_title="Ano", y_title="ha")
        fig_st_area.update_layout(barmode="stack")
        st.plotly_chart(fig_st_area, use_container_width=True)

    # ---------- Tabela + Download ----------
    st.subheader("Tabela (conforme filtro de estação)")
    # Se “Todas”, mostramos detalhe por estação; senão, só a estação escolhida
    grid = (df_raw if season_sel is None else df_raw[df_raw["season"] == season_sel]).copy()
    grid = grid.sort_values(["year", "season"])
    # formatação leve
    grid_fmt = grid.copy()
    grid_fmt["year"] = grid_fmt["year"].astype(int).astype(str)
    st.dataframe(grid_fmt, use_container_width=True, hide_index=True)

    buf = io.StringIO()
    grid.to_csv(buf, index=False)
    st.download_button("💾 Download CSV (fogos — ICNF)", data=buf.getvalue(),
                       file_name="fogos_icnf_filtrado.csv", mime="text/csv",
                       key="dl_csv_fires_icnf")
