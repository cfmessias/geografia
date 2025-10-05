# -*- coding: utf-8 -*-
from __future__ import annotations

import io
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
from pandas.api.types import is_datetime64_any_dtype
from services.i18n import t as tr
try:
    from services.i18n_boot import _ensure_lang_state
except ImportError:
    from services.i18n_boot import init_i18n_state as _ensure_lang_state


# ─────────────────────────── Constantes / Rótulos ─────────────────────────────

BASIN_LABELS = {
    "EP": "EP — Pacífico Este",
    "WP": "WP — Pacífico Oeste",
    "NI": "NI — Índico Norte",
    "SI": "SI — Índico Sul",
    "SP": "SP — Pacífico Sul",
    "NA": "NA — Atlântico Norte",
    "SA": "SA — Atlântico Sul",
    "NAN": "NA — Atlântico Norte",  # alguns dumps usam NAN
}

_IBTRACS_URLS = [
    "https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r01/access/csv/ibtracs.ALL.list.v04r01.csv",
    "https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r00/access/csv/ibtracs.ALL.list.v04r00.csv",  # fallback
]


# ─────────────────────────── Utils HTTP/CSV ───────────────────────────────────

def _http_get(url: str, timeout: int = 30) -> str:
    headers = {"User-Agent": "MeteoApp/1.0 (+streamlit; climate-indicators)"}
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or r.encoding
    return r.text


def _read_hash_csv(txt: str, delimiter: str = ",") -> pd.DataFrame:
    """Lê ficheiros da NOAA com linhas de comentário (#)."""
    lines = [ln for ln in txt.splitlines() if ln.strip() and not ln.lstrip().startswith("#")]
    if not lines:
        return pd.DataFrame()
    return pd.read_csv(io.StringIO("\n".join(lines)), delimiter=delimiter)


# ─────────────────────────── Helpers gráficos ─────────────────────────────────

def _line(df: pd.DataFrame, x: str, y: str, title: str, y_title: str) -> go.Figure:
    fig = px.line(df, x=x, y=y, title=title)
    fig.update_traces(mode="lines+markers")
    fig.update_layout(margin=dict(l=6, r=6, t=40, b=0), xaxis_title=None, yaxis_title=y_title)
    fig.update_yaxes(gridcolor="rgba(160,160,160,0.35)", gridwidth=1)
    fig.update_xaxes(gridcolor="rgba(160,160,160,0.18)", gridwidth=0.8)
    return fig


def _rolling(s: pd.Series, window: int) -> pd.Series:
    return s.rolling(window, min_periods=max(1, window // 2), center=True).mean()


# ─────────────────────────── Helpers tabelas (formatação consistente) ─────────
def _parse_time_col(s: pd.Series) -> pd.Series:
    # já é datetime? devolve como está
    if is_datetime64_any_dtype(s):
        return pd.to_datetime(s, errors="coerce")

    # normaliza para string
    s = s.astype(str).str.strip()

    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")

    # YYYY-MM-DD
    m_ymd = s.str.fullmatch(r"\d{4}-\d{2}-\d{2}")
    if m_ymd.any():
        out[m_ymd] = pd.to_datetime(s[m_ymd], format="%Y-%m-%d", errors="coerce")

    # YYYY-MM  → assume dia 1
    m_ym = s.str.fullmatch(r"\d{4}-\d{2}")
    if m_ym.any():
        out[m_ym] = pd.to_datetime(s[m_ym] + "-01", format="%Y-%m-%d", errors="coerce")

    # YYYY
    m_y = s.str.fullmatch(r"\d{4}")
    if m_y.any():
        out[m_y] = pd.to_datetime(s[m_y], format="%Y", errors="coerce")

    # fallback para o resto (mantém silêncio; sem inferência ruidosa)
    rest = out.isna() & s.ne("")
    if rest.any():
        out[rest] = pd.to_datetime(s[rest], errors="coerce")

    return out

def df_year_as_text(df: pd.DataFrame, year_col_candidates=("year", "ano")) -> pd.DataFrame:
    """Converte a coluna de ano em texto para evitar separadores de milhares."""
    df2 = df.copy()
    ycol = next((c for c in df2.columns if c.lower() in year_col_candidates), None)
    if ycol:
        df2[ycol] = pd.to_numeric(df2[ycol], errors="coerce").astype("Int64").astype(str)
    return df2


def dataframe_fmt(df: pd.DataFrame, year_col: Optional[str], int_cols: Optional[List[str]] = None,
                  float_cols: Optional[Dict[str, str]] = None,
                  **kwargs):
    """
    Mostra um st.dataframe com:
      - ano em texto (sem separador)
      - inteiros com formato %d
      - floats com formato fornecido (ex: '%.1f', '%.2f')
    """
    df2 = df.copy()
    if year_col and year_col in df2.columns:
        df2[year_col] = pd.to_numeric(df2[year_col], errors="coerce").astype("Int64").astype(str)

    # column_config
    colcfg = {}
    if year_col and year_col in df2.columns:
        colcfg[year_col] = st.column_config.TextColumn(year_col)

    if int_cols:
        for c in int_cols:
            if c in df2.columns:
                # garantir numérico (para ordenar corretamente), depois format string
                df2[c] = pd.to_numeric(df2[c], errors="coerce")
                colcfg[c] = st.column_config.NumberColumn(c, format="%d")

    if float_cols:
        for c, fmt in float_cols.items():
            if c in df2.columns:
                df2[c] = pd.to_numeric(df2[c], errors="coerce")
                colcfg[c] = st.column_config.NumberColumn(c, format=fmt)

    st.dataframe(df2, use_container_width=True, hide_index=True, column_config=colcfg, **kwargs)


# ─────────────────────────── Loaders de dados ─────────────────────────────────

@st.cache_data(ttl=6 * 3600, show_spinner=True)
def load_co2_noaa() -> tuple[pd.DataFrame, pd.DataFrame]:
    """CO₂ Mauna Loa (NOAA/GML), mensal desde 1958."""
    url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_mm_mlo.csv"
    txt = _http_get(url)
    df = _read_hash_csv(txt).rename(columns=str.lower)
    for c in ["average", "deseasonalized"]:
        if c in df.columns:
            df.loc[df[c] < 0, c] = np.nan
    df["date"] = pd.to_datetime(dict(year=df["year"].astype(int), month=df["month"].astype(int), day=1))
    df = df.sort_values("date")
    ann = (df.set_index("date")["average"].resample("YE").mean()
             .rename("co2_annual_ppm").reset_index())
    ann["year"] = ann["date"].dt.year
    mo = df[["date", "average", "deseasonalized"]].rename(
        columns={"average": "co2_ppm", "deseasonalized": "co2_ppm_deseas"}
    )
    return mo, ann[["year", "co2_annual_ppm"]]


@st.cache_data(ttl=6 * 3600, show_spinner=True)
def load_temp_gistemp() -> tuple[pd.DataFrame, pd.DataFrame]:
    """NASA GISTEMP v4 – anomalia (°C), mensal desde 1880."""
    url = "https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv"
    txt = _http_get(url)
    lines = txt.splitlines()
    start = next(i for i, ln in enumerate(lines) if ln.startswith("Year"))
    core = "\n".join(lines[start:])
    df = pd.read_csv(io.StringIO(core)).replace("***", np.nan)
    months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    long = df.melt(id_vars=["Year"], value_vars=months, var_name="month", value_name="anom_c")
    month_num = {m:i+1 for i,m in enumerate(months)}
    long["month"] = long["month"].map(month_num)
    long["Year"] = pd.to_numeric(long["Year"], errors="coerce")
    long["anom_c"] = pd.to_numeric(long["anom_c"], errors="coerce")
    long = long.dropna(subset=["Year","month"])
    long["date"] = pd.to_datetime(dict(year=long["Year"].astype(int), month=long["month"].astype(int), day=15))
    long = long.sort_values("date")
    df["J-D"] = pd.to_numeric(df["J-D"], errors="coerce")
    ann = df[["Year","J-D"]].rename(columns={"Year":"year","J-D":"anom_c"}).dropna().reset_index(drop=True)
    return long[["date","anom_c"]], ann


@st.cache_data(ttl=12 * 3600, show_spinner=True)
def load_ibtracs_list() -> pd.DataFrame:
    """Carrega o CSV global do IBTrACS (lista de tempestades)."""
    last_err = None
    for url in _IBTRACS_URLS:
        try:
            txt = _http_get(url, timeout=40)
            df = pd.read_csv(io.StringIO(txt))
            df.columns = [c.strip() for c in df.columns]
            return df
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Falha a obter IBTrACS: {last_err}")


def summarize_ibtracs(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Cria: anual global, anual major (>=96 kt), e anual por bacia."""
    if df.empty:
        return {}

    col_sid = "SID" if "SID" in df.columns else df.columns[df.columns.str.upper().str.contains("SID")][0]
    col_basin = "Basin" if "Basin" in df.columns else df.columns[df.columns.str.lower().str.contains("basin")][0]
    col_time = "ISO_TIME" if "ISO_TIME" in df.columns else df.columns[df.columns.str.upper().str.contains("TIME")][0]
    wind_col = next((c for c in ["WMO_WIND", "USA_WIND", "WIND_WMO", "WIND_USA"] if c in df.columns), None)

    dt = _parse_time_col(df[col_time])
    year = dt.dt.year

    base = pd.DataFrame({
        "SID": df[col_sid].astype(str),
        "BASIN": df[col_basin].astype(str).str.upper(),
        "YEAR": year,
    })
    base["WIND_KT"] = pd.to_numeric(df[wind_col], errors="coerce") if wind_col else np.nan
    base = base.dropna(subset=["YEAR"]).astype({"YEAR": int})

    annual_counts = (
        base.drop_duplicates(subset=[col_sid, "YEAR"])
            .groupby("YEAR", observed=False).size().rename("count").reset_index()
    )
    major_mask = base["WIND_KT"] >= 96
    annual_major = (
        base.loc[major_mask, [col_sid, "YEAR"]]
            .drop_duplicates()
            .groupby("YEAR", observed=False).size().rename("major_count").reset_index()
    )
    annual_major = annual_counts[["YEAR"]].merge(annual_major, on="YEAR", how="left").fillna({"major_count": 0}).astype({"major_count": int})

    annual_by_basin = (
        base.drop_duplicates(subset=[col_sid, "YEAR", "BASIN"])
            .groupby(["YEAR", "BASIN"], observed=False).size().rename("count").reset_index()
    )

    return {
        "annual_counts": annual_counts.sort_values("YEAR"),
        "annual_major": annual_major.sort_values("YEAR"),
        "annual_by_basin": annual_by_basin.sort_values(["YEAR", "BASIN"]),
    }


# ─────────────────────────── Render principal ─────────────────────────────────

def render_climate_indicators_tab():
    _ensure_lang_state()
    st.subheader(tr("climate_indicators.indicadores_climaticos"))
    st.caption(tr("climate_indicators.noaa_gml_co2_nasa_gistemp_temperatura_global_noaa_ncei_ibtracs_ciclones_tropicais"))

    # ── CO₂ ───────────────────────────────────────────────────────────────────
    st.markdown(tr("climate_indicators.co2_atmosferico_mauna_loa_noaa"))
    try:
        co2_mo, co2_ann = load_co2_noaa()

        c1, c2 = st.columns([2, 1])

        with c1:
            df_plot = co2_mo.copy()
            df_plot["mm_12"] = _rolling(df_plot["co2_ppm"], 12)
            fig = _line(df_plot, "date", "co2_ppm",
                        tr("climate_indicators.co2_ppm_mensal_title"),
                        tr("units.ppm"))

            fig.add_trace(go.Scatter(x=df_plot["date"], y=df_plot["mm_12"], mode="lines", name=tr("climate_indicators.media_movel_12_m"), line=dict(width=2)))
            fig.update_xaxes(tickformat="%Y")  # anos sem separador
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            co2_ann_tbl = co2_ann.rename(columns={
                "year": tr("climate_indicators.ano"),
                "co2_annual_ppm": tr("climate_indicators.co2_ppm")
            })
            dataframe_fmt(
                co2_ann_tbl, year_col="Ano",
                float_cols={"CO₂ (ppm)": "%.1f"}
            )
            # downloads
            b1 = io.StringIO(); co2_mo.to_csv(b1, index=False)
            b2 = io.StringIO(); co2_ann.to_csv(b2, index=False)
            st.download_button(tr("climate_indicators.monthly_csv_ppm"), b1.getvalue(), "co2_noaa_monthly.csv", "text/csv", key="dl_co2_m")
            st.download_button(tr("climate_indicators.annual_csv_ppm"), b2.getvalue(), "co2_noaa_annual.csv", "text/csv", key="dl_co2_a")

        st.caption(tr("climate_indicators.fonte_noaa_gml_mauna_loa_observatory"))
    except Exception as e:
        st.error(tr("climate_indicators.erro_co2", error=str(e)))

    # ── Temperatura global ────────────────────────────────────────────────────
    st.markdown(tr("labels.text"))
    st.markdown(tr("climate_indicators.temperatura_global_anomalia_nasa_gistemp_v4"))
    try:
        temp_mo, temp_ann = load_temp_gistemp()

        c1, c2 = st.columns([2, 1])

        with c1:
            df_plot = temp_mo.copy()
            df_plot["mm_12"] = _rolling(df_plot["anom_c"], 12)
            fig = _line(df_plot, "date", "anom_c",
                        tr("climate_indicators.anomalia_mensal_c_title"),
                        tr("units.deg_c"))
            fig.add_trace(go.Scatter(x=df_plot["date"], y=df_plot["mm_12"], mode="lines", name=tr("climate_indicators.media_movel_12_m"), line=dict(width=2)))
            fig.update_xaxes(tickformat="%Y")
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            temp_ann_tbl = temp_ann.rename(columns={
                "year": tr("climate_indicators.ano"),
                "anom_c": tr("climate_indicators.anomalia_c")
            })

            dataframe_fmt(
                temp_ann_tbl, year_col="Ano",
                float_cols={"Anomalia (°C)": "%.2f"}
            )
            b1 = io.StringIO(); temp_mo.to_csv(b1, index=False)
            b2 = io.StringIO(); temp_ann.to_csv(b2, index=False)
            st.download_button(tr("climate_indicators.monthly_csv_anom_c"), b1.getvalue(), "gistemp_global_monthly.csv", "text/csv", key="dl_tmp_m")
            st.download_button(tr("climate_indicators.annual_csv_anom_c"), b2.getvalue(), "gistemp_global_annual.csv", "text/csv", key="dl_tmp_a")

        st.caption(tr("climate_indicators.fonte_nasa_gistemp_v4_anomalias_relativas_a_1951_1980"))
    except Exception as e:
        st.error(tr("climate_indicators.erro_temp", error=str(e)))


    # ── IBTrACS (Ciclones) ────────────────────────────────────────────────────
    st.markdown(tr("labels.text"))
    st.markdown(tr("climate_indicators.ciclones_tropicais_contagem_global_e_por_bacia_ibtracs"))
    st.caption(tr("climate_indicators.arquivo_global_da_noaa_ncei_contagens_por_ano_e_por_bacia_atualizado_regularmente"))

    try:
        ib = load_ibtracs_list()
        out = summarize_ibtracs(ib)
        if not out:
            st.info(tr("climate_indicators.sem_dados_ibtracs"))

            return

        annual = out["annual_counts"]
        major  = out["annual_major"]
        bybas  = out["annual_by_basin"]

        # Filtros num expander (fechado por defeito)
        with st.expander(tr("climate_indicators.filtros"), expanded=False):
            min_y, max_y = int(annual["YEAR"].min()), int(annual["YEAR"].max())
            y0, y1 = st.slider(
                tr("climate_indicators.intervalo_de_anos"),
                min_value=min_y, max_value=max_y,
                value=(max(1950, min_y), max_y), step=1, key="ind_yr"
            )
            codes = list(BASIN_LABELS.keys())
            default_codes = ["EP", "NI", "SI"]
            basins_sel = st.multiselect(
                tr("climate_indicators.bacias"),
                options=codes,
                default=codes,
                format_func=lambda k: BASIN_LABELS.get(k, k),
                key="ind_basins"
            )

        # aplica filtros
        ann_f = annual[(annual["YEAR"] >= y0) & (annual["YEAR"] <= y1)].copy()
        maj_f = major[(major["YEAR"] >= y0) & (major["YEAR"] <= y1)].copy()
        bas_f = bybas[(bybas["YEAR"] >= y0) & (bybas["YEAR"] <= y1)].copy()
        bas_plot = bas_f[bas_f["BASIN"].isin(basins_sel)]

        # Global (total vs major)
        c1, c2 = st.columns([2, 1])
        with c1:
            g = ann_f.merge(maj_f, on="YEAR", how="left")
            g["major_count"] = g["major_count"].fillna(0).astype(int)

            fig = go.Figure()
            fig.add_bar(x=g["YEAR"], y=g["count"], name=tr("climate_indicators.total_ano"))
            fig.add_scatter(x=g["YEAR"], y=g["major_count"], name=tr("climate_indicators.major_cat_3"), mode="lines+markers")
            fig.update_layout(
                title=tr("climate_indicators.ciclones_ano_global_e_major"),
                margin=dict(l=6, r=6, t=40, b=0),
                xaxis_title=tr("climate_indicators.ano"), yaxis_title=tr("climate_indicators.n_o"),
                legend=dict(orientation="h", y=1.02, x=0),
            )
            fig.update_yaxes(gridcolor="rgba(160,160,160,0.35)")
            fig.update_xaxes(gridcolor="rgba(160,160,160,0.18)", tickformat="d")
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            show = g.rename(columns={"YEAR": "Ano", "count": "Total", "major_count": "Major (Cat ≥3)"})
            dataframe_fmt(
                show, year_col="Ano",
                int_cols=["Total", "Major (Cat ≥3)"]
            )
            b = io.StringIO(); show.to_csv(b, index=False)
            st.download_button(tr("climate_indicators.csv_global"), b.getvalue(), "ibtracs_global_counts.csv", "text/csv", key="dl_ibtracs_global")

        # Por bacia
        st.markdown(tr("climate_indicators.por_bacia"))
        fig2 = px.line(bas_plot, x="YEAR", y="count", color="BASIN", title=tr("climate_indicators.ciclones_ano_por_bacia"), markers=True)
        fig2.update_layout(margin=dict(l=6, r=6, t=40, b=0), xaxis_title=tr("climate_indicators.ano"), yaxis_title=tr("climate_indicators.n_o"))
        fig2.update_yaxes(gridcolor="rgba(160,160,160,0.35)")
        fig2.update_xaxes(gridcolor="rgba(160,160,160,0.18)", tickformat="d")
        st.plotly_chart(fig2, use_container_width=True)

        piv = (
            bas_plot.pivot(index="YEAR", columns="BASIN", values="count")
                   .sort_index().reset_index().rename(columns={"YEAR": "Ano"})
        )
        # opcional: renomear colunas para rótulos completos
        # piv = piv.rename(columns={k: v for k, v in BASIN_LABELS.items() if k in piv.columns})

        dataframe_fmt(
            piv.fillna(""), year_col="Ano",
            int_cols=[c for c in piv.columns if c != "Ano"]
        )
        b2 = io.StringIO(); piv.to_csv(b2, index=False)
        st.download_button(tr("climate_indicators.csv_por_bacia"), b2.getvalue(), "ibtracs_by_basin.csv", "text/csv", key="dl_ibtracs_basin")

        with st.expander(tr("climate_indicators.siglas_bacias_info_title")):
            st.markdown(
                tr("climate_indicators.na_atlantico_norte_ep_pacifico_este_wp_pacifico_oeste_ni_indico_norte_si_indico_sul_sp_pacifico_sul_sa_atlantico_sul")
            )

        st.caption(tr("climate_indicators.fonte_noaa_ncei_ibtracs_v4_lista_global"))

    except Exception as e:
        st.error(f"Falhou o carregamento de ciclones (IBTrACS): {e}")

    # Rodapé
    st.markdown(tr("labels.text"))
    with st.expander(tr("climate_indicators.notas_title")):
        st.markdown(tr("climate_indicators.notas_md"))


