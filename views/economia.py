# views/economia.py
# -*- coding: utf-8 -*-
"""
Painel de Indicadores Económicos (WDI / World Bank)
Traduzido dinamicamente (PT/EN) e compatível com múltiplos países.
"""

from __future__ import annotations
import pandas as pd
import streamlit as st
import altair as alt
from functools import lru_cache
from services.i18n_boot import _ensure_lang_state
from services.i18n import t as tr
from services.countries_names import country_display_name
# --- Configurações gerais ---
API_BASE = "https://api.worldbank.org/v2"
FALLBACKS = {
    "SI.POV.DDAY": ["SI.POV.LMIC", "SI.POV.UMIC"],
}

# ==========================================================
# Funções auxiliares
# ==========================================================

@st.cache_data(ttl=6 * 3600, show_spinner=False)
def _wb_get_json(url: str, params: dict) -> dict:
    """Obtém JSON da API do World Bank."""
    import requests
    r = requests.get(url, params=params, timeout=30)
    if r.status_code != 200:
        return {}
    try:
        return r.json()
    except Exception:
        return {}

@st.cache_data(ttl=6 * 3600, show_spinner=False)
def _wdi_fetch_indicator(iso3: str, indicator: str, date_range: str) -> pd.DataFrame:
    """Busca um indicador WDI para um país."""
    url = f"{API_BASE}/country/{iso3}/indicator/{indicator}"
    js = _wb_get_json(url, {"format": "json", "per_page": 20000, "date": date_range})
    if not isinstance(js, list) or len(js) < 2 or js[1] is None:
        return pd.DataFrame(columns=["iso3", "year", "value", "code"])
    rows = []
    for rec in js[1]:
        try:
            year = int(rec.get("date"))
        except Exception:
            continue
        rows.append({"iso3": iso3, "year": year, "value": rec.get("value"), "code": indicator})
    return pd.DataFrame(rows)


def _first_series_with_data(iso3: str, candidates: list[str], date_range: str) -> tuple[pd.DataFrame, str]:
    """Tenta várias alternativas de código e devolve a primeira com dados."""
    for code in candidates:
        df = _wdi_fetch_indicator(iso3, code, date_range)
        if not df.empty and df["value"].notna().any():
            return df, code
    return pd.DataFrame(columns=["iso3", "year", "value", "code"]), candidates[0]


@st.cache_data(ttl=6 * 3600, show_spinner=False)
def fetch_wdi_dataset(iso3: str, codes: list[str], year_min: int, year_max: int, IND: dict) -> tuple[pd.DataFrame, dict[str, str]]:
    """Obtém várias séries WDI com fallback e labels traduzidos."""
    date_range = f"{year_min}:{year_max}"
    frames, labels_map = [], {}
    for c in codes[:4]:
        df, used = _first_series_with_data(iso3, [c] + FALLBACKS.get(c, []), date_range)
        if not df.empty:
            df = df.copy()
            df["orig_code"] = c
            frames.append(df)
            labels_map[c] = IND.get(used, {}).get("label", used)
        else:
            labels_map[c] = IND.get(c, {}).get("label", c)
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["iso3", "year", "value", "code", "orig_code"])
    return out, labels_map


def get_wdi_selection(default_codes: list[str], default_years: tuple[int, int] = (2000, 2024)) -> tuple[list[str], tuple[int, int]]:
    """Lê seleção atual (ou usa defaults)."""
    codes = st.session_state.get("econ_selected_codes", default_codes)
    years = st.session_state.get("econ_year_range", default_years)
    return list(codes), tuple(years)


def _is_percent(code: str, label: str) -> bool:
    return "%" in label or code.endswith(".ZG")


def _chart_one(df: pd.DataFrame, code: str, label: str) -> alt.Chart:
    """Desenha um gráfico Altair para um indicador."""
    # Fallback caso venha label vazio/None (ex.: algum edge ao trocar de idioma)
    if not label:
        label = code

    if df.empty:
        empty = pd.DataFrame({"msg": [tr("economics.table_no_data")]})
        return (
            alt.Chart(empty, height=260)
            .mark_text(align="center", baseline="middle")
            .encode(text="msg:N")
            .properties(title=label, width="container")
        )

    sub = df[df["code"] == code].dropna(subset=["value"]).copy()
    if sub.empty:
        sub = df.copy()

    y_fmt = ",.1f" if _is_percent(code, label) else ",.0f"
    tip = ",.2f"
    return (
        alt.Chart(sub, height=260)
        .mark_line(point=True)
        .encode(
            x=alt.X("year:Q",
                    title=tr("economics.chart_tooltip.year"),
                    axis=alt.Axis(format="d")),
            # <<< título do eixo Y removido >>>
            y=alt.Y("value:Q",
                    title=None,
                    scale=alt.Scale(zero=False),
                    axis=alt.Axis(format=y_fmt)),
            tooltip=[
                alt.Tooltip("year:Q", format="d", title=tr("economics.chart_tooltip.year")),
                alt.Tooltip("value:Q", format=tip, title=tr("economics.chart_tooltip.value")),
            ],
        )
        .properties(title=label, width="container")
    )

def render_wdi_charts_2x2(df: pd.DataFrame, codes: list[str], labels_map: dict[str, str]) -> None:
    """Renderiza quatro gráficos (2x2)."""
    grid = [st.columns(2), st.columns(2)]
    for i, code in enumerate(codes[:4]):
        r, c = divmod(i, 2)
        label = labels_map.get(code, code)
        with grid[r][c]:
            st.altair_chart(_chart_one(df, code, label), use_container_width=True)


# ==========================================================
# Catálogos traduzidos
# ==========================================================

def _catalog_i18n() -> dict[str, dict[str, str]]:
    """Mapeia indicadores para labels traduzidos."""
    return {
        "NY.GDP.MKTP.CD":    {"short": tr("economics.metrics.gdp"),          "label": tr("economics.metrics.gdp")},
        "NY.GDP.MKTP.KD":    {"short": tr("economics.metrics.gdp_const"),     "label": tr("economics.metrics.gdp_const")},
        "NY.GDP.MKTP.KD.ZG": {"short": tr("economics.metrics.gdp_growth"),    "label": tr("economics.metrics.gdp_growth")},
        "NY.GDP.PCAP.CD":    {"short": tr("economics.metrics.gdp_pc"),        "label": tr("economics.metrics.gdp_pc")},
        "NY.GDP.PCAP.KD.ZG": {"short": tr("economics.metrics.gdp_pc_growth"), "label": tr("economics.metrics.gdp_pc_growth")},
        "SI.POV.DDAY":       {"short": tr("economics.metrics.poverty_215"),   "label": tr("economics.metrics.poverty_215")},
        "SI.POV.LMIC":       {"short": tr("economics.metrics.poverty_365"),   "label": tr("economics.metrics.poverty_365")},
        "SI.POV.UMIC":       {"short": tr("economics.metrics.poverty_685"),   "label": tr("economics.metrics.poverty_685")},
        "SI.POV.GINI":       {"short": tr("economics.metrics.gini"),          "label": tr("economics.metrics.gini")},
    }


def _presets_i18n() -> dict[str, list[str]]:
    """Presets traduzidos."""
    return {
        tr("economics.presets.core4"):              ["NY.GDP.MKTP.KD.ZG","NY.GDP.MKTP.CD","SI.POV.DDAY","NY.GDP.PCAP.CD"],
        tr("economics.presets.growth_income"):      ["NY.GDP.MKTP.KD.ZG","NY.GDP.MKTP.CD","NY.GDP.PCAP.CD","NY.GDP.PCAP.KD.ZG"],
        tr("economics.presets.poverty_inequality"): ["SI.POV.DDAY","SI.POV.LMIC","SI.POV.UMIC","SI.POV.GINI"],
    }



# ==========================================================
# Painel principal
# ==========================================================

def render_wdi_panel(iso3: str, country_name: str | None = None) -> None:
    _ensure_lang_state()

    IND = _catalog_i18n()
    PRESETS = _presets_i18n()

    # >>> nome do país conforme o idioma ativo
    display_name = country_display_name(iso3, country_name)

    st.subheader(f"{tr('economics.header')} — {display_name}")
    # --- seleção de preset e indicadores ---
    preset = st.selectbox(
        tr("economics.preset_label"),
        options=list(PRESETS.keys()),
        index=0,
        key=f"preset_{iso3}"
    )

    short_options  = [IND[k]["short"] for k in IND.keys()]
    code_by_short  = {IND[k]["short"]: k for k in IND.keys()}
    default_shorts = [IND[c]["short"] for c in PRESETS[preset]]

    selected_shorts = st.multiselect(
        "",
        options=short_options,
        default=default_shorts,
        key=f"indicators_{iso3}"
    )
    codes = [code_by_short[s] for s in selected_shorts] or PRESETS[preset]

    # --- slider de anos ---
    years = st.slider(
        tr("economics.years_label"),
        min_value=1960,
        max_value=2024,
        value=(2000, 2024),
        key=f"years_{iso3}"
    )
    year_min, year_max = years

    # --- carregar dados ---
    with st.spinner(tr("economics.loading")):
        df, labels_map = fetch_wdi_dataset(iso3, codes, year_min, year_max, IND)

    # --- renderizar gráficos e tabela ---
    if df.empty:
        st.info(tr("economics.table_no_data"))
        return

    render_wdi_charts_2x2(df, codes, labels_map)

    # --- tabela ---
    df_tbl = df.copy()
    df_tbl["disp_label"] = df_tbl["orig_code"].map(labels_map)
    wide = (
        df_tbl.pivot_table(index="year", columns="disp_label", values="value", aggfunc="last")
        .sort_index()
    )
    disp = wide.copy()

    def fmt(col, v):
        if pd.isna(v):
            return "–"
        return f"{float(v):,.2f}".replace(",", " ") if "%" in (col or "") else f"{float(v):,.0f}".replace(",", " ")

    for col in disp.columns:
        disp[col] = disp[col].apply(lambda x, c=col: fmt(c, x))

    # manter o índice como string para exibição
    disp.index = disp.index.map(lambda y: str(int(y)))

    # reset e rename do cabeçalho do ano (funciona quer a coluna venha como 'year' quer como 'index')
    out = disp.reset_index()
    out = out.rename(columns={
        "year": tr("economics.metrics.year"),
        "index": tr("economics.metrics.year")
    })

    st.subheader(tr("economics.table_title"))
    st.dataframe(out, use_container_width=True)
    
