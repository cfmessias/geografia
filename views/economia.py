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

# --- views/economia.py (add) -----------------------------------------------
import pandas as pd
import altair as alt
from pathlib import Path
from services.i18n import t as tr

SECTORS_VAB = ["NV.AGR.TOTL.ZS", "NV.IND.TOTL.ZS", "NV.SRV.TOTL.ZS"]
SECTORS_EMP = ["SL.AGR.EMPL.ZS", "SL.IND.EMPL.ZS", "SL.SRV.EMPL.ZS"]

# Mapas de rótulos (i18n)
def _sector_label_map():
    return {
        "NV.AGR.TOTL.ZS": tr("economics.ind.agri_vab"),
        "NV.IND.TOTL.ZS": tr("economics.ind.ind_vab"),
        "NV.SRV.TOTL.ZS": tr("economics.ind.srv_vab"),
        "SL.AGR.EMPL.ZS": tr("economics.ind.agri_emp"),
        "SL.IND.EMPL.ZS": tr("economics.ind.ind_emp"),
        "SL.SRV.EMPL.ZS": tr("economics.ind.srv_emp"),
    }

def _try_read_sectors_csv() -> pd.DataFrame:
    """Lê data/wdi_sectors_wide.csv (se existir; sep=';')."""
    p = Path(__file__).resolve().parents[1] / "data" / "wdi_sectors_wide.csv"
    if p.exists():
        try:
            df = pd.read_csv(p, sep=";", dtype=str, encoding="utf-8", keep_default_na=False)
            # normalizar tipos
            for c in df.columns:
                if c not in {"iso3", "year"}:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
            df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
            df["iso3"] = df["iso3"].astype(str).str.upper()
            return df
        except Exception:
            pass
    return pd.DataFrame()

def _fetch_sector_series_online(iso3: str, codes: list[str]) -> pd.DataFrame:
    """Vai buscar séries ao WDI usando a tua função já existente _wdi_fetch_indicator."""
    # NOTA: assumimos que _wdi_fetch_indicator(iso3, code, "1990:2024") existe no ficheiro
    frames = []
    for code in codes:
        df = _wdi_fetch_indicator(iso3, code, "1990:2024")
        if not df.empty:
            frames.append(df[["iso3","year","value"]].assign(code=code))
    if not frames:
        return pd.DataFrame(columns=["iso3","year","code","value"])
    return pd.concat(frames, ignore_index=True)

def _load_sectors_for_iso3(iso3: str) -> dict:
    """
    Devolve:
      - 'vab_long' (year, code, value) e 'vab_wide' (year, agr/ind/srv) para VAB
      - 'emp_long' e 'emp_wide' para Emprego
    Usando CSV local se existir; fallback online só para o país.
    """
    iso3u = (iso3 or "").upper().strip()
    csv_wide = _try_read_sectors_csv()

    out = {"vab_long": pd.DataFrame(), "vab_wide": pd.DataFrame(),
           "emp_long": pd.DataFrame(), "emp_wide": pd.DataFrame()}

    def _from_wide(csv: pd.DataFrame, cols: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
        if csv.empty:
            return pd.DataFrame(), pd.DataFrame()
        need = ["iso3","year"] + cols
        if not set(need).issubset(csv.columns):
            return pd.DataFrame(), pd.DataFrame()
        sub = csv.loc[csv["iso3"] == iso3u, need].copy()
        if sub.empty:
            return pd.DataFrame(), pd.DataFrame()
        long = sub.melt(id_vars=["iso3","year"], value_vars=cols, var_name="var", value_name="value")
        # mapear var → code "sintético" (mantemos nomes simples)
        long = long.rename(columns={"var":"code"})
        return long.dropna(subset=["year"]), sub

    # 1) tentar CSV local
    vab_long, vab_wide = _from_wide(csv_wide, ["agr_vab","ind_vab","srv_vab"])
    emp_long, emp_wide = _from_wide(csv_wide, ["agr_emp","ind_emp","srv_emp"])

    # 2) se faltar, buscar online só para o país
    if vab_long.empty:
        on = _fetch_sector_series_online(iso3u, SECTORS_VAB)
        if not on.empty:
            lbl = _sector_label_map()
            on["label"] = on["code"].map(lbl)
            vab_long = on.copy()
            vab_wide = (on.pivot_table(index=["iso3","year"], columns="code", values="value", aggfunc="last")
                          .reset_index()
                          .rename(columns={
                              "NV.AGR.TOTL.ZS":"agr_vab",
                              "NV.IND.TOTL.ZS":"ind_vab",
                              "NV.SRV.TOTL.ZS":"srv_vab"}))

    if emp_long.empty:
        on = _fetch_sector_series_online(iso3u, SECTORS_EMP)
        if not on.empty:
            lbl = _sector_label_map()
            on["label"] = on["code"].map(lbl)
            emp_long = on.copy()
            emp_wide = (on.pivot_table(index=["iso3","year"], columns="code", values="value", aggfunc="last")
                          .reset_index()
                          .rename(columns={
                              "SL.AGR.EMPL.ZS":"agr_emp",
                              "SL.IND.EMPL.ZS":"ind_emp",
                              "SL.SRV.EMPL.ZS":"srv_emp"}))

    # normalizar tipos
    for w in (vab_wide, emp_wide):
        if not w.empty:
            w["year"] = pd.to_numeric(w["year"], errors="coerce").astype("Int64")
            for c in w.columns:
                if c not in {"iso3","year"}:
                    w[c] = pd.to_numeric(w[c], errors="coerce")

    return {"vab_long": vab_long, "vab_wide": vab_wide, "emp_long": emp_long, "emp_wide": emp_wide}

def _latest_complete_row(wide: pd.DataFrame, cols: list[str]) -> pd.Series | None:
    if wide.empty: 
        return None
    g = wide.dropna(subset=cols, how="any").sort_values("year")
    if g.empty:
        return None
    return g.iloc[-1]

def _donut_fig(labels: list[str], values: list[float], title: str):
    # usar plotly para donut (fica mais legível)
    import plotly.express as px
    df = pd.DataFrame({"label": labels, "value": values})
    fig = px.pie(df, names="label", values="value", hole=0.55)
    fig.update_traces(textinfo="percent", hovertemplate="%{label}: %{value:.2f}%<extra></extra>")
    fig.update_layout(showlegend=True, legend=dict(orientation="h", y=-0.12), margin=dict(l=10,r=10,t=30,b=10), height=320, title=title)
    return fig

def render_sectors_panel(iso3: str):
    data = _load_sectors_for_iso3(iso3)
    lbl = _sector_label_map()

    # ——— VAB (último ano completo) ———
    vab_last = _latest_complete_row(data["vab_wide"], ["agr_vab","ind_vab","srv_vab"])
    emp_last = _latest_complete_row(data["emp_wide"], ["agr_emp","ind_emp","srv_emp"])

    st.markdown(f"### {tr('economics.charts.sectors_latest_title')}")
    c1, c2 = st.columns(2, gap="large")

    if vab_last is not None:
        y = int(vab_last["year"])
        vals = [float(vab_last["agr_vab"]), float(vab_last["ind_vab"]), float(vab_last["srv_vab"])]
        with c1:
            st.caption(tr("economics.presets.sectors_vab") + f" — {y}")
            st.metric(lbl["NV.AGR.TOTL.ZS"], f"{vals[0]:.1f}%")
            st.metric(lbl["NV.IND.TOTL.ZS"], f"{vals[1]:.1f}%")
            st.metric(lbl["NV.SRV.TOTL.ZS"], f"{vals[2]:.1f}%")
            st.plotly_chart(_donut_fig([lbl["NV.AGR.TOTL.ZS"], lbl["NV.IND.TOTL.ZS"], lbl["NV.SRV.TOTL.ZS"]], vals, ""), use_container_width=True, config={"displayModeBar": False})

    if emp_last is not None:
        y = int(emp_last["year"])
        vals = [float(emp_last["agr_emp"]), float(emp_last["ind_emp"]), float(emp_last["srv_emp"])]
        with c2:
            st.caption(tr("economics.presets.sectors_emp") + f" — {y}")
            st.metric(lbl["SL.AGR.EMPL.ZS"], f"{vals[0]:.1f}%")
            st.metric(lbl["SL.IND.EMPL.ZS"], f"{vals[1]:.1f}%")
            st.metric(lbl["SL.SRV.EMPL.ZS"], f"{vals[2]:.1f}%")
            st.plotly_chart(_donut_fig([lbl["SL.AGR.EMPL.ZS"], lbl["SL.IND.EMPL.ZS"], lbl["SL.SRV.EMPL.ZS"]], vals, ""), use_container_width=True, config={"displayModeBar": False})

    # ——— Série temporal empilhada (toggle VAB/Emprego) ———
    view = st.radio(
        tr("economics.charts.sectors_ts_title"),
        (tr("economics.presets.sectors_vab"), tr("economics.presets.sectors_emp")),
        horizontal=True, key=f"sectors_ts_view_{iso3}"
    )

    if view == tr("economics.presets.sectors_vab") and not data["vab_wide"].empty:
        w = data["vab_wide"].sort_values("year").tail(30).copy()
        long = (w.melt(id_vars=["year"], value_vars=["agr_vab","ind_vab","srv_vab"], var_name="code", value_name="value")
                  .assign(code=lambda d: d["code"].map({
                      "agr_vab": lbl["NV.AGR.TOTL.ZS"],
                      "ind_vab": lbl["NV.IND.TOTL.ZS"],
                      "srv_vab": lbl["NV.SRV.TOTL.ZS"],
                  })))
    elif view == tr("economics.presets.sectors_emp") and not data["emp_wide"].empty:
        w = data["emp_wide"].sort_values("year").tail(30).copy()
        long = (w.melt(id_vars=["year"], value_vars=["agr_emp","ind_emp","srv_emp"], var_name="code", value_name="value")
                  .assign(code=lambda d: d["code"].map({
                      "agr_emp": lbl["SL.AGR.EMPL.ZS"],
                      "ind_emp": lbl["SL.IND.EMPL.ZS"],
                      "srv_emp": lbl["SL.SRV.EMPL.ZS"],
                  })))
    else:
        long = pd.DataFrame()

    if not long.empty:
        ch = (
            alt.Chart(long)
            .mark_area(opacity=0.85)
            .encode(
                x=alt.X("year:Q", axis=alt.Axis(format="d", title=tr("economics.metrics.year"))),
                y=alt.Y("value:Q", stack="normalize", axis=alt.Axis(format=".0%"), title=None),
                color=alt.Color("code:N", title="", legend=alt.Legend(orient="bottom")),
                tooltip=[
                    alt.Tooltip("code:N", title=tr("paises.indicador")),
                    alt.Tooltip("year:Q", title=tr("economics.metrics.year"), format="d"),
                    alt.Tooltip("value:Q", title=tr("paises.valor"), format=".2f")
                ],
            )
            .properties(height=300, width="container")
        )
        st.altair_chart(ch, use_container_width=True)
    else:
        st.caption(tr("labels.sem_s_rie_temporal_para_os_indicadores_selecionados"))


# ==========================================================
# Catálogos traduzidos
# ==========================================================

def _catalog_i18n() -> dict[str, dict[str, str]]:
    """Mapeia indicadores para labels traduzidos."""
    return {
        # --- Core existentes ---
        "NY.GDP.MKTP.CD":    {"short": tr("economics.metrics.gdp"),          "label": tr("economics.metrics.gdp")},
        "NY.GDP.MKTP.KD":    {"short": tr("economics.metrics.gdp_const"),     "label": tr("economics.metrics.gdp_const")},
        "NY.GDP.MKTP.KD.ZG": {"short": tr("economics.metrics.gdp_growth"),    "label": tr("economics.metrics.gdp_growth")},
        "NY.GDP.PCAP.CD":    {"short": tr("economics.metrics.gdp_pc"),        "label": tr("economics.metrics.gdp_pc")},
        "NY.GDP.PCAP.KD.ZG": {"short": tr("economics.metrics.gdp_pc_growth"), "label": tr("economics.metrics.gdp_pc_growth")},
        "SI.POV.DDAY":       {"short": tr("economics.metrics.poverty_215"),   "label": tr("economics.metrics.poverty_215")},
        "SI.POV.LMIC":       {"short": tr("economics.metrics.poverty_365"),   "label": tr("economics.metrics.poverty_365")},
        "SI.POV.UMIC":       {"short": tr("economics.metrics.poverty_685"),   "label": tr("economics.metrics.poverty_685")},
        "SI.POV.GINI":       {"short": tr("economics.metrics.gini"),          "label": tr("economics.metrics.gini")},

        # --- Novos: Setores (VAB % PIB) ---
        "NV.AGR.TOTL.ZS":    {"short": tr("economics.ind.agri_vab"),          "label": tr("economics.ind.agri_vab")},
        "NV.IND.TOTL.ZS":    {"short": tr("economics.ind.ind_vab"),           "label": tr("economics.ind.ind_vab")},
        "NV.SRV.TOTL.ZS":    {"short": tr("economics.ind.srv_vab"),           "label": tr("economics.ind.srv_vab")},

        # --- Novos: Setores (Emprego % total) ---
        "SL.AGR.EMPL.ZS":    {"short": tr("economics.ind.agri_emp"),          "label": tr("economics.ind.agri_emp")},
        "SL.IND.EMPL.ZS":    {"short": tr("economics.ind.ind_emp"),           "label": tr("economics.ind.ind_emp")},
        "SL.SRV.EMPL.ZS":    {"short": tr("economics.ind.srv_emp"),           "label": tr("economics.ind.srv_emp")},
    }



def _presets_i18n() -> dict[str, list[str]]:
    """Presets traduzidos."""
    return {
        tr("economics.presets.core4"):              ["NY.GDP.MKTP.KD.ZG","NY.GDP.MKTP.CD","SI.POV.DDAY","NY.GDP.PCAP.CD"],
        tr("economics.presets.growth_income"):      ["NY.GDP.MKTP.KD.ZG","NY.GDP.MKTP.CD","NY.GDP.PCAP.CD","NY.GDP.PCAP.KD.ZG"],
        tr("economics.presets.poverty_inequality"): ["SI.POV.DDAY","SI.POV.LMIC","SI.POV.UMIC","SI.POV.GINI"],

        # --- Novos presets setoriais ---
        tr("economics.presets.sectors_vab"):        ["NV.AGR.TOTL.ZS","NV.IND.TOTL.ZS","NV.SRV.TOTL.ZS"],
        tr("economics.presets.sectors_emp"):        ["SL.AGR.EMPL.ZS","SL.IND.EMPL.ZS","SL.SRV.EMPL.ZS"],
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
    
    render_sectors_panel(iso3)
