# views/economics.py
from __future__ import annotations
import streamlit as st
import pandas as pd
import requests
import altair as alt

API_BASE = "https://api.worldbank.org/v2"

# ---------- Catálogo (SÓ ECONÓMICOS) ----------
IND = {
    # Growth & Income
    "NY.GDP.MKTP.CD":    {"short": "GDP (US$, current)",     "label": "GDP (current US$)"},
    "NY.GDP.MKTP.KD":    {"short": "GDP (US$, 2015 const.)", "label": "GDP (constant 2015 US$)"},
    "NY.GDP.MKTP.KD.ZG": {"short": "GDP growth (%)",         "label": "GDP growth (annual %)"},
    "NY.GDP.PCAP.CD":    {"short": "GDP per capita (US$)",   "label": "GDP per capita (current US$)"},
    "NY.GDP.PCAP.KD.ZG": {"short": "GDP pc growth (%)",      "label": "GDP per capita growth (annual %)"},
    # Poverty & Inequality
    "SI.POV.DDAY":       {"short": "Poverty $2.15 (% pop.)", "label": "Poverty headcount ratio at $2.15/day (2017 PPP) (% of population)"},
    "SI.POV.LMIC":       {"short": "Poverty $3.65",          "label": "Poverty headcount ratio at $3.65/day (2017 PPP) (% of population)"},
    "SI.POV.UMIC":       {"short": "Poverty $6.85",          "label": "Poverty headcount ratio at $6.85/day (2017 PPP) (% of population)"},
    "SI.POV.GINI":       {"short": "Gini index",             "label": "Gini index"},
}

PRESETS = {
    "Core (4)":             ["NY.GDP.MKTP.KD.ZG", "NY.GDP.MKTP.CD", "SI.POV.DDAY", "NY.GDP.PCAP.CD"],
    "Growth & Income":      ["NY.GDP.MKTP.KD.ZG", "NY.GDP.MKTP.CD", "NY.GDP.PCAP.CD", "NY.GDP.PCAP.KD.ZG"],
    "Poverty & Inequality": ["SI.POV.DDAY", "SI.POV.LMIC", "SI.POV.UMIC", "SI.POV.GINI"],
}

# Se $2.15 não tiver dados, tenta estes por ordem
FALLBACKS = {
    "SI.POV.DDAY": ["SI.POV.UMIC", "SI.POV.LMIC", "SI.POV.GINI"],
}

# ---------- Tema Altair (dark) ----------
alt.themes.register(
    "streamlit_dark",
    lambda: {"config": {
        "background": "#0e1117", "view": {"fill": "#0e1117"},
        "axis": {"domainColor": "#ffffff","gridColor": "#3a3a3a","labelColor": "#ffffff","titleColor": "#ffffff"},
        "legend": {"labelColor": "#ffffff","titleColor": "#ffffff"},
        "title": {"color": "#ffffff"},
    }}
)
alt.themes.enable("streamlit_dark")

# ---------- Helpers baixo nível ----------
def _wb_get_json(url: str, params: dict):
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

@st.cache_data(ttl=6*3600, show_spinner=False)
def _wdi_fetch_indicator(iso3: str, indicator: str, date_range: str) -> pd.DataFrame:
    url = f"{API_BASE}/country/{iso3}/indicator/{indicator}"
    js = _wb_get_json(url, {"format": "json", "per_page": 20000, "date": date_range})
    if not isinstance(js, list) or len(js) < 2 or js[1] is None:
        return pd.DataFrame(columns=["iso3","year","value","code","label"])
    label_full = IND.get(indicator, {}).get("label", indicator)
    rows = []
    for rec in js[1]:
        year = rec.get("date")
        try:
            year = int(year)
        except Exception:
            continue
        rows.append({"iso3": iso3, "year": year, "value": rec.get("value"),
                     "code": indicator, "label": label_full})
    return pd.DataFrame(rows)

def _is_percent(code: str, label: str) -> bool:
    return any(code.endswith(s) for s in (".ZG",".ZS",".ZP",".ZE")) or "%" in label

def _chart_one(sub_df: pd.DataFrame, code: str, label: str) -> alt.Chart:
    # sub_df já pode vir filtrado (com fallback); não refiltres por 'code' aqui.
    data = sub_df.dropna(subset=["value"]).copy()
    if data.empty:
        empty = pd.DataFrame({"msg": ["No data for selected years"]})
        return (alt.Chart(empty, height=260)
                .mark_text(align="center", baseline="middle")
                .encode(text="msg:N")
                .properties(title=label, width="container")
                .configure_axis(grid=False, domain=False, labels=False, ticks=False))
    y_fmt = ",.1f" if _is_percent(code, label) else ",.0f"
    tip = ",.2f"
    return (
        alt.Chart(data, height=260)
        .mark_line(point=True)
        .encode(
            x=alt.X("year:Q", title="Year", axis=alt.Axis(format="d")),
            y=alt.Y("value:Q", title=label, scale=alt.Scale(zero=False), axis=alt.Axis(format=y_fmt)),
            tooltip=[alt.Tooltip("year:Q", format="d"), alt.Tooltip("value:Q", format=tip)],
        )
        .properties(title=label, width="container")
    )

def _first_series_with_data(iso3: str, candidates: list[str], date_range: str) -> tuple[pd.DataFrame, str]:
    for code in candidates:
        df = _wdi_fetch_indicator(iso3, code, date_range)
        if not df.empty and df["value"].notna().any():
            if code in IND:
                df["label"] = IND[code]["label"]
            return df, code
    return pd.DataFrame(columns=["iso3","year","value","code","label"]), candidates[0]

# ---------- Helpers públicos (para paises.py) ----------
def get_wdi_selection(default_codes: list[str] | None = None,
                      default_years: tuple[int,int] = (2000, 2024)) -> tuple[list[str], tuple[int,int]]:
    codes = st.session_state.get("econ_selected_codes", default_codes or PRESETS["Core (4)"])
    years = st.session_state.get("econ_year_range", default_years)
    return list(codes), tuple(years)

@st.cache_data(ttl=6*3600, show_spinner=False)
def fetch_wdi_dataset(iso3: str, codes: list[str], year_min: int, year_max: int) -> tuple[pd.DataFrame, dict[str,str]]:
    date_range = f"{year_min}:{year_max}"
    frames, labels_map = [], {}
    for c in codes[:4]:
        candidates = [c] + FALLBACKS.get(c, [])
        df, used = _first_series_with_data(iso3, candidates, date_range)
        if not df.empty:
            frames.append(df)
            labels_map[c] = IND.get(used, {}).get("label", used)
        else:
            labels_map[c] = IND.get(c, {}).get("label", c)  # sem dados em nenhum → empty state
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["iso3","year","value","code","label"])
    return out, labels_map

def render_wdi_charts_2x2(df: pd.DataFrame, codes: list[str], labels_map: dict[str,str]) -> None:
    grid = [st.columns(2), st.columns(2)]
    for i, code in enumerate(codes[:4]):
        r, c = divmod(i, 2)
        label = labels_map.get(code, code)
        # tentar apanhar pelo code original; se vazio (fallback), apanhar pelo label
        sub = df[df["code"] == code]
        if sub.empty:
            sub = df[df["label"] == label].copy()
            if not sub.empty:
                sub["code"] = code  # para _chart_one não refiltrar
        with grid[r][c]:
            st.altair_chart(_chart_one(sub, code, label), use_container_width=True)

# ---------- Painel principal ----------
def render_wdi_panel(iso3: str, country_name: str | None = None) -> None:
    st.subheader(f"World Bank — {country_name or iso3}")

    # reset opcional (útil quando mudas o catálogo e ficas com lixo em sessão)
    if st.button("Reset selection & cache", help="Clears selection and cached requests for this panel"):
        st.session_state.pop("econ_selected_codes", None)
        st.session_state.pop("econ_year_range", None)
        fetch_wdi_dataset.clear()  # limpa cache deste helper
        _wdi_fetch_indicator.clear()
        st.experimental_rerun()

    year_min, year_max = st.slider("Years", min_value=1960, max_value=2024, value=(2000, 2024))

    st.markdown("#### Indicators")
    st.markdown("""
    <style>
    .stMultiSelect > div[data-baseweb="select"] { width: 100% !important; }
    .stMultiSelect > div[data-baseweb="select"] > div {
        flex-wrap: nowrap !important; overflow-x: auto !important; scrollbar-width: thin;
    }
    .stMultiSelect [data-baseweb="tag"] { max-width: none !important; }
    .stMultiSelect [data-baseweb="tag"] span { white-space: nowrap !important; overflow: visible !important; text-overflow: initial !important; }
    </style>
    """, unsafe_allow_html=True)

    # Preset + opções com rótulos curtos
    c1, c2 = st.columns([1, 2])
    with c1:
        preset = st.selectbox("Preset", options=list(PRESETS.keys()), index=0)
    short_options = [IND[k]["short"] for k in IND.keys()]
    code_by_short = {IND[k]["short"]: k for k in IND.keys()}
    with c2:
        default_shorts = [IND[c]["short"] for c in PRESETS[preset]]
        selected_shorts = st.multiselect("", options=short_options, default=default_shorts)

    codes = [code_by_short[s] for s in selected_shorts]

    # sanitize: remove qualquer código que não esteja no catálogo económico
    allowed = set(IND.keys())
    codes = [c for c in codes if c in allowed] or PRESETS["Core (4)"]

    # persistir para outras tabs
    st.session_state["econ_selected_codes"] = codes[:4]
    st.session_state["econ_year_range"] = (year_min, year_max)

    # fetch (com fallback) + render
    df, labels_map = fetch_wdi_dataset(iso3, codes, year_min, year_max)

    # Overview (último ano com dados por label)
    st.subheader("Overview")
    if df.empty:
        st.info("No data returned.")
    else:
        last_year = int(df["year"].max())
        latest = (df[df["year"] == last_year]
                  .groupby("label", as_index=False, observed=False)["value"].last())
        def human(x):
            if pd.isna(x): return "–"
            try: x = float(x)
            except: return str(x)
            for u in ["","K","M","B","T"]:
                if abs(x) < 1000: return f"{x:,.0f}{u}".replace(",", " ")
                x /= 1000
            return f"{x:,.0f}T".replace(",", " ")
        cols = st.columns(min(4, max(1, len(latest))))
        for i, row in latest.iterrows():
            cols[i % len(cols)].metric(label=row["label"], value=human(row["value"]), delta=f"Year {last_year}")

    st.subheader("Time series")
    render_wdi_charts_2x2(df, codes, labels_map)

    # Tabela (opcional; comenta se não quiseres aqui)
    if not df.empty:
        wide = df.pivot_table(index="year", columns="label", values="value", aggfunc="last").sort_index()
        disp = wide.copy()
        def fmt(col, v):
            if pd.isna(v): return "–"
            return (f"{float(v):,.2f}" if "%" in col else f"{float(v):,.0f}").replace(",", " ")
        for col in disp.columns:
            disp[col] = disp[col].apply(lambda x, c=col: fmt(c, x))
        disp.index = disp.index.map(lambda y: str(int(y)))
        st.subheader("Data table")
        st.dataframe(disp.reset_index().rename(columns={"index":"year"}), use_container_width=True)
    st.caption("Source: World Bank — World Development Indicators (WDI). API: https://api.worldbank.org/")
