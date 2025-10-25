# views/wdi_explorer.py
# World Bank WDI Explorer — country picker, year range, indicators (full-width), charts 2x2 e tabela formatada
from __future__ import annotations

import streamlit as st
import pandas as pd
import requests
import altair as alt

st.set_page_config(page_title="WDI Explorer", page_icon="🌐", layout="wide")

API_BASE = "https://api.worldbank.org/v2"

# ---------------------- Indicadores (podes ampliar este dicionário) ----------------------
WDI_INDICATORS = {
    # Economia
    "NY.GDP.MKTP.CD": "GDP (current US$)",
    "NY.GDP.MKTP.KD": "GDP (constant 2015 US$)",
    "NY.GDP.MKTP.KD.ZG": "GDP growth (annual %)",
    "NY.GDP.PCAP.CD": "GDP per capita (current US$)",
    "NY.GDP.PCAP.KD.ZG": "GDP per capita growth (annual %)",
    # Pobreza/Desigualdade
    "SI.POV.DDAY": "Poverty headcount ratio at $2.15/day (2017 PPP) (% of pop.)",
    "SI.POV.LMIC": "Poverty headcount at $3.65/day (% of pop.)",
    "SI.POV.UMIC": "Poverty headcount at $6.85/day (% of pop.)",
    "SI.POV.GINI": "Gini index",
    # Demografia
    "SP.POP.TOTL": "Population, total",
    "SP.POP.GROW": "Population growth (annual %)",
    "SP.DYN.LE00.IN": "Life expectancy at birth, total (years)",
    "SP.URB.TOTL.IN.ZS": "Urban population (% of total)",
}

DEFAULT_CODES = ["NY.GDP.MKTP.KD.ZG", "NY.GDP.MKTP.CD", "SI.POV.DDAY", "SP.POP.TOTL"]

# ---------------------- Tema Altair escuro ----------------------
alt.themes.register(
    "streamlit_dark",
    lambda: {
        "config": {
            "background": "#0e1117",
            "view": {"fill": "#0e1117"},
            "axis": {
                "domainColor": "#ffffff",
                "gridColor": "#3a3a3a",
                "labelColor": "#ffffff",
                "titleColor": "#ffffff",
            },
            "legend": {"labelColor": "#ffffff", "titleColor": "#ffffff"},
            "title": {"color": "#ffffff"},
        }
    },
)
alt.themes.enable("streamlit_dark")

# ---------------------- Helpers ----------------------
def _wb_get_json(url: str, params: dict) -> list | dict | None:
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.warning(f"World Bank API error: {e}")
        return None

@st.cache_data(ttl=24 * 3600, show_spinner=False)
def list_countries() -> pd.DataFrame:
    url = f"{API_BASE}/country"
    js = _wb_get_json(url, {"format": "json", "per_page": 4000})
    if not isinstance(js, list) or len(js) < 2 or js[1] is None:
        return pd.DataFrame(columns=["iso3", "name", "region"])
    rows = []
    for rec in js[1]:
        # excluir agregados (World, regions)
        if rec.get("region", {}).get("id") == "NA":
            continue
        rows.append(
            {
                "iso3": rec["id"],
                "name": rec["name"],
                "region": rec.get("region", {}).get("value") or "",
            }
        )
    return pd.DataFrame(rows).sort_values("name").reset_index(drop=True)

@st.cache_data(ttl=6 * 3600, show_spinner=False)
def fetch_indicator(iso3: str, indicator: str, date_range: str) -> pd.DataFrame:
    url = f"{API_BASE}/country/{iso3}/indicator/{indicator}"
    js = _wb_get_json(url, {"format": "json", "per_page": 20000, "date": date_range})
    if not isinstance(js, list) or len(js) < 2 or js[1] is None:
        return pd.DataFrame(columns=["iso3", "year", "value", "code", "label"])
    label = WDI_INDICATORS.get(indicator, indicator)
    rows = []
    for rec in js[1]:
        y = rec.get("date")
        try:
            y = int(y)
        except Exception:
            continue
        rows.append(
            {"iso3": iso3, "year": y, "value": rec.get("value"), "code": indicator, "label": label}
        )
    return pd.DataFrame(rows)

def is_percent(code: str, label: str) -> bool:
    # Heurística: muitos percentuais acabam em .ZG / .ZS / contêm '%'
    return any(code.endswith(s) for s in (".ZG", ".ZS", ".ZP", ".ZE")) or ("%" in label)

def chart_one(_df: pd.DataFrame, code: str, label: str) -> alt.Chart:
    sub = _df[_df["code"] == code].copy()
    y_fmt = ",.1f" if is_percent(code, label) else ",.0f"
    tip_fmt = ",.2f" if is_percent(code, label) else ",.2f"
    return (
        alt.Chart(sub, height=280)
        .mark_line(point=True)
        .encode(
            x=alt.X("year:Q", title="Year", axis=alt.Axis(format="d")),
            y=alt.Y("value:Q", title=label, scale=alt.Scale(zero=False), axis=alt.Axis(format=y_fmt)),
            tooltip=[alt.Tooltip("year:Q", format="d"), alt.Tooltip("value:Q", format=tip_fmt)],
        )
        .properties(title=label, width="container")
    )

def human_number(x):
    if pd.isna(x):
        return "–"
    try:
        x = float(x)
    except Exception:
        return str(x)
    for unit in ["", "K", "M", "B", "T"]:
        if abs(x) < 1000.0:
            return f"{x:,.0f}{unit}".replace(",", " ")
        x /= 1000.0
    return f"{x:,.0f}T".replace(",", " ")

# ---------------------- UI ----------------------
st.title("World Bank — WDI Explorer")
st.caption(
    "Pick a country, indicators, and years to explore macroeconomic & demographic series. "
    "Source: World Bank WDI API."
)

countries = list_countries()

# Linha 1 — Country + Years
left, mid = st.columns([1.4, 2.6])
with left:
    country_name = st.selectbox(
        "Country",
        options=countries["name"],
        index=countries["name"].tolist().index("Portugal")
        if "Portugal" in countries["name"].tolist()
        else 0,
    )
    iso3 = countries.loc[countries["name"] == country_name, "iso3"].iloc[0]
with mid:
    year_min, year_max = st.slider("Years", min_value=1960, max_value=2024, value=(2000, 2024))

# Linha 2 — Indicators (full-width)
st.markdown("#### Indicators")

# CSS para manter chips numa única linha com scroll horizontal
st.markdown(
    """
<style>
/* ocupar largura total */
.stMultiSelect > div[data-baseweb="select"] { width: 100% !important; }
/* chips numa única linha e scroll horizontal */
.stMultiSelect > div[data-baseweb="select"] > div {
    flex-wrap: nowrap !important;
    overflow-x: auto !important;
    scrollbar-width: thin;
}
/* chips sem truncar texto */
.stMultiSelect [data-baseweb="tag"] { max-width: none !important; }
.stMultiSelect [data-baseweb="tag"] span {
    white-space: nowrap !important;
    overflow: visible !important;
    text-overflow: initial !important;
}
</style>
""",
    unsafe_allow_html=True,
)

indicators = st.multiselect(
    label="",
    options=[f"{v} [{k}]" for k, v in WDI_INDICATORS.items()],
    default=[f"{WDI_INDICATORS[c]} [{c}]" for c in DEFAULT_CODES if c in WDI_INDICATORS],
)

# Extrair os códigos dos labels "Name [CODE]"
chosen_codes = [opt.rsplit("[", 1)[1].rstrip("]") for opt in indicators]
if not chosen_codes:
    st.info("Select at least one indicator to display.")
    st.stop()

date_range = f"{year_min}:{year_max}"

with st.spinner("Fetching data from World Bank…"):
    frames = [fetch_indicator(iso3, code, date_range) for code in chosen_codes]
df = (
    pd.concat(frames, ignore_index=True)
    if frames
    else pd.DataFrame(columns=["iso3", "year", "value", "code", "label"])
)

if df.empty:
    st.warning("No data returned for the selected options.")
    st.stop()

# ---------------------- Overview ----------------------
st.subheader(f"Overview — {country_name}")
last_year = int(df["year"].max())
latest = df[df["year"] == last_year].set_index("label")["value"]
cards = st.columns(min(4, max(1, len(latest))))
for i, (label, val) in enumerate(latest.items()):
    with cards[i % len(cards)]:
        st.metric(label=label, value=human_number(val), delta=f"Year {last_year}")

# ---------------------- Time series (4 gráficos 2x2) ----------------------
st.subheader("Time series")

# Até 4 indicadores (2x2)
to_plot = chosen_codes[:4]
labels_map = {
    c: (df.loc[df["code"] == c, "label"].iloc[0] if (df["code"] == c).any() else WDI_INDICATORS.get(c, c))
    for c in to_plot
}

rows = [st.columns(2), st.columns(2)]
for i, code in enumerate(to_plot):
    r, c = divmod(i, 2)
    with rows[r][c]:
        st.altair_chart(chart_one(df, code, labels_map[code]), use_container_width=True)

if len(chosen_codes) > 4:
    st.info("Showing the first 4 indicators. Select up to 4 for separate charts.")

# ---------------------- Data table ----------------------
# Pivot largo (anos em linhas; indicadores em colunas)
wide = df.pivot_table(index="year", columns="label", values="value", aggfunc="first").sort_index()

# Preparar para exibição: percentuais com 2 casas; restantes sem casas.
display_df = wide.copy()

def fmt_value(col_label: str, v):
    if pd.isna(v):
        return "–"
    return (f"{float(v):,.2f}" if "%" in col_label else f"{float(v):,.0f}").replace(",", " ")

for col in display_df.columns:
    display_df[col] = display_df[col].apply(lambda x, c=col: fmt_value(c, x))

# Garantir que o ano NÃO tem separador de milhares (mostrar como string simples)
display_df.index = display_df.index.map(lambda y: str(int(y)))
display_tbl = display_df.reset_index().rename(columns={"index": "year"})

st.subheader("Data table")
st.dataframe(
    display_tbl,
    use_container_width=True,
    height=min(520, 50 + 28 * min(len(display_tbl), 14)),
)

st.caption(
    "Source: World Bank — World Development Indicators (WDI). "
    "API: https://api.worldbank.org/ · Some series have gaps or different update frequencies."
)
