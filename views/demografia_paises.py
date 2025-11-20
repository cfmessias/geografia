# views/demography.py
# Demography panel (World Bank API) — bilingual, standalone, with expander
from __future__ import annotations

import streamlit as st
import pandas as pd
import requests
import altair as alt

API_BASE = "https://api.worldbank.org/v2"
DATA360_BASE = "https://data360files.worldbank.org/data360-data/data/WB_WDI"

# ----------------------------- Altair theme (dark) -----------------------------
alt.themes.register(
    "streamlit_dark",
    lambda: {
        "config": {
            "background": "#0e1117",
            "view": {"fill": "#0e1117"},
            "padding": {"top": 28, "right": 10, "bottom": 8, "left": 10},  # + espaço p/ título
            "axis": {
                "domainColor": "#ffffff",
                "gridColor": "#3a3a3a",
                "labelColor": "#ffffff",
                "titleColor": "#ffffff",
            },
            "legend": {"labelColor": "#ffffff", "titleColor": "#ffffff"},
            "title": {"color": "#ffffff", "fontSize": 16, "anchor": "start"},
        }
    },
)
alt.themes.enable("streamlit_dark")


# ----------------------------- Helpers -----------------------------
def _t(tr, key: str, default: str) -> str:
    try:
        if callable(tr):
            v = tr(key)
            if isinstance(v, str) and v and not v.startswith("["):
                return v
    except Exception:
        pass
    return default


def _wb_get_json(url: str, params: dict):
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def _parse_date_range(date_range: str) -> tuple[int, int]:
    """
    Converte '2000:2024' em (2000, 2024). Se falhar, usa (1960, 2024).
    """
    try:
        a, b = (date_range or "").split(":")
        return int(a), int(b)
    except Exception:
        return 1960, 2024


def _indicator_to_file_id(code: str) -> str:
    """
    SP.POP.TOTL -> WB_WDI_SP_POP_TOTL
    """
    return f"WB_WDI_{code.replace('.', '_')}"


def _wdi_fetch_indicator_csv(
    iso3: str, indicator: str, date_range: str, label: str
) -> pd.DataFrame:
    """
    Fallback via CSV Data360 (https://data360files.worldbank.org/.../WB_WDI_*.csv)

    Devolve DataFrame com colunas: iso3, year, value, code, label
    (mesma estrutura da versão API para ser plug-and-play).
    """
    iso3u = (iso3 or "").upper().strip()
    year_min, year_max = _parse_date_range(date_range)

    file_id = _indicator_to_file_id(indicator)
    url = f"{DATA360_BASE}/{file_id}.csv"

    try:
        raw = pd.read_csv(url)
    except Exception:
        # não conseguimos ler o CSV → sem dados
        return pd.DataFrame(columns=["iso3", "year", "value", "code", "label"])

    needed = {"REF_AREA", "TIME_PERIOD", "OBS_VALUE"}
    if not needed.issubset(raw.columns):
        return pd.DataFrame(columns=["iso3", "year", "value", "code", "label"])

    df = raw.copy()
    df["REF_AREA"] = df["REF_AREA"].astype(str).str.upper().str.strip()
    df = df[df["REF_AREA"] == iso3u]

    df["TIME_PERIOD_int"] = pd.to_numeric(df["TIME_PERIOD"], errors="coerce")
    df["OBS_VALUE_num"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
    df = df.dropna(subset=["TIME_PERIOD_int", "OBS_VALUE_num"])

    df = df[
        (df["TIME_PERIOD_int"] >= year_min)
        & (df["TIME_PERIOD_int"] <= year_max)
    ]
    if df.empty:
        return pd.DataFrame(columns=["iso3", "year", "value", "code", "label"])

    out = pd.DataFrame(
        {
            "iso3": iso3u,
            "year": df["TIME_PERIOD_int"].astype(int),
            "value": df["OBS_VALUE_num"],
            "code": indicator,
            "label": label,
        }
    )
    return out.reset_index(drop=True)


@st.cache_data(ttl=6 * 3600, show_spinner=False)
def _wdi_fetch_indicator(
    iso3: str, indicator: str, date_range: str, label: str
) -> pd.DataFrame:
    """
    1.º tenta o API JSON (api.worldbank.org/v2).
    Se falhar (erro de rede / resposta vazia), faz fallback para
    o CSV Data360 (https://data360files.worldbank.org/.../WB_WDI_*.csv).
    """
    url = f"{API_BASE}/country/{iso3}/indicator/{indicator}"

    # ------------------- tentativa via API JSON -------------------
    try:
        js = _wb_get_json(
            url,
            {"format": "json", "per_page": 20000, "date": date_range},
        )
        if isinstance(js, list) and len(js) >= 2 and js[1] is not None:
            rows = []
            for rec in js[1]:
                y = rec.get("date")
                try:
                    y_int = int(y)
                except Exception:
                    continue
                rows.append(
                    {
                        "iso3": iso3,
                        "year": y_int,
                        "value": rec.get("value"),
                        "code": indicator,
                        "label": label,
                    }
                )
            df = pd.DataFrame(rows)
            # Se houver pelo menos um valor não nulo, usamos este
            if not df.empty and df["value"].notna().any():
                return df
    except Exception:
        # qualquer erro → tentamos fallback CSV
        pass

    # ------------------- fallback via CSV Data360 -------------------
    return _wdi_fetch_indicator_csv(iso3, indicator, date_range, label)


def _is_percent_or_rate(code: str, label: str) -> bool:
    return any(code.endswith(s) for s in (".ZG", ".ZS")) or "%" in (label or "")


def _is_percent(code: str, label: str) -> bool:
    """
    Heurística para formatar eixo/tooltip com casas decimais quando é percentagem/variação.
    Muitos indicadores WDI percentuais terminam em .ZG (growth), .ZS (% of something),
    .ZP/.ZE; além disso, o label costuma conter '%' ou 'percent'.
    """
    try:
        code = (code or "").upper()
        label_lc = (label or "").lower()
    except Exception:
        return False

    suffix_flags = code.endswith((".ZG", ".ZS", ".ZP", ".ZE"))
    label_flags = ("%" in label) or ("percent" in label_lc) or ("growth" in label_lc)
    return suffix_flags or label_flags


def _chart_one(
    sub: pd.DataFrame,
    code: str,
    label: str,
    year_label: str,
    unit_hint: str | None,
) -> alt.Chart:
    data = sub.dropna(subset=["value"]).copy()
    if data.empty:
        empty = pd.DataFrame({"msg": ["No data for selected years"]})
        return (
            alt.Chart(empty, height=260)
            .mark_text(align="center", baseline="middle")
            .encode(text="msg:N")
            .properties(
                title=alt.TitleParams(text=label, anchor="start", offset=6),
                width="container",
            )
            .configure_axis(grid=False, domain=False, labels=False, ticks=False)
        )

    is_pct = _is_percent_or_rate(code, label)
    y_fmt = ",.1f" if is_pct else ",.0f"  # eixo: 1 casa em %
    tipfmt = ",.2f" if is_pct else ",.2f"  # tooltip: 2 casas

    # acrescentar unidade ao título do tooltip
    tip_title = f"{label} ({unit_hint})" if unit_hint and unit_hint != "%" else label

    return (
        alt.Chart(data, height=260)
        .mark_line(point=True)
        .encode(
            x=alt.X("year:Q", title=year_label, axis=alt.Axis(format="d")),
            y=alt.Y(
                "value:Q",
                title=label,
                scale=alt.Scale(zero=False),
                axis=alt.Axis(format=y_fmt),
            ),
            tooltip=[
                alt.Tooltip("year:Q", title=year_label, format="d"),
                alt.Tooltip("value:Q", title=tip_title, format=tipfmt),
            ],
        )
        .properties(
            title=alt.TitleParams(text=label, anchor="start", offset=6),
            width="container",
        )
    )


def _human(x):
    if pd.isna(x):
        return "–"
    try:
        x = float(x)
    except Exception:
        return str(x)
    for u in ["", "K", "M", "B", "T"]:
        if abs(x) < 1000.0:
            return f"{x:,.0f}{u}".replace(",", " ")
        x /= 1000.0
    return f"{x:,.0f}T".replace(",", " ")


# ----------------------------- Catalog (demography) -----------------------------
def _catalog(tr):
    return {
        "SP.POP.TOTL": {
            "label": _t(tr, "demography.population_total", "Population, total"),
            "unit_hint": _t(tr, "demography.people", "people"),
        },
        "SP.POP.GROW": {
            "label": _t(
                tr,
                "demography.population_growth_pct",
                "Population growth (annual %)",
            ),
            "unit_hint": "%",
        },
        "SP.DYN.LE00.IN": {
            "label": _t(
                tr,
                "demography.life_expectancy",
                "Life expectancy at birth, total (years)",
            ),
            "unit_hint": _t(tr, "demography.years", "years"),
        },
        "SP.URB.TOTL.IN.ZS": {
            "label": _t(
                tr,
                "demography.urban_population_pct",
                "Urban population (% of total)",
            ),
            "unit_hint": "%",
        },
    }


# ----------------------------- Public API -----------------------------
def render_demography_expander(
    iso3: str,
    country_name: str | None = None,
    tr=None,
    default_years: tuple[int, int] = (2000, 2024),
) -> None:
    """
    Renderiza um expander com:
      - métricas (último ano) para 4 indicadores demográficos fixos
      - 4 gráficos (2×2)
      - tabela (ano sem separador; percentuais com 2 casas)
    Bilingue via `tr` (passa a função tr do teu app).
    """
    title = _t(tr, "subnav.demografia", "Demography")
    with st.expander(f"{title}", expanded=False):
        # Anos
        year_min, year_max = default_years
        year_min, year_max = st.slider(
            _t(tr, "demography.years", "Years"),
            min_value=1960,
            max_value=2024,
            value=(year_min, year_max),
            key=f"demog_years_{iso3}",
        )
        date_range = f"{year_min}:{year_max}"

        # Catálogo e labels
        CAT = _catalog(tr)
        codes = list(CAT.keys())

        # Fetch
        with st.spinner(
            _t(tr, "demography.fetching", "Fetching data from World Bank…")
        ):
            frames = [
                _wdi_fetch_indicator(iso3, c, date_range, CAT[c]["label"])
                for c in codes
            ]
        df = (
            pd.concat(frames, ignore_index=True)
            if frames
            else pd.DataFrame(columns=["iso3", "year", "value", "code", "label"])
        )

        if df.empty or df["value"].notna().sum() == 0:
            st.warning(
                _t(
                    tr,
                    "demography.no_data",
                    "No data returned for the selected options.",
                )
            )
            st.caption(
                "Source: World Bank — World Development Indicators (WDI). "
                "APIs: api.worldbank.org + Data360 CSV fallback."
            )
            return

        # Overview (último ano disponível por label)
        st.subheader(_t(tr, "demography.overview", "Overview"))
        last_year = int(df["year"].max())
        latest = (
            df[df["year"] == last_year]
            .groupby("label", as_index=False, observed=False)["value"]
            .last()
        )
        cols = st.columns(min(4, max(1, len(latest))))
        for i, row in latest.iterrows():
            unit = ""
            # tenta buscar dica de unidade
            for c, meta in CAT.items():
                if meta["label"] == row["label"]:
                    unit = meta.get("unit_hint", "")
                    break
            cols[i % len(cols)].metric(
                label=row["label"],
                value=_human(row["value"])
                if unit and unit != "%"
                else f"{row['value']:.2f}"
                if pd.notna(row["value"])
                else "–",
                delta=f"{_t(tr, 'demography.year', 'Year')} {last_year}",
            )

        # 3) Time series — 2x2
        st.subheader(_t(tr, "demography.time_series", "Time series"))

        year_label = _t(tr, "demography.year", "Year")
        codes_4 = list(CAT.keys())[:4]

        grid = [st.columns(2), st.columns(2)]
        for i, code in enumerate(codes_4):
            r, c = divmod(i, 2)
            with grid[r][c]:
                sub = df[df["code"] == code]
                st.altair_chart(
                    _chart_one(
                        sub,
                        code,
                        CAT[code]["label"],
                        year_label,
                        CAT[code].get("unit_hint"),
                    ),
                    use_container_width=True,
                )

        # Table
        st.subheader(_t(tr, "demography.data_table", "Data table"))

        labels_order = [CAT[c]["label"] for c in codes_4]
        df4 = df[df["code"].isin(codes_4)].copy()
        wide = (
            df4.pivot_table(
                index="year", columns="label", values="value", aggfunc="last"
            ).sort_index()
        )
        wide = wide.reindex(columns=labels_order)

        disp = wide.copy()

        def _fmt(col, v):
            if pd.isna(v):
                return "–"
            return (
                (f"{float(v):,.2f}" if "%" in col else f"{float(v):,.0f}")
                .replace(",", " ")
            )

        for col in disp.columns:
            disp[col] = disp[col].apply(lambda x, c=col: _fmt(c, x))

        disp.index = disp.index.map(lambda y: str(int(y)))
        st.dataframe(
            disp.reset_index().rename(columns={"index": year_label}),
            use_container_width=True,
            height=min(520, 50 + 28 * min(len(disp), 14)),
        )

        st.caption(
            "Source: World Bank — World Development Indicators (WDI). "
            "APIs: api.worldbank.org + Data360 CSV fallback."
        )
