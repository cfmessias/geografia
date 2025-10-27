# views/graficos_continentes_altair.py
from __future__ import annotations

import pandas as pd
import altair as alt
import streamlit as st

# --- i18n helper (usa a tua função tr se existir) ---
def _t(tr, key: str, default: str) -> str:
    try:
        if callable(tr):
            v = tr(key)
            if isinstance(v, str) and v and not v.startswith("["):
                return v
    except Exception:
        pass
    return default

# --- Tema Altair: igual ao resto da app ---
alt.themes.register(
    "streamlit_dark",
    lambda: {
        "config": {
            "background": "#0e1117",
            "view": {"fill": "#0e1117"},
            "padding": {"top": 28, "right": 10, "bottom": 8, "left": 10},
            "axis": {
                "domainColor": "#ffffff",
                "gridColor": "#3a3a3a",
                "labelColor": "#ffffff",
                "titleColor": "#ffffff",
                "titleFontSize": 12,
                "labelFontSize": 11,
            },
            "legend": {"labelColor": "#ffffff", "titleColor": "#ffffff"},
            "title": {"color": "#ffffff", "fontSize": 20, "anchor": "start"},
        }
    },
)
alt.themes.enable("streamlit_dark")

# --- paleta por continente (usa os nomes tal como estão no teu DF) ---
CONTINENT_COLORS = {
    "África":  "#1f77b4",
    "América": "#ff7f0e",
    "Ásia":    "#2ca02c",
    "Europa":  "#d62728",
    "Oceania": "#9467bd",
}
CONTINENT_DOMAIN = list(CONTINENT_COLORS.keys())
CONTINENT_RANGE  = list(CONTINENT_COLORS.values())

def _is_percent(text: str) -> bool:
    text = (text or "").lower()
    return "%" in text or "percent" in text

# ------------------------------------------------------------------------------
# 1) Evolução por continente (linha ou barra)
# dados: DataFrame com colunas ["Year", "Continente", <valor>]
# value_col: nome da coluna com os valores numéricos
# tipo: "linha" ou "barra"
# ------------------------------------------------------------------------------
def chart_evolucao(
    dados: pd.DataFrame,
    tr=None,
    titulo: str = "População Total",
    ylabel: str = "Milhares de Habitantes",
    value_col: str = "Valor",
    tipo: str = "linha",
) -> alt.Chart:
    df = dados.copy()
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce")

    is_pct   = _is_percent(ylabel)
    y_format = ",.1f" if is_pct else ",.0f"
    tip_fmt  = ",.2f" if is_pct else ",.2f"

    base = alt.Chart(df).encode(
        x=alt.X("Year:Q", title=_t(tr, "climate_indicators.ano", "Year"), axis=alt.Axis(format="d")),
        y=alt.Y(f"{value_col}:Q",
                title=ylabel,
                axis=alt.Axis(format=y_format),
                scale=alt.Scale(zero=False)),
        color=alt.Color(
            "Continente:N",
            scale=alt.Scale(domain=CONTINENT_DOMAIN, range=CONTINENT_RANGE),
            legend=alt.Legend(title=_t(tr, "crescimento_populacional.continente", "Continent")),
        ),
        tooltip=[
            alt.Tooltip("Continente:N", title=_t(tr, "crescimento_populacional.continente", "Continent")),
            alt.Tooltip("Year:Q", title=_t(tr, "climate_indicators.ano", "Year"), format="d"),
            alt.Tooltip(f"{value_col}:Q", title=ylabel, format=tip_fmt),
        ],
    )

    mark = base.mark_line(point=True, strokeWidth=2) if tipo.lower().startswith("l") \
           else base.mark_bar(opacity=0.85)

    return mark.properties(
        title=alt.TitleParams(text=titulo, anchor="start", offset=6),
        height=320,
        width="container",
    )

# ------------------------------------------------------------------------------
# 2) Mortalidade 15–50 por sexo (DFs separados Homem/Mulher) → 2 painéis lado a lado
# df_homens: colunas ["Year","Continente","MortalidadeEntre15e50Homens"]
# df_mulheres: colunas ["Year","Continente","MortalidadeEntre15e50Mulheres"]
# years: lista de anos a mostrar (ex.: [1950,1980,2010,2020,último])
# ------------------------------------------------------------------------------
def chart_mortalidade_sexo(
    df_homens: pd.DataFrame,
    df_mulheres: pd.DataFrame,
    tr=None,
    years: list[int] | None = None,
) -> alt.Chart:

    y_label = _t(tr, "graficos.obitos_por_1_000", "Deaths per 1,000")

    if years is None:
        anos_base = [1950, 1980, 2010, 2020]
        ultimo = pd.concat([df_homens["Year"], df_mulheres["Year"]]).max()
        if ultimo not in anos_base:
            anos_base.append(int(ultimo))
        years = sorted(set(anos_base))

    # Normalizar para formato longo
    h = df_homens.rename(columns={"MortalidadeEntre15e50Homens": "Mortality"}) \
                 .assign(Sex=_t(tr, "graficos.homens", "Men"))
    m = df_mulheres.rename(columns={"MortalidadeEntre15e50Mulheres": "Mortality"}) \
                   .assign(Sex=_t(tr, "graficos.mulheres", "Women"))
    df = pd.concat([h, m], ignore_index=True)
    df = df[df["Year"].isin(years)].copy()
    df["Year"] = df["Year"].astype(str)

    base = alt.Chart(df).mark_bar().encode(
        x=alt.X("Year:O", title=_t(tr, "climate_indicators.ano", "Year")),
        y=alt.Y("Mortality:Q", title=y_label, axis=alt.Axis(format=",.0f"), scale=alt.Scale(zero=False)),
        color=alt.Color("Continente:N",
                        scale=alt.Scale(domain=CONTINENT_DOMAIN, range=CONTINENT_RANGE),
                        legend=alt.Legend(title=_t(tr, "crescimento_populacional.continente", "Continent"))),
        tooltip=[
            alt.Tooltip("Continente:N", title=_t(tr, "crescimento_populacional.continente", "Continent")),
            alt.Tooltip("Sex:N", title=_t(tr, "graficos.sexo", "Sex")),
            alt.Tooltip("Year:O", title=_t(tr, "climate_indicators.ano", "Year")),
            alt.Tooltip("Mortality:Q", title=y_label, format=",.0f"),
        ],
    )

    chart = base.properties(height=360).facet(
        column=alt.Column("Sex:N", title=None, header=alt.Header(labelColor="#ffffff", labelFontSize=14))
    )

    return chart.resolve_scale(y="shared").properties(
        title=alt.TitleParams(
            text=_t(tr, "graficos.mortalidade_entre_15_50_anos_por_sexo_e_continente",
                    "Mortality (15–50) by sex and continent"),
            anchor="start",
            offset=6,
        ),
        width="container",
    )

