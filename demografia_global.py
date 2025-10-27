# -*- coding: utf-8 -*-
"""
Página/aba: Indicadores Demográficos (com i18n)
- Usa chaves i18n para títulos e eixos dos gráficos
- Compatível com seletor de idioma via services/i18n_boot
- Requer que existam as chaves em locales/pt.json e locales/en.json:
  demografia.groups.*, demografia.titles.*, demografia.y.*
"""
from __future__ import annotations
import altair as alt

import pandas as pd
import streamlit as st
#from views.graficos_continentes_altair import chart_evolucao, chart_mortalidade_sexo
from data.carrega_dados_demografia import carregar_dados

from services.i18n import t as tr
from services.i18n_boot import _ensure_lang_state

# --- configurações compactas para os gráficos ---
FIGSIZE = (7.0, 2.4)   # ← experimenta 6.6–7.4
F_TITLE = 12           # título
F_LABEL = 9            # eixos
F_TICK  = 8            # ticks


def render_indicadores_tab() -> None:
    """Página principal da aba Demografia — versão Altair (estilo moderno)."""
    _ensure_lang_state() 

    # ────────────────────── Tema Altair dark unificado ──────────────────────
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
                "title": {"color": "#ffffff", "fontSize": 16, "anchor": "start"},
            }
        },
    )
    alt.themes.enable("streamlit_dark")

    # ─────────────────────────── Cabeçalho ───────────────────────────────
    st.markdown(tr("labels.indicadores_demogr_ficos"))

    # Carregar dados (mantém o pipeline original)
    (
        df_pop, df_dens, df_racio, df_cresc,
        df_idade_media, df_taxa_alteracao_natural,
        df_nascimentos, df_obitos,
        df_esperanca_vida, df_esperanca_vida_homens80, df_esperanca_vida_mulheres80,
        df_mortalidade_antes40, df_mortalidade_antes60, df_mortalidade_entre15e50,
        df_taxa_migracao_liquida, df_mortalidade_entre15e50Homens, df_mortalidade_entre15e50Mulheres
    ) = carregar_dados()

    # ─────────────── Grupos (mantendo estrutura e i18n) ────────────────
    grupos = {
        "pop_estr": [
            (df_pop,  "demografia.titles.pop_total",         "demografia.y.thousands_inhabitants", "Populacao"),
            (df_dens, "demografia.titles.density",           "demografia.y.inhabitants_per_km2",   "Densidade"),
            (df_racio,"demografia.titles.gender_ratio",      "demografia.y.men_per_woman",         "RacioGenero"),
            (df_cresc,"demografia.titles.growth_rate",       "demografia.y.percent",               "Crescimento"),
        ],
        "nat_mort": [
            (df_nascimentos, "demografia.titles.births",                   "demografia.y.thousands", "Nascimentos"),
            (df_obitos,      "demografia.titles.deaths",                   "demografia.y.thousands", "Obitos"),
            (df_taxa_alteracao_natural, "demografia.titles.natural_change","demografia.y.thousands", "TaxaAlteracaoNatural"),
            (df_esperanca_vida, "demografia.titles.life_expectancy",       "demografia.y.years",     "EsperancaVida"),
        ],
        "mort_esp": [
            (df_mortalidade_antes40, "demografia.titles.mort_before_40",      "demografia.y.deaths_per_1000_births", "MortalidadeAntes40"),
            (df_mortalidade_antes60, "demografia.titles.mort_before_60",      "demografia.y.deaths_per_1000_births", "MortalidadeAntes60"),
            (df_mortalidade_entre15e50Homens, "demografia.titles.mort_15_50_men",   "demografia.y.deaths_per_1000_at_15",   "MortalidadeEntre15e50Homens"),
            (df_mortalidade_entre15e50Mulheres, "demografia.titles.mort_15_50_women","demografia.y.deaths_per_1000_at_15_f","MortalidadeEntre15e50Mulheres"),
        ],
        "indic_adic": [
            (df_idade_media,             "demografia.titles.median_age",          "demografia.y.years",     "IdadeMedia"),
            (df_taxa_migracao_liquida,   "demografia.titles.net_migration",       "demografia.y.thousands", "TaxaMigracaoLiquida"),
            (df_esperanca_vida_homens80, "demografia.titles.life_expect_80_men",  "demografia.y.years",     "EsperancaVidaHomens80"),
            (df_esperanca_vida_mulheres80,"demografia.titles.life_expect_80_women","demografia.y.years",    "EsperancaVidaMulheres80"),
        ],
    }

    grupo_key = st.selectbox(
        tr("labels.selecione_um_pais"),
        list(grupos.keys()),
        format_func=lambda k: tr(f"demografia.groups.{k}")
    )

    # ─────────────── Função para construir cada gráfico Altair ───────────────
    def _chart(df: pd.DataFrame, titulo_key: str, ylabel_key: str, value_col: str) -> alt.Chart:
   
        import numpy as np
        if df is None or df.empty:
            return alt.Chart(pd.DataFrame({"msg": ["No data"]})).mark_text(
                align="center", baseline="middle"
            ).encode(text="msg:N").properties(
                title=alt.TitleParams(text=tr(titulo_key), anchor="start", offset=6),
                height=320, width="container"
            )

        # Detectar nomes reais das colunas
        x_candidates = ["Year", "year", "Ano", "ano"]
        continent_candidates = ["Continente", "continent", "Continent", "continente"]
        x_col = next((c for c in x_candidates if c in df.columns), None)
        cont_col = next((c for c in continent_candidates if c in df.columns), None)

        if x_col is None or value_col not in df.columns:
            # estado vazio amigável se estrutura não bate certo
            return alt.Chart(pd.DataFrame({"msg": ["No data (columns not found)"]})).mark_text(
                align="center", baseline="middle"
            ).encode(text="msg:N").properties(
                title=alt.TitleParams(text=tr(titulo_key), anchor="start", offset=6),
                height=320, width="container"
            )

        # Normalizações
        d = df[[x_col, value_col] + ([cont_col] if cont_col else [])].copy()
        d[x_col] = pd.to_numeric(d[x_col], errors="coerce")
        d[value_col] = pd.to_numeric(d[value_col], errors="coerce")
        d = d.dropna(subset=[x_col, value_col])

        ylabel = tr(ylabel_key)
        is_pct = ("%" in ylabel) or ("percent" in ylabel.lower())
        yfmt = ",.1f" if is_pct else ",.0f"
        tipfmt = ",.2f" if is_pct else ",.2f"

        enc = dict(
            x=alt.X(f"{x_col}:Q", title=tr("climate_indicators.ano"), axis=alt.Axis(format="d")),
            y=alt.Y(f"{value_col}:Q", title=ylabel, axis=alt.Axis(format=yfmt), scale=alt.Scale(zero=False)),
            tooltip=[
                alt.Tooltip(f"{x_col}:Q", title=tr("climate_indicators.ano"), format="d"),
                alt.Tooltip(f"{value_col}:Q", title=ylabel, format=tipfmt),
            ],
        )
        if cont_col:
            enc["color"] = alt.Color(
                f"{cont_col}:N", 
                title=tr("crescimento_populacional.continente"),
                legend=None 
            )
            enc["tooltip"].append(alt.Tooltip(f"{cont_col}:N", title=tr("crescimento_populacional.continente")))

        return (
            alt.Chart(d)
            .mark_line(point=True)
            .encode(**enc)
            .properties(title=alt.TitleParams(text=tr(titulo_key), anchor="start", offset=6),
                        height=320, width="container")
        )
    
    def _create_continent_legend(*, font_size=13, point_size=100, dy=6):
        """Cria uma legenda horizontal única para os continentes."""
        continentes_data = pd.DataFrame({
            'Continente': ['América', 'Europa', 'Oceania', 'África', 'Ásia'],
            'x_pos': [0, 1, 2, 3, 4]
        })

        legend_chart = (
            alt.Chart(continentes_data)
            .mark_point(size=point_size, filled=True)
            .encode(
                x=alt.X('x_pos:Q', axis=None, scale=alt.Scale(domain=[-0.5, 4.5])),
                color=alt.Color(
                    'Continente:N',
                    title=None,
                    legend=None,
                    scale=alt.Scale(
                        domain=['América', 'Europa', 'Oceania', 'África', 'Ásia'],
                        range=['#4c78a8', '#f58518', '#e45756', '#72b7b2', '#54a24b']
                    )
                )
            )
            .properties(width='container', height=40)
        )

        text_labels = (
            alt.Chart(continentes_data)
            .mark_text(
                align='center', baseline='top', dy=dy,
                fontSize=font_size, font='Segoe UI', color='#ffffff'
            )
            .encode(
                x=alt.X('x_pos:Q', axis=None, scale=alt.Scale(domain=[-0.5, 4.5])),
                text='Continente:N'
            )
        )

        return legend_chart + text_labels

  
    # --- Séries temporais — 4 gráficos de uma vez (2×2) ---
    
    #st.subheader(tr("demography.time_series"))
    c1, c2, c3  = st.columns([1,2,1])
    with c2:
        st.altair_chart(_create_continent_legend(), use_container_width=True)
    # CRIAR LEGENDA ÚNICA HORIZONTAL (adicione isto ANTES da grid):
    # Pegue o primeiro DataFrame com dados do continente
    df_for_legend = None
    for df_i, _, _, _ in grupos[grupo_key][:4]:
        if df_i is not None and not df_i.empty:
            cont_col_candidates = ["Continente", "continent", "Continent", "continente"]
            cont_col = next((c for c in cont_col_candidates if c in df_i.columns), None)
            if cont_col:
                df_for_legend = df_i[[cont_col]].drop_duplicates().head(10)
                break
    
    if df_for_legend is not None and cont_col:
        # Criar um gráfico dummy só para mostrar a legenda
        legend_chart = (
            alt.Chart(df_for_legend)
            .mark_point(size=0, opacity=0)  # Pontos invisíveis
            .encode(
                color=alt.Color(
                    f"{cont_col}:N",
                    title=tr("crescimento_populacional.continente"),
                    legend=alt.Legend(
                        orient='top',  # ou 'bottom' para aparecer embaixo
                        direction='horizontal',
                        titleAnchor='middle',
                        titleOrient='left',
                        columns=6  # número de colunas na legenda horizontal
                    )
                )
            )
            .properties(width=1, height=1)
        )
        st.altair_chart(legend_chart, use_container_width=True)

    # Grid original (continua igual)
    grid = [st.columns(2), st.columns(2)]  # 2 linhas × 2 colunas
    for i, (df_i, titulo_key, ylabel_key, dado) in enumerate(grupos[grupo_key][:4]):
        r, c = divmod(i, 2)
        with grid[r][c]:
            st.altair_chart(
                _chart(df_i, titulo_key, ylabel_key, dado),
                use_container_width=True
            )
    # (opcional) manter fundo transparente dos SVG
    st.markdown(
        "<style>div[data-testid='stVerticalBlock'] svg {background: transparent !important;}</style>",
        unsafe_allow_html=True,
    )  

# ── helpers globais (fora de funções) ────────────────────────────────────────
def _to_number(s: pd.Series) -> pd.Series:
    """'83.687,57' → 83687.57 ; '12,3' → 12.3 ; '' → NaN"""
    return pd.to_numeric(
        s.astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
        errors="coerce",
    )


def render_compare_tab(df_world: pd.DataFrame) -> None:

    """
    Comparação País vs Região vs Continente (2x2 gráficos + tabela + download por gráfico).
    Recebe o DataFrame do CSV 'demografia_mundial.csv' já carregado (sep=';').
    """  

    # i18n seguro
    def _t(key: str, default: str):
        try:
            return tr(key)
        except Exception:
            return default

    # ───────────────────────── tema Altair ─────────────────────────
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
                "title": {"color": "#ffffff", "fontSize": 16, "anchor": "start"},
            }
        },
    )
    alt.themes.enable("streamlit_dark")

    # ───────────────────────── helpers ─────────────────────────
    def _to_number(s: pd.Series) -> pd.Series:
        """Converte '83.687,57'→83687.57 | '12,3'→12.3 | '0.94'→0.94 | '1 234'→1234"""
        x = (
            s.astype(str)
             .str.replace("\u00A0", " ", regex=False)
             .str.strip()
        )
        m_eu = x.str.contains(r"\.") & x.str.contains(r",")
        x = x.where(~m_eu, x.str.replace(".", "", regex=False).str.replace(",", ".", regex=False))
        m_comma = (~m_eu) & x.str.contains(",")
        x = x.where(~m_comma, x.str.replace(",", ".", regex=False))
        x = x.str.replace(" ", "", regex=False)
        return pd.to_numeric(x, errors="coerce")

    def _build_hierarchy_maps(df, COL_LOC_CODE, COL_PARENT, COL_NAME, COL_TYPE):
        parent_of = (
            df[[COL_LOC_CODE, COL_PARENT]].dropna()
              .drop_duplicates(subset=[COL_LOC_CODE])
              .set_index(COL_LOC_CODE)[COL_PARENT].to_dict()
        )
        name_of = (
            df[[COL_LOC_CODE, COL_NAME]].dropna()
              .drop_duplicates(subset=[COL_LOC_CODE])
              .set_index(COL_LOC_CODE)[COL_NAME].to_dict()
        )
        is_region = (
            df[[COL_LOC_CODE, COL_TYPE]].dropna()
              .drop_duplicates(subset=[COL_LOC_CODE])
              .set_index(COL_LOC_CODE)[COL_TYPE].eq("Region").to_dict()
        )

        def resolve_chain(loc_code: int):
            # devolve (region_code, continent_code)
            region = parent_of.get(loc_code)
            continent = parent_of.get(region) if region is not None else None
            if region is not None and not is_region.get(region, False):
                continent = parent_of.get(region)
            return region, continent

        return parent_of, name_of, is_region, resolve_chain

    def _aggregate_children(df, parent_code, value_col, COL_PARENT, COL_TYPE, COL_YEAR, country_loc_code=None):
        """Agrega filhos de um parent; exclui o próprio país. Soma stocks, média taxas."""
        kids = df[df[COL_PARENT].eq(parent_code)].copy()
        if country_loc_code is not None and "Location code" in kids.columns:
            kids = kids[kids["Location code"].ne(country_loc_code)]

        kids[value_col] = _to_number(kids[value_col])
        kids[COL_YEAR] = pd.to_numeric(kids[COL_YEAR], errors="coerce")

        col_lc = value_col.lower()
        is_sum = any(k in col_lc for k in ["thousand", "totalpopulation"])
        aggfn = "sum" if is_sum else "mean"

        return (kids.groupby(COL_YEAR, as_index=False)[value_col].agg(aggfn)
                    .rename(columns={COL_YEAR: "Year"}))

    def _make_series(df, code, value_col, COL_LOC_CODE, COL_TYPE, COL_YEAR,
                     label, fallback_children=False, COL_PARENT=None, country_loc_code=None):
        """Tenta usar linha agregada (Type==Region); senão, agrega filhos."""
        if code is None:
            return pd.DataFrame(columns=["Year", "Serie", "Value"])
        reg = df[(df[COL_LOC_CODE].eq(code)) & (df[COL_TYPE].eq("Region"))]
        if not reg.empty:
            s = reg[[COL_YEAR, value_col]].rename(columns={COL_YEAR: "Year", value_col: "Value"}).copy()
        elif fallback_children and COL_PARENT is not None:
            s = _aggregate_children(df, code, value_col, COL_PARENT, COL_TYPE, COL_YEAR, country_loc_code)
            s = s.rename(columns={value_col: "Value"})
        else:
            s = pd.DataFrame(columns=["Year", "Value"])
        s["Value"] = _to_number(s["Value"])
        s["Serie"] = label
        return s[["Year", "Serie", "Value"]]

    def _is_absolute_series(colname: str) -> bool:
        c = colname.lower()
        return any(t in c for t in [
            "totalpopulation", "thousand", "populationchange(thousands)",
            "births(thousands)", "totaldeaths(thousands)",
            "naturalchange,birthsminusdeaths(thousands)"
        ])

    # ───────────────────────── aliases de colunas ─────────────────────────
    COL_NAME      = "Region, subregion, country or area *"
    COL_ISO3      = "ISO3 Alpha-code"
    COL_LOC_CODE  = "Location code"
    COL_PARENT    = "Parent code"
    COL_TYPE      = "Type"
    COL_YEAR      = "Year"

    if df_world is None or df_world.empty:
        st.warning(_t("labels.sem_dados_para_o_pais", "Sem dados."))
        return

    df = df_world.copy()

    # tipos mínimos
    for c in [COL_YEAR, COL_LOC_CODE, COL_PARENT]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    if COL_ISO3 in df.columns:
        df[COL_ISO3] = df[COL_ISO3].astype(str).str.upper().str.strip()

    # mapas hierárquicos
    parent_of, name_of, is_region, resolve_chain = _build_hierarchy_maps(
        df, COL_LOC_CODE, COL_PARENT, COL_NAME, COL_TYPE
    )

    # ───────────────────────── seletores ─────────────────────────
    df_countries = df[df[COL_TYPE].isin(["Country/Area", "Country", "Area"])].copy()
    if df_countries.empty:
        df_countries = df[~df[COL_TYPE].isin(["Region", "World"])].copy()
    df_countries["label"] = df_countries.apply(
        lambda r: f"{r.get(COL_NAME, '')} [{r.get(COL_ISO3, '')}]", axis=1
    )
    df_countries = df_countries.drop_duplicates(subset=[COL_ISO3]).sort_values("label")

    left, right = st.columns([1.6, 1])
    with left:
        sel_label = st.selectbox(_t("labels.selecione_um_pais", "País"), df_countries["label"].tolist())
    sel_iso3 = sel_label.rsplit("[", 1)[-1].replace("]", "").strip()

    y_min_all = int(pd.to_numeric(df[COL_YEAR], errors="coerce").min())
    y_max_all = int(pd.to_numeric(df[COL_YEAR], errors="coerce").max())
    with right:
        year_min, year_max = st.slider(
            _t("demography.years", "Anos"),
            min_value=y_min_all, max_value=y_max_all,
            value=(max(1950, y_min_all), y_max_all),
        )

    # ───────────────────────── país / região / continente ─────────────────────────
    df_country_full = df[df[COL_ISO3].eq(sel_iso3)].copy()
    if df_country_full.empty:
        st.warning(_t("labels.sem_dados_para_o_pais", "Sem dados para o país selecionado."))
        return

    country_loc_code = int(df_country_full[COL_LOC_CODE].dropna().iloc[0])
    country_name = df_country_full[COL_NAME].iloc[0]
    
    region_code, continent_code = resolve_chain(country_loc_code)
    region_name    = name_of.get(region_code, _t("labels.regiao_desconhecida", "Região"))
    continent_name = name_of.get(continent_code, _t("labels.continente_desconhecido", "Continente"))

    in_years = df[COL_YEAR].between(year_min, year_max)
    df_country_full = df_country_full[in_years].rename(columns={COL_YEAR: "Year"})

    # ───────────────────────── catálogo (4 por grupo) ─────────────────────────
    CAT = {
        "pop_estr": [
            ("Population Density, as of 1 July (persons per square km)", _t("demografia.titles.density", "Densidade populacional"), _t("demografia.y.inhabitants_per_km2", "hab/km²")),
            ("Population Sex Ratio, as of 1 July (males per 100 females)", _t("demografia.titles.gender_ratio", "Rácio de sexo"), _t("demografia.y.men_per_woman", "homens/100 mulheres")),
            ("PopulationGrowthRate(percentage)", _t("demografia.titles.population_growth_pct", "Taxa de crescimento (%)"), "%"),
            ("Median Age, as of 1 July (years)", _t("demografia.titles.median_age", "Idade mediana"), _t("demografia.y.years", "anos")),
        ],
        "nat_mort": [
            ("LifeExpectancyatBirth,bothsexes(years)", _t("demografia.titles.life_expectancy", "Esperança de vida (nasc.)"), _t("demografia.y.years", "anos")),
            ("InfantMortalityRate(infantdeathsper1,000livebirths)", _t("demografia.titles.mortality_infant", "Mortalidade infantil (‰)"), _t("demografia.y.per_1000", "por mil")),
            ("Under-FiveMortality(deathsunderage5per1,000livebirths)", _t("demografia.titles.mortality_under5", "<5 mortalidade (‰)"), _t("demografia.y.per_1000", "por mil")),
            ("NetMigrationRate(per1,000population)", _t("demografia.titles.net_migration_rate", "Migração líquida (‰)"), _t("demografia.y.per_1000", "por mil")),
        ],
        "indic_adic": [
            ("SexRatioatBirth(malesper100femalebirths)", _t("demografia.titles.sex_ratio_birth", "Rácio ao nascer"), _t("demografia.y.males_per_100_females", "homens/100 mulheres")),
            ("RateofNaturalChange(per1,000population)", _t("demografia.titles.growth_rate_per_1000", "Altera.º natural (‰)"), _t("demografia.y.per_1000", "por mil")),
            ("LifeExpectancyatAge65,bothsexes(years)", _t("demografia.titles.life_expectancy_65", "Esperança de vida aos 65"), _t("demografia.y.years", "anos")),
            ("LifeExpectancyatBirth,bothsexes(years)", _t("demografia.titles.life_expectancy", "Esperança de vida (nasc.)"), _t("demografia.y.years", "anos")),
        ],
    }

    grupo_key = st.selectbox(
        _t("labels.escolha_o_grupo_de_indicadores", "Grupo de indicadores"),
        list(CAT.keys()),
        format_func=lambda k: _t(f"demografia.groups.{k}", k),
    )

    show_absolute = False
    series_all = CAT[grupo_key]
    series_filtered = [t for t in series_all if show_absolute or not _is_absolute_series(t[0])]

    year_label     = _t("climate_indicators.ano", "Ano")
    label_country  = country_name  # em vez de _t("labels.pais", "País")
    label_region   = region_name
    label_continent= continent_name

    def _create_comparison_legend(
        label_country, label_region, label_continent, *,
        font_size=13, point_size=110
    ):
        """Cria uma legenda horizontal única para comparação país/região/continente."""
        legend_data = pd.DataFrame({
            'Serie': [label_country, label_region, label_continent],
            'x_pos': [0, 1, 2]
        })

        legend_chart = (
            alt.Chart(legend_data)
            .mark_point(size=point_size, filled=True)
            .encode(
                x=alt.X('x_pos:Q', axis=None, scale=alt.Scale(domain=[-0.5, 2.5])),
                color=alt.Color('Serie:N',
                    title=None, legend=None,
                    scale=alt.Scale(
                        domain=[label_country, label_region, label_continent],
                        range=["#60a5fa", "#f59e0b", "#9ca3af"]
                    )
                )
            )
            .properties(width='container', height=40)
        )

        text_labels = (
            alt.Chart(legend_data)
            .mark_text(align='center', baseline='top', dy=6,
                    fontSize=font_size, font='Segoe UI', color='#ffffff')
            .encode(
                x=alt.X('x_pos:Q', axis=None, scale=alt.Scale(domain=[-0.5, 2.5])),
                text='Serie:N'
            )
        )

        return legend_chart + text_labels

    # ───────────────────────── grelha 2x2 ─────────────────────────
    #st.subheader(_t("demography.time_series", "Time series"))
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        st.altair_chart(_create_comparison_legend(label_country, label_region, label_continent), 
                       use_container_width=True)
        
    containers = [*st.columns(2), *st.columns(2)]

    for (container, (colname, title, yhint)) in zip(containers, series_filtered):
        with container:
            # País
            s_country = df_country_full[["Year", colname]].rename(columns={colname: "Value"}).copy()
            s_country["Value"] = _to_number(s_country["Value"])
            s_country["Serie"] = label_country

            # Região
            s_region = _make_series(
                df, region_code, colname,
                COL_LOC_CODE, COL_TYPE, COL_YEAR,
                label_region, fallback_children=True,
                COL_PARENT=COL_PARENT, country_loc_code=country_loc_code
            )
            # Continente
            s_continent = _make_series(
                df, continent_code, colname,
                COL_LOC_CODE, COL_TYPE, COL_YEAR,
                label_continent, fallback_children=True,
                COL_PARENT=COL_PARENT, country_loc_code=country_loc_code
            )

            both = pd.concat([s_country, s_region, s_continent], ignore_index=True)
            both = both.dropna(subset=["Year"]).sort_values(["Serie", "Year"])

            is_pct = (yhint or "").strip() == "%"
            yfmt, tipfmt = (",.1f", ",.2f") if is_pct else (",.0f", ",.2f")

            chart = (
                alt.Chart(both)
                .mark_line(point=True, strokeWidth=2)
                .encode(
                    x=alt.X("Year:Q", title=year_label, axis=alt.Axis(format="d")),
                    y=alt.Y("Value:Q", title=title, axis=alt.Axis(format=yfmt), scale=alt.Scale(zero=False)),
                    color=alt.Color(
                        "Serie:N",
                        scale=alt.Scale(
                            domain=[label_country, label_region, label_continent],
                            range=["#60a5fa", "#f59e0b", "#9ca3af"],  # país, região, continente
                        ),
                        #legend=alt.Legend(title=None, orient="top"),
                        legend=None
                    ),
                    tooltip=[
                        alt.Tooltip("Serie:N", title=_t("labels.serie", "Série")),
                        alt.Tooltip("Year:Q", title=year_label, format="d"),
                        alt.Tooltip("Value:Q", title=title, format=tipfmt),
                    ],
                )
                .properties(
                    title=alt.TitleParams(text=title, anchor="start", offset=6),
                    height=320, width="container"
                )
            )
            st.altair_chart(chart, use_container_width=True)

            

# Opcional: permitir correr isoladamente este módulo para debug rápido
if __name__ == "__main__":
    st.set_page_config(page_title="Demografia", layout="wide")
    render_indicadores_tab()
