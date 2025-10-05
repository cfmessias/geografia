# -*- coding: utf-8 -*-
"""
Página/aba: Indicadores Demográficos (com i18n)
- Usa chaves i18n para títulos e eixos dos gráficos
- Compatível com seletor de idioma via services/i18n_boot
- Requer que existam as chaves em locales/pt.json e locales/en.json:
  demografia.groups.*, demografia.titles.*, demografia.y.*
"""

from __future__ import annotations

import io
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import streamlit as st

from data.dados import carregar_dados
from views.graficos import grafico_evolucao, grafico_mortalidade_stack  # (mantemos o import se precisares)
from services.i18n import t as tr
from services.i18n_boot import _ensure_lang_state

# --- configurações compactas para os gráficos ---
FIGSIZE = (7.0, 2.4)   # ← experimenta 6.6–7.4
F_TITLE = 12           # título
F_LABEL = 9            # eixos
F_TICK  = 8            # ticks

def _compactify_axes(ax):
    """Força tamanhos mesmo que grafico_evolucao os tenha fixado internamente."""
    ax.set_title(ax.get_title(), fontsize=F_TITLE, weight="bold")
    ax.xaxis.label.set_size(F_LABEL)
    ax.yaxis.label.set_size(F_LABEL)
    for tick in ax.get_xticklabels() + ax.get_yticklabels():
        tick.set_fontsize(F_TICK)
    # também reduz textos desenhados com ax.text()
    for txt in ax.texts:
        txt.set_fontsize(F_TICK)

def _apply_mpl_theme() -> None:
    """Tema escuro + tamanhos compactos."""
    primary = st.get_option("theme.primaryColor") or "#2563EB"
    text = st.get_option("theme.textColor") or "#FFFFFF"
    grid = "#FFFFFF"
    mpl.rcParams.update({
        # tamanhos de fonte
        "axes.titlesize": F_TITLE,
        "axes.titleweight": "bold",
        "axes.labelsize": F_LABEL,
        "axes.labelcolor": text,
        "xtick.labelsize": F_TICK,
        "ytick.labelsize": F_TICK,
        # cores
        "axes.edgecolor": text,
        "xtick.color": text,
        "ytick.color": text,
        "text.color": text,
        # grelha discreta
        "axes.grid": True,
        "grid.color": grid,
        "grid.alpha": 0.25,
        "grid.linewidth": 0.6,
        "grid.linestyle": "-",
        # fundos transparentes
        "figure.facecolor": (0, 0, 0, 0),
        "axes.facecolor": "none",
        # dpi neutro (evita textos a “explodir” em ecrãs retina)
        "figure.dpi": 100,
    })


def _legend_continents() -> None:
    """Mostra a legenda com cores de continentes, acima dos gráficos."""
    # Ajusta as cores às que usas em grafico_evolucao, se necessário.
    patches = [
        mpatches.Patch(color="#f2b01e", label=tr("ind_demograficos.america")),  # Amer./gold
        mpatches.Patch(color="#dc2626", label=tr("ind_demograficos.europa")),   # Europe/red
        mpatches.Patch(color="#6b21a8", label=tr("ind_demograficos.oceania")),  # Oceania/purple
        mpatches.Patch(color="#2563eb", label="Africa"),                        # será traduzido em runtime via série
        mpatches.Patch(color="#16a34a", label=tr("ind_demograficos.asia")),     # Asia/green
    ]
    fig = plt.figure(figsize=(1, 0.01))
    ax = fig.add_subplot(111)
    ax.axis("off")
    ax.legend(
        handles=patches,
        loc="center",
        ncol=len(patches),
        frameon=False,
        handlelength=1.2,
        handleheight=0.9,
        borderpad=0.2,
        labelspacing=0.9,
        columnspacing=1.2,
        fontsize=12,          # <= podes baixar para 11 se quiseres ainda mais compacto
    )
    # Renderiza no topo da página
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=160, transparent=True)
    plt.close(fig)
    st.image(buf.getvalue(), use_container_width=False) 


def render_indicadores_tab() -> None:
    """Entry-point da página/aba de Demografia."""
    _ensure_lang_state()
    _apply_mpl_theme()

    # Título da página/aba
    st.markdown(tr("labels.indicadores_demogr_ficos"))

    # Carregar dados (mantém a ordem/nomes do teu módulo)
    (
        df_pop, df_dens, df_racio, df_cresc,
        df_idade_media, df_taxa_alteracao_natural,
        df_nascimentos, df_obitos,
        df_esperanca_vida, df_esperanca_vida_homens80, df_esperanca_vida_mulheres80,
        df_mortalidade_antes40, df_mortalidade_antes60, df_mortalidade_entre15e50,
        df_taxa_migracao_liquida, df_mortalidade_entre15e50Homens, df_mortalidade_entre15e50Mulheres
    ) = carregar_dados()

    # ── Grupos de gráficos (usar CHAVES i18n, não texto literal) ────────────
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

    # Seletor do grupo (mostra label traduzida, mas usa a chave)
    grupo_key = st.selectbox(
        tr("labels.escolha_o_grupo_de_indicadores"),
        list(grupos.keys()),
        format_func=lambda k: tr(f"demografia.groups.{k}")
    )

    # Legenda das séries por continente (opcional)
    _legend_continents()

    # Layout dos gráficos (duas tabs com 2 gráficos cada)
    subtab1, subtab2 = st.tabs([tr("app.tabs.gr_ficos_1_e_2"), tr("app.tabs.gr_ficos_3_e_4")])

    with subtab1:
        fig, axs = plt.subplots(1, 2, figsize=FIGSIZE, constrained_layout=True)
        for i in range(0, 2):
            df, titulo_key, ylabel_key, dado = grupos[grupo_key][i]
            grafico_evolucao(df, tr(titulo_key), tr(ylabel_key), dado, "linha", axs[i])
            _compactify_axes(axs[i])
        fig.patch.set_alpha(0.0)
        st.pyplot(fig, use_container_width=False, transparent=True)  # << importante

    with subtab2:
        fig, axs = plt.subplots(1, 2, figsize=FIGSIZE, constrained_layout=True)
        for i in range(2, 4):
            df, titulo_key, ylabel_key, dado = grupos[grupo_key][i]
            grafico_evolucao(df, tr(titulo_key), tr(ylabel_key), dado, "linha", axs[i - 2])
            _compactify_axes(axs[i - 2])
        fig.patch.set_alpha(0.0)
        st.pyplot(fig, use_container_width=False, transparent=True)  # << importante


# Opcional: permitir correr isoladamente este módulo para debug rápido
if __name__ == "__main__":
    st.set_page_config(page_title="Demografia", layout="wide")
    render_indicadores_tab()
