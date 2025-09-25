# -*- coding: utf-8 -*-
# ESTE FICHEIRO FOI GERADO A PARTIR DO app.py ORIGINAL, COLOCANDO O CONTEÚDO NUMA FUNÇÃO PARA USO EM TABS.
import streamlit as st
import matplotlib.pyplot as plt
from data.dados import carregar_dados
from views.graficos import grafico_evolucao, grafico_mortalidade_stack
import matplotlib.patches as mpatches
import io
import matplotlib as mpl

# Aplica o estilo CSS personalizado

def render_indicadores_tab():
    css = """
    <style>

    </style>
    """

    # Carregar dados
    (
        df_pop, df_dens, df_racio, df_cresc,
        df_idade_media, df_taxa_alteracao_natural,
        df_nascimentos, df_obitos,
        df_esperanca_vida, df_esperanca_vida_homens80, df_esperanca_vida_mulheres80,
        df_mortalidade_antes40, df_mortalidade_antes60, df_mortalidade_entre15e50,
        df_taxa_migracao_liquida, df_mortalidade_entre15e50Homens, df_mortalidade_entre15e50Mulheres
    ) = carregar_dados()

    # Grupos de gráficos
    grupos = {
        "População e Estrutura": [
            (df_pop, "População Total", "Milhares de Habitantes", "Populacao"),
            (df_dens, "Densidade Populacional", "Habitantes/km²", "Densidade"),
            (df_racio, "Rácio de Género", "Homens por Mulher", "RacioGenero"),
            (df_cresc, "Taxa de Crescimento Populacional", "%", "Crescimento")
        ],
        "Natalidade e Mortalidade": [
            (df_nascimentos, "Nascimentos", "Milhares", "Nascimentos"),
            (df_obitos, "Óbitos", "Milhares", "Obitos"),
            (df_taxa_alteracao_natural, "Alteração Natural", "Milhares", "TaxaAlteracaoNatural"),
            (df_esperanca_vida, "Esperança de Vida", "Anos", "EsperancaVida")
        ],
        "Mortalidade Específica": [
            (df_mortalidade_antes40, "Mortalidade antes dos 40", "Óbitos/1.000 nascimentos", "MortalidadeAntes40"),
            (df_mortalidade_antes60, "Mortalidade antes dos 60", "Óbitos/1.000 nascimentos", "MortalidadeAntes60"),
            (df_mortalidade_entre15e50Homens, "Mortalidade 15–50 (Homens)", "Óbitos/1.000 vivos aos 15", "MortalidadeEntre15e50Homens"),
            (df_mortalidade_entre15e50Mulheres, "Mortalidade 15–50 (Mulheres)", "Óbitos/1.000 vivas aos 15", "MortalidadeEntre15e50Mulheres")
        ],
        "Indicadores Adicionais": [
            (df_idade_media, "Idade Média", "Anos", "IdadeMedia"),
            (df_taxa_migracao_liquida, "Migração Líquida", "Milhares", "TaxaMigracaoLiquida"),
            (df_esperanca_vida_homens80, "Esperança Vida aos 80 (Homens)", "Anos", "EsperancaVidaHomens80"),
            (df_esperanca_vida_mulheres80, "Esperança Vida aos 80 (Mulheres)", "Anos", "EsperancaVidaMulheres80")
        ]
    }

    # Interface
    st.markdown(css, unsafe_allow_html=True)

    primary = st.get_option("theme.primaryColor") or "#2563EB"
    text    = st.get_option("theme.textColor") or "#FFFFFF"  # branco
    bg      = st.get_option("theme.backgroundColor") or "#0E1117"

    mpl.rcParams.update({
        # Cores de texto/linhas dos eixos
        "axes.edgecolor": text,
        "axes.labelcolor": text,
        "xtick.color": text,
        "ytick.color": text,
        "text.color": text,

        # Grelha branca semi-transparente
        "axes.grid": True,            # ativa grid por defeito em todos os eixos
        "grid.color": "#FFFFFF",
        "grid.alpha": 0.25,
        "grid.linewidth": 0.6,
        "grid.linestyle": "-",

        # Fundos transparentes (para st.pyplot(transparent=True))
        "figure.facecolor": (0,0,0,0),
        "axes.facecolor": "none",
    })
    #st.sidebar.markdown('<div class="sidebar-title-vertical">📊 INDICADORES DEMOGRÁFICOS</div>', unsafe_allow_html=True)
    st.markdown("📊 INDICADORES DEMOGRÁFICOS")
    # Escolha do grupo
    grupo_escolhido = st.selectbox("Escolha o grupo de indicadores:", list(grupos.keys()))

    # Criar legenda compacta
    fig_legend = plt.figure(figsize=(6, 0.4), dpi=300)
    ax_legend = fig_legend.add_axes([0, 0, 1, 1])
    ax_legend.axis('off')

    patches = [
        mpatches.Patch(color='orange', label='América'),
        mpatches.Patch(color='red', label='Europa'),
        mpatches.Patch(color='purple', label='Oceania'),
        mpatches.Patch(color='blue', label='África'),
        mpatches.Patch(color='green', label='Ásia')
    ]

    ax_legend.legend(
        handles=patches,
        loc='center',
        ncol=5,
        frameon=False,
        fontsize='xx-small',
        columnspacing=0.5,
        handlelength=1.0,
        handletextpad=0.4,
        borderpad=0.0
    )

    buf = io.BytesIO()
    fig_legend.savefig(buf, format="png", bbox_inches="tight", transparent=True, pad_inches=0)
    buf.seek(0)
    st.image(buf)

    # JavaScript para enviar o user agent e largura para o backend
    st.markdown("""
    <script>
        const isMobile = /Mobi|Android/i.test(navigator.userAgent);
        const width = window.innerWidth;
        window.parent.postMessage({
            type: "streamlit:setComponentValue",
            key: "detectar_dispositivo",
            value: { largura: width, isMobile: isMobile },
        }, "*");
    </script>
    """, unsafe_allow_html=True)

    # Placeholder experimental
    info_dispositivo = st.session_state.get("detectar_dispositivo", {"largura": 1200, "isMobile": False})
    usar_layout_vertical = info_dispositivo.get("isMobile", False) or info_dispositivo.get("largura", 0) < 768

    if usar_layout_vertical:
        for i in range(4):
            df, titulo, ylabel, dado = grupos[grupo_escolhido][i]
            fig, ax = plt.subplots(figsize=(6, 2.8))
            grafico_evolucao(df, titulo, ylabel, dado, 'linha', ax)
            fig.patch.set_alpha(0.0)
            st.pyplot(fig, transparent=True)
    else:
        subtab1, subtab2 = st.tabs(["Gráficos 1 e 2", "Gráficos 3 e 4"])

        with subtab1:
            fig, axs = plt.subplots(1, 2, figsize=(9.6, 3))
            for i in range(0, 2):
                df, titulo, ylabel, dado = grupos[grupo_escolhido][i]
                grafico_evolucao(df, titulo, ylabel, dado, 'linha', axs[i])
            fig.patch.set_alpha(0.0)
            st.pyplot(fig, transparent=True)

        with subtab2:
            fig, axs = plt.subplots(1, 2, figsize=(9.6, 3))
            for i in range(2, 4):
                df, titulo, ylabel, dado = grupos[grupo_escolhido][i]
                grafico_evolucao(df, titulo, ylabel, dado, 'linha', axs[i - 2])
            fig.patch.set_alpha(0.0)
            st.pyplot(fig, transparent=True)
