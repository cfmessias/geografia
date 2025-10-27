import matplotlib.pyplot as plt
import matplotlib.colors as mc
import matplotlib.ticker as mticker
import colorsys
from services.i18n import t as tr

try:
    from services.i18n_boot import _ensure_lang_state
except ImportError:
    from services.i18n_boot import init_i18n_state as _ensure_lang_state
_ensure_lang_state()

# -------------------- Tema / constantes (estilo "dark" da app) --------------------
DARK_BG   = "#0e1117"
GRID_COL  = "#3a3a3a"
FG_COL    = "#ffffff"
TITLE_SZ  = 16
LABEL_SZ  = 12
TICK_SZ   = 11

cores_continentes = {
    "África":  "#1f77b4",
    "América": "#ff7f0e",
    "Ásia":    "#2ca02c",
    "Europa":  "#d62728",
    "Oceania": "#9467bd",
}

def lighten_color(color, factor=1.2):
    try:
        c = mc.cnames[color]
    except Exception:
        c = color
    rgb = mc.to_rgb(c)
    hls = colorsys.rgb_to_hls(*rgb)
    lighter_rgb = colorsys.hls_to_rgb(hls[0], min(1, hls[1] * factor), hls[2])
    return lighter_rgb

# -------------------- helpers de estilo --------------------
def _apply_dark_style(ax):
    """Aplica o tema dark ao Axes."""
    fig = ax.figure
    fig.set_facecolor(DARK_BG)
    ax.set_facecolor(DARK_BG)

    # spines / grelha
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    ax.spines["left"].set_color(FG_COL)
    ax.spines["bottom"].set_color(FG_COL)

    ax.grid(True, linestyle="--", linewidth=0.8, alpha=0.4, color=GRID_COL)

    # ticks e labels
    ax.tick_params(colors=FG_COL, labelsize=TICK_SZ)
    ax.xaxis.label.set_color(FG_COL)
    ax.yaxis.label.set_color(FG_COL)

def _is_percent_ylabel(ylabel: str) -> bool:
    yl = (ylabel or "").lower()
    return "%" in yl or "percent" in yl

def _format_yaxis(ax, ylabel: str):
    """Formata eixo Y: 1 casa em %, milhares com espaço nos restantes."""
    if _is_percent_ylabel(ylabel):
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.1f"))
    else:
        ax.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, pos: f"{x:,.0f}".replace(",", " "))
        )

def _title(ax, text: str):
    ax.set_title(text, loc="left", color=FG_COL, fontsize=TITLE_SZ, pad=8)

# -------------------- gráficos --------------------
def grafico_evolucao(dados, titulo, ylabel, dado, tipo, ax):
    """
    dados: DataFrame com colunas 'Year', 'Continente' e <dado>
    titulo / ylabel: strings
    dado: nome da coluna a usar
    tipo: 'barra' ou 'linha'
    ax: Axes a reutilizar
    """
    _apply_dark_style(ax)

    continentes = dados["Continente"].unique()
    for continente in continentes:
        dfc = dados[dados["Continente"] == continente]
        cor = cores_continentes.get(continente, None)

        if tipo == "barra":
            ax.bar(dfc["Year"], dfc[dado], label=continente, color=cor, alpha=0.75)
        else:
            ax.plot(dfc["Year"], dfc[dado], label=continente, color=cor, marker="o", linewidth=2)

    _title(ax, titulo)
    ax.set_ylabel(ylabel, fontsize=LABEL_SZ)
    ax.set_xlabel(tr("climate_indicators.ano"), fontsize=LABEL_SZ)
    _format_yaxis(ax, ylabel)

    # legenda discreta (sem caixa)
    leg = ax.legend(title=tr("crescimento_populacional.continente"), frameon=False, loc="best")
    if leg:
        leg.set_draggable(True)
        leg.get_title().set_color(FG_COL)
        for txt in leg.get_texts():
            txt.set_color(FG_COL)

def grafico_mortalidade_stack(df_homens, df_mulheres):
    """
    Barras lado-a-lado por continente, em anos selecionados.
    Retorna figure (para quem precisar guardar/mostrar).
    """
    anos_selecionados = [1950, 1980, 2010, 2020]
    ultimo_ano = df_homens["Year"].max()
    if ultimo_ano not in anos_selecionados:
        anos_selecionados.append(ultimo_ano)

    df_h = df_homens[df_homens["Year"].isin(anos_selecionados)]
    df_m = df_mulheres[df_mulheres["Year"].isin(anos_selecionados)]

    continentes = df_h["Continente"].unique()
    anos = sorted(df_h["Year"].unique())

    largura = 0.35
    x = range(len(anos))
    fig, ax = plt.subplots(figsize=(10, 6), constrained_layout=True)

    _apply_dark_style(ax)

    for i, continente in enumerate(continentes):
        cor_base = cores_continentes.get(continente, "#999999")
        cor_homens = cor_base
        cor_mulheres = lighten_color(cor_base, 1.4)

        y_h = df_h[df_h["Continente"] == continente].set_index("Year").loc[anos]["MortalidadeEntre15e50Homens"]
        y_m = df_m[df_m["Continente"] == continente].set_index("Year").loc[anos]["MortalidadeEntre15e50Mulheres"]

        pos = [val + i * largura * 2 for val in x]
        ax.bar([p - largura/2 for p in pos], y_h, width=largura, label=f"{continente} — " + tr("graficos.homens"), color=cor_homens, alpha=0.9)
        ax.bar([p + largura/2 for p in pos], y_m, width=largura, label=f"{continente} — " + tr("graficos.mulheres"), color=cor_mulheres, alpha=0.9)

    ax.set_xticks([p + (len(continentes)-1) * largura for p in x])
    ax.set_xticklabels(anos, color=FG_COL)
    ylabel = tr("graficos.obitos_por_1_000")
    _title(ax, tr("graficos.mortalidade_entre_15_50_anos_por_sexo_e_continente"))
    ax.set_ylabel(ylabel, fontsize=LABEL_SZ)
    ax.set_xlabel(tr("climate_indicators.ano"), fontsize=LABEL_SZ)
    _format_yaxis(ax, ylabel)

    leg = ax.legend(
        title=tr("crescimento_populacional.continente"),
        frameon=False,
        loc="upper left",
        bbox_to_anchor=(1.0, 1.02),
    )
    if leg:
        leg.get_title().set_color(FG_COL)
        for txt in leg.get_texts():
            txt.set_color(FG_COL)

    return fig
