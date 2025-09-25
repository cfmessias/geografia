import matplotlib.pyplot as plt
import matplotlib.colors as mc
import colorsys

cores_continentes = {
    "África": "#1f77b4",
    "América": "#ff7f0e",
    "Ásia": "#2ca02c",
    "Europa": "#d62728",
    "Oceania": "#9467bd"
}

def grafico_evolucao(dados, titulo, ylabel, dado, tipo,ax):
    continentes = dados["Continente"].unique()

    for continente in continentes:
        df_continente = dados[dados["Continente"] == continente]
        cor = cores_continentes.get(continente, None)
        
        if tipo == 'barra':
            ax.bar(df_continente["Year"], df_continente[dado], label=continente, color=cor, alpha=0.7)
        else:
            ax.plot(df_continente["Year"], df_continente[dado], label=continente, color=cor)

    ax.set_title(titulo)
    ax.set_ylabel(ylabel)
    ax.set_xlabel("Ano")
    ax.grid(True, linestyle="--", alpha=0.5)
    #legenda = ax.legend(title="Continente", loc="best")
    #legenda.get_frame().set_facecolor("none")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def grafico_mortalidade_stack(df_homens, df_mulheres):
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
    fig, ax = plt.subplots(figsize=(10, 6))

    for i, continente in enumerate(continentes):
        cor_base = cores_continentes.get(continente, "#999999")
        cor_homens = cor_base
        cor_mulheres = lighten_color(cor_base, 1.4)

        y_h = df_h[df_h["Continente"] == continente].set_index("Year").loc[anos]["MortalidadeEntre15e50Homens"]
        y_m = df_m[df_m["Continente"] == continente].set_index("Year").loc[anos]["MortalidadeEntre15e50Mulheres"]

        pos = [val + i * largura * 2 for val in x]
        ax.bar([p - largura/2 for p in pos], y_h, width=largura, label=f"{continente} (Homens)", color=cor_homens)
        ax.bar([p + largura/2 for p in pos], y_m, width=largura, label=f"{continente} (Mulheres)", color=cor_mulheres)

    ax.set_xticks([p + largura for p in x])
    ax.set_xticklabels(anos)
    ax.set_ylabel("Óbitos por 1.000")
    ax.set_title("Mortalidade entre 15–50 anos por sexo e continente")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    legenda = ax.legend(title="Continente", frameon=False, loc="upper left", bbox_to_anchor=(1.0, 1.0))
    legenda.get_frame().set_facecolor('none')
    return fig

def lighten_color(color, factor=1.2):

    try:
        c = mc.cnames[color]
    except:
        c = color
    rgb = mc.to_rgb(c)
    hls = colorsys.rgb_to_hls(*rgb)
    lighter_rgb = colorsys.hls_to_rgb(hls[0], min(1, hls[1] * factor), hls[2])
    return lighter_rgb
