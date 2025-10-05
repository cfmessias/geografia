import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from services.i18n import t as tr
try:
    from services.i18n_boot import _ensure_lang_state
except ImportError:
    from services.i18n_boot import init_i18n_state as _ensure_lang_state

# Título da app
st.title(tr("crescimento_populacional.evolucao_populacional_por_continente"))
st.set_page_config(layout="wide")
_ensure_lang_state()
# Carregar os dados
df = pd.read_csv("data/demografia_mundial.csv", sep=";", encoding="utf-8", 
                 skipinitialspace=True, decimal=",", low_memory=False)

# Padronizar nomes de colunas
df.columns = [col.strip() for col in df.columns]

# Renomear coluna de regiões
df.rename(columns={"Region, subregion, country or area *": "Regiao"}, inplace=True)

# Eliminar espaços nos valores da coluna "Regiao"
df["Regiao"] = df["Regiao"].str.strip()

# Definir as regiões válidas (sem espaços)
regioes_validas = [
    "Africa", "Asia", "Europe",
    "Latin America and the Caribbean", "Northern America", "Oceania"
]
df = df[df["Regiao"].isin(regioes_validas)]

# Mapear para continentes
def mapear_continente(regiao):
    if regiao in ["Latin America and the Caribbean", "Northern America"]:
        return "América"
    elif regiao == "Africa":
        return "África"
    elif regiao == "Asia":
        return "Ásia"
    elif regiao == "Europe":
        return "Europa"
    elif regiao == "Oceania":
        return "Oceania"
    else:
        return None

df["Continente"] = df["Regiao"].apply(mapear_continente)

# Converter coluna de população
pop_col = "TotalPopulation,asof1January(thousands)"
df[pop_col] = pd.to_numeric(df[pop_col], errors="coerce")
# Agrupamento para população
df_grouped_pop = df.groupby(["Continente", "Year"], observed=False)[pop_col].sum().reset_index()

# Substituir vírgula por ponto e depois converter
densidade= "Population Density, as of 1 July (persons per square km)"
df[densidade] = df[densidade].astype(str).str.replace(',', '.', regex=False)
df[densidade] = pd.to_numeric(df[densidade], errors="coerce")
# Agrupamento para densidade (média faz mais sentido que soma)
df_grouped_densidade = df.groupby(["Continente", "Year"], observed=False)[densidade].mean().reset_index()


racio_genero="Population Sex Ratio, as of 1 July (males per 100 females)"
df[racio_genero] = df[racio_genero].astype(str).str.replace(',', '.', regex=False)
df[racio_genero] = pd.to_numeric(df[racio_genero], errors="coerce")
df_grouped_racio_genero = df.groupby(["Continente", "Year"], observed=False)[racio_genero].mean().reset_index()

crescimento_populacional = "PopulationGrowthRate(percentage)"
df[crescimento_populacional] = df[crescimento_populacional].astype(str).str.replace(',', '.', regex=False)
df[crescimento_populacional] = pd.to_numeric(df[crescimento_populacional], errors="coerce")
df_grouped_crescimento_populacional = df.groupby(["Continente", "Year"], observed=False)[crescimento_populacional].mean().reset_index()

# Primeiro gráfico (população)
fig, ax = plt.subplots(figsize=(8, 4.8))
for continente in df_grouped_pop["Continente"].unique():
    dados = df_grouped_pop[df_grouped_pop["Continente"] == continente]
    ax.plot(dados["Year"], dados[pop_col] / 1e3, label=continente)

ax.set_title(tr("crescimento_populacional.evolucao_da_populacao_por_continente_1980_ao_mais_recente"))
ax.set_xlabel(tr("climate_indicators.ano"))
ax.set_ylabel(tr("crescimento_populacional.populacao_milhoes"))
ax.legend(title=tr("crescimento_populacional.continente"))
ax.grid(True)


# Segundo gráfico (densidade)
fig1, ax = plt.subplots(figsize=(8, 4.8))
for continente in df_grouped_densidade["Continente"].unique():
    dados = df_grouped_densidade[df_grouped_densidade["Continente"] == continente]
    ax.plot(dados["Year"], dados[densidade], label=continente)  

ax.set_title(tr("crescimento_populacional.densidade_populacional_por_continente_habitantes_por_km2"))
ax.set_xlabel(tr("climate_indicators.ano"))
ax.set_ylabel(tr("crescimento_populacional.habitantes_por_km2"))
ax.legend(title=tr("crescimento_populacional.continente"))
ax.grid(True)


fig2, ax = plt.subplots(figsize=(8, 4.8))
for continente in df_grouped_racio_genero["Continente"].unique():
    dados = df_grouped_racio_genero[df_grouped_racio_genero["Continente"] == continente]
    ax.plot(dados["Year"], dados[racio_genero] , label=continente)

ax.set_title(tr("crescimento_populacional.racio_numero_de_homens_por_cada_100_mulheres_por_continente_1980_ao_mais_recente"))
ax.set_xlabel(tr("climate_indicators.ano"))
ax.set_ylabel(tr("crescimento_populacional.numero_de_homens"))
ax.legend(title=tr("crescimento_populacional.continente"))
ax.grid(True)


#Gráfico de linhas para crescimento populacional
fig3, ax = plt.subplots(figsize=(8, 4.8))
for continente in df_grouped_crescimento_populacional["Continente"].unique():
    dados = df_grouped_crescimento_populacional[df_grouped_crescimento_populacional["Continente"] == continente]
    ax.plot(dados["Year"], dados[crescimento_populacional], label=continente)

ax.set_title(tr("crescimento_populacional.crescimento_da_populacao_por_continente_1980_ao_mais_recente"))
ax.set_xlabel(tr("climate_indicators.ano"))
ax.set_ylabel(tr("crescimento_populacional.taxa_de_crescimento"))
ax.legend(title=tr("crescimento_populacional.continente"))
ax.grid(True)


cols = st.columns(2)
with cols[0]:
    st.pyplot(fig)
with cols[1]:
    st.pyplot(fig1)

cols1 = st.columns(2)
with cols1[0]:
    st.pyplot(fig2)
with cols1[1]:
    st.pyplot(fig3)

#============================================================================================================
# Gráfico de barras para densidade populacional
fig99, ax = plt.subplots(figsize=(15, 8))

# Preparar dados para barras
continentes = df_grouped_racio_genero["Continente"].unique()
anos_unicos = sorted(df_grouped_racio_genero["Year"].unique())

# Selecionar alguns anos específicos para não ficar muito carregado
# Você pode ajustar esta lista conforme necessário
anos_selecionados = [1950, 1970, 1990, 2010, 2020]  # Ajuste conforme seus dados
anos_disponiveis = [ano for ano in anos_selecionados if ano in anos_unicos]

# Se quiser todos os anos (atenção: pode ficar muito carregado):
# anos_disponiveis = anos_unicos[::5]  # Pega de 5 em 5 anos

n_continentes = len(continentes)
n_anos = len(anos_disponiveis)
largura_barra = 0.8 / n_continentes  # Largura de cada barra

# Cores para cada continente
cores = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD']

for i, continente in enumerate(continentes):
    dados_continente = df_grouped_racio_genero[df_grouped_racio_genero["Continente"] == continente]
    
    valores = []
    for ano in anos_disponiveis:
        valor = dados_continente[dados_continente["Year"] == ano][racio_genero]
        if not valor.empty:
            valores.append(valor.iloc[0])
        else:
            valores.append(0)  # ou None se preferir
    
    # Posição das barras para este continente
    posicoes = [x + i * largura_barra for x in range(n_anos)]
    
    ax.bar(posicoes, valores, largura_barra, 
           label=continente, color=cores[i % len(cores)], alpha=0.8)

# Configurar eixo X
ax.set_xlabel(tr("climate_indicators.ano"))
ax.set_ylabel(tr("crescimento_populacional.numero_de_homens"))
ax.set_title(tr("crescimento_populacional.racio_numero_de_homens_por_cada_100_mulheres_por_continente"))
ax.set_xticks([x + largura_barra * (n_continentes-1) / 2 for x in range(n_anos)])
ax.set_xticklabels(anos_disponiveis)
ax.legend(title=tr("crescimento_populacional.continente"))
ax.grid(True, alpha=0.3, axis='y')

plt.tight_layout()
#st.pyplot(fig99)
