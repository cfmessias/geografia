import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

# Configurar página para layout wide
st.set_page_config(layout="wide", page_title="Demografia Mundial")

st.title("📈 Evolução Populacional por Continente")

df = pd.read_csv("demografia_mundial.csv", sep=";", encoding="utf-8", 
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
df_grouped_pop = df.groupby(["Continente", "Year"])[pop_col].sum().reset_index()

# Substituir vírgula por ponto e depois converter
densidade= "Population Density, as of 1 July (persons per square km)"
df[densidade] = df[densidade].astype(str).str.replace(',', '.', regex=False)
df[densidade] = pd.to_numeric(df[densidade], errors="coerce")
# Agrupamento para densidade (média faz mais sentido que soma)
df_grouped_densidade = df.groupby(["Continente", "Year"])[densidade].mean().reset_index()


racio_genero="Population Sex Ratio, as of 1 July (males per 100 females)"
df[racio_genero] = df[racio_genero].astype(str).str.replace(',', '.', regex=False)
df[racio_genero] = pd.to_numeric(df[racio_genero], errors="coerce")
df_grouped_racio_genero = df.groupby(["Continente", "Year"])[racio_genero].mean().reset_index()

crescimento_populacional = "PopulationGrowthRate(percentage)"
df[crescimento_populacional] = df[crescimento_populacional].astype(str).str.replace(',', '.', regex=False)
df[crescimento_populacional] = pd.to_numeric(df[crescimento_populacional], errors="coerce")
df_grouped_crescimento_populacional = df.groupby(["Continente", "Year"])[crescimento_populacional].mean().reset_index()

# Seus gráficos...
fig1, ax1 = plt.subplots(figsize=(8, 4.8))  # Tamanho maior
for continente in df_grouped_pop["Continente"].unique():
    dados = df_grouped_pop[df_grouped_pop["Continente"] == continente]
    ax1.plot(dados["Year"], dados[pop_col] / 1e3, label=continente)

ax1.set_title("Evolução da População por Continente (1980 ao mais recente)")
ax1.set_xlabel("Ano")
ax1.set_ylabel("População (milhões)")
ax1.legend(title="Continente")
ax1.grid(True)

fig2, ax2 = plt.subplots(figsize=(8, 4.8))  # Tamanho maior
for continente in df_grouped_densidade["Continente"].unique():
    dados = df_grouped_densidade[df_grouped_densidade["Continente"] == continente]
    ax2.plot(dados["Year"], dados[densidade], label=continente)  

ax2.set_title("Densidade Populacional por Continente (habitantes por km²)")
ax2.set_xlabel("Ano")
ax2.set_ylabel("Habitantes por km²")
ax2.legend(title="Continente")
ax2.grid(True)

fig3, ax3 = plt.subplots(figsize=(8, 4.8))  # Tamanho maior
for continente in df_grouped_racio_genero["Continente"].unique():
    dados = df_grouped_racio_genero[df_grouped_racio_genero["Continente"] == continente]
    ax3.plot(dados["Year"], dados[racio_genero] , label=continente)

ax3.set_title("Racio - Número de homens por cada 100 mulheres por Continente (1980 ao mais recente)")
ax3.set_xlabel("Ano")
ax3.set_ylabel("Número de homens")
ax3.legend(title="Continente")
ax3.grid(True)


fig4, ax4 = plt.subplots(figsize=(8, 4.8))  # Tamanho maior
for continente in df_grouped_crescimento_populacional["Continente"].unique():
    dados = df_grouped_crescimento_populacional[df_grouped_crescimento_populacional["Continente"] == continente]
    ax4.plot(dados["Year"], dados[crescimento_populacional], label=continente)

ax4.set_title("Crescimento da População por Continente (1980 ao mais recente)")
ax4.set_xlabel("Ano")
ax4.set_ylabel("Taxa de crescimento")
ax4.legend(title="Continente")
ax4.grid(True)


# Layout em colunas
col1, col2 = st.columns(2, gap="large")  # gap="large" para mais espaço entre colunas

with col1:
    fig1.tight_layout()  # Ajusta automaticamente o espaçamento
    st.pyplot(fig1, use_container_width=True)
    
with col2:
    fig2.tight_layout()  # Ajusta automaticamente o espaçamento
    st.pyplot(fig2, use_container_width=True)

# Segunda linha de gráficos
col3, col4 = st.columns(2, gap="large")

with col3:
    fig3.tight_layout()  # Ajusta automaticamente o espaçamento
    st.pyplot(fig3, use_container_width=True)
    
with col4:
    fig4.tight_layout()  # Ajusta automaticamente o espaçamento
    st.pyplot(fig4, use_container_width=True)