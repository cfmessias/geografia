import pandas as pd
import json

# Carregar ficheiro
df = pd.read_csv("data/demografia_mundial.csv", sep=";")

coluna = "Region, subregion, country or area"
valores = df[coluna].dropna().tolist()

# Definir lista de continentes "raiz"
continentes = [
    "Africa", "Asia", "Europe", "Oceania",
    "Northern America", "Central America", "South America", "Caribbean"
]

hierarquia = {}
nivel_atual = {"continente": None, "subregiao": None}

for nome in valores:
    if nome in continentes:
        # Novo continente
        nivel_atual["continente"] = nome
        hierarquia[nome] = {}
        nivel_atual["subregiao"] = None
    elif nivel_atual["continente"] and nome not in hierarquia[nivel_atual["continente"]]:
        # Nova sub-região
        nivel_atual["subregiao"] = nome
        hierarquia[nivel_atual["continente"]][nome] = []
    else:
        # País
        cont = nivel_atual["continente"]
        sub = nivel_atual["subregiao"]
        if cont and sub:
            hierarquia[cont][sub].append(nome)
        else:
            print(f"⚠️ Ignorado (sem contexto): {nome}")

# Exportar
with open("data/hierarquia_regional.json", "w", encoding="utf-8") as f:
    json.dump(hierarquia, f, ensure_ascii=False, indent=2)

print("✅ Hierarquia construída com sucesso.")
