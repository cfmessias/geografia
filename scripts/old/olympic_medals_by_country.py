import requests
import pandas as pd
from bs4 import BeautifulSoup
from bs4 import BeautifulSoup

soup = BeautifulSoup(response.text, "lxml")
tables = pd.read_html(str(soup))

url = "https://en.wikipedia.org/wiki/All-time_Olympic_Games_medal_table"
headers = {'User-Agent': 'Mozilla/5.0'}

# Utilizando pandas.read_html com headers

response = requests.get(url, headers=headers)
dfs = pd.read_html(response.text)
def fetch_medal_table(url, table_index=0):
    """Fetch the medal table from a Wikipedia URL."""
    dfs = pd.read_html(url)
    # Sometimes the first table isn't the medal table; allow table index selection
    df = dfs[table_index]
    # Padroniza os nomes das colunas
    df.columns = [str(c).strip() for c in df.columns]
    return df

def clean_medal_table(df, country_col='NOC', drop_totals=True):
    # Remove linhas de totais e colunas desnecessárias
    if drop_totals:
        df = df[~df[country_col].str.contains('Total', na=False, case=False)]
    # Remove notas ou footnotes dos nomes dos países
    df[country_col] = df[country_col].str.replace(r"\[.*\]", "", regex=True).str.strip()
    # Mantém apenas as colunas relevantes
    medal_cols = [c for c in df.columns if any(w in c.lower() for w in ['gold', 'silver', 'bronze', 'total', country_col.lower()])]
    df = df[medal_cols]
    # Converte medalhas para int
    for col in ['Gold', 'Silver', 'Bronze', 'Total']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    return df

# URLs das tabelas de medalhas olímpicas (Verão e Inverno)
urls = [
    # Jogos Olímpicos de Verão
    ("https://en.wikipedia.org/wiki/All-time_Olympic_Games_medal_table", "NOC"),
    # Jogos Olímpicos de Inverno
    ("https://en.wikipedia.org/wiki/All-time_Olympic_Games_medal_table#Winter_Games", "NOC"),
]

all_medals = []

for url, country_col in urls:
    # Busca todas as tabelas da página (Verão e Inverno estão na mesma página)
    dfs = pd.read_html(url)
    # Tipicamente, verão é a primeira tabela, inverno é a segunda
    for i in range(2):
        try:
            df = dfs[i]
            if country_col not in df.columns:
                continue
            df = clean_medal_table(df, country_col)
            all_medals.append(df)
        except Exception as e:
            continue

# Juntar as medalhas de verão e inverno
df_medals = pd.concat(all_medals)
# Agrupa por país
df_final = df_medals.groupby('NOC', as_index=False, observed=False)[['Gold', 'Silver', 'Bronze', 'Total']].sum()
# Ordena por ouro, prata, bronze
df_final = df_final.sort_values(['Gold', 'Silver', 'Bronze'], ascending=False)

# Salva como CSV
df_final.to_csv("olympic_medals_by_country.csv", index=False, encoding='utf-8')
print("Arquivo 'olympic_medals_by_country.csv' gerado com sucesso!")
