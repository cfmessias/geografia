import requests, pandas as pd

country_iso3 = "PRT"  # Portugal

# --- World Bank: população & densidade ---
wb_indicators = {
    "SP.POP.TOTL": "population",
    "EN.POP.DNST": "pop_density",
}
frames = []
for code, name in wb_indicators.items():
    url = f"https://api.worldbank.org/v2/country/{country_iso3}/indicator/{code}?format=json&per_page=60"
    data = requests.get(url, timeout=30).json()[1]
    df = pd.DataFrame([{"year": int(d["date"]), name: d["value"]} for d in data])
    frames.append(df)
wb = frames[0].merge(frames[1], on="year")

# --- Wikidata: ano de fundação (P571) e cidades > 100k hab. ---
sparql = """
SELECT ?countryLabel ?inception ?cityLabel ?pop WHERE {
  VALUES ?country { wd:Q45 }  # Portugal
  OPTIONAL { ?country wdt:P571 ?inception. }
  OPTIONAL {
    ?city wdt:P17 ?country ;
          wdt:P31/wdt:P279* wd:Q515 ;   # instancia de cidade (ou subclasse)
          wdt:P1082 ?pop .
    FILTER(?pop > 100000)
  }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "pt,en". }
}
ORDER BY DESC(?pop)
"""
resp = requests.get(
    "https://query.wikidata.org/sparql",
    params={"query": sparql, "format": "json"},
    headers={"User-Agent": "PT-Demo/1.0"}
).json()

rows = [
    {
        "country": b["countryLabel"]["value"],
        "inception": b.get("inception", {}).get("value"),
        "city": b.get("cityLabel", {}).get("value"),
        "city_population": int(float(b["pop"]["value"])) if "pop" in b else None,
    }
    for b in resp["results"]["bindings"]
]
wk = pd.DataFrame(rows)

# Resultado resumido
print(wb.sort_values("year", ascending=False).head(3))
print("\nAno de fundação (Wikidata):", wk["inception"].dropna().unique()[:1])
print("\nTop cidades (>=100k hab):")
print(wk.dropna(subset=["city"]).drop_duplicates(subset=["city"]).head(10))
