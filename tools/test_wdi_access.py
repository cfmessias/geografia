# tools/test_wdi_access.py
# -*- coding: utf-8 -*-
"""
Testes de acesso à World Bank Data360 (WDI).

- TESTE 1: /data360/indicators  → confirma acesso ao dataset WB_WDI
- TESTE 2: CSV por indicador    → lê o CSV de um indicador (SP.POP.TOTL, NY.GDP.MKTP.CD)
                                   a partir de data360files.worldbank.org,
                                   filtra por país/anos e mostra algumas linhas.

NOTA: o endpoint /data360/data é genérico e devolve o dataset inteiro,
      não é prático para "país + indicador". O caminho simples é usar os CSVs.
"""

import sys
import json
import requests
import pandas as pd

BASE_API = "https://data360api.worldbank.org"
BASE_FILES = "https://data360files.worldbank.org/data360-data/data/WB_WDI"

DATASET_ID = "WB_WDI"
DATABASE_ID = "WB_WDI"  # usado apenas no /data360/indicators


def pretty(obj, max_len: int = 600) -> str:
    """Converte obj em JSON indentado, truncado a max_len chars."""
    try:
        s = json.dumps(obj, indent=2, ensure_ascii=False)
    except Exception:
        s = str(obj)
    if len(s) > max_len:
        return s[:max_len] + " ..."
    return s


def test_indicators():
    """
    Teste ao endpoint /data360/indicators.
    Devolve lista/meta de indicadores para o dataset WB_WDI.
    """
    url = f"{BASE_API}/data360/indicators"
    params = {
        "datasetId": DATASET_ID,
        "databaseId": DATABASE_ID,
        "per_page": 20,
    }

    print("=" * 80)
    print("TESTE 1 – GET /data360/indicators")
    print("URL:", url)
    print("Params:", params)

    try:
        r = requests.get(url, params=params, timeout=30)
    except Exception as e:
        print("ERRO de rede ao fazer GET:", repr(e))
        return

    print("HTTP status:", r.status_code)

    try:
        js = r.json()
        print("JSON (truncado):")
        print(pretty(js))
    except Exception as e:
        print("ERRO ao fazer .json():", repr(e))
        print("Corpo bruto (primeiros 400 chars):")
        print(r.text[:400].replace("\n", " "))


def _indicator_to_file_id(indicator_wdi: str) -> str:
    """
    Converte código WDI clássico (ex: SP.POP.TOTL)
    para ID usado nos ficheiros Data360 (ex: WB_WDI_SP_POP_TOTL).
    """
    return f"WB_WDI_{indicator_wdi.replace('.', '_')}"


def test_csv_indicator(iso3: str, indicator_wdi: str, year_min: int = 2000, year_max: int = 2024):
    """
    Teste ao ficheiro CSV de um indicador WDI:

    - Constrói o URL do CSV em data360files.worldbank.org
    - Lê o CSV com pandas
    - Filtra por REF_AREA (iso3) e TIME_PERIOD (ano)
    - Mostra as primeiras linhas
    """
    file_id = _indicator_to_file_id(indicator_wdi)
    url = f"{BASE_FILES}/{file_id}.csv"

    print("=" * 80)
    print(f"TESTE CSV – {iso3} | {indicator_wdi} → {file_id}")
    print("URL CSV:", url)

    try:
        # ler CSV diretamente da URL
        df = pd.read_csv(url)
    except Exception as e:
        print("ERRO a ler CSV:", repr(e))
        return

    # Ver algumas colunas relevantes (se existirem)
    expected_cols = ["REF_AREA", "TIME_PERIOD", "OBS_VALUE", "INDICATOR"]
    print("Colunas disponíveis:", list(df.columns)[:20])

    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        print("⚠️ Falta(m) colunas esperadas no CSV:", missing)
        print(df.head())
        return

    # Filtrar por país e anos
    df_country = df[df["REF_AREA"] == iso3.upper()].copy()

    # TIME_PERIOD → int (algumas linhas podem não ser anos válidos)
    df_country["TIME_PERIOD_int"] = pd.to_numeric(df_country["TIME_PERIOD"], errors="coerce")
    df_country = df_country.dropna(subset=["TIME_PERIOD_int"])
    df_country["TIME_PERIOD_int"] = df_country["TIME_PERIOD_int"].astype(int)

    mask_years = (df_country["TIME_PERIOD_int"] >= year_min) & (df_country["TIME_PERIOD_int"] <= year_max)
    df_filtered = df_country.loc[mask_years].copy()

    print(f"Total linhas no CSV: {len(df)}")
    print(f"Linhas para REF_AREA={iso3}: {len(df_country)}")
    print(f"Linhas para {iso3} entre {year_min}-{year_max}: {len(df_filtered)}")

    if df_filtered.empty:
        print("⚠️ Sem dados para este país/intervalo de anos.")
        return

    # Mostrar as primeiras 10 linhas relevantes (ano, valor)
    out = df_filtered[["REF_AREA", "TIME_PERIOD_int", "OBS_VALUE", "INDICATOR"]].sort_values(
        "TIME_PERIOD_int"
    )
    print("Amostra de linhas filtradas:")
    print(out.head(10).to_string(index=False))


def main():
    # 1) Teste de conectividade e lista de indicadores
    test_indicators()

    # 2) Testes CSV por indicador/país
    tests = [
        ("PRT", "SP.POP.TOTL"),      # população total Portugal
        ("PRT", "NY.GDP.MKTP.CD"),   # PIB nominal Portugal
        ("DEU", "SP.POP.TOTL"),      # população total Alemanha
    ]

    for iso3, ind in tests:
        test_csv_indicator(iso3, ind)


if __name__ == "__main__":
    main()
