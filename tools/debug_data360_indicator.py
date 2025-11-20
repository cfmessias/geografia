# tools/debug_data360_indicator.py
import pandas as pd

def debug_indicator(iso3: str, indicator: str, year_min=2000, year_max=2024):
    iso3 = iso3.upper()
    file_id = f"WB_WDI_{indicator.replace('.', '_')}"
    url = f"https://data360files.worldbank.org/data360-data/data/WB_WDI/{file_id}.csv"

    print(f"=== {iso3} | {indicator} | {url}")
    try:
        df = pd.read_csv(url)
    except Exception as e:
        print("ERRO a ler CSV:", e)
        return

    print("Colunas:", list(df.columns)[:10])

    df["REF_AREA"] = df["REF_AREA"].astype(str).str.upper().str.strip()
    df["TIME_PERIOD_int"] = pd.to_numeric(df["TIME_PERIOD"], errors="coerce")
    df = df[(df["REF_AREA"] == iso3) &
            (df["TIME_PERIOD_int"] >= year_min) &
            (df["TIME_PERIOD_int"] <= year_max)]

    print(f"Linhas para {iso3} entre {year_min}-{year_max}: {len(df)}")
    print(df[["REF_AREA", "TIME_PERIOD_int", "OBS_VALUE"]].head(10))

if __name__ == "__main__":
    debug_indicator("DEU", "NY.GDP.MKTP.KD.ZG")   # Crescimento PIB
    debug_indicator("DEU", "NY.GDP.PCAP.CD")      # PIB per capita
    debug_indicator("DEU", "SI.POV.DDAY")         # Pobreza 2.15$
