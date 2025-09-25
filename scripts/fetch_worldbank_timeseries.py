# scripts/fetch_worldbank_timeseries.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import csv, os, sys, time, gc
import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SEED_PATH    = PROJECT_ROOT / "data" / "countries_seed.csv"
OUT_PATH     = PROJECT_ROOT / "data" / "wb_timeseries.csv"

# Indicadores (WB):
# - SP.POP.TOTL: População total
# - EN.POP.DNST: Densidade (hab/km²)
# - SP.URB.TOTL.IN.ZS: % população urbana
INDICATORS = {
    "pop_total": "SP.POP.TOTL",
    "pop_density": "EN.POP.DNST",
    "urban_pct": "SP.URB.TOTL.IN.ZS",
}

UA = "GeoWB/1.0"
S = requests.Session()
S.headers.update({"User-Agent": UA})

def load_seed() -> pd.DataFrame:
    if not SEED_PATH.exists():
        print(f"❌ Falta {SEED_PATH}. Faz antes: python scripts/build_country_seed.py")
        sys.exit(1)
    df = pd.read_csv(SEED_PATH)
    if "iso3" not in df.columns:
        print("❌ countries_seed.csv sem coluna iso3.")
        sys.exit(2)
    return df

def fetch_indicator(iso3: str, ind: str) -> dict[int, float | None]:
    url = f"https://api.worldbank.org/v2/country/{iso3}/indicator/{ind}"
    params = {"format": "json", "per_page": 20000}
    out = {}
    for _ in range(2):  # tentativas
        try:
            r = S.get(url, params=params, timeout=15)
            r.raise_for_status()
            js = r.json()
            if not isinstance(js, list) or len(js) < 2:
                return out
            for row in js[1]:
                yr = row.get("date")
                val = row.get("value")
                try:
                    y = int(yr)
                except Exception:
                    continue
                out[y] = val
            return out
        except Exception:
            time.sleep(0.5)
    return out

def main():
    df = load_seed()
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    header = not OUT_PATH.exists()
    with OUT_PATH.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if header:
            w.writerow(["iso3","year","pop_total","pop_density","urban_pct"])
        written = 0
        for _, r in df.iterrows():
            iso3 = str(r["iso3"]).upper().strip()
            if not iso3:
                continue
            # buscar cada indicador e cruzar por ano
            data = {k: fetch_indicator(iso3, code) for k, code in INDICATORS.items()}
            years = sorted(set().union(*(set(d.keys()) for d in data.values())))
            for y in years:
                row = [iso3, y] + [data[k].get(y) for k in ("pop_total","pop_density","urban_pct")]
                w.writerow(row)
            f.flush(); os.fsync(f.fileno())
            written += 1
            gc.collect()
            print(f"[WB] {iso3}: {len(years)} anos")
    print(f"✔️ Escrevi/atualizei {OUT_PATH}")

if __name__ == "__main__":
    main()
