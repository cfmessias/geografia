# -*- coding: utf-8 -*-
"""
Baixa séries de MIGRAÇÃO do World Bank e escreve CSVs em data/.

Saídas:
  - data/migration_timeseries.csv
      colunas: iso3, country, indicator, indicator_name, year, value
  - data/migration_latest.csv
      último valor por iso3+indicator

Indicadores (World Bank WDI):
  SM.POP.TOTL       → International migrant stock, total
  SM.POP.TOTL.ZS    → International migrant stock (% of population)
  SM.POP.NETM       → Net migration
  SM.POP.REFG       → Refugee population by country or territory of asylum
  BX.TRF.PWKR.CD.DT → Personal remittances, received (current US$)
  BX.TRF.PWKR.DT.GD.ZS → Personal remittances, received (% of GDP)

Fontes:
 - WDI docs dos indicadores. Ver metadados nas páginas de referência.
"""
from __future__ import annotations
from pathlib import Path
import time, random, sys, re
import requests
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = PROJECT_ROOT / "data"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Indicadores alvo (código -> nome legível)
INDICATORS = {
    "SM.POP.TOTL": "Migrant stock, total",                 # :contentReference[oaicite:2]{index=2}
    "SM.POP.TOTL.ZS": "Migrant stock, % population",       # :contentReference[oaicite:3]{index=3}
    "SM.POP.NETM": "Net migration",                        # :contentReference[oaicite:4]{index=4}
    "SM.POP.REFG": "Refugee population (asylum)",          # :contentReference[oaicite:5]{index=5}
    "BX.TRF.PWKR.CD.DT": "Remittances received (US$)",     # :contentReference[oaicite:6]{index=6}
    "BX.TRF.PWKR.DT.GD.ZS": "Remittances received (%GDP)", # :contentReference[oaicite:7]{index=7}
}

WB_BASE = "https://api.worldbank.org/v2/country/all/indicator/{code}"
UA = "GeoProject/1.0 (contact: you@example.com)"

S = requests.Session()
S.headers.update({"User-Agent": UA})

def _fetch_indicator(code: str) -> pd.DataFrame:
    """
    Vai à API JSON do WB e traz todas as observações (todas as páginas).
    """
    url = WB_BASE.format(code=code)
    params = {"format": "json", "per_page": 20000}
    out = []
    for attempt in range(5):
        try:
            r = S.get(url, params=params, timeout=40)
            r.raise_for_status()
            js = r.json()
            if not isinstance(js, list) or len(js) < 2:
                raise RuntimeError("JSON inesperado")
            meta, rows = js[0], js[1]
            # Se houver mais páginas (raro com per_page grande), percorre:
            pages = int(meta.get("pages", 1) or 1)
            out.extend(rows)
            for page in range(2, pages + 1):
                r = S.get(url, params={"format":"json","per_page":20000,"page":page}, timeout=40)
                r.raise_for_status()
                js2 = r.json()
                out.extend(js2[1])
            break
        except Exception as e:
            time.sleep(0.6 + random.random())
            if attempt == 4:
                print(f"[WB] falhou {code}: {e}", file=sys.stderr)
                return pd.DataFrame()

    # Filtrar só países (iso3 com 3 letras) — evita agregados regionais
    rows = []
    for z in out:
        iso3 = (z.get("countryiso3code") or "").upper()
        if not iso3 or not re.fullmatch(r"[A-Z]{3}", iso3):
            continue
        country = (z.get("country") or {}).get("value") or ""
        year = z.get("date")
        val = z.get("value")
        rows.append([iso3, country, code, INDICATORS.get(code, code), int(year), None if val is None else float(val)])
    df = pd.DataFrame(rows, columns=["iso3","country","indicator","indicator_name","year","value"])
    return df

def main() -> None:
    frames = []
    for code in INDICATORS:
        print(f"[WB] {code} …")
        df = _fetch_indicator(code)
        if not df.empty:
            frames.append(df)
    if not frames:
        print("❌ Sem dados do World Bank.")
        sys.exit(1)

    ts = pd.concat(frames, ignore_index=True)
    ts = ts.sort_values(["indicator","iso3","year"])
    ts.to_csv(OUT_DIR / "migration_timeseries.csv", index=False, encoding="utf-8")
    print(f"✔️ Escrevi {OUT_DIR / 'migration_timeseries.csv'} ({len(ts):,} linhas)")

    # último valor por iso3+indicator
    latest = (
        ts.dropna(subset=["value"])
          .sort_values(["iso3","indicator","year"])
          .groupby(["iso3","indicator","indicator_name","country"], as_index=False, observed=False)
          .tail(1)
          .sort_values(["indicator","value"], ascending=[True, False])
    )
    latest.to_csv(OUT_DIR / "migration_latest.csv", index=False, encoding="utf-8")
    print(f"✔️ Escrevi {OUT_DIR / 'migration_latest.csv'} ({len(latest):,} linhas)")

if __name__ == "__main__":
    main()

