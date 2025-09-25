# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import csv, os, sys, time
import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SEED_PATH    = PROJECT_ROOT / "data" / "countries_seed.csv"
OUT_PATH     = PROJECT_ROOT / "data" / "unesco_all.csv"

WDQS = "https://query.wikidata.org/sparql"
UA   = "GeoUNESCO/1.0 (+streamlit demo)"
TIMEOUT = 25
SLEEP = 0.25
HEAD = ["iso3","country","site","site_qid","type","year","lat","lon"]

def _seed():
    if not SEED_PATH.exists():
        print(f"❌ Falta {SEED_PATH}. Corre scripts/build_country_seed.py")
        sys.exit(1)
    return pd.read_csv(SEED_PATH)

def _sparql(q):
    for i in range(3):
        try:
            r = requests.get(WDQS, params={"query": q, "format":"json"},
                             headers={"User-Agent": UA}, timeout=TIMEOUT)
            r.raise_for_status(); return r.json()
        except Exception:
            time.sleep(SLEEP * (2**i))
    return None

def _q(iso3: str) -> str:
    # Q9259 = World Heritage Site
    return f"""
SELECT ?site ?siteLabel ?typLabel ?y ?lat ?lon WHERE {{
  ?country wdt:P298 "{iso3}" .
  ?site wdt:P1435 wd:Q9259 .
  ?site wdt:P17 ?country .
  OPTIONAL {{
    ?site p:P1435 ?st .
    ?st pq:P580 ?start .
    BIND(YEAR(?start) AS ?y)
  }}
  OPTIONAL {{
    ?site p:P625/psv:P625 ?v .
    ?v wikibase:geoLatitude ?lat; wikibase:geoLongitude ?lon .
  }}
  OPTIONAL {{ ?site wdt:P31 ?typ . ?typ rdfs:label ?typLabel FILTER(LANG(?typLabel)='pt') }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
}}
"""

def main():
    seed = _seed()
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    write_head = not OUT_PATH.exists()
    with OUT_PATH.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f); 
        if write_head: w.writerow(HEAD)

        for _, r in seed.iterrows():
            iso3 = str(r["iso3"]).upper()
            country = r.get("name_pt") or r.get("name_en") or iso3
            print(f"[unesco] {iso3} {country}")
            js = _sparql(_q(iso3))
            if not js: 
                print("  … falhou SPARQL"); continue
            for b in js.get("results",{}).get("bindings",[]):
                get = lambda k: b.get(k,{}).get("value")
                site_qid = (get("site") or "").split("/")[-1]
                site = get("siteLabel") or ""
                typ = get("typLabel") or ""
                y = get("y"); y = int(y) if y else None
                lat = get("lat"); lon = get("lon")
                lat = float(lat) if lat else None
                lon = float(lon) if lon else None
                w.writerow([iso3, country, site, site_qid, typ, y, lat, lon])
                f.flush(); os.fsync(f.fileno())
            time.sleep(SLEEP)

    print(f"✔️ Atualizado {OUT_PATH}")

if __name__ == "__main__":
    main()
