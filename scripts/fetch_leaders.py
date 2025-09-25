# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import csv, os, sys, time
import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SEED_PATH    = PROJECT_ROOT / "data" / "countries_seed.csv"
OUT_CURR     = PROJECT_ROOT / "data" / "leaders_current.csv"
OUT_HIST     = PROJECT_ROOT / "data" / "leaders_history.csv"

WDQS = "https://query.wikidata.org/sparql"
UA   = "GeoLeaders/1.0 (+streamlit demo)"
TIMEOUT = 25
SLEEP = 0.25

HEAD_CURR = ["iso3","country","role","person","person_qid","start","end"]
HEAD_HIST = HEAD_CURR

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

def _q_role(iso3: str, prop: str) -> str:
    # prop = "P6" (head of government) ou "P35" (head of state)
    return f"""
SELECT ?person ?personLabel ?start ?end WHERE {{
  ?country wdt:P298 "{iso3}" .
  ?country p:{prop} ?st .
  ?st ps:{prop} ?person .
  OPTIONAL {{ ?st pq:P580 ?start }}
  OPTIONAL {{ ?st pq:P582 ?end }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
}}
"""

def main():
    seed = _seed()
    OUT_CURR.parent.mkdir(parents=True, exist_ok=True)
    wc = not OUT_CURR.exists()
    wh = not OUT_HIST.exists()
    fc = OUT_CURR.open("a", newline="", encoding="utf-8")
    fh = OUT_HIST.open("a", newline="", encoding="utf-8")
    wcsv = csv.writer(fc); hcsv = csv.writer(fh)
    if wc: wcsv.writerow(HEAD_CURR)
    if wh: hcsv.writerow(HEAD_HIST)

    for _, r in seed.iterrows():
        iso3 = str(r["iso3"]).upper()
        country = r.get("name_pt") or r.get("name_en") or iso3
        print(f"[leaders] {iso3} {country}")

        all_rows = []
        for role, prop in (("head_of_government","P6"), ("head_of_state","P35")):
            js = _sparql(_q_role(iso3, prop))
            if not js: 
                print(f"  … falhou {prop}"); continue
            for b in js.get("results",{}).get("bindings",[]):
                g = lambda k: b.get(k,{}).get("value")
                qid = (g("person") or "").split("/")[-1]
                person = g("personLabel") or ""
                start = g("start") or ""
                end   = g("end") or ""
                all_rows.append((role, person, qid, start, end))
                hcsv.writerow([iso3, country, role, person, qid, start, end])
                fh.flush(); os.fsync(fh.fileno())

        # escolher atual: sem 'end' ou com start mais recente
        for role in ("head_of_government","head_of_state"):
            subset = [r for r in all_rows if r[0]==role]
            if not subset: continue
            current = None
            open_terms = [r for r in subset if not r[4]]
            if open_terms:
                # se vários sem fim, escolher o de start mais recente
                current = max(open_terms, key=lambda t: t[3] or "")
            else:
                current = max(subset, key=lambda t: t[3] or "")
            wcsv.writerow([iso3, country, role, current[1], current[2], current[3], current[4]])
            fc.flush(); os.fsync(fc.fileno())
        time.sleep(SLEEP)

    fc.close(); fh.close()
    print(f"✔️ Atualizado {OUT_CURR} e {OUT_HIST}")

if __name__ == "__main__":
    main()
