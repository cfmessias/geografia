# scripts/fetch_country_languages.py
from __future__ import annotations
import csv, time, sys
from pathlib import Path
import requests
import pandas as pd

WDQS = "https://query.wikidata.org/sparql"
UA   = {"User-Agent": "Good2Know/1.0 (data collection for non-commercial app)"}
ROOT = Path(__file__).resolve().parents[1]
SEED = ROOT / "data" / "countries_seed.csv"
OUT1 = ROOT / "data" / "country_languages_official.csv"
OUT2 = ROOT / "data" / "country_languages_used.csv"

def _sparql(q: str) -> dict:
    r = requests.get(WDQS, params={"query": q, "format": "json"}, headers=UA, timeout=60)
    r.raise_for_status()
    return r.json()

def _qid_from_iso3(iso3: str) -> str | None:
    q = f"""
    SELECT ?c WHERE {{
      ?c wdt:P298 "{iso3}" .
    }} LIMIT 1
    """
    js = _sparql(q)
    b = js["results"]["bindings"]
    return b[0]["c"]["value"].rpartition("/")[-1] if b else None

def _languages_official(qid: str, lang_pref="pt,en"):
    q = f"""
    SELECT ?lang ?langLabel ?jurLabel ?partLabel ?start ?end WHERE {{
      VALUES ?country {{ wd:{qid} }}
      ?country p:P37 ?st .
      ?st ps:P37 ?lang .
      OPTIONAL {{ ?st pq:P1001 ?jur. }}
      OPTIONAL {{ ?st pq:P518  ?part. }}
      OPTIONAL {{ ?st pq:P580  ?start. }}
      OPTIONAL {{ ?st pq:P582  ?end.  }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang_pref}" . }}
    }}
    """
    return _sparql(q)["results"]["bindings"]

def _languages_used(qid: str, lang_pref="pt,en"):
    q = f"""
    SELECT ?lang ?langLabel WHERE {{
      VALUES ?country {{ wd:{qid} }}
      ?country wdt:P2936 ?lang .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang_pref}" . }}
    }}
    """
    return _sparql(q)["results"]["bindings"]

def main(only_iso3: list[str] | None = None):
    df = pd.read_csv(SEED,sep=";", dtype=str)
    iso_list = (only_iso3 or df["iso3"].dropna().unique().tolist())

    out1_rows, out2_rows = [], []
    for iso3 in iso_list:
        iso3 = iso3.strip().upper()
        try:
            qid = _qid_from_iso3(iso3)
            if not qid:
                print(f"[warn] sem QID para {iso3}")
                continue

            # oficiais (nacionais vs regionais)
            rows = _languages_official(qid)
            for r in rows:
                lang_qid = r["lang"]["value"].rpartition("/")[-1]
                lang_label = r.get("langLabel", {}).get("value", "")
                jur = r.get("jurLabel", {}).get("value", "")
                part = r.get("partLabel", {}).get("value", "")
                scope = "national" if not (jur or part) else "regional"
                start = r.get("start", {}).get("value", "")
                end   = r.get("end", {}).get("value", "")
                out1_rows.append({
                    "iso3": iso3, "qid_country": qid,
                    "lang_qid": lang_qid, "lang_label": lang_label,
                    "scope": scope, "region_label": jur or part,
                    "start_year": start[:4] if start else "",
                    "end_year": end[:4] if end else "",
                })

            # usadas
            rows = _languages_used(qid)
            for r in rows:
                lang_qid = r["lang"]["value"].rpartition("/")[-1]
                lang_label = r.get("langLabel", {}).get("value", "")
                out2_rows.append({
                    "iso3": iso3, "qid_country": qid,
                    "lang_qid": lang_qid, "lang_label": lang_label
                })

            print(f"[ok] {iso3}: {len([r for r in out1_rows if r['iso3']==iso3])} oficiais; "
                  f"{len([r for r in out2_rows if r['iso3']==iso3])} usadas")
            time.sleep(0.2)
        except Exception as e:
            print(f"[err] {iso3}: {e}")

    # escrever CSVs
    if out1_rows:
        OUT1.parent.mkdir(parents=True, exist_ok=True)
        with OUT1.open("w", newline="", encoding="utf-8") as f:
            wr = csv.DictWriter(f, fieldnames=list(out1_rows[0].keys()), delimiter=";", quoting=csv.QUOTE_MINIMAL)
            wr.writeheader(); wr.writerows(out1_rows)
        print(f"[save] {OUT1}")
    if out2_rows:
        with OUT2.open("w", newline="", encoding="utf-8") as f:
            wr = csv.DictWriter(f, fieldnames=list(out2_rows[0].keys()), delimiter=";", quoting=csv.QUOTE_MINIMAL)
            wr.writeheader(); wr.writerows(out2_rows)
        print(f"[save] {OUT2}")

if __name__ == "__main__":
    only = [a for a in sys.argv[1:] if len(a)==3] or None
    main(only)
