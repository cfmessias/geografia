# scripts/build_conflict_types.py
from __future__ import annotations
import csv, sys, time
from pathlib import Path
from typing import Dict, List, Set, Tuple
import requests

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
OUT_CSV  = DATA_DIR / "conflict_types.csv"  # type_qid;type_label;root_qid;root_label

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
UA = "GeoMundi-ConfTypes/1.0 (+cfmessias@gmail.com)"

# "Raízes" (podes ajustar/expandir aqui):
ROOTS: List[Tuple[str, str]] = [
    ("Q198",    "war"),
    ("Q180684", "military conflict"),
    ("Q178561", "rebellion"),
    ("Q645883", "civil conflict"),
    ("Q350604", "battle"),
    ("Q65171",  "world war"),  # incluída explicitamente para não depender da hierarquia
]

def run(q: str) -> Dict:
    for i in range(4):
        try:
            r = requests.post(SPARQL_ENDPOINT, data={"query": q}, headers={"Accept":"application/sparql-results+json","User-Agent":UA}, timeout=90)
            if r.status_code == 200:
                return r.json()
            sys.stderr.write(f"[warn] HTTP {r.status_code}: {r.text[:200]}\n")
        except Exception as e:
            sys.stderr.write(f"[err] {e}\n")
        time.sleep(2*(i+1))
    raise RuntimeError("Falha WDQS")

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    rows: List[Tuple[str,str,str,str]] = []
    seen: Set[str] = set()
    for root_qid, root_label in ROOTS:
        q = f"""
SELECT DISTINCT ?t ?tLabel WHERE {{
  VALUES ?root {{ wd:{root_qid} }}
  ?t wdt:P279* ?root .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
}}
"""
        js = run(q)
        for b in js.get("results", {}).get("bindings", []):
            t_uri = b["t"]["value"]
            t_qid = t_uri.rsplit("/", 1)[-1]
            t_lbl = b.get("tLabel", {}).get("value", "")
            key = (t_qid, root_qid)
            if key in seen:
                continue
            seen.add(key)
            rows.append((t_qid, t_lbl, root_qid, root_label))

    # grava
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["type_qid","type_label","root_qid","root_label"])
        w.writerows(rows)
    print(f"✔️ {OUT_CSV} ({len(rows)} linhas)")

if __name__ == "__main__":
    main()
