# scripts/fetch_gastronomy_all.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse, csv, io, os, sys, textwrap, time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

# ---------------- Paths / Const ----------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
SEED_PATH    = DATA_DIR / "countries_seed.csv"
OUT_PATH     = DATA_DIR / "gastronomy_all.csv"

CSV_DELIM = ";"
TIMEOUT, RETRIES, SLEEP = 25, 3, 0.25
UA = "GeoGastronomy-Strict/1.0 (+https://github.com/)"
WDQS = "https://query.wikidata.org/sparql"

# Cabeçalho compatível com o teu app
HEAD = [
    "iso3","country","kind","item","item_qid","description","admin",
    "instance_of","image","wikipedia_pt","wikipedia_en","commons","website",
    "source","confidence"
]

# P31 whitelists / blacklists (Wikidata)
ALLOWED_DISH     = {"Q2095","Q746549","Q28803","Q431130","Q34770","Q3314483"}  # food/prato/pão/queijo/sopa/doce
ALLOWED_BEVERAGE = {"Q40050","Q154","Q282","Q44","Q925295"}                    # bebida/alcóolica/vinho/cerveja/licor
EXCLUDED_GENERIC = {
    # organizações/empresas/adegas/regiões DOC/VR/castas/cultivares/marcas/TV/DO
    "Q783794","Q43229","Q8054","Q82794","Q7540549","Q4886","Q5398426",
    "Q431289","Q5719543","Q4167836","Q15642541",
    # linguagens e variantes
    "Q315","Q33742","Q13217683","Q18130966","Q33999","Q591990","Q2537","Q13217676"
}

@dataclass
class Country:
    iso3: str
    pt: str
    en: str

# ---------------- CSV helpers ----------------
def ensure_header(path: Path, rebuild: bool=False) -> tuple[csv.writer, io.TextIOWrapper]:
    path.parent.mkdir(parents=True, exist_ok=True)
    if rebuild and path.exists():
        b = path.with_suffix(".bak")
        try: b.unlink(missing_ok=True)
        except Exception: pass
        path.rename(b)
    mode, need_header = ("a", True)
    if path.exists() and path.stat().st_size > 0:
        first = path.open("r", encoding="utf-8").readline()
        try: cols = next(csv.reader(io.StringIO(first), delimiter=CSV_DELIM))
        except Exception: cols = []
        cols = [c.strip() for c in cols]
        if len(cols)==len(HEAD) and all(a==b for a,b in zip(cols, HEAD)):
            need_header = False
        else:
            b = path.with_suffix(".bak")
            try: b.unlink(missing_ok=True)
            except Exception: pass
            path.rename(b)
            mode, need_header = "w", True
    else:
        mode, need_header = "w", True
    f = path.open(mode, newline="", encoding="utf-8")
    w = csv.writer(f, delimiter=CSV_DELIM, lineterminator="\n")
    if need_header:
        w.writerow(HEAD); f.flush(); os.fsync(f.fileno())
    return w, f

def existing_keys(path: Path) -> set[tuple[str,str]]:
    if not path.exists() or path.stat().st_size==0: return set()
    df = None
    for kw in ({"sep":CSV_DELIM,"encoding":"utf-8"},
               {"sep":CSV_DELIM,"encoding":"utf-8-sig"},
               {"engine":"python","sep":None,"encoding":"utf-8"}):
        try: df = pd.read_csv(path, **kw); break
        except Exception: df = None
    if df is None or df.empty: return set()
    df.columns = [str(c).replace("\ufeff","").strip() for c in df.columns]
    if "iso3" not in df.columns: return set()
    if "item_qid" not in df.columns: df["item_qid"] = ""
    if "item" not in df.columns: df["item"] = ""
    keys=set()
    for _,r in df.iterrows():
        iso3 = str(r["iso3"]).upper()
        qid  = str(r["item_qid"] or "")
        name = str(r["item"] or "").lower().strip()
        if iso3: keys.add((iso3, qid if qid else name))
    return keys

def write_rows(rows: Iterable[list[str]], w: csv.writer, f: io.TextIOWrapper):
    for row in rows:
        w.writerow([x if x is not None else "" for x in row])
        f.flush(); os.fsync(f.fileno())

# ---------------- Seed ----------------
def load_seed() -> list[Country]:
    if not SEED_PATH.exists():
        print(f"❌ Falta {SEED_PATH}"); sys.exit(1)
    df = None
    for kw in (
        {"engine":"python","sep":None,"encoding":"utf-8"},
        {"sep":";","encoding":"utf-8"},
        {"sep":",","encoding":"utf-8"},
        {"encoding":"utf-8"},
    ):
        try: df = pd.read_csv(SEED_PATH, **kw); break
        except Exception: df = None
    if df is None or df.empty:
        raise RuntimeError(f"Seed vazio/ilegível: {SEED_PATH}")
    # garantir colunas
    for c in ("iso3","name_pt","name_en"):
        if c not in df.columns: df[c] = ""
    df["iso3"]    = df["iso3"].astype(str).str.strip().str.upper()
    df["name_pt"] = df["name_pt"].astype(str).str.strip()
    df["name_en"] = df["name_en"].astype(str).str.strip()
    out=[]
    for _,r in df.iterrows():
        iso3 = (r["iso3"] or "").upper()
        if len(iso3)!=3: continue
        pt = r["name_pt"] or r["name_en"] or iso3
        en = r["name_en"] or r["name_pt"] or iso3
        out.append(Country(iso3=iso3, pt=pt, en=en))
    print(f"Seed: {len(out)} países")
    return out

# ---------------- HTTP ----------------
_session = requests.Session()
_session.headers.update({"User-Agent": UA})

def _get(url: str, **params) -> requests.Response | None:
    for i in range(RETRIES):
        try:
            r = _session.get(url, params=params or None, timeout=TIMEOUT)
            r.raise_for_status(); return r
        except Exception:
            time.sleep(SLEEP*(2**i))
    return None

# ---------------- SPARQL (estrito) ----------------
def q_wikidata_strict(iso3: str) -> str:
    ok_d = " ".join(f"wd:{q}" for q in ALLOWED_DISH)
    ok_b = " ".join(f"wd:{q}" for q in ALLOWED_BEVERAGE)
    bad  = " ".join(f"wd:{q}" for q in EXCLUDED_GENERIC)
    # apenas P495 = país e sem outro P495 → evita bebidas “partilhadas” tipo sangria
    # classifica prato/bebida com whitelists; exclui linguagens/empresas/etc.
    return textwrap.dedent(f"""
    SELECT DISTINCT ?item ?itemLabel ?desc ?adminLabel ?kind ?instLabel WHERE {{
      ?country wdt:P298 "{iso3}" .
      ?item wdt:P495 ?country .
      FILTER NOT EXISTS {{ ?item wdt:P495 ?other . FILTER(?other != ?country) }}

      # kind = beverage se tiver classe de bebidas; caso contrário dish se tiver classe de comida
      BIND(EXISTS {{ ?item wdt:P31/wdt:P279* ?kb . VALUES ?kb {{ {ok_b} }} }} AS ?isBev)
      BIND(EXISTS {{ ?item wdt:P31/wdt:P279* ?kd . VALUES ?kd {{ {ok_d} }} }} AS ?isDish)
      FILTER(?isBev || ?isDish)
      BIND(IF(?isBev, "beverage", "dish") AS ?kind)

      # exclusões (empresas, adegas, DOC/VR, castas, linguagens, etc.)
      FILTER NOT EXISTS {{ ?item wdt:P31/wdt:P279* ?bad . VALUES ?bad {{ {bad} }} }}

      OPTIONAL {{ ?item wdt:P31 ?inst . ?inst rdfs:label ?instLabel FILTER(LANG(?instLabel)="pt") }}
      OPTIONAL {{ ?item wdt:P131 ?adm . ?adm rdfs:label ?adminLabel FILTER(LANG(?adminLabel)="pt") }}
      OPTIONAL {{ ?item schema:description ?desc FILTER(LANG(?desc)="pt") }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
    }}
    """).strip()

def fetch_country_rows(c: Country) -> list[list[str]]:
    r = _get(WDQS, query=q_wikidata_strict(c.iso3), format="json")
    if not r: return []
    bset = r.json().get("results",{}).get("bindings",[])
    g = lambda b,k: b.get(k,{}).get("value","")
    rows=[]
    for b in bset:
        iri  = g(b,"item"); qid = iri.split("/")[-1] if iri else ""
        rows.append([
            c.iso3, c.pt, g(b,"kind") or "", g(b,"itemLabel") or "", qid,
            g(b,"desc") or "", g(b,"adminLabel") or "", g(b,"instLabel") or "",
            "", "", "", "", "", "wikidata_strict", "0.90"
        ])
    return rows

# ---------------- score/dedup ----------------
def score_row(row: list[str]) -> float:
    base = float(row[-1] or 0.5)
    if row[2]=="beverage": base += 0.05
    return base

# ---------------- main ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rebuild", action="store_true", help="recria CSV (faz backup .bak)")
    ap.add_argument("--only", type=str, default="", help="ISO3 separados por vírgulas (ex.: PRT,ESP)")
    args = ap.parse_args()

    countries = load_seed()
    if args.only:
        allow = {x.strip().upper() for x in args.only.split(",") if x.strip()}
        countries = [c for c in countries if c.iso3 in allow]
        print(f"Filtro --only ⇒ {len(countries)} países")

    w, f = ensure_header(OUT_PATH, rebuild=args.rebuild)
    keys = existing_keys(OUT_PATH)
    total = 0

    for c in countries:
        print(f"[{c.iso3}] {c.pt}")
        rows = fetch_country_rows(c)
        print(f"  obtidos: {len(rows)}")

        # dedup por (iso3, qid|nome) e escrita imediata
        best: dict[tuple[str,str], tuple[float, list[str]]] = {}
        for row in rows:
            key = (row[0], row[4] if row[4] else row[3].lower().strip())
            if key in keys: 
                continue
            sc = score_row(row)
            if key not in best or sc > best[key][0]:
                best[key] = (sc, row)

        if best:
            ordered = [r for _, r in sorted(best.values(), key=lambda t:(t[0], t[1][3].lower()))]
            write_rows(ordered, w, f)
            keys.update(best.keys())
            total += len(ordered)
            print(f"  gravadas: {len(ordered)} (total {total})")
        else:
            print("  (nada novo)")

        time.sleep(SLEEP)

    try: f.close()
    except Exception: pass
    print(f"✔️ Finalizado: +{total} linhas em {OUT_PATH}")

if __name__ == "__main__":
    main()
