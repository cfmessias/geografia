# scripts/fetch_conflicts_countries_from_participants.py
# -*- coding: utf-8 -*-
"""
Gera data/conflicts_countries.csv a partir de data/conflicts_participants.csv:
  1) extrai conflict_qid distintos
  2) busca PAÍSES por conflito em 4 vias separadas (todas ativas por defeito):
       - P17   : ?conflict wdt:P17 ?country
       - P276  : ?conflict wdt:P276 ?place . ?place wdt:P17 ?country
       - P131+ : ?conflict wdt:P131+ ?place . ?place wdt:P17 ?country
       - P710C : ?conflict wdt:P710 ?country (onde ?country é (country|sovereign state))
     Em TODAS as vias: OPTIONAL { ?country wdt:P298 ?iso3 }  # ISO3 direto da Wikidata
  3) mapped_iso3 = iso3_query or forms_all.csv or countries_seed.csv

Saída (sep=';'):
  conflict_qid;country_qid;point_in_time;source;mapped_iso3
"""

from __future__ import annotations
from pathlib import Path
from typing import List, Tuple
import argparse, csv, os, random, re, sys, time
import requests

# ---------------- Paths ----------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR     = PROJECT_ROOT / "data"
IN_PARTS     = DATA_DIR / "conflicts_participants.csv"
FORMS_CSV    = DATA_DIR / "forms_all.csv"
SEED_CSV     = DATA_DIR / "countries_seed.csv"
OUT_CSV      = DATA_DIR / "conflicts_countries.csv"
OUT_WORK     = OUT_CSV.with_suffix(".tmp.csv")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ---------------- Config ----------------
ENDPOINT   = "https://query.wikidata.org/sparql"
UA         = "GeoMundi/1.0 (countries-from-parts; contact: you@example.com)"
TIMEOUT_S  = 60
SLEEP_BATCH= 0.25

HEADER = ["conflict_qid","country_qid","point_in_time","source","mapped_iso3"]
_QID_RE = re.compile(r"Q\d+$")

# ---------------- Utils ----------------
def _unquote(lit: str) -> str:
    # remove aspas exteriores, se existirem
    s = (lit or "").strip()
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        return s[1:-1]
    return s

def _clean_pit(pit: str) -> str:
    # aceita "YYYY-MM-DD", ou "…T…Z"^^xsd:dateTime -> devolve "YYYY-MM-DD"
    s = _unquote(pit)
    if "T" in s:
        s = s.split("T", 1)[0]
    return s

def qid(x: str) -> str:
    if not x: return ""
    s = str(x).strip()
    if s.startswith("<") and s.endswith(">"): s = s[1:-1]
    if "/" in s: s = s.rsplit("/",1)[-1]
    s = s.strip('>"\' \t\r\n')
    return s if _QID_RE.match(s) else ""

def http_post(query: str, accept: str):
    headers = {"User-Agent": UA, "Accept": accept, "Connection": "close"}
    return requests.post(ENDPOINT, data={"query": query}, headers=headers, timeout=TIMEOUT_S)

def fetch_tsv_or_json(query: str):
    r = http_post(query, "text/tab-separated-values; charset=utf-8")
    if r.status_code == 200 and r.text:
        return r.text
    rj = http_post(query, "application/sparql-results+json; charset=utf-8")
    if rj.status_code == 200:
        return rj.json()
    raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")

def parse_pairs(payload) -> List[Tuple[str, str, str, str]]:
    """
    Retorna [(conflict_qid, country_qid, pit, iso3_query)] de TSV ou JSON.
    - pit sai em 'YYYY-MM-DD' (se vier com T/...Z, é truncado)
    - iso3 sai sem aspas e em maiúsculas
    """
    # JSON
    if isinstance(payload, dict):
        out = []
        for b in payload.get("results", {}).get("bindings", []):
            c   = qid(b.get("conflict", {}).get("value", ""))
            k   = qid(b.get("country",  {}).get("value", ""))
            pit = _clean_pit(b.get("pit", {}).get("value", ""))
            iso = _unquote(b.get("iso3", {}).get("value", "")).upper()
            if c or k or pit or iso:
                out.append((c, k, pit, iso))
        return out

    # TSV
    lines = payload.splitlines()
    if not lines:
        return []

    # header normalizado
    raw_header = lines[0].lstrip("\ufeff").strip()
    header = [h.strip().strip("?") for h in raw_header.split("\t")]

    def idx(name: str, default_idx: int) -> int:
        try:
            return header.index(name)
        except ValueError:
            return default_idx

    i_conflict = idx("conflict", 0)
    i_country  = idx("country",  1)
    i_pit      = idx("pit",      2)
    i_iso3     = header.index("iso3") if "iso3" in header else -1

    def safe(cells: List[str], i: int) -> str:
        return cells[i] if 0 <= i < len(cells) else ""

    out: List[Tuple[str, str, str, str]] = []
    for line in lines[1:]:
        if not line.strip():
            continue
        cells = line.split("\t")
        c_raw   = safe(cells, i_conflict)
        k_raw   = safe(cells, i_country)
        pit_raw = safe(cells, i_pit)
        iso_raw = safe(cells, i_iso3) if i_iso3 >= 0 else ""

        c   = qid(c_raw)
        k   = qid(k_raw)
        pit = _clean_pit(pit_raw)           # <-- LIMPO AQUI
        iso = _unquote(iso_raw).upper()      # <-- LIMPO AQUI

        if c or k or pit or iso:
            out.append((c, k, pit, iso))
    return out


# ---------------- Seed ISO3 ----------------
def load_forms_map(forms_csv: Path) -> dict[str,str]:
    """Aceita colunas ['qid','iso3'] OU ['form_qid','iso3'] (sep=';')."""
    m = {}
    try:
        import pandas as pd
        if forms_csv.exists():
            df = pd.read_csv(forms_csv, sep=";", dtype=str, encoding="utf-8-sig").fillna("")
            cols = {c.lower(): c for c in df.columns}
            qcol = cols.get("qid") or cols.get("form_qid")
            icol = cols.get("iso3")
            if qcol and icol:
                for _, r in df.iterrows():
                    q = str(r[qcol]).strip()
                    i = str(r[icol]).strip().upper()
                    if q and i:
                        m[q] = i
    except Exception:
        pass
    return m

def load_seed_iso3(seed_csv: Path) -> dict[str,str]:
    m = {}
    try:
        import pandas as pd
        if seed_csv.exists():
            df = pd.read_csv(seed_csv, sep=";", dtype=str, encoding="utf-8-sig").fillna("")
            if {"iso3","country_qid"}.issubset(df.columns):
                for _, r in df.iterrows():
                    q = str(r["country_qid"]).strip()
                    i = str(r["iso3"]).strip().upper()
                    if q and i:
                        m[q] = i
    except Exception:
        pass
    return m

# ---------------- Queries (4 vias, todas com OPTIONAL ?iso3) ----------------
def _vals(conflicts: List[str]) -> str:
    return " ".join(f"wd:{c}" for c in conflicts if c)

def _pit_block() -> str:
    # devolve ?pit já como string 'YYYY-MM-DD' (sem ^^xsd:dateTime)
    return """
  OPTIONAL { ?conflict wdt:P585 ?pit0 }
  OPTIONAL { ?conflict wdt:P580 ?start }
  OPTIONAL { ?conflict wdt:P582 ?end   }
  BIND(COALESCE(?pit0, ?start, ?end) AS ?_pitDT)
  BIND(SUBSTR(STR(?_pitDT), 1, 10) AS ?pit)
""".rstrip()

def _country_filter_with_iso3() -> str:
    # devolve ?iso3 já como string "ABC" (mas sem necessidade de tratar datatype)
    return """
  ?country wdt:P31/wdt:P279* ?cls .
  VALUES ?cls { wd:Q6256 wd:Q3624078 }
  OPTIONAL { ?country wdt:P298 ?iso3_raw }
  BIND(STR(?iso3_raw) AS ?iso3)
""".strip()


def q_countries_p17(conflicts: List[str]) -> str:
    return f"""
SELECT DISTINCT ?conflict ?country ?pit ?iso3 WHERE {{
  VALUES ?conflict {{ {_vals(conflicts)} }}
  {_pit_block()}
  ?conflict wdt:P17 ?country .
  {_country_filter_with_iso3()}
}}
""".strip()

def q_countries_p276(conflicts: List[str]) -> str:
    return f"""
SELECT DISTINCT ?conflict ?country ?pit ?iso3 WHERE {{
  VALUES ?conflict {{ {_vals(conflicts)} }}
  {_pit_block()}
  ?conflict wdt:P276 ?place .
  ?place    wdt:P17 ?country .
  {_country_filter_with_iso3()}
}}
""".strip()

def q_countries_p131(conflicts: List[str]) -> str:
    return f"""
SELECT DISTINCT ?conflict ?country ?pit ?iso3 WHERE {{
  VALUES ?conflict {{ {_vals(conflicts)} }}
  {_pit_block()}
  ?conflict wdt:P131+ ?place .
  ?place    wdt:P17 ?country .
  {_country_filter_with_iso3()}
}}
""".strip()

def q_countries_p710_is_country(conflicts: List[str]) -> str:
    return f"""
SELECT DISTINCT ?conflict ?country ?pit ?iso3 WHERE {{
  VALUES ?conflict {{ {_vals(conflicts)} }}
  {_pit_block()}
  ?conflict wdt:P710 ?country .
  {_country_filter_with_iso3()}
}}
""".strip()

# ---------------- Core helpers ----------------
def read_conflict_list_from_parts(in_csv: Path) -> List[str]:
    import pandas as pd
    if not in_csv.exists():
        raise SystemExit(f"[erro] não encontrei {in_csv}")
    df = pd.read_csv(in_csv, sep=";", dtype=str, encoding="utf-8-sig").fillna("")
    if "conflict_qid" not in df.columns:
        raise SystemExit("[erro] conflicts_participants.csv sem coluna 'conflict_qid'")
    ids = df["conflict_qid"].astype(str).str.strip()
    seen, out = set(), []
    for q in ids:
        if q and q not in seen:
            seen.add(q); out.append(q)
    return out

def safe_append_rows(rows: List[List[str]]):
    """Escreve só linhas com 5 colunas na ordem do HEADER."""
    if not rows:
        return
    good = []
    for r in rows:
        if isinstance(r, (list, tuple)) and len(r) == 5:
            good.append(list(r))
    if not good:
        return
    with OUT_WORK.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f, delimiter=";").writerows(good)

def resolve_iso3(country_qid: str, iso3_query: str, forms_map: dict[str,str], seed_map: dict[str,str]) -> str:
    if iso3_query:
        return iso3_query.strip().upper()
    if country_qid in forms_map:
        return forms_map[country_qid]
    if country_qid in seed_map:
        return seed_map[country_qid]
    return ""

def run_batches(conflict_ids: List[str], builder, source: str, batch_size: int,
                forms_map: dict[str,str], seed_map: dict[str,str]) -> int:
    total = 0
    for i in range(0, len(conflict_ids), batch_size):
        chunk = conflict_ids[i:i+batch_size]
        payload = fetch_tsv_or_json(builder(chunk))
        pairs = parse_pairs(payload)  # [(conflict_qid, country_qid, pit, iso3)]
        rows = []
        for c, country, pit, iso3q in pairs:
            iso3 = resolve_iso3(country, iso3q, forms_map, seed_map)
            rows.append([c, country, pit, source, iso3])
        if rows:
            print(f"[{source}] batch {i//batch_size+1} sample:", rows[0])
        safe_append_rows(rows)
        total += len(rows)
        print(f"[{source}] batch {i//batch_size+1}: +{len(rows)} (acc {total})")
        time.sleep(SLEEP_BATCH + random.uniform(0, 0.2))
    return total

# ---------------- Main ----------------
def main():
    ap = argparse.ArgumentParser(description="Conflitos → países a partir de conflicts_participants.csv")
    ap.add_argument("--in", dest="in_csv", default=str(IN_PARTS), help="CSV de participantes (default: data/conflicts_participants.csv)")
    ap.add_argument("--batch", type=int, default=128, help="tamanho do batch VALUES (default 128)")
    ap.add_argument("--enable", default="P17,P276,P131,P710C", help="vias: P17,P276,P131,P710C (default todas)")
    ap.add_argument("--forms", default=str(FORMS_CSV), help="forms_all.csv (default: data/forms_all.csv)")
    ap.add_argument("--seed",  default=str(SEED_CSV),  help="countries_seed.csv (default: data/countries_seed.csv)")
    ap.add_argument("--out",   default=str(OUT_CSV),   help="CSV de saída (default data/conflicts_countries.csv)")
    args = ap.parse_args()

    in_csv   = Path(args.in_csv).resolve()
    out_csv  = Path(args.out).resolve()
    out_work = out_csv.with_suffix(".tmp.csv")
    forms_csv= Path(args.forms).resolve()
    seed_csv = Path(args.seed).resolve()
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    # overwrite estrito
    for p in (out_work, out_csv):
        try:
            p.unlink(missing_ok=True)
        except TypeError:
            if p.exists(): p.unlink()

    with out_work.open("w", newline="", encoding="utf-8") as f:
        csv.writer(f, delimiter=";").writerow(HEADER)

    forms_map = load_forms_map(forms_csv)
    seed_map  = load_seed_iso3(seed_csv)
    conflicts = read_conflict_list_from_parts(in_csv)
    print(f"[info] conflicts distintos: {len(conflicts)}")
    print(f"[info] formas no forms_all.csv: {len(forms_map)} ; seed países: {len(seed_map)}")

    enable = {x.strip().upper() for x in args.enable.split(",") if x.strip()}
    use_p17   = "P17"   in enable
    use_p276  = "P276"  in enable
    use_p131  = "P131"  in enable
    use_p710c = "P710C" in enable

    total = 0
    try:
        if use_p17:
            total += run_batches(conflicts, q_countries_p17,  "P17",  args.batch, forms_map, seed_map)
        if use_p276:
            total += run_batches(conflicts, q_countries_p276, "P276", args.batch, forms_map, seed_map)
        if use_p131:
            total += run_batches(conflicts, q_countries_p131, "P131", args.batch, forms_map, seed_map)
        if use_p710c:
            total += run_batches(conflicts, q_countries_p710_is_country, "P710C", args.batch, forms_map, seed_map)
    finally:
        try:
            os.replace(out_work, out_csv)
            print(f"[write] overwrite → {out_csv} ; linhas={total}")
        except PermissionError as e:
            raise SystemExit(f"[erro] não consegui substituir '{out_csv}' (está aberto?). Fecha e volta a correr.") from e

if __name__ == "__main__":
    main()
