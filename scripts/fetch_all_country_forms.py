# scripts/fetch_all_country_forms.py
# -*- coding: utf-8 -*-
"""
Extrai todas as 'formas' (QIDs) associadas a cada país (por ISO3) e grava em CSV ; (iso3;qid).

Mudanças vs. versão anterior:
- Sem mega-UNION: micro-queries independentes por relação → muito mais rápido e confiável.
- Passo 1: obter países atuais (?country) pelo ISO3 via P298.
- Passo 2: para esses ?country, correr 5 queries:
    Q1  — o(s) próprio(s) país(es) (BIND ?country AS ?state)
    Q2a — sucessão: ?state (P1365|P1366) ?country
    Q2b — sucessão inversa: ?country (P1365|P1366) ?state
    Q3  — P3842 (present-day country)
    Q4  — P1269 (facet of)
    Q5  — (P17|P495) limitado a tipos "state-like"
- Deduplicação e append por ISO3.

Uso típico:
  python scripts/fetch_all_country_forms.py --only PRT --out data/forms_all.csv
"""

from __future__ import annotations
import argparse
import time
import random
import json
import hashlib
import tempfile
from pathlib import Path
from typing import List, Iterable, Set

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlencode

WDQS_URL = "https://query.wikidata.org/sparql"
HEADERS = {
    "User-Agent": "GeoWars-Forms/2.0 (+streamlit)",
    "Accept": "application/sparql-results+json; charset=utf-8",
}

# --------------------------------------------------------------------
# WDQS client com retry + cache leve
# --------------------------------------------------------------------
_session = requests.Session()
_retry = Retry(
    total=8, connect=5, read=5,
    status_forcelist=(429, 500, 502, 503, 504),
    backoff_factor=1.0,
    allowed_methods=frozenset(["GET", "POST"]),
    raise_on_status=False,
)

_session.mount("https://", HTTPAdapter(max_retries=_retry))

CACHE_DIR = Path(tempfile.gettempdir()) / "wdqs_cache_forms_v2"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_TTL = 24 * 3600  # 1 dia

def _cache_key(q: str) -> Path:
    return CACHE_DIR / (hashlib.sha1(q.encode("utf-8")).hexdigest() + ".json")

def wdqs(query: str, attempts: int = 3, timeout: int = 60):
    """Tenta cache, depois GET (se curto), depois POST; com retries exponenciais."""
    k = _cache_key(query)
    now = time.time()
    if k.exists() and now - k.stat().st_mtime < CACHE_TTL:
        try:
            return json.loads(k.read_text(encoding="utf-8"))
        except Exception:
            pass

    def _parse(r: requests.Response):
        r.raise_for_status()
        return r.json()["results"]["bindings"]

    last_err = None
    delay = 1.0
    for i in range(1, attempts + 1):
        try:
            if len(query) < 7500:
                url = f"{WDQS_URL}?{urlencode({'query': query})}"
                r = _session.get(url, headers=HEADERS, timeout=timeout)
                if r.status_code == 200:
                    rows = _parse(r)
                    k.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
                    return rows
            r = _session.post(WDQS_URL, data={"query": query}, headers=HEADERS, timeout=timeout)
            rows = _parse(r)
            k.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
            return rows
        except Exception as e:
            last_err = e
            sleep_s = min(12.0, delay) + random.uniform(0.0, 0.6)
            print(f"[wdqs] tentativa {i}/{attempts} falhou: {e} → dormir {sleep_s:.1f}s", flush=True)
            time.sleep(sleep_s)
            delay *= 1.8
    print(f"[wdqs] erro definitivo: {last_err}", flush=True)
    return []

# --------------------------------------------------------------------
# SPARQL helpers (sem UNION; 1 relação por query)
# --------------------------------------------------------------------
CLS_STATE_LIKE = " ".join([
    "wd:Q3624078",  # sovereign state
    "wd:Q6256",     # country
    "wd:Q417175",   # kingdom
    "wd:Q3024240",  # former country
    "wd:Q2277",     # empire
    "wd:Q7269",     # monarchy
    "wd:Q7270",     # republic
    "wd:Q41614",    # caliphate
    "wd:Q184558",   # sultanate
    "wd:Q143357",   # dominion
    "wd:Q133156",   # colony
    "wd:Q178561",   # confederation
    "wd:Q179164",   # federation
    "wd:Q28108",    # commonwealth
])

EXCLUDE_SUBNAT = "FILTER NOT EXISTS { ?state wdt:P31/wdt:P279* wd:Q56061 }"

def q_country_by_iso3(iso3: str) -> str:
    # devolve ?country (IRI)
    return f"""
SELECT ?country WHERE {{
  ?country wdt:P298 "{iso3}" .
}}
""".strip()

def _vals_iri(countries: List[str]) -> str:
    # VALUES ?country { <IRI> <IRI> }
    vals = " ".join(f"<{iri}>" for iri in countries)
    return f"VALUES ?country {{ {vals} }}"

def q_Q1_self(countries: List[str]) -> str:
    vals = _vals_iri(countries)
    return f"""
SELECT ?state WHERE {{
  {vals}
  BIND(?country AS ?state)
}}
""".strip()

def q_Q2a_succession_forward(countries: List[str]) -> str:
    vals = _vals_iri(countries)
    return f"""
SELECT ?state WHERE {{
  {vals}
  VALUES ?cls {{ {CLS_STATE_LIKE} }}
  ?state wdt:P31/wdt:P279* ?cls .
  ?state (wdt:P1365|wdt:P1366) ?country .
  {EXCLUDE_SUBNAT}
}}
""".strip()

def q_Q2b_succession_backward(countries: List[str]) -> str:
    vals = _vals_iri(countries)
    return f"""
SELECT ?state WHERE {{
  {vals}
  VALUES ?cls {{ {CLS_STATE_LIKE} }}
  ?state wdt:P31/wdt:P279* ?cls .
  ?country (wdt:P1365|wdt:P1366) ?state .
  {EXCLUDE_SUBNAT}
}}
""".strip()

def q_Q3_present_day(countries: List[str]) -> str:
    vals = _vals_iri(countries)
    return f"""
SELECT ?state WHERE {{
  {vals}
  VALUES ?cls {{ {CLS_STATE_LIKE} }}
  ?state wdt:P31/wdt:P279* ?cls ;
         wdt:P3842 ?country .
  {EXCLUDE_SUBNAT}
}}
""".strip()

def q_Q4_facet_of(countries: List[str]) -> str:
    # ANTES não tinha filtro de classe → origem do ruído
    vals = _vals_iri(countries)
    return f"""
SELECT ?state WHERE {{
  {vals}
  VALUES ?cls {{ {CLS_STATE_LIKE} }}
  ?state wdt:P31/wdt:P279* ?cls ;
         wdt:P1269 ?country .
  {EXCLUDE_SUBNAT}
}}
""".strip()

def q_Q5a_p17(countries: list[str]) -> str:
    vals = _vals_iri(countries)
    return f"""
SELECT ?state WHERE {{
  {vals}
  VALUES ?cls {{ {CLS_STATE_LIKE} }}
  ?state wdt:P31/wdt:P279* ?cls ;
         wdt:P17 ?country .
  {EXCLUDE_SUBNAT}
}}
""".strip()

def q_Q5b_p495(countries: list[str]) -> str:
    vals = _vals_iri(countries)
    return f"""
SELECT ?state WHERE {{
  {vals}
  VALUES ?cls {{ {CLS_STATE_LIKE} }}
  ?state wdt:P31/wdt:P279* ?cls ;
         wdt:P495 ?country .
  {EXCLUDE_SUBNAT}
}}
""".strip()


def parse_qids(rows, var="state") -> List[str]:
    out: List[str] = []
    for r in rows:
        v = (r.get(var) or {}).get("value", "")
        if isinstance(v, str) and "/entity/" in v:
            q = v.rsplit("/", 1)[-1]
            if q.startswith("Q") and q[1:].isdigit():
                out.append(q)
    return out

def parse_countries(rows) -> List[str]:
    out: List[str] = []
    for r in rows:
        v = (r.get("country") or {}).get("value", "")
        if isinstance(v, str) and v.startswith("http"):
            out.append(v)
    return out

# --------------------------------------------------------------------
# Paths helpers
# --------------------------------------------------------------------
def find_project_root() -> Path:
    here = Path(__file__).resolve()
    for p in here.parents:
        if (p / "data").is_dir() and (p / "scripts").is_dir():
            return p
    for p in here.parents:
        if (p / "data").is_dir():
            return p
    return Path.cwd()

def resolve_path(p: str) -> Path:
    pp = Path(p)
    if pp.is_absolute():
        return pp
    root = find_project_root()
    return (root / pp).resolve()

def q_validate_types(qids: List[str]) -> str:
    values = " ".join(f"wd:{q}" for q in qids)
    return f"""
SELECT ?state WHERE {{
  VALUES ?state {{ {values} }}
  ?state wdt:P31/wdt:P279* ?cls .
  VALUES ?cls {{ {CLS_STATE_LIKE} }}
  FILTER NOT EXISTS {{ ?state wdt:P31/wdt:P279* wd:Q56061 }}
}}
""".strip()

def validate_qids(qids: Set[str]) -> Set[str]:
    out: Set[str] = set()
    lst = sorted(qids)
    # validar em blocos de 300 para não rebentar a URL
    for i in range(0, len(lst), 300):
        blk = lst[i:i+300]
        rows = wdqs(q_validate_types(blk))
        out.update(parse_qids(rows, var="state"))
        time.sleep(0.12 + random.uniform(0.0, 0.15))
    return out

# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Extrai forms (QIDs) por ISO3 → data/forms_all.csv (sem mega-UNION)")
    ap.add_argument("--profiles", default="data/countries_profiles.csv",
                    help="CSV (sep=';') com coluna iso3")
    ap.add_argument("--out", default="data/forms_all.csv",
                    help="CSV de saída (sep=';') com colunas iso3;qid")
    ap.add_argument("--only", default="", help="Processar só um ISO3 (ex.: PRT)")
    ap.add_argument("--limit", type=int, default=0, help="Limitar nº de países")
    ap.add_argument("--sleep", type=float, default=0.35, help="Pausa entre países (seg)")
    args = ap.parse_args()

    profiles = resolve_path(args.profiles)
    out_csv  = resolve_path(args.out)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    # overwrite garantido no arranque
    try:
        out_csv.unlink(missing_ok=True)
    except TypeError:
        if out_csv.exists():
            out_csv.unlink()

    if not profiles.exists():
        raise FileNotFoundError(
            f"Não encontrei o ficheiro de perfis: {profiles}\n"
            f"Sugestão: passe --profiles C:/.../data/countries_profiles.csv"
        )

    dfp = pd.read_csv(profiles, sep=";", dtype=str, encoding="utf-8-sig").fillna("")
    if "iso3" not in dfp.columns:
        raise RuntimeError("countries_profiles.csv precisa da coluna 'iso3'.")

    iso_list: List[str] = (
        [args.only.upper().strip()] if args.only
        else dfp["iso3"].astype(str).str.upper().str.strip().tolist()
    )
    if args.limit > 0:
        iso_list = iso_list[:args.limit]

    # escrever cabeçalho uma vez
    pd.DataFrame(columns=["iso3", "qid"]).to_csv(
        out_csv, sep=";", index=False, encoding="utf-8-sig", mode="w", header=True
    )

    total_rows = 0
    for i, iso3 in enumerate(iso_list, start=1):
        if not iso3:
            continue
        print(f"[{i}/{len(iso_list)}] {iso3}")

        # Passo 1 — países atuais por ISO3
        rows_c = wdqs(q_country_by_iso3(iso3))
        countries = parse_countries(rows_c)
        if not countries:
            print("  └ (sem países para este ISO3)", flush=True)
            time.sleep(max(0.0, args.sleep) + random.uniform(0.0, 0.3))
            continue

        # Passo 2 — micro-queries
        qids: Set[str] = set()
        builders = (
            q_Q1_self, q_Q2a_succession_forward, q_Q2b_succession_backward,
            q_Q3_present_day, q_Q4_facet_of, q_Q5a_p17, q_Q5b_p495
        )
        for b in builders:
            q = b(countries)
            try:
                rows = wdqs(q)  # com mais attempts e timeout
            except Exception as e:
                print(f"  └ [warn] micro-query {b.__name__} falhou: {e}")
                rows = []
            qids.update(parse_qids(rows, var="state"))
            time.sleep(0.2 + random.uniform(0.0, 0.2))

        # depois de juntares os qids dos 5/6 blocos…
        qids = validate_qids(qids)

        # escrever chunk deduplicado
        chunk_df = (
            pd.DataFrame({"iso3": iso3, "qid": sorted(qids)})
            .drop_duplicates()
            .sort_values(["iso3", "qid"], kind="mergesort")
        )
        chunk_df.to_csv(out_csv, sep=";", index=False, encoding="utf-8-sig", mode="a", header=False)

        total_rows += len(chunk_df)
        print(f"  └ {len(chunk_df)} QIDs", flush=True)

        time.sleep(max(0.0, args.sleep) + random.uniform(0.0, 0.35))

    print(f"[done] {total_rows} linhas → {out_csv}")

if __name__ == "__main__":
    main()
