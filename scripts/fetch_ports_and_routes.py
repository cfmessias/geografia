# -*- coding: utf-8 -*-
"""
scripts/fetch_ports_and_routes.py
Recolhe portos e feições aquáticas (mares, oceanos, canais, estreitos)
para todos os países listados em data/countries_profiles.csv.

Output: data/ports_and_routes.csv
Campos: iso3;qid;country_pt;country_en;ports_pt;ports_en;waters_pt;waters_en;has_ports;has_routes
"""

from __future__ import annotations
from pathlib import Path
import csv, sys, time, random, json, requests
from typing import Dict, List, Set, Tuple

# --- Configuração base ---
PROJECT_ROOT  = Path(__file__).resolve().parent.parent
DATA_DIR      = PROJECT_ROOT / "data"
PROFILES_CSV  = DATA_DIR / "countries_profiles.csv"
OUT_CSV       = DATA_DIR / "ports_and_routes.csv"

WDQS_URL = "https://query.wikidata.org/sparql"
UA       = "GeografiaApp/1.0 (+https://cfmessias.pt)"

BATCH_COUNTRIES = 4
BATCH_LABELS    = 400
RETRIES         = 5
TIMEOUT         = 90

# --- Funções utilitárias ---
def sniff_sep(path: Path) -> str:
    sample = path.read_text(encoding="utf-8", errors="ignore")[:4096]
    return ";" if sample.count(";") >= sample.count(",") else ","

def read_profiles(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        print(f"[erro] Não encontrei {path}", file=sys.stderr)
        sys.exit(1)
    sep = sniff_sep(path)
    with path.open("r", encoding="utf-8", newline="") as f:
        rows = [dict((k.strip(), (v or "").strip()) for k, v in r.items())
                for r in csv.DictReader(f, delimiter=sep)]
    return rows

def chunked(lst: List, n: int) -> List[List]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

# --- Função robusta de chamada SPARQL ---
def _get(url: str, query: str) -> dict:
    """Executa query SPARQL de forma robusta usando GET (para evitar Resposta não-JSON)."""
    headers = {
        "User-Agent": UA,
        "Accept": "application/sparql-results+json"
    }
    params = {"query": query, "format": "json"}

    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)
            r.raise_for_status()

            # Garantir que a resposta é JSON
            ctype = (r.headers.get("Content-Type") or "").lower()
            if "application/sparql-results+json" in ctype or "application/json" in ctype:
                try:
                    return r.json()
                except json.JSONDecodeError:
                    raise ValueError("JSON truncado ou inválido")
            else:
                raise ValueError(f"Resposta não-JSON ({ctype})")

        except Exception as e:
            if attempt < RETRIES:
                wait = 2.5 * attempt + random.uniform(0, 2.0)
                print(f"[warn] tentativa {attempt}/{RETRIES} falhou: {e}", file=sys.stderr)
                time.sleep(wait)
            else:
                print(f"[erro] falhou em definitivo: {e}", file=sys.stderr)
                return {}

def qvals(qids: List[str]) -> str:
    return " ".join(f"wd:{q}" for q in qids)

# --- Queries ---
PORTS_SPARQL = """
SELECT DISTINCT ?country ?port WHERE {
  VALUES ?country { %VALUES% }
  {
    ?port wdt:P31/wdt:P279* wd:Q44782 .
    ?port wdt:P17 ?country .
  }
  UNION
  {
    ?port wdt:P31/wdt:P279* wd:Q44782 .
    ?port wdt:P131* ?adm .
    ?adm wdt:P17 ?country .
  }
}
"""

WATERS_SPARQL = """
SELECT DISTINCT ?country ?water WHERE {
  VALUES ?country { %VALUES% }
  ?country wdt:P206 ?water .
  ?water wdt:P31/wdt:P279* ?wcls .
  VALUES ?wcls { wd:Q9430 wd:Q39816 wd:Q12284 wd:Q2592810 }  # ocean, sea, canal, strait
}
"""

LABELS_SPARQL = """
SELECT ?item
       (SAMPLE(?pt_) AS ?pt)
       (SAMPLE(?en_) AS ?en)
WHERE {
  VALUES ?item { %VALUES% }
  OPTIONAL { ?item rdfs:label ?pt_ . FILTER(LANG(?pt_) = "pt") }
  OPTIONAL { ?item rdfs:label ?en_ . FILTER(LANG(?en_) = "en") }
}
GROUP BY ?item
"""

# --- Funções de recolha ---
def fetch_ports_by_country(batch_qids: List[str]) -> Dict[str, Set[str]]:
    if not batch_qids:
        return {}
    q = PORTS_SPARQL.replace("%VALUES%", qvals(batch_qids))
    data = _get(WDQS_URL, q)
    out: Dict[str, Set[str]] = {}
    for b in data.get("results", {}).get("bindings", []):
        c_uri = b.get("country", {}).get("value", "")
        p_uri = b.get("port", {}).get("value", "")
        cq = c_uri.split("/")[-1] if c_uri else ""
        pq = p_uri.split("/")[-1] if p_uri else ""
        if cq and pq:
            out.setdefault(cq, set()).add(pq)
    return out

def fetch_waters_by_country(batch_qids: List[str]) -> Dict[str, Set[str]]:
    if not batch_qids:
        return {}
    q = WATERS_SPARQL.replace("%VALUES%", qvals(batch_qids))
    data = _get(WDQS_URL, q)
    out: Dict[str, Set[str]] = {}
    for b in data.get("results", {}).get("bindings", []):
        c_uri = b.get("country", {}).get("value", "")
        w_uri = b.get("water", {}).get("value", "")
        cq = c_uri.split("/")[-1] if c_uri else ""
        wq = w_uri.split("/")[-1] if w_uri else ""
        if cq and wq:
            out.setdefault(cq, set()).add(wq)
    return out

def fetch_labels_pt_en(qids: List[str]) -> Dict[str, Tuple[str,str]]:
    out: Dict[str, Tuple[str,str]] = {}
    for batch in chunked(qids, BATCH_LABELS):
        q = LABELS_SPARQL.replace("%VALUES%", qvals(batch))
        data = _get(WDQS_URL, q)
        for b in data.get("results", {}).get("bindings", []):
            it  = b.get("item", {}).get("value", "")
            qid = it.split("/")[-1] if it else ""
            pt  = (b.get("pt", {}) or {}).get("value", "")
            en  = (b.get("en", {}) or {}).get("value", "")
            if qid:
                out[qid] = (pt, en)
        time.sleep(0.2)
    return out

# --- Execução principal ---
def main():
    print("[fetch_ports_and_routes] A recolher dados (2-pass)…")

    profs = read_profiles(PROFILES_CSV)
    qid_to_iso3: Dict[str, str] = {}
    ordered_qids: List[str] = []
    for r in profs:
        qid  = (r.get("qid") or "").strip()
        iso3 = (r.get("iso3") or "").strip().upper()
        if qid and iso3:
            qid_to_iso3[qid] = iso3
            ordered_qids.append(qid)

    ports_by_country: Dict[str, Set[str]]  = {}
    waters_by_country: Dict[str, Set[str]] = {}

    total = len(ordered_qids)
    done  = 0
    for batch in chunked(ordered_qids, BATCH_COUNTRIES):
        lo = done + 1
        hi = min(done + len(batch), total)
        print(f"[batch-QIDs] {lo}-{hi} / {total} (size={len(batch)}) — ports")
        pmap = fetch_ports_by_country(batch)
        for c, s in pmap.items():
            ports_by_country.setdefault(c, set()).update(s)

        time.sleep(0.6)

        print(f"[batch-QIDs] {lo}-{hi} / {total} (size={len(batch)}) — waters")
        wmap = fetch_waters_by_country(batch)
        for c, s in wmap.items():
            waters_by_country.setdefault(c, set()).update(s)

        done += len(batch)
        time.sleep(0.6)

    label_qids: Set[str] = set(ordered_qids)
    for s in ports_by_country.values():
        label_qids.update(s)
    for s in waters_by_country.values():
        label_qids.update(s)

    print(f"[labels] a resolver {len(label_qids)} itens em batches de {BATCH_LABELS}…")
    labels = fetch_labels_pt_en(sorted(label_qids))

    def label_pt(q: str) -> str: return labels.get(q, ("",""))[0]
    def label_en(q: str) -> str: return labels.get(q, ("",""))[1]

    rows_out: List[Dict[str,str]] = []
    for cq in ordered_qids:
        iso3 = qid_to_iso3.get(cq, "")
        c_pt = label_pt(cq)
        c_en = label_en(cq)
        port_list = sorted(ports_by_country.get(cq, set()))
        water_list = sorted(waters_by_country.get(cq, set()))
        ports_pt = ", ".join(filter(None, (label_pt(p) for p in port_list)))
        ports_en = ", ".join(filter(None, (label_en(p) for p in port_list)))
        waters_pt = ", ".join(filter(None, (label_pt(w) for w in water_list)))
        waters_en = ", ".join(filter(None, (label_en(w) for w in water_list)))

        rows_out.append({
            "iso3": iso3,
            "qid": cq,
            "country_pt": c_pt,
            "country_en": c_en,
            "ports_pt": ports_pt,
            "ports_en": ports_en,
            "waters_pt": waters_pt,
            "waters_en": waters_en,
            "has_ports": "Sim" if port_list else "Não",
            "has_routes": "Sim" if water_list else "Não",
        })

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["iso3","qid","country_pt","country_en","ports_pt","ports_en",
                        "waters_pt","waters_en","has_ports","has_routes"],
            delimiter=";")
        w.writeheader()
        w.writerows(rows_out)

    print(f"[ok] Escrito {OUT_CSV} | países: {len(rows_out)}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[info] Interrompido pelo utilizador.")
        sys.exit(130)
