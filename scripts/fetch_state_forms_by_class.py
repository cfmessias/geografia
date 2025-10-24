# scripts/fetch_state_forms_by_class.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import csv
import time
import sys
import argparse
from pathlib import Path
from typing import Dict, Iterable, Tuple, Set

import requests

# ================== Config ==================
OUT_CSV = Path(__file__).resolve().parent.parent / "data" / "state_forms_by_class.csv"
SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = "GeoMundi-StateForms/1.2 (+cfmessias@gmail.com)"

# Lista corrigida (memória)
CLS_STATE_LIKE: Tuple[Tuple[str, str], ...] = (
    ("Q3624078", "sovereign state"),
    ("Q6256",    "country"),
    ("Q417175",  "kingdom"),
    ("Q3024240", "former country"),
    ("Q48349",   "empire"),
    ("Q7269",    "monarchy"),
    ("Q7270",    "republic"),
    ("Q41614",   "caliphate"),
    ("Q184558",  "sultanate"),
    ("Q133156",  "colony"),
    ("Q170156",  "confederation"),
    ("Q179164",  "federation"),
    ("Q28108",   "commonwealth"),
)

# Paginação e limites
PAGE_SIZE = 5000
REQUEST_TIMEOUT = 120
RETRY_MAX = 4
RETRY_BACKOFF = 6
THROTTLE_BETWEEN_REQUESTS = 1.0

# ================== Helpers ==================
def ensure_outfile() -> None:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    if not OUT_CSV.exists():
        with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["cls_qid", "cls_label", "item_qid", "item_label", "iso3"])

def load_written_index() -> Set[tuple[str, str]]:
    """Devolve set de (cls_qid, item_qid) já escritos (para dedupe global)."""
    idx: Set[tuple[str, str]] = set()
    if not OUT_CSV.exists():
        return idx
    with OUT_CSV.open("r", encoding="utf-8", errors="ignore") as f:
        next(f, None)  # header
        for line in f:
            parts = line.rstrip("\n").split(";")
            if len(parts) >= 3:
                cls_qid, _, item_qid = parts[0], parts[1], parts[2]
                if cls_qid and item_qid:
                    idx.add((cls_qid, item_qid))
    return idx

def class_already_written(cls_qid: str, written_idx: Set[tuple[str, str]]) -> bool:
    return any(k[0] == cls_qid for k in written_idx)

def remove_class_rows(cls_qid: str) -> None:
    """Remove todas as linhas dessa classe (refresh por classe)."""
    if not OUT_CSV.exists():
        return
    tmp = OUT_CSV.with_suffix(".tmp")
    with OUT_CSV.open("r", encoding="utf-8", errors="ignore") as fin, \
         tmp.open("w", newline="", encoding="utf-8") as fout:
        w = csv.writer(fout, delimiter=";")
        header = fin.readline()
        if header:
            fout.write(header)
        for line in fin:
            if not line.startswith(f"{cls_qid};"):
                fout.write(line)
    tmp.replace(OUT_CSV)

def run_sparql(query: str) -> Dict:
    headers = {"Accept": "application/sparql-results+json","User-Agent": USER_AGENT}
    for attempt in range(1, RETRY_MAX + 1):
        try:
            resp = requests.post(SPARQL_ENDPOINT, data={"query": query}, headers=headers, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
            else:
                sys.stderr.write(f"[warn] HTTP {resp.status_code}: {resp.text[:200]}\n")
        except requests.RequestException as e:
            sys.stderr.write(f"[err] {type(e).__name__}: {e}\n")
        sleep_s = RETRY_BACKOFF * (2 ** (attempt - 1))
        sys.stderr.write(f"[info] retry {attempt}/{RETRY_MAX} em {sleep_s}s…\n")
        time.sleep(sleep_s)
    raise RuntimeError("Falha a obter resposta do WDQS após retries.")

def build_query(cls_qid: str, limit: int, offset: int) -> str:
    return f"""
SELECT DISTINCT ?item ?itemLabel ?iso3 WHERE {{
  ?item wdt:P31/wdt:P279* wd:{cls_qid} .
  OPTIONAL {{ ?item wdt:P298 ?iso3 }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
}}
LIMIT {limit}
OFFSET {offset}
""".strip()

def extract_bindings(js: Dict) -> Iterable[tuple[str, str, str]]:
    for b in js.get("results", {}).get("bindings", []):
        uri = b["item"]["value"]
        item_qid = uri.rsplit("/", 1)[-1]
        item_label = b.get("itemLabel", {}).get("value", "")
        iso3 = b.get("iso3", {}).get("value", "")
        yield (item_qid, item_label, iso3)

def write_rows(cls_qid: str, cls_label: str, rows: Iterable[tuple[str, str, str]]) -> int:
    count = 0
    with OUT_CSV.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        for item_qid, item_label, iso3 in rows:
            w.writerow([cls_qid, cls_label, item_qid, item_label, iso3])
            count += 1
    return count

# ================== Main ==================
def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch state-like forms por classe, incremental, sem duplicados.")
    ap.add_argument("--refresh-class", help="QID da classe a refrescar (apaga linhas existentes e volta a escrever). Ex.: Q48349")
    args = ap.parse_args()

    ensure_outfile()
    written_idx = load_written_index()
    total_written = 0

    for cls_qid, cls_label in CLS_STATE_LIKE:
        if args.refresh_class and args.refresh_class == cls_qid:
            print(f"[refresh] {cls_qid} ({cls_label}) — a limpar linhas existentes…", flush=True)
            remove_class_rows(cls_qid)
            # reconstruir índice sem essa classe
            written_idx = {k for k in written_idx if k[0] != cls_qid}

        if not args.refresh_class and class_already_written(cls_qid, written_idx):
            print(f"[skip] {cls_qid} ({cls_label}) já presente — a saltar.", flush=True)
            continue

        print(f"[proc] {cls_qid} ({cls_label})…", flush=True)
        offset = 0
        written_cls = 0
        seen_local: set[str] = set()

        while True:
            js = run_sparql(build_query(cls_qid, PAGE_SIZE, offset))
            batch = list(extract_bindings(js))
            if not batch:
                break

            # dedupe local + dedupe global
            filtered = []
            for item_qid, item_label, iso3 in batch:
                if item_qid in seen_local:
                    continue
                if (cls_qid, item_qid) in written_idx:
                    continue
                seen_local.add(item_qid)
                filtered.append((item_qid, item_label, iso3))

            if filtered:
                n = write_rows(cls_qid, cls_label, filtered)
                written_cls += n
                total_written += n
                # atualizar índice global
                for item_qid, _, _ in filtered:
                    written_idx.add((cls_qid, item_qid))

            print(f"  [page] offset={offset} wrote={len(filtered)} acum_cls={written_cls}", flush=True)
            offset += PAGE_SIZE
            time.sleep(THROTTLE_BETWEEN_REQUESTS)

        print(f"[done] {cls_qid} ({cls_label}) — {written_cls} registos.", flush=True)

    print(f"✔️ Concluído. Total escrito nesta execução: {total_written}. Ficheiro: {OUT_CSV}", flush=True)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[info] Interrompido pelo utilizador. Dados já escritos permanecem no CSV.", file=sys.stderr)
        sys.exit(130)
