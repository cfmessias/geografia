# scripts/fetch_successions.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import csv
import sys
import time
import argparse
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

import requests

# ================== Paths ==================
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
INPUT_CSV    = DATA_DIR / "state_forms_by_class.csv"   # cls_qid;cls_label;item_qid;item_label;iso3
OUT_CSV      = DATA_DIR / "state_successions.csv"      # item_qid;item_label;item_iso3;relation;related_qid;related_label;related_iso3
DONE_FILE    = DATA_DIR / "state_successions.done"     # lista de item_qid já processados

# ================== WDQS ==================
SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT      = "GeoMundi-Successions/1.0 (+cfmessias@gmail.com)"
REQUEST_TIMEOUT = 90
RETRY_MAX       = 4
BACKOFF_BASE_S  = 6
THROTTLE_S      = 0.8  # pausa entre pedidos, bom-cidadao

# ================== I/O helpers ==================
def ensure_outputs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not OUT_CSV.exists():
        with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["item_qid","item_label","item_iso3","relation","related_qid","related_label","related_iso3"])
    if not DONE_FILE.exists():
        DONE_FILE.write_text("", encoding="utf-8")

def read_input_items() -> List[Tuple[str, str, str]]:
    """Lê do INPUT_CSV e devolve lista de (item_qid, item_label, item_iso3) únicos."""
    seen: Set[str] = set()
    items: List[Tuple[str, str, str]] = []
    with INPUT_CSV.open("r", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f, delimiter=";")
        for row in r:
            qid  = str(row.get("item_qid","")).strip()
            if not qid or qid in seen:
                continue
            seen.add(qid)
            items.append((qid, str(row.get("item_label","")).strip(), str(row.get("iso3","")).strip()))
    return items

def load_done() -> Set[str]:
    try:
        return {line.strip() for line in DONE_FILE.read_text(encoding="utf-8").splitlines() if line.strip()}
    except FileNotFoundError:
        return set()

def append_done(qid: str) -> None:
    with DONE_FILE.open("a", encoding="utf-8") as f:
        f.write(qid + "\n")

def load_written_index() -> Set[Tuple[str, str, str]]:
    """Índice de linhas já escritas: (item_qid, relation, related_qid)."""
    idx: Set[Tuple[str, str, str]] = set()
    if not OUT_CSV.exists():
        return idx
    with OUT_CSV.open("r", encoding="utf-8", errors="ignore") as f:
        next(f, None)  # header
        for line in f:
            parts = line.rstrip("\n").split(";")
            if len(parts) >= 5:
                item_qid, _, _, relation, related_qid = parts[0], parts[1], parts[2], parts[3], parts[4]
                if item_qid and relation and related_qid:
                    idx.add((item_qid, relation, related_qid))
    return idx

def remove_item_rows(item_qid: str) -> None:
    """Apaga linhas do OUT_CSV referentes a um item (para refresh desse item)."""
    if not OUT_CSV.exists():
        return
    tmp = OUT_CSV.with_suffix(".tmp")
    with OUT_CSV.open("r", encoding="utf-8", errors="ignore") as fin, \
         tmp.open("w", newline="", encoding="utf-8") as fout:
        header = fin.readline()
        fout.write(header)
        for line in fin:
            if not line.startswith(f"{item_qid};"):
                fout.write(line)
    tmp.replace(OUT_CSV)
    # remover do DONE
    if DONE_FILE.exists():
        done = [ln for ln in DONE_FILE.read_text(encoding="utf-8").splitlines() if ln.strip() and ln.strip()!=item_qid]
        DONE_FILE.write_text("\n".join(done) + ("\n" if done else ""), encoding="utf-8")

def write_rows(rows: Iterable[Tuple[str,str,str,str,str,str,str]]) -> int:
    count = 0
    with OUT_CSV.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        for row in rows:
            w.writerow(row)
            count += 1
    return count

# ================== WDQS helpers ==================
def run_sparql(query: str) -> Dict:
    headers = {"Accept": "application/sparql-results+json", "User-Agent": USER_AGENT}
    for attempt in range(1, RETRY_MAX+1):
        try:
            resp = requests.post(SPARQL_ENDPOINT, data={"query": query}, headers=headers, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
            sys.stderr.write(f"[warn] HTTP {resp.status_code}: {resp.text[:200]}\n")
        except requests.RequestException as e:
            sys.stderr.write(f"[err] {type(e).__name__}: {e}\n")
        sleep_s = BACKOFF_BASE_S * (2 ** (attempt-1))
        sys.stderr.write(f"[info] retry {attempt}/{RETRY_MAX} em {sleep_s}s…\n")
        time.sleep(sleep_s)
    raise RuntimeError("Falhou após vários retries ao WDQS.")

def q_prev(item_qid: str) -> str:
    return f"""
SELECT DISTINCT ?item ?itemLabel ?itemQID ?itemISO3 ?rel ?relLabel ?relQID ?relISO3 WHERE {{
  VALUES ?item {{ wd:{item_qid} }}
  OPTIONAL {{ ?item wdt:P298 ?itemISO3 }}
  {{ ?item wdt:P1365 ?rel }} UNION {{ ?item wdt:P155 ?rel }}
  OPTIONAL {{ ?rel wdt:P298 ?relISO3 }}
  BIND(STRAFTER(STR(?item), "entity/") AS ?itemQID)
  BIND(STRAFTER(STR(?rel),  "entity/") AS ?relQID)
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
}}
""".strip()

def q_next(item_qid: str) -> str:
    return f"""
SELECT DISTINCT ?item ?itemLabel ?itemQID ?itemISO3 ?rel ?relLabel ?relQID ?relISO3 WHERE {{
  VALUES ?item {{ wd:{item_qid} }}
  OPTIONAL {{ ?item wdt:P298 ?itemISO3 }}
  {{ ?item wdt:P1366 ?rel }} UNION {{ ?item wdt:P156 ?rel }}
  OPTIONAL {{ ?rel wdt:P298 ?relISO3 }}
  BIND(STRAFTER(STR(?item), "entity/") AS ?itemQID)
  BIND(STRAFTER(STR(?rel),  "entity/") AS ?relQID)
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
}}
""".strip()

def parse_bindings(js: Dict) -> List[Tuple[str,str,str,str,str,str,str]]:
    """Mapeia resultado para linhas CSV: item_qid,item_label,item_iso3,relation,related_qid,related_label,related_iso3.
       A coluna 'relation' será preenchida pelo chamador ('prev' ou 'next')."""
    rows = []
    for b in js.get("results", {}).get("bindings", []):
        item_qid  = b.get("itemQID", {}).get("value", "")
        item_lbl  = b.get("itemLabel", {}).get("value", "")
        item_iso3 = b.get("itemISO3", {}).get("value", "")
        rel_qid   = b.get("relQID", {}).get("value", "")
        rel_lbl   = b.get("relLabel", {}).get("value", "")
        rel_iso3  = b.get("relISO3", {}).get("value", "")
        rows.append((item_qid, item_lbl, item_iso3, "", rel_qid, rel_lbl, rel_iso3))  # relation vazio por agora
    return rows

# ================== Main ==================
def main() -> None:
    ap = argparse.ArgumentParser(description="Extrai predecessores (P1365/P155) e sucessores (P1366/P156) por item, incremental.")
    ap.add_argument("--limit", type=int, help="Processar no máximo N itens (para teste).")
    ap.add_argument("--refresh-item", help="QID do item a refazer (apaga linhas existentes desse item). Ex.: Q17167")
    args = ap.parse_args()

    ensure_outputs()
    items = read_input_items()
    done  = load_done()
    idx   = load_written_index()

    if args.refresh_item:
        remove_item_rows(args.refresh_item)
        done.discard(args.refresh_item)
        # idx será atualizado à medida que reescrevemos

    to_process = []
    for qid, lbl, iso3 in items:
        if args.limit and len(to_process) >= args.limit:
            break
        if args.refresh_item:
            if qid == args.refresh_item:
                to_process.append((qid, lbl, iso3))
        else:
            if qid not in done:
                to_process.append((qid, lbl, iso3))

    total_new = 0
    for i, (item_qid, item_lbl_hint, item_iso3_hint) in enumerate(to_process, 1):
        print(f"[{i}/{len(to_process)}] {item_qid} …", flush=True)

        # --- PREDECESSORES ---
        try:
            js_prev = run_sparql(q_prev(item_qid))
            rows_prev = parse_bindings(js_prev)
            # fix relation e dedupe global
            filtered_prev = []
            for (iq, ilbl, iiso3, _, rq, rlbl, riso3) in rows_prev:
                relation = "prev"
                key = (iq, relation, rq)
                if not rq or key in idx:
                    continue
                filtered_prev.append((iq, ilbl or item_lbl_hint, iiso3 or item_iso3_hint, relation, rq, rlbl, riso3))
                idx.add(key)
            if filtered_prev:
                total_new += write_rows(filtered_prev)
            time.sleep(THROTTLE_S)
        except Exception as e:
            print(f"[warn] falhou prev {item_qid}: {e}", file=sys.stderr)

        # --- SUCESSORES ---
        try:
            js_next = run_sparql(q_next(item_qid))
            rows_next = parse_bindings(js_next)
            filtered_next = []
            for (iq, ilbl, iiso3, _, rq, rlbl, riso3) in rows_next:
                relation = "next"
                key = (iq, relation, rq)
                if not rq or key in idx:
                    continue
                filtered_next.append((iq, ilbl or item_lbl_hint, iiso3 or item_iso3_hint, relation, rq, rlbl, riso3))
                idx.add(key)
            if filtered_next:
                total_new += write_rows(filtered_next)
            time.sleep(THROTTLE_S)
        except Exception as e:
            print(f"[warn] falhou next {item_qid}: {e}", file=sys.stderr)

        # marca como concluído (mesmo que não tenha relações; evita repetição desnecessária)
        append_done(item_qid)

    print(f"✔️ Terminado. Novas linhas escritas: {total_new}.")
    print(f"    Output: {OUT_CSV}")
    print(f"    Done  : {DONE_FILE}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[info] Interrompido pelo utilizador. Progresso gravado.", file=sys.stderr)
        sys.exit(130)
