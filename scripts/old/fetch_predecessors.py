# scripts/fetch_predecessors.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import csv, sys, time, argparse
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
INPUT_CSV    = DATA_DIR / "state_forms_by_class.csv"   # cls_qid;cls_label;item_qid;item_label;iso3
OUT_CSV      = DATA_DIR / "state_predecessors.csv"     # item_qid;item_label;item_iso3;related_qid;related_label;related_iso3
DONE_FILE    = DATA_DIR / "state_predecessors.done"

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT      = "GeoMundi-Predecessors/1.1 (+cfmessias@gmail.com)"
REQUEST_TIMEOUT = 90
RETRY_MAX       = 4
BACKOFF_BASE_S  = 6
THROTTLE_S      = 0.8

# === classes permitidas (CLS_STATE_LIKE) ===
CLS = """
VALUES ?cls {
  wd:Q3624078  # sovereign state
  wd:Q6256     # country
  wd:Q417175   # kingdom
  wd:Q3024240  # former country
  wd:Q48349    # empire
  wd:Q7269     # monarchy
  wd:Q7270     # republic
  wd:Q41614    # caliphate
  wd:Q184558   # sultanate
  wd:Q133156   # colony
  wd:Q170156   # confederation
  wd:Q179164   # federation
  wd:Q28108    # commonwealth
}
""".strip()

def ensure_outputs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not OUT_CSV.exists():
        with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f, delimiter=";").writerow(
                ["item_qid","item_label","item_iso3","related_qid","related_label","related_iso3"]
            )
    if not DONE_FILE.exists():
        DONE_FILE.write_text("", encoding="utf-8")

def read_input_items() -> List[Tuple[str, str, str]]:
    seen: Set[str] = set(); items: List[Tuple[str,str,str]] = []
    with INPUT_CSV.open("r", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f, delimiter=";")
        for row in r:
            qid = str(row.get("item_qid","")).strip()
            if not qid or qid in seen: continue
            seen.add(qid)
            items.append((qid, str(row.get("item_label","")).strip(), str(row.get("iso3","")).strip()))
    return items

def load_done() -> Set[str]:
    try:
        return {ln.strip() for ln in DONE_FILE.read_text(encoding="utf-8").splitlines() if ln.strip()}
    except FileNotFoundError:
        return set()

def append_done(qid: str) -> None:
    with DONE_FILE.open("a", encoding="utf-8") as f:
        f.write(qid + "\n")

def load_written_index() -> Set[Tuple[str, str]]:
    idx: Set[Tuple[str,str]] = set()
    if not OUT_CSV.exists(): return idx
    with OUT_CSV.open("r", encoding="utf-8", errors="ignore") as f:
        next(f, None)
        for line in f:
            parts = line.rstrip("\n").split(";")
            if len(parts) >= 4:
                idx.add((parts[0], parts[3]))
    return idx

def remove_item_rows(item_qid: str) -> None:
    if not OUT_CSV.exists(): return
    tmp = OUT_CSV.with_suffix(".tmp")
    with OUT_CSV.open("r", encoding="utf-8", errors="ignore") as fin, \
         tmp.open("w", newline="", encoding="utf-8") as fout:
        header = fin.readline(); fout.write(header)
        for line in fin:
            if not line.startswith(f"{item_qid};"):
                fout.write(line)
    tmp.replace(OUT_CSV)
    if DONE_FILE.exists():
        done = [ln for ln in DONE_FILE.read_text(encoding="utf-8").splitlines() if ln.strip() and ln.strip()!=item_qid]
        DONE_FILE.write_text("\n".join(done) + ("\n" if done else ""), encoding="utf-8")

def write_rows(rows: Iterable[Tuple[str,str,str,str,str,str]]) -> int:
    cnt = 0
    with OUT_CSV.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        for row in rows:
            w.writerow(row); cnt += 1
    return cnt

def run_sparql(query: str) -> Dict:
    headers = {"Accept":"application/sparql-results+json","User-Agent":USER_AGENT}
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
    raise RuntimeError("Falha após retries WDQS.")

def q_prev(item_qid: str) -> str:
    return f"""
SELECT DISTINCT ?item ?itemLabel ?itemQID ?itemISO3 ?rel ?relLabel ?relQID ?relISO3 WHERE {{
  VALUES ?item {{ wd:{item_qid} }}
  OPTIONAL {{ ?item wdt:P298 ?itemISO3 }}

  {CLS}
  {{ ?item wdt:P1365 ?rel }} UNION {{ ?item wdt:P155 ?rel }}
  ?rel wdt:P31/wdt:P279* ?cls .          # <-- restringe o relacionado ao universo CLS_STATE_LIKE
  OPTIONAL {{ ?rel wdt:P298 ?relISO3 }}

  BIND(STRAFTER(STR(?item), "entity/") AS ?itemQID)
  BIND(STRAFTER(STR(?rel),  "entity/") AS ?relQID)

  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
}}""".strip()

def parse(js: Dict) -> List[Tuple[str,str,str,str,str,str]]:
    out: List[Tuple[str,str,str,str,str,str]] = []
    for b in js.get("results", {}).get("bindings", []):
        iq  = b.get("itemQID", {}).get("value","")
        il  = b.get("itemLabel", {}).get("value","")
        iso = b.get("itemISO3", {}).get("value","")
        rq  = b.get("relQID", {}).get("value","")
        rl  = b.get("relLabel", {}).get("value","")
        riso= b.get("relISO3", {}).get("value","")
        if rq:
            out.append((iq, il, iso, rq, rl, riso))
    return out

def main() -> None:
    ap = argparse.ArgumentParser(description="Antecessores (P1365/P155) — incremental, filtrado ao universo CLS_STATE_LIKE.")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--refresh-item")
    args = ap.parse_args()

    ensure_outputs()
    items = read_input_items()
    done  = load_done()
    idx   = load_written_index()

    if args.refresh_item:
        remove_item_rows(args.refresh_item)
        done.discard(args.refresh_item)

    to_process: List[Tuple[str,str,str]] = []
    for qid, lbl, iso3 in items:
        if args.limit and len(to_process) >= args.limit: break
        if args.refresh_item:
            if qid == args.refresh_item: to_process.append((qid,lbl,iso3))
        else:
            if qid not in done: to_process.append((qid,lbl,iso3))

    new_total = 0
    for i, (qid, lbl_hint, iso_hint) in enumerate(to_process, 1):
        print(f"[{i}/{len(to_process)}] prev {qid}…", flush=True)
        try:
            js = run_sparql(q_prev(qid))
            rows = parse(js)
            filtered = []
            for (iq, il, iso, rq, rl, riso) in rows:
                key = (iq, rq)
                if key in idx: continue
                filtered.append((iq, il or lbl_hint, iso or iso_hint, rq, rl, riso))
                idx.add(key)
            if filtered:
                new_total += write_rows(filtered)
            time.sleep(THROTTLE_S)
        except Exception as e:
            print(f"[warn] falhou {qid}: {e}", file=sys.stderr)
        append_done(qid)

    print(f"✔️ predecessors: novas linhas {new_total} -> {OUT_CSV}")
    print(f"   done em: {DONE_FILE}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[info] Interrompido. Progresso gravado.", file=sys.stderr)
        sys.exit(130)
