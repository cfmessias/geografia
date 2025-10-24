# scripts/fetch_conflict_countries.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import csv, sys, time, argparse, requests
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
CATALOG  = DATA_DIR / "conflict_catalog.csv"        # conflict_qid;conflict_label;...
OUT_CSV  = DATA_DIR / "conflict_countries.csv"      # conflict_qid;conflict_label;country_qid;country_label
DONE     = DATA_DIR / "conflict_countries.done"     # QIDs processados com sucesso (SPARQL ou JSON)
FAILED   = DATA_DIR / "conflict_countries.failed"   # QIDs que falharam (rede/erro)
EMPTY_CSV= DATA_DIR / "conflict_countries.empty.csv"# conflitos sem P17 (vazios)

SPARQL  = "https://query.wikidata.org/sparql"
USERAG  = "GeoMundi-CCountries/1.2 (+cfmessias@gmail.com)"
TIMEOUT = 90

def sniff_delim(p: Path) -> str:
    txt = p.read_text(encoding="utf-8", errors="ignore")[:4096]
    for d in (";", ",", "\t", "|"):
        if d in txt: return d
    return ";"

def ensure_outputs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not OUT_CSV.exists():
        with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f, delimiter=";").writerow(
                ["conflict_qid","conflict_label","country_qid","country_label"]
            )
    if not EMPTY_CSV.exists():
        with EMPTY_CSV.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f, delimiter=";").writerow(
                ["conflict_qid","conflict_label","checked_via"]
            )
    for p in (DONE, FAILED):
        if not p.exists(): p.write_text("", encoding="utf-8")

def load_done() -> Set[str]:
    return {ln.strip().upper() for ln in DONE.read_text(encoding="utf-8").splitlines() if ln.strip()} if DONE.exists() else set()

def append_done(qid: str):
    with DONE.open("a", encoding="utf-8") as f: f.write(qid.upper()+"\n")

def append_failed(qid: str, msg: str=""):
    with FAILED.open("a", encoding="utf-8") as f: f.write(qid.upper()+(" | "+msg if msg else "")+"\n")

def append_empty(qid: str, clabel: str, via: str):
    with EMPTY_CSV.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f, delimiter=";").writerow([qid, clabel, via])

def read_catalog(limit: Optional[int], offset: int) -> List[Tuple[str,str]]:
    if not CATALOG.exists(): raise FileNotFoundError(f"Falta {CATALOG}")
    sep = sniff_delim(CATALOG)
    rows: List[Tuple[str,str]] = []
    with CATALOG.open("r", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f, delimiter=sep)
        for i, row in enumerate(r):
            if i < offset: continue
            q = (row.get("conflict_qid") or "").strip().upper()
            l = (row.get("conflict_label") or "").strip()
            if q.startswith("Q"):
                rows.append((q, l))
                if limit and len(rows) >= limit: break
    return rows

def sparql_countries(qid: str) -> List[Tuple[str,str]]:
    q = f"""
SELECT ?country ?countryLabel WHERE {{
  VALUES ?conflict {{ wd:{qid} }}
  ?conflict wdt:P17 ?country .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
}}
"""
    headers = {"Accept":"application/sparql-results+json", "User-Agent": USERAG}
    resp = requests.get(SPARQL, params={"query": q}, headers=headers, timeout=TIMEOUT)
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}")
    js = resp.json()
    out: List[Tuple[str,str]] = []
    for b in js.get("results",{}).get("bindings", []):
        uri = b.get("country", {}).get("value", "")
        lbl = b.get("countryLabel", {}).get("value", "")
        if not uri or not lbl: continue
        cq = uri.rsplit("/",1)[-1].upper()
        if cq.startswith("Q") and lbl.upper() != cq:
            out.append((cq, lbl))
    return out

def json_countries(qid: str) -> List[Tuple[str,str]]:
    url = f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
    js = requests.get(url, headers={"User-Agent": USERAG}, timeout=TIMEOUT).json()
    ent = js["entities"][qid]
    claims = ent.get("claims", {})
    vals: List[str] = []
    for st in claims.get("P17", []):
        try:
            tgt = st["mainsnak"]["datavalue"]["value"]["id"]
            if isinstance(tgt, str) and tgt.startswith("Q"):
                vals.append(tgt.upper())
        except Exception:
            continue
    out: List[Tuple[str,str]] = []
    for v in vals:
        lbl = ""
        if v in js.get("entities", {}):
            labd = js["entities"][v].get("labels", {})
            lbl = labd.get("pt", labd.get("en", {})).get("value", "")
        if not lbl:
            j2 = requests.get(f"https://www.wikidata.org/wiki/Special:EntityData/{v}.json",
                              headers={"User-Agent": USERAG}, timeout=TIMEOUT).json()
            labd = j2["entities"][v].get("labels", {})
            lbl = labd.get("pt", labd.get("en", {})).get("value", "")
        if lbl and lbl.upper() != v:
            out.append((v, lbl))
    return out

def fmt_eta(done, total, start_t):
    if done == 0: return "ETA --:--"
    rate = done / max(1, (time.perf_counter() - start_t))
    remain = total - done
    secs = int(remain / max(rate, 1e-9))
    m, s = divmod(secs, 60)
    return f"ETA {m:02d}:{s:02d}"

def main():
    ap = argparse.ArgumentParser(description="Extrai países (P17) por conflito, com fallback JSON, ticker e registo de vazios.")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--offset", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=0.6)
    ap.add_argument("--no-ticker", action="store_true", help="não atualizar a mesma linha; imprime linhas normais")
    args = ap.parse_args()

    ensure_outputs()
    done_set = load_done()
    todo_all  = read_catalog(args.limit, args.offset)
    todo = [(q,l) for (q,l) in todo_all if q not in done_set]
    total = len(todo)
    if total == 0:
        print("Nada a fazer (todos em .done)."); return

    written = 0
    ok = 0
    empty = 0
    fails = 0
    t0 = time.perf_counter()

    with OUT_CSV.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        for i, (qid, clabel) in enumerate(todo, 1):
            status = ""
            try:
                rows = sparql_countries(qid)
                via = "sparql"
                if not rows:
                    rows = json_countries(qid)
                    via = "both"  # tentou sparql e json
                if rows:
                    for cq, lbl in rows:
                        w.writerow([qid, clabel, cq, lbl])
                    written += len(rows)
                    ok += 1
                    status = "ok"
                else:
                    empty += 1  # consultado mas sem P17
                    append_empty(qid, clabel, via)
                    status = "vazio"
                append_done(qid)
            except Exception as e:
                fails += 1
                append_failed(qid, f"{type(e).__name__}:{e}")
                status = "falha"

            if args.no_ticker:
                print(f"[{i}/{total}] {qid} — {status}")
            else:
                pct = int(i * 100 / total)
                eta = fmt_eta(i, total, t0)
                msg = f"\r[{i}/{total}] {pct:3d}%  ok:{ok} vazios:{empty} falhas:{fails} linhas:{written}  {eta}"
                sys.stdout.write(msg); sys.stdout.flush()

            time.sleep(args.sleep)

    if not args.no_ticker:
        print()  # quebra de linha do ticker
    elapsed = int(time.perf_counter() - t0)
    mm, ss = divmod(elapsed, 60)
    print(f"✔️ países → {OUT_CSV} | linhas:{written} | ok:{ok} vazios:{empty} falhas:{fails} | tempo {mm:02d}:{ss:02d}")

if __name__ == "__main__":
    main()
