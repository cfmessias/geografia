# scripts/fetch_conflicts_from_types.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import csv, sys, time, argparse, re
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import requests

# ---------------- Paths ----------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"

STATE_DETAILS_CSV = DATA_DIR / "state_lineage_level2_details.csv"  # Iso3Start;Level;QID;Label;FormationYear
TYPES_2COL_CSV    = DATA_DIR / "conflict_types.2col.csv"           # qid;label
OUT_CSV           = DATA_DIR / "conflicts_for_render.csv"          # ver colunas no ensure_outputs()
DONE_FILE         = DATA_DIR / "conflicts_for_render.done"         # QIDs já processados (incremental)

# ---------------- WDQS -----------------
SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT      = "GeoMundi-Conflicts/3.0 (+cfmessias@gmail.com)"
REQUEST_TIMEOUT = 90
RETRY_MAX       = 4

REL_QUERIES = {
    "P17"  : "?conflict wdt:P17  ?actor .",   # country
    "P710" : "?conflict wdt:P710 ?actor .",   # participant
    "P1344": "?actor    wdt:P1344 ?conflict ."# participant in
}

# --------------- Utils -----------------
def sniff_delim(p: Path) -> str:
    txt = p.read_text(encoding="utf-8", errors="ignore")[:4096]
    for d in (";", ",", "\t", "|"):
        if d in txt: return d
    return ";"

def run_query(q: str, timeout=REQUEST_TIMEOUT) -> Dict:
    headers = {"Accept":"application/sparql-results+json", "User-Agent":USER_AGENT}
    for attempt in range(1, RETRY_MAX+1):
        try:
            resp = requests.post(SPARQL_ENDPOINT, data={"query": q}, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            sys.stderr.write(f"[warn] HTTP {resp.status_code}: {resp.text[:200]}\n")
        except requests.RequestException as e:
            sys.stderr.write(f"[err] {type(e).__name__}: {e}\n")
        # backoff simples
        time.sleep(2 * attempt)
    raise RuntimeError("Falha WDQS")

def ensure_outputs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not OUT_CSV.exists():
        with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow([
                "mapped_iso3","entity_qid","entity_label",
                "conflict_qid","conflict_label",
                "relation","type_qid","type_label",
                "start","end","start_year","end_year"
            ])
    if not DONE_FILE.exists():
        DONE_FILE.write_text("", encoding="utf-8")

def load_done() -> Set[str]:
    try:
        return {ln.strip().upper() for ln in DONE_FILE.read_text(encoding="utf-8").splitlines() if ln.strip()}
    except FileNotFoundError:
        return set()

def append_done(qid: str) -> None:
    with DONE_FILE.open("a", encoding="utf-8") as f:
        f.write(qid.upper() + "\n")

def load_index() -> Set[Tuple[str,str,str]]:
    """Dedupe por (mapped_iso3, entity_qid, conflict_qid)."""
    idx: Set[Tuple[str,str,str]] = set()
    if OUT_CSV.exists():
        with OUT_CSV.open("r", encoding="utf-8", errors="ignore") as f:
            next(f, None)
            for line in f:
                p = line.rstrip("\n").split(";")
                if len(p) >= 4:
                    idx.add((p[0].upper(), p[1].upper(), p[3].upper()))
    return idx

def write_rows(rows: List[Tuple[str,str,str,str,str,str,str,str,str,str,str,str]]) -> None:
    with OUT_CSV.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        for r in rows:
            w.writerow(r)

def to_year(dt: str) -> str:
    s = (dt or "").strip()
    if not s: return ""
    m = re.match(r"^(-?\d{1,4})", s)
    return m.group(1) if m else ""

# ------------- Load inputs --------------
def read_states(limit: Optional[int]=None, start_offset: int=0) -> List[Tuple[str,str,str]]:
    """[(iso3, qid, label)] a partir do state_lineage_level2_details.csv (Iso3Start;QID;Label)."""
    if not STATE_DETAILS_CSV.exists():
        raise FileNotFoundError(f"Falta {STATE_DETAILS_CSV}")
    sep = sniff_delim(STATE_DETAILS_CSV)
    out: List[Tuple[str,str,str]] = []
    seen_q: Set[str] = set()
    i = 0
    with STATE_DETAILS_CSV.open("r", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f, delimiter=sep)
        for row in r:
            iso = (row.get("Iso3Start","") or "").strip().upper()
            qid = (row.get("QID","") or "").strip().upper()
            lbl = (row.get("Label","") or "").strip()
            if not iso or not qid or not qid.startswith("Q"): continue
            if qid in seen_q: continue
            seen_q.add(qid)
            if i >= start_offset:
                out.append((iso, qid, lbl))
                if limit and len(out) >= limit: break
            i += 1
    return out

def read_types_2col() -> List[Tuple[str,str]]:
    """[(type_qid, type_label)] da 2-col. Dedupe mantendo ordem."""
    if not TYPES_2COL_CSV.exists():
        raise FileNotFoundError(f"Falta {TYPES_2COL_CSV}")
    sep = sniff_delim(TYPES_2COL_CSV)
    seen: Set[str] = set()
    out: List[Tuple[str,str]] = []
    with TYPES_2COL_CSV.open("r", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f, delimiter=sep)
        # tolerância a nomes de colunas
        cols = {c.lower(): c for c in (r.fieldnames or [])}
        q_col = cols.get("qid") or list(r.fieldnames)[0]
        l_col = cols.get("label") or list(r.fieldnames)[1]
        for row in r:
            tq = (row.get(q_col,"") or "").strip().upper()
            tl = (row.get(l_col,"") or "").strip()
            if not tq.startswith("Q"): continue
            if tq in seen: continue
            seen.add(tq)
            out.append((tq, tl))
    return out

# ------------- SPARQL build -------------
def build_query(actor_qid: str, type_chunk: List[str], relation: str) -> str:
    types_vals = " ".join(f"wd:{t}" for t in type_chunk)
    rel_block  = REL_QUERIES[relation]
    return f"""
SELECT DISTINCT ?conflict ?conflictLabel ?conflictQID ?type ?typeLabel ?typeQID ?start ?end WHERE {{
  VALUES ?actor {{ wd:{actor_qid} }}
  VALUES ?type  {{ {types_vals} }}

  ?conflict wdt:P31/wdt:P279* ?type .
  {rel_block}

  OPTIONAL {{ ?conflict wdt:P580 ?start }}
  OPTIONAL {{ ?conflict wdt:P582 ?end }}
  OPTIONAL {{ ?conflict wdt:P585 ?pit }}

  BIND(STRAFTER(STR(?conflict), "entity/") AS ?conflictQID)
  BIND(STRAFTER(STR(?type),     "entity/") AS ?typeQID)

  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
}}
""".strip()

# ------------- Exec per actor -----------
def process_actor(
    iso3: str, qid: str, label: str,
    types: List[Tuple[str,str]],               # [(type_qid, type_label)]
    idx: Set[Tuple[str,str,str]],
    relations: List[str],
    chunk_size: int,
    sleep_s: float
) -> int:
    new_rows = 0

    # criar dicionários para lookup rápido de label por type_qid
    t_label_by_q: Dict[str,str] = {tq: tl for tq, tl in types}

    # lista plana só de QIDs para chunking
    type_qids: List[str] = [tq for tq, _ in types]

    # por relação (P17 / P710 / P1344)
    for rel in relations:
        # iterar por chunks
        i = 0
        while i < len(type_qids):
            chunk = type_qids[i:i+chunk_size]
            q = build_query(qid, chunk, rel)

            # adaptive fallback: se falhar, reduzir chunk
            cur_chunk = chunk
            cur_size  = len(cur_chunk)
            while True:
                try:
                    js = run_query(q)
                    break
                except Exception as e:
                    msg = str(e)
                    # se der erro, reduz chunk até 1
                    if cur_size > 1:
                        cur_size = max(1, cur_size // 2)
                        cur_chunk = type_qids[i:i+cur_size]
                        q = build_query(qid, cur_chunk, rel)
                        sys.stderr.write(f"[info] {qid} {rel}: fallback -> chunk {cur_size}\n")
                        time.sleep(sleep_s)
                        continue
                    else:
                        sys.stderr.write(f"[warn] {qid} {rel}: {e}\n")
                        js = {"results":{"bindings":[]}}
                        break

            out_rows: List[Tuple[str,str,str,str,str,str,str,str,str,str,str,str]] = []
            for b in js.get("results", {}).get("bindings", []):
                cq  = (b.get("conflictQID", {}).get("value","") or "").upper()
                cl  = (b.get("conflictLabel", {}).get("value","") or "")
                tq  = (b.get("typeQID", {}).get("value","") or "").upper()
                tl  = t_label_by_q.get(tq, b.get("typeLabel", {}).get("value",""))
                st  = (b.get("start", {}).get("value","") or "")
                en  = (b.get("end",   {}).get("value","") or "")
                if not cq: continue
                key = (iso3, qid, cq)
                if key in idx: continue
                idx.add(key)
                out_rows.append((
                    iso3, qid, label,
                    cq, cl,
                    rel, tq, tl,
                    st, en, to_year(st), to_year(en)
                ))

            if out_rows:
                write_rows(out_rows)
                new_rows += len(out_rows)

            time.sleep(sleep_s)
            i += len(chunk)  # avançar pelo chunk original, mesmo que tenha havido fallback

    return new_rows

# ---------------- Main ------------------
def main():
    ap = argparse.ArgumentParser(description="Conflitos diretos por tipo (granular, incremental).")
    ap.add_argument("--chunk-size", type=int, default=20, help="Tipos por query (reduz automaticamente se falhar).")
    ap.add_argument("--sleep", type=float, default=0.8, help="Pausa mínima entre pedidos.")
    ap.add_argument("--relations", default="P17,P710,P1344", help="Relações a usar, separadas por vírgulas.")
    ap.add_argument("--limit", type=int, help="Limitar nº de atores (para testes).")
    ap.add_argument("--offset", type=int, default=0, help="Offset de atores (para retomar manualmente).")
    ap.add_argument("--refresh-qid", help="Refazer um QID específico (remove do .done).")
    args = ap.parse_args()

    ensure_outputs()
    idx  = load_index()
    done = load_done()

    # carregar seed de estados
    states = read_states(limit=args.limit, start_offset=args.offset)

    # refresh de um QID específico (não mexe no OUT, só remove do done)
    if args.refresh_qid:
        rq = args.refresh_qid.strip().upper()
        done.discard(rq)
        DONE_FILE.write_text("\n".join(sorted(done)) + ("\n" if done else ""), encoding="utf-8")

    # carregar tipos (2 colunas)
    types = read_types_2col()

    # relações válidas
    relations = [r.strip().upper() for r in args.relations.split(",") if r.strip().upper() in REL_QUERIES]
    if not relations:
        relations = ["P17"]

    total_new = 0
    for i, (iso3, qid, label) in enumerate(states, 1):
        if qid in done:
            continue
        print(f"[{i}/{len(states)}] {iso3} — {qid} — {label}", flush=True)
        try:
            total_new += process_actor(
                iso3=iso3, qid=qid, label=label,
                types=types, idx=idx,
                relations=relations, chunk_size=args.chunk_size, sleep_s=args.sleep
            )
        except Exception as e:
            sys.stderr.write(f"[warn] actor {qid}: {e}\n")
        append_done(qid)

    print(f"✔️ Concluído. Novas linhas: {total_new} -> {OUT_CSV}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[info] Interrompido. Progresso gravado.", file=sys.stderr)
        sys.exit(130)
