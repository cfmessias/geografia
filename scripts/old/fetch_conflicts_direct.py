# scripts/fetch_conflicts_direct.py
from __future__ import annotations
import csv, sys, time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import requests

DATA_DIR      = Path(__file__).resolve().parent.parent / "data"
SEED_DETAILS  = DATA_DIR / "state_lineage_level2_details.csv"  # Iso3Start;Level;QID;Label;FormationYear
TYPES_CSV     = DATA_DIR / "conflict_types.csv"                # type_qid;type_label;root_qid;root_label
STATE_CSV     = DATA_DIR / "state_forms_by_class.csv"          # cls_qid;cls_label;item_qid;item_label;iso3
OUT_CSV       = DATA_DIR / "conflicts_direct.csv"              # Iso3;ActorQID;ActorLabel;ActorIso3;ConflictQID;ConflictLabel;TypeQID;TypeLabel;Start;End;Tag
DONE_FILE     = DATA_DIR / "conflicts_direct.done"

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
UA = "GeoMundi-ConflictsDirect/1.2 (+cfmessias@gmail.com)"

# tag -> (min_date, max_date). Ajusta/ordena como preferires
DATE_TAGS = {
    "all":    (None,         None),
    "lt1900": (None,         "1900-01-01"),
    "ge1900": ("1900-01-01", None),
}

def sniff_delim(p: Path) -> str:
    sample = p.read_text(encoding="utf-8", errors="ignore")[:4096]
    for d in (";", ",", "\t", "|"):
        if d in sample:
            return d
    return ";"

def run(q: str) -> Dict:
    for i in range(4):
        try:
            r = requests.post(
                SPARQL_ENDPOINT, data={"query": q},
                headers={"Accept":"application/sparql-results+json","User-Agent":UA},
                timeout=90
            )
            if r.status_code == 200:
                return r.json()
            sys.stderr.write(f"[warn] HTTP {r.status_code}: {r.text[:200]}\n")
        except Exception as e:
            sys.stderr.write(f"[err] {e}\n")
        time.sleep(2*(i+1))
    raise RuntimeError("Falha WDQS")

def read_seed_qids() -> List[Tuple[str,str]]:
    seen: Set[str] = set()
    out: List[Tuple[str,str]] = []
    sep = sniff_delim(SEED_DETAILS)
    with SEED_DETAILS.open("r", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f, delimiter=sep)
        for row in r:
            iso = (row.get("Iso3Start","") or "").strip().upper()
            qid = (row.get("QID","") or "").strip().upper()
            if not iso or not qid or not qid.startswith("Q"):
                continue
            if qid in seen:
                continue
            seen.add(qid)
            out.append((iso, qid))
    return out

def read_types() -> List[str]:
    if not TYPES_CSV.exists():
        raise FileNotFoundError(f"Falta {TYPES_CSV}. Corre primeiro build_conflict_types.py")
    sep = sniff_delim(TYPES_CSV)
    types: List[str] = []
    with TYPES_CSV.open("r", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f, delimiter=sep)
        for row in r:
            t = (row.get("type_qid","") or "").strip().upper()
            if t.startswith("Q"):
                types.append(t)
    # dedupe mantendo ordem
    seen: Set[str] = set(); out=[]
    for t in types:
        if t not in seen:
            seen.add(t); out.append(t)
    return out

def read_actor_maps() -> Tuple[Dict[str,str], Dict[str,str]]:
    """Devolve (qid->label, qid->iso3)."""
    if not STATE_CSV.exists():
        return {}, {}
    sep = sniff_delim(STATE_CSV)
    qid_to_label: Dict[str,str] = {}
    qid_to_iso3: Dict[str,str] = {}
    with STATE_CSV.open("r", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f, delimiter=sep)
        # nomes comuns
        cols = {c.lower(): c for c in (r.fieldnames or [])}
        qid_col   = cols.get("item_qid") or cols.get("qid")
        label_col = cols.get("item_label") or cols.get("label")
        iso_col   = cols.get("iso3")
        if not qid_col:
            return {}, {}
        for row in r:
            qid = (row.get(qid_col,"") or "").strip().upper()
            if not qid:
                continue
            if label_col:
                lbl = (row.get(label_col,"") or "").strip()
                if lbl and qid not in qid_to_label:
                    qid_to_label[qid] = lbl
            if iso_col:
                iso = (row.get(iso_col,"") or "").strip().upper()
                if iso and qid not in qid_to_iso3:
                    qid_to_iso3[qid] = iso
    return qid_to_label, qid_to_iso3

def build_query(actor_qid: str, type_list: List[str], min_date: Optional[str], max_date: Optional[str]) -> str:
    types_vals = " ".join(f"wd:{t}" for t in type_list)
    date_filters = []
    if min_date:
        date_filters.append(
            f'((BOUND(?start) && ?start >= "{min_date}"^^xsd:dateTime) || '
            f'(BOUND(?end) && ?end >= "{min_date}"^^xsd:dateTime) || '
            f'(BOUND(?pit) && ?pit >= "{min_date}"^^xsd:dateTime))'
        )
    if max_date:
        date_filters.append(
            f'((BOUND(?start) && ?start < "{max_date}"^^xsd:dateTime) || '
            f'(BOUND(?end) && ?end < "{max_date}"^^xsd:dateTime) || '
            f'(BOUND(?pit) && ?pit < "{max_date}"^^xsd:dateTime))'
        )
    date_block = f"FILTER({' && '.join(date_filters)})" if date_filters else ""

    return f"""
SELECT DISTINCT ?conflict ?conflictLabel ?conflictQID ?type ?typeLabel ?typeQID ?start ?end WHERE {{
  VALUES ?actor {{ wd:{actor_qid} }}
  VALUES ?type {{ {types_vals} }}

  ?conflict wdt:P31/wdt:P279* ?type .

  {{
    ?conflict wdt:P710 ?actor .        # participant
  }} UNION {{
    ?actor wdt:P1344 ?conflict .       # participant in
  }} UNION {{
    ?conflict wdt:P17 ?actor .         # country
  }}

  OPTIONAL {{ ?conflict wdt:P580 ?start }}
  OPTIONAL {{ ?conflict wdt:P582 ?end }}
  OPTIONAL {{ ?conflict wdt:P585 ?pit }}

  {date_block}

  BIND(STRAFTER(STR(?conflict), "entity/") AS ?conflictQID)
  BIND(STRAFTER(STR(?type),     "entity/") AS ?typeQID)

  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
}}
""".strip()

def ensure_outputs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not OUT_CSV.exists():
        with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f, delimiter=";").writerow(
                ["Iso3","ActorQID","ActorLabel","ActorIso3","ConflictQID","ConflictLabel","TypeQID","TypeLabel","Start","End","Tag"]
            )
    if not DONE_FILE.exists():
        DONE_FILE.write_text("", encoding="utf-8")

def load_done() -> Set[str]:
    try:
        return {ln.strip().upper() for ln in DONE_FILE.read_text(encoding="utf-8").splitlines() if ln.strip()}
    except FileNotFoundError:
        return set()

def append_done(qid: str):
    with DONE_FILE.open("a", encoding="utf-8") as f:
        f.write(qid.upper()+"\n")

def load_index() -> Set[Tuple[str,str,str]]:
    idx: Set[Tuple[str,str,str]] = set()
    if not OUT_CSV.exists(): return idx
    with OUT_CSV.open("r", encoding="utf-8", errors="ignore") as f:
        next(f, None)
        for line in f:
            p = line.rstrip("\n").split(";")
            if len(p) >= 5:
                idx.add((p[0].upper(), p[1].upper(), p[4].upper()))  # (Iso3, ActorQID, ConflictQID)
    return idx

def write_rows(rows: List[Tuple[str,str,str,str,str,str,str,str,str,str,str]]):
    with OUT_CSV.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        for r in rows:
            w.writerow(r)

def main():
    ensure_outputs()
    seeds = read_seed_qids()
    types = read_types()
    qid_to_label, qid_to_iso3 = read_actor_maps()
    done  = load_done()
    idx   = load_index()

    total_new = 0
    for i,(iso3, actor_qid) in enumerate(seeds, 1):
        if actor_qid in done:
            continue
        print(f"[{i}/{len(seeds)}] {iso3} — {actor_qid}", flush=True)

        actor_label = qid_to_label.get(actor_qid, "")
        actor_iso3  = qid_to_iso3.get(actor_qid, "")  # pode ficar vazio para entidades históricas

        for tag, (min_d, max_d) in DATE_TAGS.items():
            try:
                q = build_query(actor_qid, types, min_d, max_d)
                js = run(q)
                out_rows: List[Tuple[str,str,str,str,str,str,str,str,str,str,str]] = []
                for b in js.get("results", {}).get("bindings", []):
                    cq  = b.get("conflictQID", {}).get("value","").upper()
                    cl  = b.get("conflictLabel", {}).get("value","")
                    tq  = b.get("typeQID", {}).get("value","").upper()
                    tl  = b.get("typeLabel", {}).get("value","")
                    st  = b.get("start", {}).get("value","")
                    en  = b.get("end", {}).get("value","")
                    if not cq:
                        continue
                    key = (iso3, actor_qid, cq)
                    if key in idx:
                        continue
                    idx.add(key)
                    out_rows.append((iso3, actor_qid, actor_label, actor_iso3, cq, cl, tq, tl, st, en, tag))
                if out_rows:
                    write_rows(out_rows)
                    total_new += len(out_rows)
                time.sleep(0.8)
            except Exception as e:
                print(f"[warn] {actor_qid} {tag}: {e}", file=sys.stderr)

        append_done(actor_qid)

    print(f"✔️ Concluído. Novas linhas: {total_new} -> {OUT_CSV}")

if __name__ == "__main__":
    main()
