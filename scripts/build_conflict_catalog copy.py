# scripts/build_conflict_catalog.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import csv, sys, time, argparse, re
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import requests

# === Paths ===
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"

TYPES_CSV    = DATA_DIR / "conflict_types.2col.csv"      # type_qid;type_label
MILMAP_CSV   = DATA_DIR / "conflict_types.military.csv"  # type_qid;is_military
OVR_CSV      = DATA_DIR / "conflict_catalog.overrides.csv"  # conflict_qid;is_military (opcional)

OUT_CSV      = DATA_DIR / "conflict_catalog.csv"

# === SPARQL setup ===
ENDPOINT     = "https://query.wikidata.org/sparql"
USER_AGENT   = "GeoMundi-BuildConflictCatalog/1.0 (+contact@example.com)"
BATCH_SIZE   = 1   # consulta por tipo (estável e gentil para o endpoint)
RETRY_MAX    = 5
RETRY_SLEEP  = 2.0

# === Palavras-chave de classificação ===
# NÃO militar (PT/EN + alguns falsos amigos)
NONMIL_HINTS = {
    "trial","court","lawsuit","legal case","tribunal","process","processo","julgamento",
    "peace treaty","treaty","agreement","accord","armistice","convention",
    "conference","negotiation","peace","ceasefire","parley","diplomatic","diplomático",
    "election","referendum","plebiscite"
}
NONMIL_EXACT = {  # falsos amigos que contêm 'war'
    "trade war","currency war","culture war","price war","information war","war of words"
}

# militar (PT/EN)
MIL_HINTS = {
    "armed conflict","military conflict","war","civil war","battle","battle of","siege",
    "campaign","operation","military operation","offensive","counteroffensive","invasion",
    "intervention","naval battle","naval operation","airstrike","air raid",
    "bombing","bombardment","shelling","uprising","rebellion","insurgency",
    "coup","coup d'état","clash","skirmish","raid","massacre",
    "guerra","batalha","cerco","campanha","operação","ofensiva","invasão",
    "intervenção","batalha naval","bombardeamento","levantamento","rebelião",
    "insurgência","golpe de estado","confronto","escaramuça","incursão","massacre"
}

# === Utilitários ===

def log(msg: str) -> None:
    print(msg, flush=True)

def sniff_delim(path: Path) -> str:
    sample = path.read_text(encoding="utf-8", errors="ignore")[:4096]
    for d in (";", ",", "\t", "|"):
        if d in sample:
            return d
    return ";"

def run_sparql(query: str) -> dict:
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": USER_AGENT
    }
    last_exc: Optional[Exception] = None
    for attempt in range(1, RETRY_MAX + 1):
        try:
            r = requests.post(ENDPOINT, data={"query": query}, headers=headers, timeout=90)
            if r.status_code == 200:
                return r.json()
            # 429 / 502 / 503 / 504 or other
            last_exc = Exception(f"HTTP {r.status_code}")
        except requests.RequestException as e:
            last_exc = e
        time.sleep(RETRY_SLEEP * attempt)
    raise RuntimeError(f"Falha SPARQL após {RETRY_MAX} tentativas: {last_exc}")

def load_types() -> List[Tuple[str, str]]:
    if not TYPES_CSV.exists():
        raise FileNotFoundError(f"Ficheiro não encontrado: {TYPES_CSV}")
    sep = sniff_delim(TYPES_CSV)
    out: List[Tuple[str,str]] = []
    with TYPES_CSV.open("r", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f, delimiter=sep)
        cols = {c.lower(): c for c in (r.fieldnames or [])}
        qcol = cols.get("type_qid") or cols.get("qid") or list(cols.values())[0]
        lcol = cols.get("type_label") or cols.get("label") or (list(cols.values())[1] if len(cols)>1 else None)
        for row in r:
            q = (row.get(qcol, "") or "").strip().upper()
            l = (row.get(lcol, "") or "").strip()
            if q.startswith("Q"):
                out.append((q, l))
    if not out:
        raise ValueError("conflict_types.2col.csv não trouxe QIDs válidos.")
    return out

def load_milmap() -> Dict[str, str]:
    mp: Dict[str,str] = {}
    if not MILMAP_CSV.exists():
        return mp
    sep = sniff_delim(MILMAP_CSV)
    with MILMAP_CSV.open("r", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f, delimiter=sep)
        cols = {c.lower(): c for c in (r.fieldnames or [])}
        qcol = cols.get("type_qid") or cols.get("qid") or list(cols.values())[0]
        vcol = cols.get("is_military")  or (list(cols.values())[1] if len(cols)>1 else None)
        if not vcol:
            return mp
        for row in r:
            q = (row.get(qcol, "") or "").strip().upper()
            v = (row.get(vcol, "") or "").strip().lower()
            if q.startswith("Q") and v in {"military","non-military"}:
                mp[q] = v
    return mp

def load_overrides() -> Dict[str, str]:
    mp: Dict[str,str] = {}
    if not OVR_CSV.exists():
        return mp
    sep = sniff_delim(OVR_CSV)
    with OVR_CSV.open("r", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f, delimiter=sep)
        cols = {c.lower(): c for c in (r.fieldnames or [])}
        qcol = cols.get("conflict_qid") or list(cols.values())[0]
        vcol = cols.get("is_military")  or (list(cols.values())[1] if len(cols)>1 else None)
        if not vcol:
            return mp
        for row in r:
            q = (row.get(qcol, "") or "").strip().upper()
            v = (row.get(vcol, "") or "").strip().lower()
            if q.startswith("Q") and v in {"military","non-military"}:
                mp[q] = v
    return mp

def classify_military(conflict_qid: str, type_qid: str, type_label: str, conflict_label: str,
                      milmap: Dict[str, str], overrides: Dict[str, str]) -> str:
    # 1) override por CONFLITO
    cq = (conflict_qid or "").strip().upper()
    if cq in overrides:
        return overrides[cq]
    # 2) mapa por TIPO
    v = milmap.get((type_qid or "").strip().upper())
    if v in {"military","non-military"}:
        return v
    # 3) heurística conservadora
    tl = (type_label or "").lower()
    cl = (conflict_label or "").lower()
    combo = f"{tl} | {cl}"
    if any(x in combo for x in NONMIL_EXACT):
        return "non-military"
    if any(x in combo for x in NONMIL_HINTS):
        return "non-military"
    if any(x in combo for x in MIL_HINTS):
        return "military"
    # 4) sem indícios → vazio
    return ""

# === SPARQL builder ===

SPARQL_TEMPLATE = """
SELECT ?item ?itemLabel ?type ?typeLabel ?start ?end ?pit WHERE {{
  VALUES ?type {{ wd:{type_qid} }}
  ?item wdt:P31 ?type .
  OPTIONAL {{ ?item wdt:P580 ?start. }}
  OPTIONAL {{ ?item wdt:P582 ?end. }}
  OPTIONAL {{ ?item wdt:P585 ?pit. }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,pt,fr,es,de" }}
}}
"""

def build_query(type_qid: str) -> str:
    return SPARQL_TEMPLATE.format(type_qid=type_qid)

def parse_time(t: Optional[str]) -> Optional[str]:
    # t vem como literal xsd:dateTime, e.g. "1945-11-20T00:00:00Z"
    s = (t or "").strip()
    return s or None

def year_or_none(dt: Optional[str]) -> Optional[int]:
    if not dt:
        return None
    m = re.match(r"^(-?\d{1,4})", dt)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None

def earliest_year_of(start: Optional[str], pit: Optional[str]) -> Optional[int]:
    ys = [y for y in (year_or_none(start), year_or_none(pit)) if y is not None]
    return min(ys) if ys else None

def latest_year_of(end: Optional[str], pit: Optional[str]) -> Optional[int]:
    ys = [y for y in (year_or_none(end), year_or_none(pit)) if y is not None]
    return max(ys) if ys else None

# === Escrita CSV ===

HEADER = [
    "conflict_qid","conflict_label",
    "type_qid","type_label",
    "start","end","point_in_time",
    "earliest_year","latest_year",
    "is_military",
]

def write_rows(rows: List[List[str]], *, append: bool) -> None:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if append and OUT_CSV.exists() else "w"
    with OUT_CSV.open(mode, encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        if mode == "w":
            w.writerow(HEADER)
        w.writerows(rows)

# === Principal ===

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Constrói data/conflict_catalog.csv a partir de tipos e Wikidata, classificando is_military na origem."
    )
    ap.add_argument("--append", action="store_true", help="Acrescenta ao ficheiro existente (default: reescreve se não existir).")
    ap.add_argument("--rebuild", action="store_true", help="Apaga o conflict_catalog.csv antes de escrever.")
    ap.add_argument("--limit-per-type", type=int, default=0, help="Limite de registos por tipo (0 = sem limite).")
    args = ap.parse_args()

    if args.rebuild and OUT_CSV.exists():
        OUT_CSV.unlink()
        log(f"[segurança] Apaguei {OUT_CSV} para reconstrução.")

    types = load_types()              # [(type_qid, type_label)]
    milmap = load_milmap()            # {type_qid: 'military'|'non-military'}
    overrides = load_overrides()      # {conflict_qid: 'military'|'non-military'}
    log(f"[info] tipos={len(types)} | milmap={len(milmap)} | overrides={len(overrides)}")

    total_rows = 0
    all_rows: List[List[str]] = []

    for idx, (type_qid, type_label) in enumerate(types, 1):
        q = build_query(type_qid)
        log(f"[{idx}/{len(types)}] tipo {type_qid} — '{type_label or ''}'")
        try:
            js = run_sparql(q)
        except Exception as e:
            log(f"[erro] SPARQL tipo {type_qid}: {e}")
            continue

        bindings = js.get("results", {}).get("bindings", [])
        rows_for_type: List[List[str]] = []

        for b in bindings[: args.limit_per_type or None]:
            item_uri = b.get("item", {}).get("value", "")
            conflict_qid = item_uri.rsplit("/", 1)[-1] if item_uri else ""
            conflict_label = b.get("itemLabel", {}).get("value", "")
            start = parse_time(b.get("start", {}).get("value"))
            end   = parse_time(b.get("end", {}).get("value"))
            pit   = parse_time(b.get("pit", {}).get("value"))

            ey = earliest_year_of(start, pit)
            ly = latest_year_of(end, pit)

            is_mil = classify_military(
                conflict_qid=conflict_qid,
                type_qid=type_qid,
                type_label=type_label,
                conflict_label=conflict_label,
                milmap=milmap,
                overrides=overrides
            )

            rows_for_type.append([
                conflict_qid, conflict_label,
                type_qid, type_label,
                start or "", end or "", pit or "",
                "" if ey is None else str(ey),
                "" if ly is None else str(ly),
                is_mil
            ])

        # Dedup por conflito dentro do mesmo tipo (mantém o mais “antigo” em caso de repetição)
        seen: Dict[str, Tuple[int, List[str]]] = {}
        for r in rows_for_type:
            qid = r[0]
            ey = int(r[7]) if r[7] else 10**9
            prev = seen.get(qid)
            if prev is None or ey < prev[0]:
                seen[qid] = (ey, r)
        out_rows = [t[1] for t in seen.values()]

        total_rows += len(out_rows)
        all_rows.extend(out_rows)
        log(f"  → obtidos {len(bindings)} | após dedup p/tipo: {len(out_rows)} | acumulado: {total_rows}")

        # escrita incremental para não acumular muito em memória
        if len(all_rows) >= 5000:
            write_rows(all_rows, append=args.append or OUT_CSV.exists())
            args.append = True
            all_rows.clear()

    # flush final
    if all_rows:
        write_rows(all_rows, append=args.append or OUT_CSV.exists())
        all_rows.clear()

    log(f"✔️ Escrevi {OUT_CSV}")

if __name__ == "__main__":
    main()
