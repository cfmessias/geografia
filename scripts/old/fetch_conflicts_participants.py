# scripts/fetch_conflicts_participants.py
# -*- coding: utf-8 -*-
"""
Extrai PARTICIPANTES (P710) por conflito, em 6 runs independentes (TYPE_LIST).
Mantém as características do conflito:
  - label e data canónica do conflito (COALESCE P585,P580,P582) aplicadas a todas as linhas.
Na query de participantes:
  - descobre país do participante (caminhos: self país/estado; P17; P495; P131→P17; P3842)
  - tenta ISO3 no mesmo passo; se falhar, usa forms_all.csv (iso3;qid) para mapear country_qid → ISO3.
Escreve SEMPRE por cima: data/conflicts_participants.csv
"""

from __future__ import annotations
from pathlib import Path
import csv, time, random, sys, os, re
from typing import Iterable, List, Dict

import requests
import pandas as pd

# ---------- paths ----------
def _find_project_root() -> Path:
    here = Path(__file__).resolve()
    for p in here.parents:
        if (p / "data").exists():
            return p
    return here.parents[2]

PROJECT_ROOT = _find_project_root()
DATA_DIR     = PROJECT_ROOT / "data"
OUT_CSV      = DATA_DIR / "conflicts_participants.csv"
OUT_WORK     = OUT_CSV.with_suffix(".tmp.csv")
FORMS_CSV    = DATA_DIR / "forms_all.csv"           # iso3;qid
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ---------- config ----------
USER_AGENT = "GeoMundi/fetch-conflicts-participants/2.0 (+contact: you@example.com)"
WDQS_URL   = "https://query.wikidata.org/sparql"

PAGE_SIZE_CONFLICTS = 10000  # paginação de IDs
BATCH_VALUES        = 200    # nº de QIDs por VALUES (participantes / meta)
SLEEP_BASE          = 0.6
MAX_RETRIES         = 6
TIMEOUT_S           = 120

TYPE_LIST = [
    ("Q198",    "all",    None,         None        ),
    ("Q178561", "all",    None,         None        ),
    ("Q645883", "all",    None,         None        ),
    ("Q350604", "all",    None,         None        ),
    ("Q180684", "lt1900", None,         "1900-01-01"),
    ("Q180684", "ge1900", "1900-01-01", None        ),
]

HEADER = [
    "conflict_qid","conflict_label",
    "participant_qid","participant_label",
    "point_in_time","type_qid","window",
    "mapped_country_qid","mapped_country_label",
    "mapped_iso3","mapped_iso3_source","is_human"
]

# ---------- utils ----------
_QID_RE = re.compile(r"^Q\d+$")

def chunks(seq: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def extract_qid(value: str) -> str:
    if not value: return ""
    s = str(value).strip()
    if s.startswith("<") and s.endswith(">"): s = s[1:-1].strip()
    if "/" in s: s = s.rsplit("/", 1)[-1]
    s = s.strip('>"\' \t\r\n')
    return s if _QID_RE.match(s) else ""

def http_post(query: str, accept: str):
    headers = {"User-Agent": USER_AGENT, "Accept": accept, "Connection": "close"}
    return requests.post(WDQS_URL, data={"query": query}, headers=headers, timeout=TIMEOUT_S)

def with_retry_fetch(query: str, accept_json_first=True):
    last_exc = None
    for attempt in range(1, MAX_RETRIES+1):
        try:
            if accept_json_first:
                r = http_post(query, "application/sparql-results+json; charset=utf-8")
                if r.status_code == 200:
                    try: return ("json", r.json())
                    except Exception: pass
            r2 = http_post(query, "text/tab-separated-values; charset=utf-8")
            if r2.status_code == 200 and r2.text:
                return ("tsv", r2.text)
            code = r.status_code if accept_json_first else r2.status_code
            txt  = (r.text if accept_json_first else r2.text)[:160]
            raise RuntimeError(f"HTTP {code}: {txt}")
        except Exception as e:
            last_exc = e
            wait = min(60, 1.7**attempt) + random.uniform(0, 0.9)
            print(f"[warn] {type(e).__name__}: {e} → retry {attempt}/{MAX_RETRIES} in {wait:.1f}s")
            time.sleep(wait)
    raise RuntimeError(f"Max retries exceeded: {last_exc}")

def init_output():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for p in (OUT_WORK, OUT_CSV):
        try:
            p.unlink(missing_ok=True)
        except TypeError:
            if p.exists(): p.unlink()
    with OUT_WORK.open("w", newline="", encoding="utf-8") as f:
        csv.writer(f, delimiter=";").writerow(HEADER)

def append_rows(rows):
    if not rows: return
    with OUT_WORK.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f, delimiter=";").writerows(rows)

MW_API = "https://www.wikidata.org/w/api.php"

def fetch_labels_mw(qids: list[str], lang: str = "pt|en", batch: int = 200) -> dict[str, str]:
    out: dict[str, str] = {}
    sess = requests.Session()
    sess.headers.update({"User-Agent": USER_AGENT})
    for blk in chunks(qids, batch):
        params = {
            "action": "wbgetentities",
            "ids": "|".join(blk),
            "props": "labels",
            "languages": lang,
            "format": "json",
        }
        try:
            r = sess.get(MW_API, params=params, timeout=60)
            r.raise_for_status()
            js = r.json().get("entities", {})
            for q, ent in js.items():
                lbs = ent.get("labels", {}) or {}
                # preferir pt, cair para en
                v = lbs.get("pt", {}).get("value") or lbs.get("en", {}).get("value") or ""
                if v:
                    out[q] = v
        except Exception as e:
            print(f"[warn] labels batch falhou: {e}")
        time.sleep(0.25 + random.random()*0.4)
    return out

# ---------- ISO3 fallback via forms_all.csv ----------
def load_qid_to_iso3_from_forms() -> Dict[str, str]:
    """
    Devolve {country_qid -> ISO3} usando forms_all.csv (iso3;qid).
    """
    m: Dict[str, str] = {}
    if not FORMS_CSV.exists():
        return m
    try:
        df = pd.read_csv(FORMS_CSV, sep=";", dtype=str, encoding="utf-8-sig").fillna("")
        cols = {c.lower(): c for c in df.columns}
        icol = cols.get("iso3")
        qcol = cols.get("qid") or cols.get("form_qid")
        if not icol or not qcol:
            return m
        for _, r in df.iterrows():
            iso = str(r[icol]).strip().upper()
            qid = str(r[qcol]).strip()
            if iso and len(iso) == 3 and qid.startswith("Q"):
                if qid not in m:
                    m[qid] = iso
    except Exception:
        pass
    return m

# ---------- queries ----------
HINT_TIMEOUT = '  hint:Query hint:timeout "60000".'

def q_conflicts_ids(type_qid: str, date_from: str|None, date_to: str|None, limit: int, offset: int) -> str:
    date_lines = [
        "OPTIONAL { ?conflict wdt:P585 ?pit0 . }",
        "OPTIONAL { ?conflict wdt:P580 ?start . }",
        "OPTIONAL { ?conflict wdt:P582 ?end   . }",
        "BIND(COALESCE(?pit0, ?start, ?end) AS ?pit)"
    ]
    if date_from:
        date_lines.append(f'FILTER(?pit >= "{date_from}T00:00:00Z"^^xsd:dateTime)')
    if date_to:
        date_lines.append(f'FILTER(?pit <  "{date_to}T00:00:00Z"^^xsd:dateTime)')
    return f"""
SELECT DISTINCT ?conflict WHERE {{
  ?conflict wdt:P31/wdt:P279* wd:{type_qid} .
  {' '.join(date_lines)}
}}
LIMIT {limit}
OFFSET {offset}
""".strip()

def q_conflicts_meta(conflict_qids: list[str]) -> str:
    values = " ".join(f"wd:{q}" for q in conflict_qids if q)
    return f"""
SELECT ?conflict ?pit ?start ?end WHERE {{
  VALUES ?conflict {{ {values} }}
  OPTIONAL {{ ?conflict wdt:P585 ?pit }}
  OPTIONAL {{ ?conflict wdt:P580 ?start }}
  OPTIONAL {{ ?conflict wdt:P582 ?end   }}
}}
""".strip()


def q_participants_for_conflicts(conflict_qids: list[str]) -> str:
    values = " ".join(f"wd:{q}" for q in conflict_qids if q)
    return f"""
SELECT
  ?conflict ?conflictLabel
  ?participant ?participantLabel
  ?pit
  (SAMPLE(?country0) AS ?country)
  (SAMPLE(?iso0)     AS ?iso3)
  ?isHuman
  (SAMPLE(?how0)     AS ?how)
WHERE {{
  VALUES ?conflict {{ {values} }}

  {{
    ?conflict wdt:P710 ?participant .
  }}
  UNION
  {{
    ?conflict p:P710 ?stmt .
    ?stmt ps:P710 ?participant .
    OPTIONAL {{ ?stmt pq:P585 ?pit0 }}
    OPTIONAL {{ ?stmt pq:P580 ?start }}
    OPTIONAL {{ ?stmt pq:P582 ?end   }}
  }}

  BIND(COALESCE(?pit0, ?start, ?end) AS ?pit)

  BIND( EXISTS {{ ?participant wdt:P31/wdt:P279* wd:Q5 }} AS ?isHuman )

  OPTIONAL {{
    {{
      ?participant wdt:P31/wdt:P279* ?cls .
      VALUES ?cls {{ wd:Q6256 wd:Q3624078 }}
      BIND(?participant AS ?country0)
      ?country0 wdt:P298 ?iso0 .
      BIND("self_p298" AS ?how0)
    }}
    UNION {{
      ?participant (wdt:P17|wdt:P495) ?c .
      ?c wdt:P298 ?iso0 .
      BIND(?c AS ?country0)
      BIND("p17_or_p495" AS ?how0)
    }}
    UNION {{
      ?participant wdt:P131+ ?place .
      ?place wdt:P17 ?c2 .
      ?c2 wdt:P298 ?iso0 .
      BIND(?c2 AS ?country0)
      BIND("p131_to_p17" AS ?how0)
    }}
    UNION {{
      ?participant wdt:P3842 ?present .
      ?present wdt:P298 ?iso0 .
      BIND(?present AS ?country0)
      BIND("p3842_present" AS ?how0)
    }}
    FILTER(STRLEN(?iso0)=3)
  }}

  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
}}
GROUP BY ?conflict ?conflictLabel ?participant ?participantLabel ?pit ?isHuman
""".strip()

# ---------- parse ----------
def parse_ids(payload_mode, payload):
    ids = []
    if payload_mode == "json":
        for b in payload.get("results", {}).get("bindings", []):
            ids.append(extract_qid(b.get("conflict", {}).get("value","")))
    else:
        lines = payload.splitlines()
        if len(lines) > 1:
            for line in lines[1:]:
                s = line.strip()
                if s: ids.append(extract_qid(s))
    seen, out = set(), []
    for q in ids:
        if q and q not in seen:
            seen.add(q); out.append(q)
    return out

def parse_participants(payload_mode, payload, type_qid: str, window: str):
    """
    Devolve linhas base com tudo o que vem da query:
    [conflict_qid, conflict_label, participant_qid, participant_label,
     pit, type_qid, window,
     country_qid, country_label, iso3_from_query, how, is_human_flag]
    """
    rows = []
    if payload_mode == "json":
        for b in payload.get("results", {}).get("bindings", []):
            rows.append([
                extract_qid(b.get("conflict", {}).get("value","")),
                b.get("conflictLabel", {}).get("value",""),
                extract_qid(b.get("participant", {}).get("value","")),
                b.get("participantLabel", {}).get("value",""),
                b.get("pit", {}).get("value",""),
                type_qid, window,
                extract_qid(b.get("country", {}).get("value","")),           # 7  country_qid
                b.get("countryLabel", {}).get("value",""),                    # 8  country_label (pode vir vazio)
                (b.get("iso3", {}) if isinstance(b.get("iso3", {}), dict) else {"value": b.get("iso3","")}).get("value","").strip().upper(),  # 9  iso3_from_query
                (b.get("how", {}) if isinstance(b.get("how", {}), dict) else {"value": b.get("how","")}).get("value",""),                      # 10 how
                "1" if str((b.get("isHuman", {}) if isinstance(b.get("isHuman", {}), dict) else {"value": b.get("isHuman","")}).get("value","")).lower() == "true" else "0",  # 11 is_human
            ])
    else:
        lines = payload.splitlines()
        if not lines:
            return rows
        header = [h.strip() for h in lines[0].split("\t")]
        idx = {name: i for i, name in enumerate(header)}
        for line in lines[1:]:
            if not line.strip():
                continue
            cells = line.split("\t")
            def get(name): return cells[idx.get(name)] if name in idx else ""
            rows.append([
                extract_qid(get("conflict")),
                get("conflictLabel"),
                extract_qid(get("participant")),
                get("participantLabel"),
                get("pit"),
                type_qid, window,
                extract_qid(get("country")),          # 7
                get("countryLabel"),                  # 8
                get("iso3").strip().upper(),         # 9
                get("how"),                           # 10
                "1" if get("isHuman").lower() in ("true","1","yes") else "0",  # 11
            ])
    return rows

# ---------- meta de conflitos (label+datas) ----------
def _norm_date(s: str) -> str:
    return (s or "").strip()

def _coalesce_conflict_date(pit: str, start: str, end: str) -> str:
    # prioridade: pit > start > end; se várias, usa a mais antiga (lexicográfica)
    cands = [d for d in map(_norm_date, [pit, start, end]) if d]
    return min(cands) if cands else ""

def fetch_conflicts_meta(sess: requests.Session, conflict_qids: list[str], batch: int = 80) -> dict[str, dict]:
    meta: dict[str, dict] = {}
    # 3a) datas em WDQS (sem labels), em TSV para ser ainda mais leve
    for block in chunks(conflict_qids, batch):
        q = q_conflicts_meta(block)
        mode, payload = with_retry_fetch(q, accept_json_first=False)  # ← TSV first
        bindings = []
        if mode == "json":
            bindings = payload.get("results", {}).get("bindings", [])
        else:
            lines = payload.splitlines()
            if lines:
                head = lines[0].split("\t")
                idx = {h.strip(): i for i, h in enumerate(head)}
                for ln in lines[1:]:
                    if not ln.strip(): continue
                    cells = ln.split("\t")
                    def get(name): return cells[idx[name]] if name in idx else ""
                    bindings.append({
                        "conflict": {"value": get("conflict")},
                        "pit":      {"value": get("pit")},
                        "start":    {"value": get("start")},
                        "end":      {"value": get("end")},
                    })
        for b in bindings:
            cq = extract_qid(b.get("conflict", {}).get("value",""))
            if not cq: continue
            pit   = (b.get("pit",   {}) or {}).get("value","")
            start = (b.get("start", {}) or {}).get("value","")
            end   = (b.get("end",   {}) or {}).get("value","")
            meta[cq] = {
                "pit_canon": _coalesce_conflict_date(pit, start, end),
            }
        time.sleep(SLEEP_BASE + random.uniform(0, 0.6))

    # 3b) labels via MediaWiki (agora sim)
    labels = fetch_labels_mw(list(meta.keys()))
    for q, d in meta.items():
        d["conflict_label"] = labels.get(q, "")

    return meta

# ---------- run one type ----------
def run_one_type(type_qid: str, window: str, dfrom: str|None, dto: str|None):
    print(f"[run] tipo={type_qid} janela={window} from={dfrom} to={dto}")
    total_written = 0

    # 1) IDs por páginas
    offset = 0; page = 0; all_conflicts: list[str] = []
    while True:
        q_ids = q_conflicts_ids(type_qid, dfrom, dto, PAGE_SIZE_CONFLICTS, offset)
        mode, payload = with_retry_fetch(q_ids, accept_json_first=False)
        ids = parse_ids(mode, payload)
        if not ids:
            print(f"[ids] página {page} vazia → fim. IDs totais: {len(all_conflicts)}")
            break
        all_conflicts.extend(ids)
        got = len(ids)
        print(f"[ids] página {page}: +{got} (acum {len(all_conflicts)})")
        if got < PAGE_SIZE_CONFLICTS:
            print(f"[ids] última página."); break
        page += 1; offset += PAGE_SIZE_CONFLICTS
        time.sleep(SLEEP_BASE + random.uniform(0, 0.4))

    if not all_conflicts:
        print(f"[ok] {type_qid}/{window}: 0 conflitos → 0 linhas")
        return 0

    # 2) Metadados canónicos por conflito (label + data canónica)
    meta = fetch_conflicts_meta(requests.Session(), all_conflicts, batch=BATCH_VALUES)
    
    # 3) Participantes por batches
    #    Fallback ISO3 via forms_all.csv (qid->iso3)
    qid2iso = load_qid_to_iso3_from_forms()

    for i in range(0, len(all_conflicts), BATCH_VALUES):
        chunk = all_conflicts[i:i+BATCH_VALUES]
        q_part = q_participants_for_conflicts(chunk)
        mode, payload = with_retry_fetch(q_part, accept_json_first=True)
        base_rows = parse_participants(mode, payload, type_qid, window)

        rows = []
        for r in base_rows:
            # r = [conflict_qid, conflict_label, participant_qid, participant_label,
            #      pit, type_qid, window, country_qid, country_label, iso3_from_query, how, is_human]
            cq            = r[0]
            clabel_query  = r[1] or ""
            pq            = r[2]
            plabel        = r[3]
            pit_query     = (r[4] or "").strip()
            type_q        = r[5]; win = r[6]
            country_qid   = r[7] or ""
            country_label = r[8] or ""
            iso3_query    = (r[9] or "").strip().upper()
            how           = r[10] or ""
            is_human      = r[11] or "0"

            # 3A) conflito: label + data canónica (se a linha não trouxer pit)
            
            cm = meta.get(cq, {})
            clabel = clabel_query or cm.get("conflict_label", "")
            pit    = (r[4] or "").strip() or cm.get("pit_canon", "")

            # 3B) ISO3: preferir da query; senão, forms_all.csv pelo country_qid
            if iso3_query:
                iso3 = iso3_query
                src  = how if how else "sparql"
            else:
                iso3 = qid2iso.get(country_qid, "")
                src  = "forms" if iso3 else ""

            rows.append([
                cq, clabel,
                pq, plabel,
                pit, type_q, win,
                country_qid,
                country_label,
                iso3,
                src,
                is_human,
            ])

        append_rows(rows)
        total_written += len(rows)
        print(f"[part] batch {i//BATCH_VALUES+1}: conflitos {i+1}..{i+len(chunk)} → +{len(rows)} (acum {total_written})")
        time.sleep(SLEEP_BASE + random.uniform(0, 0.5))

    print(f"[ok] {type_qid}/{window}: {total_written} linhas escritas")
    return total_written

# ---------- main ----------
def main():
    init_output()
    grand = 0
    try:
        for type_qid, window, dfrom, dto in TYPE_LIST:
            try:
                grand += run_one_type(type_qid, window, dfrom, dto)
            except KeyboardInterrupt:
                print("\n[abort] interrompido pelo utilizador", file=sys.stderr); raise
            except Exception as e:
                print(f"[fail] {type_qid}/{window}: {e}")
            time.sleep(1.0 + random.uniform(0, 0.5))
    finally:
        # overwrite atómico; se falhar (ficheiro aberto), aborta com msg clara
        try:
            os.replace(OUT_WORK, OUT_CSV)
            print(f"[write] overwrite → {OUT_CSV}")
        except PermissionError as e:
            raise SystemExit(
                f"[erro] Não consegui substituir '{OUT_CSV}' (está aberto?). "
                f"Fecha o ficheiro e volta a correr."
            ) from e

    print(f"[done] total → {OUT_CSV} : {grand} linhas")

if __name__ == "__main__":
    main()
