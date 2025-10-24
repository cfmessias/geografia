# scripts/enrich_conflicts_long_for_ui.py
# -*- coding: utf-8 -*-
"""
Enriquece data/conflicts_long_for_ui.csv com:
  - point_date (YYYY-MM-DD, BCE com sinal) e point_year (YYYY / -YYYY)
  - start_date/end_date + start_year/end_year via SPARQL (P580/P582)
  - mapped_iso3 para participantes via forms_all.csv (se faltar) + mapped_iso3_source="forms_all"
  - dedupe por (conflict_qid, role, entity_qid) mantendo a data mais antiga
  - export dinâmico: preserva colunas extra se existirem (is_human, citizenship_*)

Saída: data/conflicts_long_for_ui.enriched.csv
"""

from __future__ import annotations
from pathlib import Path
import csv, os, re, time, random, sys, math
from typing import Dict, List, Tuple
import requests

# ---------- paths ----------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR     = PROJECT_ROOT / "data"
IN_LONG      = DATA_DIR / "conflicts_long_for_ui.csv"
FORMS_CSV    = DATA_DIR / "forms_all.csv"
OUT_CSV      = DATA_DIR / "conflicts_long_for_ui.enriched.csv"
OUT_TMP      = OUT_CSV.with_suffix(".tmp.csv")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ---------- WDQS ----------
ENDPOINT  = "https://query.wikidata.org/sparql"
UA        = "GeoMundi/1.0 (enrich-conflicts-long; contact: you@example.com)"
TIMEOUT_S = 60
BATCH     = 300

_QID_RE  = re.compile(r"Q\d+$")
YEAR_RE  = re.compile(r"^(-?\d+)(?:-\d{2}-\d{2})?$")  # captura ano (com sinal opcional)

# Cabeçalho BASE (fixo) — compatível com o UI
BASE_FIELDS = [
    "conflict_qid", "conflict_label",
    "role", "entity_qid", "entity_label",
    "mapped_iso3", "source",
    "point_date", "point_year",
    "start_date", "start_year", "end_date", "end_year",
]

# Colunas EXTRA candidatas (só entram se existirem nos inputs ou forem geradas aqui)
PREFERRED_EXTRAS = [
    "mapped_iso3_source",
    "is_human",
    "citizenship_qid",
    "citizenship_label",
]

def qid(x: str) -> str:
    if not x: return ""
    s = str(x).strip()
    if s.startswith("<") and s.endswith(">"): s = s[1:-1]
    if "/" in s: s = s.rsplit("/", 1)[-1]
    s = s.strip('>"\' \t\r\n')
    return s if _QID_RE.match(s) else ""

def norm_date_literal(x: str) -> str:
    """
    Aceita: "YYYY-MM-DD", "YYYY-MM-DDT...Z", "\"...\"^^xsd:dateTime", BCE com zeros no ano ("-0228-01-01")
    Devolve: "YYYY-MM-DD" com:
      - ano sem zeros à esquerda (preserva sinal para BCE)
      - mês/dia com 2 dígitos
    """
    if not x: return ""
    s = str(x).strip().strip('"')
    if "^^" in s:
        s = s.split("^^", 1)[0].strip('"')
    if "T" in s:
        s = s.split("T", 1)[0]
    sign = ""
    if s.startswith("-"):
        sign = "-"
        s = s[1:]
    parts = s.split("-")
    if len(parts) >= 3 and parts[0].isdigit():
        year  = parts[0].lstrip("0") or "0"
        month = parts[1].zfill(2)
        day   = parts[2].zfill(2)
        return f"{sign}{year}-{month}-{day}"
    return ""

def year_from_date(d: str) -> str:
    """Recebe 'YYYY-MM-DD' (ou '-YYYY-MM-DD') e devolve 'YYYY' (ou '-YYYY')."""
    if not d: return ""
    m = YEAR_RE.match(d)
    if m:
        y = m.group(1)
        if y.startswith("-"):
            core = y[1:].lstrip("0")
            return "-" + (core if core else "0")
        return y.lstrip("0") or "0"
    # fallbacks leves
    if "T" in d: d = d.split("T",1)[0]
    if "^^" in d: d = d.split("^^",1)[0].strip('"')
    if "-" in d:
        y = d.split("-",1)[0]
        if y.startswith("-"):
            return "-" + (y[1:].lstrip("0") or "0")
        return y.lstrip("0") or "0"
    return ""

def read_long_rows(path: Path) -> List[Dict[str,str]]:
    import pandas as pd
    if not path.exists():
        raise SystemExit(f"[erro] não encontrei {path}")
    df = pd.read_csv(path, sep=";", dtype=str, encoding="utf-8-sig").fillna("")
    df.columns = [c.strip().lower() for c in df.columns]
    # assegura base mínima
    for c in ["conflict_qid","conflict_label","point_in_time","role","entity_qid","entity_label","source","mapped_iso3"]:
        if c not in df.columns: df[c] = ""
    return df.to_dict(orient="records")

def load_forms_map(path: Path) -> Dict[str,str]:
    """forms_all.csv: aceita colunas 'qid' ou 'form_qid' + 'iso3'."""
    m: Dict[str,str] = {}
    try:
        import pandas as pd
        if path.exists():
            df = pd.read_csv(path, sep=";", dtype=str, encoding="utf-8-sig").fillna("")
            cols = {c.lower(): c for c in df.columns}
            qcol = cols.get("qid") or cols.get("form_qid")
            icol = cols.get("iso3")
            if qcol and icol:
                for _, r in df.iterrows():
                    q = str(r[qcol]).strip()
                    i = str(r[icol]).strip().upper()
                    if q and i: m[q] = i
    except Exception:
        pass
    return m

def fetch_start_end_for_conflicts(conflict_ids: List[str]) -> Dict[str, Tuple[str,str]]:
    """{ conflict_qid: (start_date, end_date) } com datas normalizadas."""
    out: Dict[str, Tuple[str,str]] = {}
    if not conflict_ids: return out

    def chunks(seq, n):
        for i in range(0, len(seq), n):
            yield seq[i:i+n]

    for block in chunks(conflict_ids, BATCH):
        values = " ".join(f"wd:{q}" for q in block if q)
        query = f"""
SELECT ?c ?start ?end WHERE {{
  VALUES ?c {{ {values} }}
  OPTIONAL {{ ?c wdt:P580 ?s0 }}
  OPTIONAL {{ ?c wdt:P582 ?e0 }}
  BIND(STR(?s0) AS ?start)
  BIND(STR(?e0) AS ?end)
}}
""".strip()
        headers = {"User-Agent": UA, "Accept": "application/sparql-results+json"}
        r = requests.post(ENDPOINT, data={"query": query}, headers=headers, timeout=TIMEOUT_S)
        if r.status_code != 200:
            time.sleep(0.6 + random.uniform(0,0.3))
            r = requests.post(ENDPOINT, data={"query": query}, headers=headers, timeout=TIMEOUT_S)
            if r.status_code != 200:
                print(f"[warn] dates HTTP {r.status_code}: {r.text[:160]}", file=sys.stderr)
                continue
        try:
            data = r.json()
        except Exception:
            print("[warn] dates: JSON parse falhou", file=sys.stderr)
            continue
        for q in block:
            out.setdefault(q, ("",""))
        for b in data.get("results", {}).get("bindings", []):
            c = qid(b.get("c", {}).get("value", ""))
            s = norm_date_literal(b.get("start", {}).get("value", ""))
            e = norm_date_literal(b.get("end",   {}).get("value", ""))
            if c:
                out[c] = (s, e)
    return out

def main():
    # overwrite estrito
    for p in (OUT_TMP, OUT_CSV):
        try:
            p.unlink(missing_ok=True)
        except TypeError:
            if p.exists():
                p.unlink()

    rows = read_long_rows(IN_LONG)
    forms_map = load_forms_map(FORMS_CSV)

    # 1) normalizar point_date/point_year
    for r in rows:
        pd_norm = norm_date_literal(str(r.get("point_in_time", "")))
        r["point_date"] = pd_norm
        r["point_year"] = year_from_date(pd_norm) if pd_norm else ""

    # 2) mapped_iso3 para participantes (via forms_all)
    for r in rows:
        role = (r.get("role","") or "").lower()
        if role == "participant":
            if not r.get("mapped_iso3"):
                eq  = qid(r.get("entity_qid",""))
                iso = forms_map.get(eq, "")
                if iso:
                    r["mapped_iso3"] = iso
                    # marca a origem deste preenchimento
                    if not r.get("mapped_iso3_source"):
                        r["mapped_iso3_source"] = "forms_all"
        # normalizar maiúsculas
        r["mapped_iso3"] = str(r.get("mapped_iso3", "")).strip().upper()

    # 3) start/end (e years)
    conflict_ids = sorted({qid(r.get("conflict_qid", "")) for r in rows if r.get("conflict_qid")})
    date_map = fetch_start_end_for_conflicts(conflict_ids)
    def date_key_prep(d: str) -> str:
        return norm_date_literal(d) if d else ""
    for r in rows:
        cq = qid(r.get("conflict_qid", ""))
        s, e = date_map.get(cq, ("", ""))
        s = date_key_prep(s); e = date_key_prep(e)
        r["start_date"] = s
        r["end_date"]   = e
        r["start_year"] = year_from_date(s)
        r["end_year"]   = year_from_date(e)
        # se point_date vazio, usar start_date
        if not r["point_date"] and s:
            r["point_date"] = s
            r["point_year"] = r["start_year"]

    # 4) dedupe por (conflict_qid, role, entity_qid) mantendo data mais antiga
    DATE_RE = re.compile(r'^(-?)(\d+)(?:-(\d{2})-(\d{2}))?$')  # sign, year, [mm, dd]
    def date_key(d: str) -> tuple:
        """
        Ordenação cronológica robusta:
          - Vazio/ inválido vai para o fim
          - BCE < CE (anos negativos)
          - Falta de mês/dia assume 01
        Retorna tuplo comparável.
        """
        if not d or not isinstance(d, str):
            return (1, math.inf, 99, 99)  # vazios no fim
        s = d.strip()
        if "^^" in s: s = s.split("^^", 1)[0].strip('"')
        if "T"  in s: s = s.split("T", 1)[0]
        m = DATE_RE.match(s)
        if not m: return (1, math.inf, 99, 99)
        sign, year_txt, mm_txt, dd_txt = m.groups()
        if not year_txt or not year_txt.isdigit(): return (1, math.inf, 99, 99)
        year = int(year_txt)
        if sign == "-": year = -year
        mm = int(mm_txt) if mm_txt and mm_txt.isdigit() else 1
        dd = int(dd_txt) if dd_txt and dd_txt.isdigit() else 1
        return (0, year, mm, dd)

    dedup: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    for r in rows:
        cq   = qid(r.get("conflict_qid", ""))
        role = (r.get("role", "") or "").lower()
        eq   = qid(r.get("entity_qid", ""))
        if not (cq and role and eq):
            continue
        d_this = r.get("point_date", "") or r.get("start_date", "")
        k = (cq, role, eq)
        cur = dedup.get(k)
        if cur is None:
            dedup[k] = r
        else:
            d_cur = cur.get("point_date", "") or cur.get("start_date", "")
            if date_key(d_this) < date_key(d_cur):
                dedup[k] = r

    # 5) ordenar
    def _sort_key(rec: Dict[str, str]):
        return (
            rec.get("conflict_qid", ""),
            (rec.get("role", "") or "").lower(),
            rec.get("entity_qid", ""),
            date_key(rec.get("point_date", "") or rec.get("start_date", "")),
        )
    out_rows = sorted(dedup.values(), key=_sort_key)

    # 6) escrever (layout dinâmico: base + extras existentes)
    # Descobre extras presentes nos inputs/geradas aqui
    all_keys = set().union(*(r.keys() for r in out_rows)) if out_rows else set()
    extras_present = [c for c in PREFERRED_EXTRAS if c in all_keys]

    field_order = BASE_FIELDS + extras_present

    # garante chaves
    for r in out_rows:
        for c in field_order:
            r.setdefault(c, "")

    with OUT_TMP.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(field_order)
        for r in out_rows:
            w.writerow([r.get(c, "") for c in field_order])

    try:
        os.replace(OUT_TMP, OUT_CSV)
        print(f"[write] → {OUT_CSV}  (linhas={len(out_rows)})")
        print(f"[cols]  → {', '.join(field_order)}")
    except PermissionError as e:
        raise SystemExit(f"[erro] Não consegui substituir '{OUT_CSV}' (está aberto?).") from e


if __name__ == "__main__":
    main()
