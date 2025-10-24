# scripts/merge_conflicts_with_labels.py
# -*- coding: utf-8 -*-
"""
Consolida:
  - data/conflicts_participants.csv
  - data/conflicts_countries.csv
em UM ficheiro longo pronto para o UI:

Saída: data/conflicts_long_for_ui.csv (sep=';')

Colunas BASE (sempre presentes e na mesma ordem):
  conflict_qid;conflict_label;point_in_time;role;entity_qid;entity_label;source;mapped_iso3

Colunas EXTRA (apenas se existirem nas entradas; ordem preferida):
  point_year;start_date;start_year;end_date;end_year;mapped_iso3_source;is_human;citizenship_qid;citizenship_label

Notas:
- role = "participant" (participantes, P710) OU "country" (países)
- point_in_time normalizada para YYYY-MM-DD
- labels resolvidas em PT com fallback EN (via SPARQL, em batches)
- overwrite estrito (tmp -> os.replace)
"""

from __future__ import annotations
from pathlib import Path
import csv, os, sys, time, random, re
from typing import Dict, List, Iterable, Tuple, Set
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR     = PROJECT_ROOT / "data"

IN_PARTS     = DATA_DIR / "conflicts_participants.csv"
IN_COUNTRIES = DATA_DIR / "conflicts_countries.csv"
OUT_CSV      = DATA_DIR / "conflicts_long_for_ui.csv"
OUT_TMP      = OUT_CSV.with_suffix(".tmp.csv")

ENDPOINT   = "https://query.wikidata.org/sparql"
UA         = "GeoMundi/1.0 (merge-conflicts-for-ui; contact: you@example.com)"
TIMEOUT_S  = 60
BATCH_QIDS = 300

# Cabeçalho BASE (fixo) — compatibilidade
BASE_HEADER = [
    "conflict_qid", "conflict_label", "point_in_time",
    "role", "entity_qid", "entity_label", "source", "mapped_iso3",
]

# Ordem preferida para EXTRAS; só entram se existirem nas entradas
PREFERRED_EXTRAS = [
    "point_year",
    "start_date", "start_year",
    "end_date",   "end_year",
    "mapped_iso3_source",
    "is_human",
    "citizenship_qid", "citizenship_label",
]

_QID_RE = re.compile(r"Q\d+$")


def qid(x: str) -> str:
    if not x:
        return ""
    s = str(x).strip()
    if s.startswith("<") and s.endswith(">"):
        s = s[1:-1]
    if "/" in s:
        s = s.rsplit("/", 1)[-1]
    s = s.strip('>"\' \t\r\n')
    return s if _QID_RE.match(s) else ""

def norm_date(pit: str) -> str:
    """YYYY-MM-DD; aceita 'YYYY-MM-DDTHH:MM:SS' e '...^^xsd:dateTime'."""
    if not pit:
        return ""
    s = pit.strip().strip('"')
    if "T" in s:
        s = s.split("T", 1)[0]
    if "^^" in s:
        s = s.split("^^", 1)[0].strip('"')
    return s

def year_from(date_like: str) -> str:
    """Extrai ano, aceitando também negativos/BC e strings 'YYYY-MM-DD'."""
    s = (date_like or "").strip().strip('"')
    if not s:
        return ""
    m = re.match(r"^(-?\d{1,4})", s)
    return m.group(1) if m else ""

def sniff_sep(path: Path) -> str:
    txt = path.read_text(encoding="utf-8", errors="ignore")[:4096] if path.exists() else ""
    for d in (";", ",", "|", "\t"):
        if d in txt:
            return d
    return ";"

def _key_tuple(row: dict) -> tuple:
    """Chave para dedupe: (conflito, role, entidade, ano)."""
    py = (row.get("point_year") or year_from(row.get("point_in_time", "")) or "").strip()
    return (
        (row.get("conflict_qid") or "").strip(),
        (row.get("role") or "").strip(),
        (row.get("entity_qid") or "").strip(),
        py,
    )

def _merge_rows(old: dict, new: dict, header: list[str]) -> dict:
    """Funde duas linhas iguais na chave, preferindo valores 'melhores'."""
    merged = dict(old)

    def pick(col: str, a: str, b: str) -> str:
        a = a or ""
        b = b or ""
        if col == "mapped_iso3":
            a3 = a.strip().upper()
            b3 = b.strip().upper()
            # preferir ISO3 válido
            if len(a3) == 3 and len(b3) != 3: return a3
            if len(b3) == 3 and len(a3) != 3: return b3
            if len(a3) == 3 and len(b3) == 3: return a3  # mantém o primeiro
            return b or a
        if col in ("mapped_iso3_source", "source"):
            return b or a
        if col in ("entity_label", "conflict_label",
                   "participant_country_label", "citizenship_label"):
            return b or a
        if col == "point_in_time":
            # mantemos o primeiro (a) para estabilidade; o ano já está na chave
            return a or b
        # default: preencher vazios
        return b if (not a and b) else a

    for c in header:
        merged[c] = pick(c, merged.get(c, ""), new.get(c, ""))
    return merged

def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    import pandas as pd
    if not path.exists():
        return []
    df = pd.read_csv(path, sep=sniff_sep(path), dtype=str, encoding="utf-8-sig").fillna("")
    # normaliza headers para lowercase para acesso simples
    df.columns = [c.strip().lower() for c in df.columns]
    return df.to_dict(orient="records")

def chunked(seq: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def fetch_labels_pt_en(qids: List[str]) -> Dict[str, str]:
    """Devolve {qid: label_pt_fallback_en}. Usa batches via SERVICE wikibase:label."""
    out: Dict[str, str] = {}
    if not qids:
        return out
    headers = {"User-Agent": UA, "Accept": "application/sparql-results+json"}
    for block in chunked(qids, BATCH_QIDS):
        values = " ".join(f"wd:{q}" for q in block if q)
        query = f"""
SELECT ?e ?eLabel WHERE {{
  VALUES ?e {{ {values} }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
}}
""".strip()
        r = requests.post(ENDPOINT, data={"query": query}, headers=headers, timeout=TIMEOUT_S)
        if r.status_code != 200:
            time.sleep(0.6 + random.uniform(0, 0.3))
            r = requests.post(ENDPOINT, data={"query": query}, headers=headers, timeout=TIMEOUT_S)
            if r.status_code != 200:
                sys.stderr.write(f"[warn] labels HTTP {r.status_code}: {r.text[:160]}\n")
                continue
        try:
            data = r.json()
        except Exception:
            sys.stderr.write("[warn] labels: JSON parse falhou\n")
            continue
        for b in data.get("results", {}).get("bindings", []):
            e = qid(b.get("e", {}).get("value", ""))
            lbl = b.get("eLabel", {}).get("value", "")
            if e and lbl:
                out[e] = lbl
    return out

def main():
    # overwrite estrito
    for p in (OUT_TMP, OUT_CSV):
        try:
            p.unlink(missing_ok=True)
        except TypeError:
            if p.exists():
                p.unlink()

    parts_rows = read_csv_rows(IN_PARTS)
    countries_rows = read_csv_rows(IN_COUNTRIES)

    if not parts_rows and not countries_rows:
        print("[erro] Nenhum input encontrado.", file=sys.stderr)
        sys.exit(1)

    # Construir set de QIDs a etiquetar (conflicts + participants + countries + mapped_country_qid)
    qids: Set[str] = set()
    for r in parts_rows:
        qids.add(qid(r.get("conflict_qid", "")))
        qids.add(qid(r.get("participant_qid", "")))
        if r.get("mapped_country_qid", ""):
            qids.add(qid(r["mapped_country_qid"]))
    for r in countries_rows:
        qids.add(qid(r.get("conflict_qid", "")))
        qids.add(qid(r.get("country_qid", "")))

    qids = {q for q in qids if q}
    labels = fetch_labels_pt_en(sorted(qids))

    # Descobrir colunas EXTRA presentes em qualquer input
    input_cols = set()
    for r in parts_rows:
        input_cols.update(r.keys())
    for r in countries_rows:
        input_cols.update(r.keys())

    extras_present = [c for c in PREFERRED_EXTRAS if c in input_cols]

    header = BASE_HEADER + extras_present

    with OUT_TMP.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(header)

        aggregated: Dict[tuple, dict] = {}

        # 1) PARTICIPANTES → role=participant
        for r in parts_rows:
            conflict_qid = qid(r.get("conflict_qid", ""))
            conflict_label = (r.get("conflict_label", "") or labels.get(conflict_qid, "")).strip()

            pit_raw = (r.get("point_in_time") or r.get("point_date") or r.get("pit") or "").strip()
            pit = norm_date(pit_raw)

            participant_qid   = qid(r.get("participant_qid", "") or r.get("entity_qid",""))
            participant_label = (r.get("participant_label","") or r.get("entity_label","") or labels.get(participant_qid, "")).strip()

            row = {
                "conflict_qid":   conflict_qid,
                "conflict_label": conflict_label,
                "point_in_time":  pit,
                "role":           "participant",
                "entity_qid":     participant_qid,
                "entity_label":   participant_label,
                "source":         (r.get("source","") or "P710").strip(),
                "mapped_iso3":    (r.get("mapped_iso3","") or "").strip().upper(),
            }
            # extras padrão
            for c in extras_present:
                v = (r.get(c, "") or "").strip()
                if not v and c == "point_year":
                    v = year_from(pit)
                row[c] = v

            k = _key_tuple(row)
            if k in aggregated:
                aggregated[k] = _merge_rows(aggregated[k], row, header)
            else:
                aggregated[k] = row

        # 2) PAÍSES → role=country
        for r in countries_rows:
            conflict_qid = qid(r.get("conflict_qid", ""))
            conflict_label = (r.get("conflict_label", "") or labels.get(conflict_qid, "")).strip()

            pit_raw = (r.get("point_in_time") or r.get("point_date") or r.get("pit") or "").strip()
            pit = norm_date(pit_raw)

            country_qid = qid(r.get("country_qid", ""))
            country_label = (r.get("country_label", "") or labels.get(country_qid, "")).strip()
            mapped_iso3 = (r.get("mapped_iso3", "") or "").strip().upper()
            source = (r.get("source", "") or "country").strip()

            row = {
                "conflict_qid":  conflict_qid,
                "conflict_label": conflict_label,
                "point_in_time": pit,
                "role":           "country",
                "entity_qid":     country_qid,
                "entity_label":   country_label,
                "source":         source,
                "mapped_iso3":    mapped_iso3,
            }
            for c in extras_present:
                v = (r.get(c, "") or "").strip()
                if not v and c == "point_year":
                    v = year_from(pit)
                row[c] = v

            k = _key_tuple(row)
            if k in aggregated:
                aggregated[k] = _merge_rows(aggregated[k], row, header)
            else:
                aggregated[k] = row

        # 3) escrever, preservando ordem de inserção
        for row in aggregated.values():
            w.writerow([row.get(c, "") for c in header])


    # mover tmp -> final
    try:
        os.replace(OUT_TMP, OUT_CSV)
        print(f"[write] → {OUT_CSV}")
        print(f"[cols]  → {', '.join(header)}")
    except PermissionError as e:
        raise SystemExit(
            f"[erro] Não consegui substituir '{OUT_CSV}' (está aberto?). Fecha e volta a correr."
        ) from e

if __name__ == "__main__":
    main()
