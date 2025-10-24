# scripts/merge_conflicts_with_forms.py
# -*- coding: utf-8 -*-
"""
Merge de conflitos com forms_all (apenas iso3+qid) + datas início/fim.

Entrada (por omissão em data/):
  - conflicts_participants.csv
      colunas: conflict_qid;conflict_label;participant_qid;participant_label;point_in_time;type_qid;window
  - forms_all.csv
      colunas mínimas: iso3;qid   (pode chamar-se form ou form_qid — mapeamos)
  - countries_seed.csv (opcional) para injetar o QID atual do país como forma

Saída:
  - conflicts_participants.forms.enriched.csv (sep=';')

Flags:
  --inject-current yes|no   (default: yes) → se "yes", adiciona o country_qid do seed como forma
  --in/--forms/--seed/--out para paths explícitos
"""

from __future__ import annotations
from pathlib import Path
import argparse
import time, random, requests
import pandas as pd
import os

ENDPOINT   = "https://query.wikidata.org/sparql"
USER_AGENT = "GeoMundi/1.0 (merge-conflicts-forms; contact: you@example.com)"
BATCH      = 180  # nº de conflitos por VALUES

# ---------- paths ----------
def resolve_data_dir(cli_out: str | None) -> Path:
    if cli_out:
        return Path(cli_out).expanduser().resolve().parent
    env = os.getenv("GEO_DATA_DIR")
    if env:
        return Path(env).expanduser().resolve()
    here = Path(__file__).resolve()
    for p in here.parents:
        if (p / "data").is_dir():
            return (p / "data").resolve()
    return here.parents[1] / "data"

# ---------- io ----------
def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, sep=";", dtype=str, encoding="utf-8-sig").fillna("")

def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df

def _save_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, sep=";", index=False, encoding="utf-8-sig")

# ---------- wdqs ----------
def run_sparql(query: str, max_retries: int = 6, timeout: int = 90) -> dict:
    for attempt in range(1, max_retries+1):
        try:
            r = requests.post(
                ENDPOINT,
                data={"query": query},
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/sparql-results+json; charset=utf-8",
                    "Connection": "close",
                },
                timeout=timeout,
            )
            if r.status_code == 200:
                return r.json()
            elif r.status_code in (429, 502, 503, 504):
                wait = min(60, 1.7**attempt) + random.uniform(0, 1.0)
                print(f"[warn] HTTP {r.status_code}; retry {attempt}/{max_retries} in {wait:.1f}s")
                time.sleep(wait)
            else:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
        except Exception as e:
            wait = min(60, 1.7**attempt) + random.uniform(0, 1.0)
            print(f"[err] {type(e).__name__}: {e} → retry {attempt}/{max_retries} in {wait:.1f}s")
            time.sleep(wait)
    raise RuntimeError("Max retries exceeded")

def q_dates_for_conflicts(qids: list[str]) -> str:
    values = " ".join(f"wd:{q}" for q in qids if q)
    return f"""
SELECT DISTINCT ?conflict ?start ?end WHERE {{
  VALUES ?conflict {{ {values} }}
  OPTIONAL {{ ?conflict wdt:P580 ?start }}
  OPTIONAL {{ ?conflict wdt:P582 ?end   }}
}}
""".strip()

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_csv", default=None,
                    help="CSV de participantes (default: data/conflicts_participants.csv)")
    ap.add_argument("--forms", dest="forms_csv", default=None,
                    help="CSV de formas (default: data/forms_all.csv) — precisa de colunas iso3 e qid")
    ap.add_argument("--seed", dest="seed_csv", default=None,
                    help="countries_seed.csv (para injetar QID atual como forma)")
    ap.add_argument("--out", dest="out_csv", default=None,
                    help="CSV de saída (default: data/conflicts_participants.forms.enriched.csv)")
    ap.add_argument("--inject-current", dest="inject_current", choices=("yes","no"), default="yes",
                    help="Se 'yes', injeta country_qid do seed como forma do país (default: yes)")
    args = ap.parse_args()

    data_dir = resolve_data_dir(args.out_csv)
    in_parts = Path(args.in_csv).resolve()  if args.in_csv  else (data_dir / "conflicts_participants.csv")
    in_forms = Path(args.forms_csv).resolve() if args.forms_csv else (data_dir / "forms_all.csv")
    in_seed  = Path(args.seed_csv).resolve()  if args.seed_csv  else (data_dir / "countries_seed.csv")
    out_csv  = Path(args.out_csv).resolve()   if args.out_csv   else (data_dir / "conflicts_participants.forms.enriched.csv")

    print(f"[paths] data_dir={data_dir}")
    print(f"[paths] in_parts={in_parts}")
    print(f"[paths] in_forms={in_forms}")
    print(f"[paths] in_seed={in_seed}")
    print(f"[paths] out_csv={out_csv}")
    print(f"[cfg] inject_current={args.inject_current}")

    # 1) ler datasets
    parts = _norm_cols(_load_csv(in_parts))
    forms = _norm_cols(_load_csv(in_forms))
    seed  = _norm_cols(_load_csv(in_seed))

    if parts.empty:
        raise SystemExit(f"[erro] não encontrei dados em {in_parts}")
    if forms.empty:
        raise SystemExit(f"[erro] não encontrei dados em {in_forms}")

    # 2) normalizar colunas essenciais do 'parts'
    for c in ("conflict_qid","participant_qid","point_in_time","type_qid","window"):
        if c not in parts.columns: parts[c] = ""
    parts["participant_qid"] = parts["participant_qid"].astype(str).str.strip()

    # 3) preparar forms: reduzir a apenas (iso3, form_qid)
    # aceita colunas alternativas: qid / form / form_qid
    col_qid = None
    for cand in ("form_qid","qid","form"):
        if cand in forms.columns:
            col_qid = cand
            break
    if col_qid is None:
        raise SystemExit("[erro] forms_all.csv precisa de uma coluna 'qid' (ou 'form_qid' / 'form').")

    if "iso3" not in forms.columns:
        raise SystemExit("[erro] forms_all.csv precisa da coluna 'iso3'.")

    forms = forms[["iso3", col_qid]].rename(columns={col_qid: "form_qid"})
    forms["iso3"]    = forms["iso3"].astype(str).str.upper().str.strip()
    forms["form_qid"]= forms["form_qid"].astype(str).str.strip()

    # 4) injetar o QID atual de cada país como forma (opcional)
    if args.inject_current == "yes" and not seed.empty and "country_qid" in seed.columns:
        seed["iso3"] = seed["iso3"].astype(str).str.upper().str.strip()
        seed_min = seed[["iso3","country_qid"]].rename(columns={"country_qid":"form_qid"})
        already = set(zip(forms["iso3"], forms["form_qid"]))
        add_rows = []
        for iso3, q in seed_min.itertuples(index=False):
            q = str(q).strip()
            if q and (iso3, q) not in already:
                add_rows.append({"iso3": iso3, "form_qid": q})
        if add_rows:
            forms = pd.concat([forms, pd.DataFrame(add_rows)], ignore_index=True)

    # 5) INNER JOIN → manter só participantes que são formas de países
    merged = parts.merge(
        forms, left_on="participant_qid", right_on="form_qid",
        how="inner", copy=False
    )

    # 6) deduplicar por (conflito, participante, pit)
    key = ["conflict_qid","participant_qid","point_in_time"]
    before = len(merged)
    merged = merged.drop_duplicates(subset=key, keep="first").reset_index(drop=True)
    print(f"[dedup] {before} → {len(merged)}")

    # 7) datas início/fim por batches
    conflicts = merged["conflict_qid"].dropna().astype(str).unique().tolist()
    dates = {}
    for i in range(0, len(conflicts), BATCH):
        batch = conflicts[i:i+BATCH]
        js = run_sparql(q_dates_for_conflicts(batch))
        for b in js.get("results", {}).get("bindings", []):
            qid   = b.get("conflict", {}).get("value","").rsplit("/",1)[-1]
            start = b.get("start", {}).get("value","")
            end   = b.get("end", {}).get("value","")
            if qid and qid not in dates:
                dates[qid] = (start, end)
        print(f"[dates] batch {i//BATCH+1}: +{len(batch)} conflitos")
        time.sleep(0.4 + random.uniform(0,0.3))

    merged["start_date"] = merged["conflict_qid"].map(lambda q: dates.get(q, ("",""))[0])
    merged["end_date"]   = merged["conflict_qid"].map(lambda q: dates.get(q, ("",""))[1])

    # 8) ordenar e guardar (sem labels de formas)
    cols_order = [
        "iso3","form_qid",
        "conflict_qid","conflict_label",
        "participant_qid","participant_label",
        "point_in_time","start_date","end_date",
        "type_qid","window"
    ]
    for c in cols_order:
        if c not in merged.columns: merged[c] = ""
    merged = merged[cols_order]

    _save_csv(merged, out_csv)
    print(f"[ok] escrito: {out_csv} ({len(merged)} linhas)")

if __name__ == "__main__":
    main()
