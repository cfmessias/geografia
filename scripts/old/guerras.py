# -*- coding: utf-8 -*-
"""
pipeline_wars_battles_csv.py

Pipeline WDQS em 3 passos (+labels) e CSVs temporários:
  1) forms_<ISO3>.csv:      QIDs das formas de Estado (P3842 + país atual)
  2) conflicts_<ISO3>.csv:  conflitos militares (war/battle/armed conflict/conflict) com P710=forma + datas
  3) wars_battles_<ISO3>.csv (final): país ISO3 filtrado entre os intervenientes + labels

Fonte de países: data/countries_profiles.csv (colunas 'iso3' e 'm49').
"""

from __future__ import annotations
import argparse, time, random, csv, tempfile
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Set

import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --------------------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------------------

WDQS_URL = "https://query.wikidata.org/sparql"
WDQS_HEADERS = {
    "User-Agent": "GeoWars/1.0 (+streamlit; contact: you@example.com)",
    "Accept": "application/sparql-results+json",
}

# Tipos militares (QIDs) — enxuto para manter leve
CONFLICT_CLASSES = [
    "Q198",      # war
    "Q178561",   # battle
    "Q350604",   # armed conflict
    "Q180684",   # conflict
]

# Batching
BATCH = 30

# Caminhos default
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
PROFILES_CSV = DATA_DIR / "countries_profiles.csv"  # deve ter 'iso3' e 'm49'

# --------------------------------------------------------------------------------------
# WDQS client (Session + Retry) + proteção contra respostas não-JSON
# --------------------------------------------------------------------------------------

_session = requests.Session()
_retry = Retry(
    total=8, connect=5, read=5,
    status_forcelist=(429, 500, 502, 503, 504),
    backoff_factor=0.7,
    allowed_methods=frozenset(["POST"]),
    raise_on_status=False,
)
_session.mount("https://", HTTPAdapter(max_retries=_retry))
_session.mount("http://", HTTPAdapter(max_retries=_retry))

def _wdqs(query: str, attempts: int = 2, timeout: int = 60):
    delay = 1.0
    for i in range(1, attempts + 1):
        try:
            r = _session.post(WDQS_URL, data={"query": query}, headers=WDQS_HEADERS, timeout=timeout)
            try:
                js = r.json()
            except ValueError:
                snippet = (r.text or "")[:200].replace("\n", " ")
                print(f"[wdqs] resposta não-JSON (len={len(r.text)}): {snippet}", flush=True)
                raise
            return js.get("results", {}).get("bindings", [])
        except Exception as e:
            sleep_s = delay + random.uniform(0, 0.6)
            print(f"[wdqs] tentativa {i}/{attempts} falhou: {e} -> dormir {sleep_s:.1f}s", flush=True)
            time.sleep(sleep_s)
            delay = min(delay * 1.9, 12.0)
    print("[wdqs] erro definitivo; a devolver lista vazia", flush=True)
    return []

# --------------------------------------------------------------------------------------
# Utils
# --------------------------------------------------------------------------------------

def _qid(uri: str) -> str:
    return uri.rpartition("/")[-1] if uri else ""

def _y4(x: Optional[str]) -> str:
    if not x: return ""
    try: return str(int(x[:4]))
    except Exception: return ""

def _load_profiles(profiles_csv: Path) -> pd.DataFrame:
    """
    Lê data/countries_profiles.csv (separador ';') e devolve df com 'iso3' e 'm49'.
    """
    if not profiles_csv.exists():
        raise FileNotFoundError(f"{profiles_csv} não encontrado (esperado com colunas 'iso3' e 'm49').")
    df = pd.read_csv(profiles_csv, sep=";", dtype=str, encoding="utf-8", keep_default_na=False)
    if "iso3" not in df.columns:
        raise RuntimeError("countries_profiles.csv não tem coluna 'iso3'.")
    if "m49" not in df.columns:
        raise RuntimeError("countries_profiles.csv não tem coluna 'm49'.")
    df["iso3"] = df["iso3"].str.upper().str.strip()
    df["m49"]  = df["m49"].str.strip()
    return df



# --------------------------------------------------------------------------------------
# Q1: formas de Estado (P3842 + o próprio país) — NÃO usa q_root_from_iso3
# --------------------------------------------------------------------------------------

def q_state_forms(iso3: str) -> pd.DataFrame:
    """
    Formas de Estado ligadas a um país (por ISO-3), via P3842 + o próprio país.
    Devolve DataFrame com colunas: iso3, who_qid, country_qid.
    (Mantém o nome 'q_state_forms' para não partir o resto do script.)
    """
    iso3 = (iso3 or "").upper().strip()
    if not iso3:
        return pd.DataFrame(columns=["iso3", "who_qid", "country_qid"])

    q = f"""
SELECT DISTINCT ?qid ?country_qid WHERE {{
  VALUES ?iso3 {{ "{iso3}" }}
  ?country wdt:P298 ?iso3 .

  {{
    VALUES ?cls {{
      wd:Q3624078   # sovereign state
      wd:Q6256      # country
      wd:Q417175    # kingdom
      wd:Q3024240   # former country
      wd:Q41410     # state of the Holy Roman Empire
    }}
    ?state wdt:P31/wdt:P279* ?cls ;
           wdt:P3842 ?country .
  }}
  UNION
  {{ BIND(?country AS ?state) }}

  BIND(REPLACE(STR(?state),   ".*/(Q[0-9]+)$", "$1") AS ?qid)
  BIND(REPLACE(STR(?country), ".*/(Q[0-9]+)$", "$1") AS ?country_qid)
}}
""".strip()

    rows = _wdqs(q)
    if not rows:
        return pd.DataFrame(columns=["iso3", "who_qid", "country_qid"])

    data = []
    for r in rows:
        who = r.get("qid", {}).get("value", "")
        cq  = r.get("country_qid", {}).get("value", "")
        if who.startswith("Q") and cq.startswith("Q"):
            data.append({"iso3": iso3, "who_qid": who, "country_qid": cq})

    return pd.DataFrame(data).drop_duplicates(["iso3", "who_qid"])

# --------------------------------------------------------------------------------------
# Q2: conflitos militares com P710 = who + datas
# --------------------------------------------------------------------------------------

def q_conflicts_for_who(df_who: pd.DataFrame) -> pd.DataFrame:
    if df_who.empty:
        return pd.DataFrame(columns=["iso3","conflict_qid","who_qid","kind_qid","start_year","end_year","point_year","country_qid"])
    allowed = " ".join(f"wd:{c}" for c in CONFLICT_CLASSES)
    out_rows = []
    who_list = df_who["who_qid"].unique().tolist()
    for i in range(0, len(who_list), BATCH):
        chunk = who_list[i:i+BATCH]
        vals = " ".join(f"wd:{q}" for q in chunk)
        q = f"""
SELECT DISTINCT ?conflict ?who ?ctype ?start ?end ?point WHERE {{
  VALUES ?who {{ {vals} }}
  ?conflict p:P710 ?st .
  ?st ps:P710 ?who .

  VALUES ?ctype {{ {allowed} }}
  ?conflict wdt:P31/wdt:P279* ?ctype .

  OPTIONAL {{ ?conflict wdt:P580 ?start }}
  OPTIONAL {{ ?conflict wdt:P582 ?end }}
  OPTIONAL {{ ?conflict wdt:P585 ?point }}
}} LIMIT 10000
""".strip()
        rows = _wdqs(q)
        time.sleep(0.35 + random.uniform(0.0, 0.25))  # micro pausa entre batches
        for r in rows:
            c = _qid(r.get("conflict", {}).get("value",""))
            w = _qid(r.get("who", {}).get("value",""))
            k = _qid(r.get("ctype", {}).get("value",""))
            if not (c.startswith("Q") and w.startswith("Q") and k.startswith("Q")):
                continue
            # iso3 e country_qid vêm de df_who
            tmp = df_who[df_who["who_qid"] == w][["iso3","country_qid"]].drop_duplicates()
            for _, row in tmp.iterrows():
                out_rows.append({
                    "iso3": row["iso3"],
                    "country_qid": row["country_qid"],
                    "conflict_qid": c,
                    "who_qid": w,
                    "kind_qid": k,
                    "start_year": _y4(r.get("start", {}).get("value")),
                    "end_year": _y4(r.get("end", {}).get("value")),
                    "point_year": _y4(r.get("point", {}).get("value")),
                })
    if not out_rows:
        return pd.DataFrame(columns=["iso3","conflict_qid","who_qid","kind_qid","start_year","end_year","point_year","country_qid"])
    df = pd.DataFrame(out_rows).drop_duplicates(["iso3","conflict_qid","who_qid"])
    return df

# --------------------------------------------------------------------------------------
# Q3: países intervenientes (apenas países com P298)
# --------------------------------------------------------------------------------------

def q_participant_countries(conflict_qids: List[str]) -> pd.DataFrame:
    if not conflict_qids:
        return pd.DataFrame(columns=["conflict_qid","country_qid","country_iso3"])
    out_rows = []
    for i in range(0, len(conflict_qids), BATCH):
        chunk = conflict_qids[i:i+BATCH]
        vals = " ".join(f"wd:{q}" for q in chunk)
        q = f"""
SELECT DISTINCT ?conflict ?country ?iso WHERE {{
  VALUES ?conflict {{ {vals} }}
  {{
    ?conflict p:P710 ?s1 . ?s1 ps:P710 ?country .
    ?country wdt:P298 ?iso .
  }}
  UNION
  {{
    ?conflict p:P710 ?s2 . ?s2 ps:P710 ?actor2 .
    ?actor2 wdt:P17 ?country .
    ?country wdt:P298 ?iso .
  }}
  UNION
  {{
    ?conflict p:P710 ?s3 . ?s3 ps:P710 ?actor3 .
    ?actor3 wdt:P27 ?country .
    ?country wdt:P298 ?iso .
  }}
}} LIMIT 20000
""".strip()
        rows = _wdqs(q)
        time.sleep(0.35 + random.uniform(0.0, 0.25))  # micro pausa entre batches
        for r in rows:
            c = _qid(r.get("conflict", {}).get("value",""))
            k = _qid(r.get("country", {}).get("value",""))
            iso = (r.get("iso", {}).get("value","") or "").upper()
            if c.startswith("Q") and k.startswith("Q") and len(iso)==3:
                out_rows.append({"conflict_qid": c, "country_qid": k, "country_iso3": iso})
    if not out_rows:
        return pd.DataFrame(columns=["conflict_qid","country_qid","country_iso3"])
    return pd.DataFrame(out_rows).drop_duplicates()

# --------------------------------------------------------------------------------------
# Q4: labels (pt→en) para lista de QIDs
# --------------------------------------------------------------------------------------

def q_labels_for_qids(qids: List[str]) -> pd.DataFrame:
    qids = [q for q in qids if q.startswith("Q")]
    if not qids:
        return pd.DataFrame(columns=["id","label"])
    out = []
    for i in range(0, len(qids), 200):
        chunk = qids[i:i+200]
        vals = " ".join(f"wd:{q}" for q in chunk)
        q = f"""
SELECT ?id (COALESCE(?pt, ?en) AS ?label) WHERE {{
  VALUES ?id {{ {vals} }}
  OPTIONAL {{ ?id rdfs:label ?pt FILTER(LANG(?pt)="pt") }}
  OPTIONAL {{ ?id rdfs:label ?en FILTER(LANG(?en)="en") }}
}}
""".strip()
        rows = _wdqs(q)
        for r in rows:
            _id = _qid(r.get("id", {}).get("value",""))
            lab = r.get("label", {}).get("value","")
            if _id.startswith("Q"):
                out.append({"id": _id, "label": lab})
    return pd.DataFrame(out).drop_duplicates("id")

# --------------------------------------------------------------------------------------
# CSV helpers
# --------------------------------------------------------------------------------------

def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")

# --------------------------------------------------------------------------------------
# Processar um país (gera 3 CSVs, o final com labels + m49 do profiles)
# --------------------------------------------------------------------------------------

def process_country(iso3: str, out_dir: Path, profiles_df: pd.DataFrame) -> Path:
    iso3 = iso3.upper().strip()
    print(f"[country] {iso3}", flush=True)

    # info do profiles
    prof = profiles_df.loc[profiles_df["iso3"] == iso3]
    m49 = str(prof["m49"].iloc[0]) if not prof.empty else ""

    # paths temporários
    forms_path     = out_dir / f"forms_{iso3}.csv"
    conflicts_path = out_dir / f"conflicts_{iso3}.csv"
    final_path     = out_dir / f"wars_battles_{iso3}.csv"

    # 1) forms (P3842 + país atual)
    df_forms = q_state_forms(iso3)
    print(f"  └ forms: {len(df_forms)}")
    write_csv(df_forms, forms_path)

    # 2) conflicts
    df_conf = q_conflicts_for_who(df_forms)
    n_conf = df_conf["conflict_qid"].nunique() if not df_conf.empty else 0
    print(f"  └ conflicts: {n_conf}")
    write_csv(df_conf, conflicts_path)

    # 3) participant countries -> filtra para o próprio ISO3
    conflict_ids = df_conf["conflict_qid"].unique().tolist() if not df_conf.empty else []
    df_ctry = q_participant_countries(conflict_ids)
    mine = pd.DataFrame(columns=["conflict_qid"])
    if not df_ctry.empty:
        mine = df_ctry[df_ctry["country_iso3"] == iso3][["conflict_qid"]].drop_duplicates()

    # merge com metadados do conflito e labels
    df_final = pd.DataFrame(columns=[
        "iso3","m49","country_qid","country_label",
        "conflict_qid","conflict_label",
        "kind_qid","kind_label",
        "start_year","end_year","point_year"
    ])
    if not mine.empty and not df_conf.empty:
        df_final = mine.merge(
            df_conf.drop_duplicates("conflict_qid")[["conflict_qid","country_qid","kind_qid","start_year","end_year","point_year"]],
            on="conflict_qid",
            how="left"
        )
        df_final.insert(0, "iso3", iso3)
        df_final.insert(1, "m49", m49)

        # labels (conflict, kind, country)
        want_labels: Set[str] = set()
        want_labels.update(df_final["conflict_qid"].dropna().tolist())
        want_labels.update(df_final["kind_qid"].dropna().tolist())
        want_labels.update(df_final["country_qid"].dropna().tolist())
        df_labels = q_labels_for_qids(sorted(want_labels))
        lab_map = dict(zip(df_labels["id"], df_labels["label"]))
        df_final["conflict_label"] = df_final["conflict_qid"].map(lab_map).fillna("")
        df_final["kind_label"]     = df_final["kind_qid"].map(lab_map).fillna("")
        df_final["country_label"]  = df_final["country_qid"].map(lab_map).fillna("")

    print(f"  └ final rows: {len(df_final)}")
    write_csv(df_final, final_path)
    return final_path

# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="WDQS pipeline (3 passos + labels) com countries_profiles.csv")
    ap.add_argument("--only", default="", help="ISO3 único (ex.: PRT).")
    ap.add_argument("--profiles", default=str(PROFILES_CSV), help="data/countries_profiles.csv (com iso3 e m49).")
    ap.add_argument("--out",  default="", help="Pasta de saída (default: diretoria temporária).")
    ap.add_argument("--limit", type=int, default=0, help="Limita nº de países.")
    ap.add_argument("--sleep", type=float, default=0.8, help="Pausa entre países.")
    args = ap.parse_args()

    profiles_csv = Path(args.profiles)
    profiles_df  = _load_profiles(profiles_csv)

    if args.out:
        out_dir = Path(args.out)
    else:
        out_dir = Path(tempfile.gettempdir()) / "wars_battles_tmp"
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[info] out_dir: {out_dir}", flush=True)

    if args.only:
        iso_list = [args.only.upper().strip()]
    else:
        iso_list = profiles_df["iso3"].dropna().astype(str).str.upper().str.strip().tolist()

    if args.limit and args.limit > 0:
        iso_list = iso_list[:args.limit]

    for i, iso3 in enumerate(iso_list, start=1):
        print(f"[{i}/{len(iso_list)}] {iso3}")
        try:
            process_country(iso3, out_dir, profiles_df)
        except KeyboardInterrupt:
            print("\n[warn] interrompido pelo utilizador.")
            break
        except Exception as e:
            print(f"[err] {iso3}: {e}", flush=True)
        time.sleep(args.sleep + random.uniform(0.0, 0.6))

    print("[done] pipeline concluído.", flush=True)

if __name__ == "__main__":
    main()
