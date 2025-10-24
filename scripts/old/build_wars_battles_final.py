# -*- coding: utf-8 -*-
# build_wars_battles_final.py
from __future__ import annotations
import argparse, time, random, json, hashlib, tempfile, csv
from pathlib import Path
from typing import List, Dict, Set
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlencode

WDQS_URL = "https://query.wikidata.org/sparql"
HEADERS = {"User-Agent": "GeoWars-Final/1.1 (+streamlit)", "Accept": "application/sparql-results+json"}

# ------------------------- WDQS + Cache --------------------------
_session = requests.Session()
_retry = Retry(
    total=8, connect=5, read=5,
    status_forcelist=(429,500,502,503,504),
    backoff_factor=0.7,
    allowed_methods=frozenset(["GET","POST"]),
    raise_on_status=False,
)
_session.mount("https://", HTTPAdapter(max_retries=_retry))

CACHE_DIR = Path(tempfile.gettempdir()) / "wdqs_cache_labels"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_TTL = 24*3600  # 1 dia

def _cache_key(q: str) -> Path:
    return CACHE_DIR / (hashlib.sha1(q.encode("utf-8")).hexdigest() + ".json")

def _wdqs(query: str, attempts: int = 2, timeout: int = 60):
    # tenta cache
    ck = _cache_key(query)
    now = time.time()
    if ck.exists() and now - ck.stat().st_mtime < CACHE_TTL:
        try:
            return json.loads(ck.read_text(encoding="utf-8"))
        except Exception:
            pass

    def _parse(r: requests.Response):
        r.raise_for_status()
        return r.json()["results"]["bindings"]

    delay = 1.0
    for i in range(1, attempts+1):
        try:
            if len(query) < 7500:
                url = f"{WDQS_URL}?{urlencode({'query': query})}"
                r = _session.get(url, headers=HEADERS, timeout=timeout)
                if r.status_code == 200:
                    rows = _parse(r); ck.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8"); return rows
            r = _session.post(WDQS_URL, data={"query": query}, headers=HEADERS, timeout=timeout)
            rows = _parse(r); ck.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8"); return rows
        except Exception as e:
            sleep_s = delay + random.uniform(0, 0.6)
            print(f"[wdqs] tentativa {i}/{attempts} falhou: {e} -> dormir {sleep_s:.1f}s", flush=True)
            time.sleep(sleep_s); delay = min(delay*1.9, 12.0)
    print("[wdqs] erro definitivo; a devolver lista vazia", flush=True)
    return []

# --------------------------- Helpers -----------------------------
def _qid(uri: str) -> str:
    return uri.rpartition("/")[-1] if uri else ""

def fetch_labels_multi(qids: List[str]) -> pd.DataFrame:
    """
    Devolve labels PT/EN para uma lista de QIDs.
    Saída: id, label_pt, label_en
    """
    qids = [q for q in qids if q and q.startswith("Q")]
    if not qids:
        return pd.DataFrame(columns=["id","label_pt","label_en"])

    out = []
    for i in range(0, len(qids), 200):
        chunk = qids[i:i+200]
        vals = " ".join(f"wd:{q}" for q in chunk)
        q = f"""
SELECT ?id ?pt ?en WHERE {{
  VALUES ?id {{ {vals} }}
  OPTIONAL {{ ?id rdfs:label ?pt FILTER(LANG(?pt)="pt") }}
  OPTIONAL {{ ?id rdfs:label ?en FILTER(LANG(?en)="en") }}
}}
""".strip()
        rows = _wdqs(q)
        for r in rows:
            _id = _qid(r.get("id",{}).get("value",""))
            pt  = r.get("pt",{}).get("value","")
            en  = r.get("en",{}).get("value","")
            if _id:
                out.append({"id": _id, "label_pt": pt, "label_en": en})
        time.sleep(0.15 + random.uniform(0.0, 0.15))

    return pd.DataFrame(out).drop_duplicates("id")

def _sniff_sep(path: Path) -> str:
    sample = path.read_text(encoding="utf-8", errors="ignore")[:4096]
    try:
        return csv.Sniffer().sniff(sample, delimiters=";,|\t").delimiter
    except Exception:
        return ";" if sample.count(";") >= sample.count(",") else ","

def _clean_text(s: str) -> str:
    s = ("" if s is None else str(s)).strip()
    return "" if s.lower() in {"nan","none","null"} else s

# ----------------------------- Main ------------------------------
def main():
    ap = argparse.ArgumentParser(description="Gera data/wars_battles.csv com labels PT/EN para conflitos e tipos (sep=';').")
    ap.add_argument("--conflicts", default="data/conflicts_all.csv", help="Input com colunas iso3,who_qid,conflict_qid,kind_qid,...")
    ap.add_argument("--out",       default="data/wars_battles.csv", help="Output final para o UI (sep=';')")
    ap.add_argument("--limit",     type=int, default=0, help="Limitar nº de países (para testes, baseado em iso3)")
    args = ap.parse_args()

    in_csv  = Path(args.conflicts)
    out_csv = Path(args.out)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    # lê com deteção de separador (há ficheiros antigos a vírgula)
    sep_in = _sniff_sep(in_csv)
    df = pd.read_csv(in_csv, sep=sep_in, dtype=str, encoding="utf-8", keep_default_na=False)
    if not {"iso3","conflict_qid","kind_qid"} <= set(df.columns):
        raise RuntimeError("conflicts_all.csv precisa de colunas 'iso3','conflict_qid','kind_qid'.")

    # Normaliza e (opcional) limita # países
    df["iso3"] = df["iso3"].str.upper().str.strip()
    if args.limit > 0:
        keep_iso = df["iso3"].drop_duplicates().head(args.limit).tolist()
        df = df[df["iso3"].isin(keep_iso)].copy()

    # -> “vista por país”: mantém 1 linha por (iso3, conflict_qid)
    meta_cols = ["kind_qid","start_year","end_year","point_year"]
    for c in meta_cols:
        if c not in df.columns:
            df[c] = ""
    df_meta = (df
               .sort_values(meta_cols)
               .drop_duplicates(["iso3","conflict_qid"])
               [["iso3","conflict_qid"] + meta_cols]
               .reset_index(drop=True))

    # Labels PT/EN para conflicts e kinds
    want_conflicts: Set[str] = set(df_meta["conflict_qid"])
    want_kinds:     Set[str] = set(df_meta["kind_qid"].dropna())
    lbl = fetch_labels_multi(sorted(want_conflicts | want_kinds))
    lab_pt = dict(zip(lbl["id"], lbl["label_pt"]))
    lab_en = dict(zip(lbl["id"], lbl["label_en"]))

    df_meta["conflict_label_pt"] = df_meta["conflict_qid"].map(lab_pt).fillna("")
    df_meta["conflict_label_en"] = df_meta["conflict_qid"].map(lab_en).fillna("")
    # Fallback PT <- EN
    empty_pt = df_meta["conflict_label_pt"].astype(str).str.strip().eq("")
    if empty_pt.any():
        df_meta.loc[empty_pt, "conflict_label_pt"] = df_meta.loc[empty_pt, "conflict_label_en"]

    # Limpeza de literais 'nan/None/null'
    for col in ["conflict_label_pt","conflict_label_en"]:
        df_meta[col] = df_meta[col].map(_clean_text)

    df_meta["kind_label"] = df_meta["kind_qid"].map(lambda q: lab_pt.get(q) or lab_en.get(q) or "")

    # Ordenação amigável (ano desc, conflito asc)
    def _y4(x: str) -> int | None:
        try:
            return int(str(x)[:4])
        except Exception:
            return None
    df_meta["_sort_year"] = df_meta["point_year"].apply(_y4).fillna(df_meta["start_year"].apply(_y4))

    final_cols = [
        "iso3",
        "conflict_qid", "conflict_label_pt", "conflict_label_en",
        "kind_qid", "kind_label",
        "start_year", "end_year", "point_year"
    ]
    out_df = df_meta.sort_values(by=["_sort_year","conflict_label_pt","conflict_qid"],
                                 ascending=[False, True, True])[final_cols].copy()

    # **SAÍDA: sempre ';'**
    out_df.to_csv(out_csv, sep=";", index=False, encoding="utf-8")
    print(f"[done] {len(out_df)} linhas → {out_csv} (sep=';')")

if __name__ == "__main__":
    main()
