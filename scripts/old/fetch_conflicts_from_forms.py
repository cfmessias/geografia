# -*- coding: utf-8 -*-
# conflicts_from_forms.py
from __future__ import annotations
import argparse, time, random, json, hashlib, tempfile, csv
from pathlib import Path
from typing import List, Dict
from datetime import datetime, timezone


import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlencode

WDQS_URL = "https://query.wikidata.org/sparql"
HEADERS = {
    "User-Agent": "GeoWars-Conflicts/1.1 (+streamlit)",
    "Accept": "application/sparql-results+json",
}

# Tipos de conflito
CONFLICT_CLASSES_BROAD  = ["Q198","Q178561","Q645883","Q350604","Q180684"]  # war/battle/operation/armed conflict/conflict
CONFLICT_CLASSES_NARROW = ["Q198","Q178561"]  # mais leve: war/battle

# Excluir guerras fictícias
FICTIONAL_WAR_QID = "Q17198419"  # fictional war

# ------------- WDQS client com retry + cache em disco -------------
_session = requests.Session()
_retry = Retry(
    total=8, connect=5, read=5,
    status_forcelist=(429, 500, 502, 503, 504),
    backoff_factor=0.7,
    allowed_methods=frozenset(["GET", "POST"]),
    raise_on_status=False,
)
_session.mount("https://", HTTPAdapter(max_retries=_retry))

CACHE_DIR = Path(tempfile.gettempdir()) / "wdqs_cache_conflicts"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_TTL = 24 * 3600  # 1 dia

def _cache_key(q: str) -> Path:
    return CACHE_DIR / (hashlib.sha1(q.encode("utf-8")).hexdigest() + ".json")

def wdqs(query: str, attempts: int = 2, timeout: int = 60):
    # tenta cache
    k = _cache_key(query)
    now = time.time()
    if k.exists() and now - k.stat().st_mtime < CACHE_TTL:
        try:
            return json.loads(k.read_text(encoding="utf-8"))
        except Exception:
            pass

    def _parse(r: requests.Response):
        r.raise_for_status()
        return r.json()["results"]["bindings"]

    delay = 1.0
    for i in range(1, attempts + 1):
        try:
            if len(query) < 7500:
                url = f"{WDQS_URL}?{urlencode({'query': query})}"
                r = _session.get(url, headers=HEADERS, timeout=timeout)
                if r.status_code == 200:
                    rows = _parse(r)
                    k.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
                    return rows
            r = _session.post(WDQS_URL, data={"query": query}, headers=HEADERS, timeout=timeout)
            rows = _parse(r)
            k.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
            return rows
        except Exception as e:
            sleep_s = delay + random.uniform(0, 0.6)
            print(f"[wdqs] tentativa {i}/{attempts} falhou: {e} -> dormir {sleep_s:.1f}s", flush=True)
            time.sleep(sleep_s); delay = min(delay * 1.9, 10.0)

    print("[wdqs] erro definitivo; a devolver lista vazia", flush=True)
    return []

# ---------------------------- utils ----------------------------
def _qid(uri: str) -> str:
    return uri.rpartition("/")[-1] if uri else ""

def _y4(x: str | None) -> str:
    if not x: return ""
    try: return str(int(str(x)[:4]))
    except Exception: return ""

def sniff_sep(path: Path) -> str:
    sample = path.read_text(encoding="utf-8", errors="ignore")[:4096]
    try:
        return csv.Sniffer().sniff(sample, delimiters=";,|\t").delimiter
    except Exception:
        return ";" if sample.count(";") >= sample.count(",") else ","

# ---------------------------- core ----------------------------
def fetch_conflicts_for_iso3_qids(iso3: str, qids: List[str], conflict_classes: List[str], batch: int = 25) -> pd.DataFrame:
    """
    Para um país (iso3) e o seu conjunto de QIDs de formas, devolve DataFrame:
      iso3, who_qid, conflict_qid, kind_qid, start_year, end_year, point_year
    Apenas participação DIRETA: ?conflict p:P710/ps:P710 ?who
    Exclui guerras fictícias (P31/P279* Q17198419).
    """
    if not qids:
        return pd.DataFrame(columns=["iso3","who_qid","conflict_qid","kind_qid","start_year","end_year","point_year"])

    allowed = " ".join(f"wd:{c}" for c in conflict_classes)
    rows_all = []

    for i in range(0, len(qids), batch):
        chunk = qids[i:i+batch]
        vals = " ".join(f"wd:{q}" for q in chunk)
        q = f"""
SELECT DISTINCT ?conflict ?who ?ctype ?start ?end ?point WHERE {{
  VALUES ?who {{ {vals} }}
  ?conflict p:P710 ?st . ?st ps:P710 ?who .
  VALUES ?ctype {{ {allowed} }}
  ?conflict wdt:P31/wdt:P279* ?ctype .

  # EXCLUSÃO: guerras fictícias
  FILTER NOT EXISTS {{ ?conflict wdt:P31/wdt:P279* wd:{FICTIONAL_WAR_QID} }}

  OPTIONAL {{ ?conflict wdt:P580 ?start }}
  OPTIONAL {{ ?conflict wdt:P582 ?end }}
  OPTIONAL {{ ?conflict wdt:P585 ?point }}
}}
""".strip()
        rows = wdqs(q)
        rows_all.extend(rows)
        time.sleep(0.25 + random.uniform(0.0, 0.25))

    data = []
    for r in rows_all:
        conflict = _qid(r.get("conflict", {}).get("value", ""))
        who      = _qid(r.get("who", {}).get("value", ""))
        ctype    = _qid(r.get("ctype", {}).get("value", ""))
        if not (conflict.startswith("Q") and who.startswith("Q") and ctype.startswith("Q")):
            continue
        data.append({
            "iso3": iso3,
            "who_qid": who,
            "conflict_qid": conflict,
            "kind_qid": ctype,
            "start_year": _y4(r.get("start", {}).get("value")),
            "end_year":   _y4(r.get("end", {}).get("value")),
            "point_year": _y4(r.get("point", {}).get("value")),
        })
    if not data:
        return pd.DataFrame(columns=["iso3","who_qid","conflict_qid","kind_qid","start_year","end_year","point_year"])

    df = pd.DataFrame(data).drop_duplicates(["iso3","who_qid","conflict_qid","kind_qid","start_year","end_year","point_year"])

    # (opcional) EXCLUI datas futuras como salvaguarda
    THIS_YEAR = datetime.now(timezone.utc).year
    for c in ["start_year","end_year","point_year"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    mask_future = (df["point_year"] > THIS_YEAR) | (df["start_year"] > THIS_YEAR) | (df["end_year"] > THIS_YEAR)
    if mask_future.any():
        df = df.loc[~mask_future].copy()

    # volta a strings (UI espera strings)
    for c in ["start_year","end_year","point_year"]:
        df[c] = df[c].fillna("").astype(str).str.replace(".0","", regex=False)

    return df

# ---------------------------- main ----------------------------
def main():
    ap = argparse.ArgumentParser(description="Extrai conflitos diretos (P710) para QIDs em data/forms_all.csv, excluindo guerras fictícias.")
    ap.add_argument("--forms", default="data/forms_all.csv", help="CSV de input com colunas iso3;qid")
    ap.add_argument("--out",   default="data/conflicts_all.csv", help="CSV de output (sep=';')")
    ap.add_argument("--narrow", action="store_true", help="Usar só war/battle (mais leve)")
    ap.add_argument("--batch", type=int, default=25, help="Batch de QIDs por query (default 25)")
    ap.add_argument("--sleep", type=float, default=0.6, help="Pausa entre países")
    ap.add_argument("--limit", type=int, default=0, help="Limitar nº de países (para testes)")
    args = ap.parse_args()

    forms_csv = Path(args.forms)
    out_csv   = Path(args.out)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    sep_in = sniff_sep(forms_csv)
    df = pd.read_csv(forms_csv, sep=sep_in, dtype=str, encoding="utf-8", keep_default_na=False)
    if not {"iso3","qid"} <= set(df.columns):
        raise RuntimeError("forms_all.csv precisa de colunas 'iso3' e 'qid'.")

    conflict_classes = CONFLICT_CLASSES_NARROW if args.narrow else CONFLICT_CLASSES_BROAD

    groups: Dict[str, List[str]] = (
        df.groupby("iso3")["qid"].apply(lambda s: [q for q in s.astype(str).tolist() if q.startswith("Q")]).to_dict()
    )
    iso_list = list(groups.keys())
    if args.limit > 0:
        iso_list = iso_list[:args.limit]

    header_needed = not out_csv.exists()
    total_rows = 0

    for i, iso3 in enumerate(iso_list, start=1):
        qids = groups.get(iso3, [])
        print(f"[{i}/{len(iso_list)}] {iso3} — {len(qids)} forms")
        try:
            df_conf = fetch_conflicts_for_iso3_qids(iso3, qids, conflict_classes, batch=args.batch)
            if df_conf.empty:
                print("  └ 0 conflitos")
            else:
                # **SAÍDA com SEP=';'**
                df_conf.to_csv(out_csv, mode="a", index=False, encoding="utf-8", header=header_needed, sep=";")
                header_needed = False
                total_rows += len(df_conf)
                print(f"  └ +{len(df_conf)} linhas (acumulado {total_rows})")
        except KeyboardInterrupt:
            print("\n[warn] interrompido."); break
        except Exception as e:
            print(f"[err] {iso3}: {e}")
        time.sleep(args.sleep + random.uniform(0.0, 0.6))

    print(f"[done] {total_rows} linhas → {out_csv}")

if __name__ == "__main__":
    main()
