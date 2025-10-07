# -*- coding: utf-8 -*-
# scripts/fetch_colonization_pt.py — versão otimizada
from __future__ import annotations
import csv, sys, time, random
from pathlib import Path
from typing import Dict, Any, List, Optional, Iterable, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd

WDQS = "https://query.wikidata.org/sparql"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
UA = {
    "User-Agent": "Good2Know/1.0 (colonization fetch; contact: cfmessias@gmail.com)",
    "Accept": "application/sparql-results+json"
}

ROOT = Path(__file__).resolve().parents[1]
SEED = ROOT / "data" / "countries_seed.csv"
OUT  = ROOT / "data" / "colonization.csv"

# ----------------- Helpers -----------------
def _read_seed_df(path: Path) -> pd.DataFrame:
    # tenta ; e , e limpa BOM/espacos dos headers
    for sep in (";", ","):
        try:
            df = pd.read_csv(path, dtype=str, sep=sep)
            if df.shape[1] > 1:
                break
        except Exception:
            continue
    else:
        df = pd.read_csv(path, dtype=str, engine="python")
    df.columns = [c.replace("\ufeff", "").strip().lower().replace(" ", "_") for c in df.columns]
    return df

def _ensure_iso3(df: pd.DataFrame) -> pd.DataFrame:
    # mapeia nomes comuns para 'iso3'
    aliases = {"iso_3": "iso3", "iso-3": "iso3", "alpha3": "iso3", "alpha_3": "iso3", "alpha-3": "iso3"}
    for old, new in aliases.items():
        if old in df.columns and "iso3" not in df.columns:
            df = df.rename(columns={old: new})
    # fallback via m49 → iso3 (se existir o ficheiro de mapeamento)
    if "iso3" not in df.columns and "m49" in df.columns:
        map_path = ROOT / "data" / "un_m49_iso.csv"
        if map_path.exists():
            map_df = pd.read_csv(map_path, dtype=str, sep=";")
            map_df.columns = [c.replace("\ufeff", "").strip().lower().replace(" ", "_") for c in map_df.columns]
            df["m49"] = df["m49"].astype(str).str.strip()
            map_df["m49"] = map_df["m49"].astype(str).str.strip()
            df = df.merge(map_df[["m49", "iso3"]], on="m49", how="left")
    if "iso3" not in df.columns:
        raise KeyError(f"Coluna 'iso3' não encontrada. Cabeçalhos: {list(df.columns)}")
    return df

# ---------------- Sessão HTTP robusta ----------------
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6, connect=3, read=3, backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "POST"])
    )
    ad = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", ad); s.mount("http://", ad)
    s.headers.update(UA)
    return s

SESSION = make_session()

# ---------------- Helpers SPARQL ----------------
def _sparql(query: str, timeout_sec: int = 70) -> dict:
    # POST é mais estável e evita query string gigante
    r = SESSION.post(WDQS, data={"query": query, "format": "json"}, timeout=timeout_sec)
    r.raise_for_status()
    return r.json()

def _qid_from_iso3(iso3: str, cache: Dict[str, str]) -> Optional[str]:
    if iso3 in cache:
        return cache[iso3]
    q = f'SELECT ?c WHERE {{ ?c wdt:P298 "{iso3}" . }} LIMIT 1'
    js = _sparql(q)
    b = js.get("results", {}).get("bindings", [])
    qid = b[0]["c"]["value"].rpartition("/")[-1] if b else None
    if qid:
        cache[iso3] = qid
    return qid

# --------- Queries (SEM labels; só QIDs + datas) ---------
def _q_as_colony_fast(qid: str) -> str:
    # “foi colónia de”: detail (instância/subclasse de colónia Q133156) e país atual por P3842
    return f"""
    SELECT ?detail ?colonizer ?start ?end WHERE {{
      VALUES ?country {{ wd:{qid} }}
      ?detail wdt:P31/wdt:P279* wd:Q133156 ;
              wdt:P3842 ?country .
      OPTIONAL {{ ?detail wdt:P17  ?colonizer. }}
      OPTIONAL {{ ?detail wdt:P580 ?start.     }}
      OPTIONAL {{ ?detail wdt:P582 ?end.       }}
    }}
    """

def _q_as_colony_slow(qid: str) -> str:
    # Fallback com P131* (mais lento)
    return f"""
    PREFIX hint: <http://www.bigdata.com/queryHints#>
    SELECT ?detail ?colonizer ?start ?end WHERE {{
      hint:Query hint:timeout "60000" .
      VALUES ?country {{ wd:{qid} }}
      ?detail wdt:P31/wdt:P279* wd:Q133156 .
      {{ ?detail wdt:P3842 ?country }}
      UNION
      {{ ?detail wdt:P131* ?country }}
      OPTIONAL {{ ?detail wdt:P17  ?colonizer. }}
      OPTIONAL {{ ?detail wdt:P580 ?start.     }}
      OPTIONAL {{ ?detail wdt:P582 ?end.       }}
    }}
    """

def _q_as_colonizer_fast(qid: str) -> str:
    # “foi colonizador de”: país colonizador em P17; o “país atual” do território vem por P3842
    return f"""
    SELECT ?detail ?today ?start ?end WHERE {{
      VALUES ?country {{ wd:{qid} }}
      ?detail wdt:P31/wdt:P279* wd:Q133156 ;
              wdt:P17 ?country .
      OPTIONAL {{ ?detail wdt:P3842 ?today . }}
      OPTIONAL {{ ?detail wdt:P580 ?start. }}
      OPTIONAL {{ ?detail wdt:P582 ?end.   }}
    }}
    """

def _q_as_colonizer_slow(qid: str) -> str:
    # Fallback sem P3842: tenta derivar “país atual” via cadeia administrativa com ISO3
    return f"""
    PREFIX hint: <http://www.bigdata.com/queryHints#>
    SELECT ?detail ?today ?start ?end WHERE {{
      hint:Query hint:timeout "60000" .
      VALUES ?country {{ wd:{qid} }}
      ?detail wdt:P31/wdt:P279* wd:Q133156 ;
              wdt:P17 ?country .
      OPTIONAL {{
        ?detail wdt:P131 ?a1 .
        ?a1 (wdt:P131)* ?today .
        ?today wdt:P298 ?iso3 .
      }}
      OPTIONAL {{ ?detail wdt:P580 ?start. }}
      OPTIONAL {{ ?detail wdt:P582 ?end.   }}
    }}
    """

def _run_bindings(query: str) -> List[Dict[str, Any]]:
    js = _sparql(query)
    return js.get("results", {}).get("bindings", [])

# ---------------- Labels (API, em lote, com cache) ----------------
def fetch_labels(qids: Iterable[str], lang_order: Tuple[str, str] = ("pt", "en"),
                 cache: Dict[str, str] | None = None) -> Dict[str, str]:
    """
    Obtém labels pt com fallback en pela API wbgetentities, em lotes até 50.
    Usa e atualiza cache {QID: label}.
    """
    if cache is None:
        cache = {}
    qset = [q for q in set(qids) if q and q not in cache]
    if not qset:
        return cache
    batch_size = 50
    for i in range(0, len(qset), batch_size):
        chunk = qset[i:i+batch_size]
        # Tenta pt primeiro
        label_map = _wbgetentities_labels(chunk, lang=lang_order[0])
        # Falta alguém? tenta en
        missing = [q for q in chunk if q not in label_map or not label_map[q]]
        if missing and len(lang_order) > 1:
            label_map_en = _wbgetentities_labels(missing, lang=lang_order[1])
            label_map.update({q: label_map_en.get(q, "") for q in missing})
        # Atualiza cache
        for q in chunk:
            cache[q] = label_map.get(q, q) or q
        time.sleep(0.1)
    return cache

def _wbgetentities_labels(qids: List[str], lang: str) -> Dict[str, str]:
    params = {
        "action": "wbgetentities",
        "ids": "|".join(qids),
        "props": "labels",
        "languages": lang,
        "format": "json"
    }
    r = SESSION.get(WIKIDATA_API, params=params, timeout=40)
    r.raise_for_status()
    data = r.json().get("entities", {})
    out = {}
    for qid, ent in data.items():
        lbl = ent.get("labels", {}).get(lang, {}).get("value", "")
        out[qid] = lbl
    return out

# ---------------- Pipeline por país ----------------
def process_country(iso3: str, fast_only: bool,
                    qid_cache: Dict[str, str],
                    label_cache: Dict[str, str]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    qid = _qid_from_iso3(iso3, qid_cache)
    if not qid:
        print(f"[warn] sem QID para {iso3}")
        return rows

    def year(v: Optional[str]) -> str:
        return (v or "")[:4]

    # A) foi colónia de…
    try:
        b = _run_bindings(_q_as_colony_fast(qid))
        if not b and not fast_only:
            b = _run_bindings(_q_as_colony_slow(qid))
        for r in b:
            colonizer_q = r.get("colonizer", {}).get("value", "")
            colonizer_q = colonizer_q.rpartition("/")[-1] if colonizer_q else ""
            detail_q = r["detail"]["value"].rpartition("/")[-1]
            rows.append({
                "iso3": iso3, "role": "as_colony",
                "other_qid": colonizer_q,  # colonizador
                "detail_item_qid": detail_q,
                "from_year": year(r.get("start", {}).get("value")),
                "to_year": year(r.get("end", {}).get("value")),
            })
    except Exception as e:
        print(f"[warn] {iso3} as_colony: {e}")

    # B) foi colonizador de…
    try:
        b = _run_bindings(_q_as_colonizer_fast(qid))
        if not b and not fast_only:
            b = _run_bindings(_q_as_colonizer_slow(qid))
        for r in b:
            today_q = r.get("today", {}).get("value", "")
            today_q = today_q.rpartition("/")[-1] if today_q else ""
            detail_q = r["detail"]["value"].rpartition("/")[-1]
            rows.append({
                "iso3": iso3, "role": "as_colonizer",
                "other_qid": today_q,  # território/país colonizado (atual)
                "detail_item_qid": detail_q,
                "from_year": year(r.get("start", {}).get("value")),
                "to_year": year(r.get("end", {}).get("value")),
            })
    except Exception as e:
        print(f"[warn] {iso3} as_colonizer: {e}")

    # Buscar labels para other_qid e detail_item_qid em lote (uma vez por país)
    qids_to_label = set()
    for r in rows:
        if r["other_qid"]:
            qids_to_label.add(r["other_qid"])
        if r["detail_item_qid"]:
            qids_to_label.add(r["detail_item_qid"])

    fetch_labels(list(qids_to_label), ("pt", "en"), cache=label_cache)

    # Enriquecer com labels sem reconsultar WDQS
    for r in rows:
        r["other_label"] = label_cache.get(r["other_qid"], r["other_qid"])
        r["detail_item_label"] = label_cache.get(r["detail_item_qid"], r["detail_item_qid"])

    return rows

# ---------------- Main ----------------
def main(argv: List[str]):
    # flags simples
    fast_only = ("--fast-only" in argv)
    only_iso = [a for a in argv if len(a) == 3 and a.isalpha()]

    # lê seed (aceita ; ou , automaticamente)
    #sep = ";" if SEED.suffix.lower()==".csv" else None
    df = _ensure_iso3(_read_seed_df(SEED))
    iso_list = (
    only_iso
        or df["iso3"].astype(str).str.strip().str.upper().dropna().unique().tolist()
    )
    

    qid_cache: Dict[str, str] = {}
    label_cache: Dict[str, str] = {}
    all_rows: List[Dict[str, str]] = []

    for idx, iso3 in enumerate(iso_list, 1):
        iso3 = iso3.strip().upper()
        try:
            rows = process_country(iso3, fast_only, qid_cache, label_cache)
            all_rows.extend(rows)
            print(f"[ok] {iso3}: {sum(1 for r in rows if r['role']=='as_colony')} colónia; "
                  f"{sum(1 for r in rows if r['role']=='as_colonizer')} colonizador "
                  f"({idx}/{len(iso_list)})")
        except requests.HTTPError as e:
            print(f"[err] {iso3}: HTTP {e.response.status_code}")
        except Exception as e:
            print(f"[err] {iso3}: {e}")

        # pequeno jitter para suavizar rate limits
        time.sleep(0.35 + random.uniform(0.0, 0.3))

        # gravação incremental a cada 25 países, para segurança
        if idx % 25 == 0 and all_rows:
            _save_csv(all_rows, OUT)
            print(f"[save] parcial @ {idx}: {OUT}")

    if all_rows:
        _save_csv(all_rows, OUT)
        print(f"[save] final: {OUT}")
    else:
        print("[info] sem linhas para guardar.")

def _save_csv(rows: List[Dict[str, str]], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    # garantir colunas consistentes
    cols = ["iso3","role","other_qid","other_label","from_year","to_year","detail_item_qid","detail_item_label"]
    with path.open("w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=cols, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        wr.writeheader()
        for r in rows:
            wr.writerow({c: r.get(c, "") for c in cols})

if __name__ == "__main__":
    main(sys.argv[1:])
