# -*- coding: utf-8 -*-
# scripts/fetch_colonization_pt.py
from __future__ import annotations

import csv
import sys
import time
import random
from pathlib import Path
from typing import Dict, Any, List, Optional, Iterable, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==============================
# Configuração
# ==============================
WDQS = "https://query.wikidata.org/sparql"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
UA = {
    "User-Agent": "Good2Know/1.0 (colonization fetch; contact: cfmessias@gmail.com)",
    "Accept": "application/sparql-results+json",
}
ROOT = Path(__file__).resolve().parents[1]
SEED = ROOT / "data" / "countries_seed.csv"
OUT  = ROOT / "data" / "colonization.csv"

# Ritmo recomendado (≈1 req/s). Ajusta se necessário.
SLEEP_PER_COUNTRY = (1.0, 1.6)   # intervalo (min,max) em segundos
SAVE_EVERY = 25                  # grava parcial a cada N países

# Backoff interno para o WDQS (além do Retry do adapter)
MAX_ATTEMPTS = 3
BASE_BACKOFF = 8.0
JITTER_RANGE = (0.0, 2.0)

# Classes relevantes (para modo "STRICT")
STRICT_COLONIAL_CLASSES = [
    "Q133156",  # colony
    "Q1321760", # protectorate
    "Q180686",  # League of Nations mandate
    "Q215158",  # United Nations trust territory
    "Q46395",   # overseas territory
    "Q42962",   # dominion
    "Q23397",   # viceroyalty
    "Q192299",  # captaincy
    "Q855697",  # dependent territory
    "Q3024240", # condominium (shared sovereignty)
]

# ==============================
# Sessão HTTP robusta
# ==============================
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6,
        connect=3,
        read=3,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "POST"]),
        respect_retry_after_header=True,
    )
    ad = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", ad)
    s.mount("http://", ad)
    s.headers.update(UA)
    return s

SESSION = make_session()

# ==============================
# Utilitários CSV / seed
# ==============================
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

# ==============================
# SPARQL helpers
# ==============================
def _sparql(query: str, timeout_sec: int = 70) -> dict:
    """Chama WDQS com backoff adaptativo; em caso de erro persistente, devolve 0 linhas."""
    for k in range(MAX_ATTEMPTS):
        try:
            r = SESSION.post(WDQS, data={"query": query, "format": "json"}, timeout=timeout_sec)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RetryError:
            sleep_s = BASE_BACKOFF * (k + 1) + random.uniform(*JITTER_RANGE)
            print(f"[wdqs] RetryError → backoff {sleep_s:.1f}s (attempt {k+1}/{MAX_ATTEMPTS})")
            time.sleep(sleep_s)
            continue
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else None
            if code in (500, 502, 503, 504, 429) and k < MAX_ATTEMPTS - 1:
                try:
                    ra = e.response.headers.get("Retry-After")
                    ra_s = float(ra) if ra and ra.isdigit() else None
                except Exception:
                    ra_s = None
                sleep_s = (ra_s or (BASE_BACKOFF * (k + 1))) + random.uniform(*JITTER_RANGE)
                print(f"[wdqs] HTTP {code} → backoff {sleep_s:.1f}s (attempt {k+1}/{MAX_ATTEMPTS})")
                time.sleep(sleep_s)
                continue
            print(f"[wdqs] giving up ({code}); returning 0 linhas")
            return {"results": {"bindings": []}}
        except requests.RequestException as e:
            sleep_s = BASE_BACKOFF * (k + 1) + random.uniform(*JITTER_RANGE)
            print(f"[wdqs] {type(e).__name__} → backoff {sleep_s:.1f}s (attempt {k+1}/{MAX_ATTEMPTS})")
            time.sleep(sleep_s)
            continue
    return {"results": {"bindings": []}}

def _run_bindings(query: str) -> List[Dict[str, Any]]:
    js = _sparql(query)
    return js.get("results", {}).get("bindings", [])

def _qid_from_iso3(iso3: str, cache: Dict[str, str]) -> Optional[str]:
    if iso3 in cache:
        return cache[iso3]
    q = f'SELECT ?c WHERE {{ ?c wdt:P298 "{iso3}" . }} LIMIT 1'
    b = _run_bindings(q)
    qid = b[0]["c"]["value"].rpartition("/")[-1] if b else None
    if qid:
        cache[iso3] = qid
    return qid

# ==============================
# Queries (SEM labels; só QIDs + datas)
# ==============================
def _q_as_colony_strict(qid: str) -> str:
    classes = " ".join(f"wd:{c}" for c in STRICT_COLONIAL_CLASSES)
    return f"""
    SELECT ?detail ?colonizer ?start ?end ?inc ?diss WHERE {{
      VALUES ?country {{ wd:{qid} }}
      VALUES ?cls {{ {classes} }}
      ?detail wdt:P31/wdt:P279* ?cls ;
              wdt:P3842 ?country ;
              wdt:P17 ?colonizer .
      FILTER (?colonizer != ?country)
      OPTIONAL {{ ?detail wdt:P580 ?start. }}   # start time
      OPTIONAL {{ ?detail wdt:P582 ?end.   }}   # end time
      OPTIONAL {{ ?detail wdt:P571 ?inc.   }}   # inception
      OPTIONAL {{ ?detail wdt:P576 ?diss.  }}   # dissolved/abolished
    }}
    """

def _q_as_colony_relaxed(qid: str) -> str:
    # Sem filtro de classe — maior recall, continua leve (sem P131*)
    return f"""
    SELECT ?detail ?colonizer ?start ?end ?inc ?diss WHERE {{
      VALUES ?country {{ wd:{qid} }}
      ?detail wdt:P3842 ?country ;
              wdt:P17 ?colonizer .
      FILTER (?colonizer != ?country)
      OPTIONAL {{ ?detail wdt:P580 ?start. }}
      OPTIONAL {{ ?detail wdt:P582 ?end.   }}
      OPTIONAL {{ ?detail wdt:P571 ?inc.   }}
      OPTIONAL {{ ?detail wdt:P576 ?diss.  }}
    }}
    """

def _q_as_colonizer_strict(qid: str) -> str:
    classes = " ".join(f"wd:{c}" for c in STRICT_COLONIAL_CLASSES)
    return f"""
    SELECT ?detail ?today ?start ?end ?inc ?diss WHERE {{
      VALUES ?country {{ wd:{qid} }}
      VALUES ?cls {{ {classes} }}
      ?detail wdt:P31/wdt:P279* ?cls ;
              wdt:P17 ?country ;
              wdt:P3842 ?today .
      FILTER (?today != ?country)
      OPTIONAL {{ ?detail wdt:P580 ?start. }}
      OPTIONAL {{ ?detail wdt:P582 ?end.   }}
      OPTIONAL {{ ?detail wdt:P571 ?inc.   }}
      OPTIONAL {{ ?detail wdt:P576 ?diss.  }}
    }}
    """

def _q_as_colonizer_relaxed(qid: str) -> str:
    return f"""
    SELECT ?detail ?today ?start ?end ?inc ?diss WHERE {{
      VALUES ?country {{ wd:{qid} }}
      ?detail wdt:P17 ?country ;
              wdt:P3842 ?today .
      FILTER (?today != ?country)
      OPTIONAL {{ ?detail wdt:P580 ?start. }}
      OPTIONAL {{ ?detail wdt:P582 ?end.   }}
      OPTIONAL {{ ?detail wdt:P571 ?inc.   }}
      OPTIONAL {{ ?detail wdt:P576 ?diss.  }}
    }}
    """

# ==============================
# Labels (API, em lote, com cache)
# ==============================
def _wbgetentities_labels(qids: List[str], lang: str) -> Dict[str, str]:
    params = {
        "action": "wbgetentities",
        "ids": "|".join(qids),
        "props": "labels",
        "languages": lang,
        "format": "json",
    }
    r = SESSION.get(WIKIDATA_API, params=params, timeout=40)
    r.raise_for_status()
    data = r.json().get("entities", {})
    out = {}
    for qid, ent in data.items():
        lbl = ent.get("labels", {}).get(lang, {}).get("value", "")
        out[qid] = lbl
    return out

def fetch_labels(qids: Iterable[str], lang_order: Tuple[str, str] = ("pt", "en"),
                 cache: Dict[str, str] | None = None) -> Dict[str, str]:
    if cache is None:
        cache = {}
    qset = [q for q in set(qids) if q and q not in cache]
    if not qset:
        return cache
    batch_size = 50
    for i in range(0, len(qset), batch_size):
        chunk = qset[i:i+batch_size]
        label_map = _wbgetentities_labels(chunk, lang=lang_order[0])
        missing = [q for q in chunk if not label_map.get(q)]
        if missing and len(lang_order) > 1:
            label_map_en = _wbgetentities_labels(missing, lang=lang_order[1])
            label_map.update({q: label_map_en.get(q, "") for q in missing})
        for q in chunk:
            cache[q] = label_map.get(q, q) or q
        time.sleep(0.1)
    return cache

# ==============================
# Pipeline por país
# ==============================
def _year4(v: Optional[str]) -> str:
    return (v or "")[:4]

def process_country(iso3: str,
                    strict_only: bool,
                    qid_cache: Dict[str, str],
                    label_cache: Dict[str, str]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    qid = _qid_from_iso3(iso3, qid_cache)
    if not qid:
        print(f"[warn] sem QID para {iso3}")
        return rows

    # A) foi colónia de (STRICT → RELAXED)
    try:
        b = _run_bindings(_q_as_colony_strict(qid))
        if not b and not strict_only:
            b = _run_bindings(_q_as_colony_relaxed(qid))
        for r in b:
            colonizer_q = r.get("colonizer", {}).get("value", "")
            colonizer_q = colonizer_q.rpartition("/")[-1] if colonizer_q else ""
            detail_q    = r["detail"]["value"].rpartition("/")[-1]
            fy = _year4(r.get("start", {}).get("value")) or _year4(r.get("inc", {}).get("value"))
            ty = _year4(r.get("end",   {}).get("value")) or _year4(r.get("diss", {}).get("value"))
            rows.append({
                "iso3": iso3, "role": "as_colony",
                "other_qid": colonizer_q,
                "detail_item_qid": detail_q,
                "from_year": fy, "to_year": ty,
            })
    except Exception as e:
        print(f"[warn] {iso3} as_colony: {e}")

    # B) foi colonizador de (STRICT → RELAXED)
    try:
        b = _run_bindings(_q_as_colonizer_strict(qid))
        if not b and not strict_only:
            b = _run_bindings(_q_as_colonizer_relaxed(qid))
        for r in b:
            today_q  = r.get("today", {}).get("value", "")
            today_q  = today_q.rpartition("/")[-1] if today_q else ""
            detail_q = r["detail"]["value"].rpartition("/")[-1]
            fy = _year4(r.get("start", {}).get("value")) or _year4(r.get("inc", {}).get("value"))
            ty = _year4(r.get("end",   {}).get("value")) or _year4(r.get("diss", {}).get("value"))
            rows.append({
                "iso3": iso3, "role": "as_colonizer",
                "other_qid": today_q,
                "detail_item_qid": detail_q,
                "from_year": fy, "to_year": ty,
            })
    except Exception as e:
        print(f"[warn] {iso3} as_colonizer: {e}")

    # labels em lote (pt→en)
    qids_to_label = {r["other_qid"] for r in rows if r["other_qid"]}
    qids_to_label |= {r["detail_item_qid"] for r in rows if r["detail_item_qid"]}
    fetch_labels(list(qids_to_label), ("pt", "en"), cache=label_cache)
    for r in rows:
        r["other_label"] = label_cache.get(r["other_qid"], r["other_qid"])
        r["detail_item_label"] = label_cache.get(r["detail_item_qid"], r["detail_item_qid"])

    # remove duplicados por (iso3, role, other_qid, detail_item_qid)
    seen = set()
    deduped = []
    for r in rows:
        key = (r["iso3"], r["role"], r["other_qid"], r["detail_item_qid"], r.get("from_year",""), r.get("to_year",""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    return deduped

# ==============================
# Gravação
# ==============================
COLS = ["iso3","role","other_qid","other_label","from_year","to_year","detail_item_qid","detail_item_label"]

def _save_csv(rows: List[Dict[str, str]], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=COLS, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        wr.writeheader()
        for r in rows:
            wr.writerow({c: r.get(c, "") for c in COLS})

# ==============================
# Main
# ==============================
def main(argv: List[str]):
    # flags:
    # --strict-only : não usa o fallback relaxado
    # 3-letras      : limitar a países específicos (ISO3)
    strict_only = ("--strict-only" in argv)
    only_iso = [a for a in argv if len(a) == 3 and a.isalpha()]

    df = _ensure_iso3(_read_seed_df(SEED))
    iso_list = (only_iso or df["iso3"].astype(str).str.strip().str.upper().dropna().unique().tolist())

    print(f"[info] países no seed: {len(iso_list)}")
    qid_cache: Dict[str, str] = {}
    label_cache: Dict[str, str] = {}
    all_rows: List[Dict[str, str]] = []

    # mapear todos (para log)
    mapped = 0
    for iso3 in iso_list:
        if _qid_from_iso3(iso3, qid_cache):
            mapped += 1
        time.sleep(0.02)
    print(f"[info] mapeados {mapped} iso3 → QID")

    for idx, iso3 in enumerate(iso_list, 1):
        iso3 = iso3.strip().upper()
        try:
            rows = process_country(iso3, strict_only, qid_cache, label_cache)
            all_rows.extend(rows)
            print(f"[ok] {iso3}: "
                  f"{sum(1 for r in rows if r['role']=='as_colony')} colónia; "
                  f"{sum(1 for r in rows if r['role']=='as_colonizer')} colonizador "
                  f"({idx}/{len(iso_list)})")
        except Exception as e:
            print(f"[err] {iso3}: {e}")

        # ritmo para não agredir WDQS
        time.sleep(random.uniform(*SLEEP_PER_COUNTRY))

        if idx % SAVE_EVERY == 0 and all_rows:
            # deduplicar global antes de gravar
            all_rows = _dedup_global(all_rows)
            _save_csv(all_rows, OUT)
            print(f"[save] parcial @ {idx}: {OUT}")

    if all_rows:
        all_rows = _dedup_global(all_rows)
        _save_csv(all_rows, OUT)
        print(f"[save] final: {OUT} ({len(all_rows)} linhas)")
    else:
        print("[info] sem linhas para guardar.")

def _dedup_global(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out = []
    for r in rows:
        key = (r["iso3"], r["role"], r["other_qid"], r["detail_item_qid"], r.get("from_year",""), r.get("to_year",""))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

if __name__ == "__main__":
    main(sys.argv[1:])
