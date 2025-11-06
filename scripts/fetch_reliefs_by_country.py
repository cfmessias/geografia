# scripts/fetch_reliefs_by_country.py
# -*- coding: utf-8 -*-
"""
Fetch reliefs/plateaus (montanhas, cordilheiras, planaltos, colinas, vales) por país a partir da Wikidata.
Abordagem em 2 fases:
  - Fase A: recolhe apenas QIDs por país, fatiando por classe e por níveis P131 (evita P131*).
  - Fase B: recolhe propriedades (label/descrição, tipo, elevação, coordenadas) em batches.

Saída: data/reliefs.csv (sep=";")
Colunas:
  iso3;feature_qid;feature_label;feature_description;type_qid;type_label;elevation_m;lat;lon;source

Parâmetros típicos:
  --only PRT,ESP           (limitar a alguns países)
  --depth 1                (0..2; profundidade P131 — 0=apenas P17)
  --limit-per-branch 1500  (IDs por ramo classe×depth)
  --batch-size 25          (tamanho do batch para recolha de propriedades)
  --overwrite              (recria o CSV)

Requisitos:
  - data/countries_profiles.csv com iso3 e qid (ou country_iso3 / country_qid)

Autor: (projecto Geografia)
"""

from __future__ import annotations

import argparse
import random
import sys
import time
from pathlib import Path
from typing import Dict, List

import requests
import pandas as pd


# -------------------- Paths & Consts --------------------
DATA_DIR  = Path("data")
PROFILES  = DATA_DIR / "countries_profiles.csv"
OUT_CSV   = DATA_DIR / "reliefs.csv"

WDQS_URL   = "https://query.wikidata.org/sparql"
USER_AGENT = "GeografiaReliefs/1.0 (+https://cfmessias.pt; contact: cfmessias@gmail.com)"

REQUEST_TIMEOUT = 60
MAX_RETRIES     = 4

# Classes (CONFIRMAR QIDs conforme necessidade):
CLASS_MOUNTAIN        = "Q8502"     # mountain
CLASS_MOUNTAIN_RANGE  = "Q46831"    # mountain range (confirma que é o correto para "cordilheira")
CLASS_PLATEAU         = "Q259171"   # plateau
CLASS_HILL            = "Q54050"    # hill
CLASS_VALLEY          = "Q39816"    # valley

RELIEF_CLASSES: List[str] = [
    CLASS_MOUNTAIN,
    CLASS_MOUNTAIN_RANGE,
    CLASS_PLATEAU,
    CLASS_HILL,
    CLASS_VALLEY,
]

CSV_COLS = [
    "iso3", "feature_qid", "feature_label", "feature_description",
    "kind_qid", "kind_label",           # <-- novo
    "type_qid", "type_label",
    "elevation_m", "lat", "lon",
    "source",
]

# -------------------- CSV helpers --------------------
def read_csv_safe(path: Path, sep: str = ";") -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, sep=sep, dtype=str, keep_default_na=False, encoding="utf-8")
    except Exception:
        return pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8")


def upsert_rows(path: Path, rows: List[dict]) -> None:
    """UPSERT por (iso3, feature_qid)."""
    if not rows:
        return
    new_df = pd.DataFrame(rows, columns=CSV_COLS)
    if not path.exists():
        new_df.to_csv(path, sep=";", index=False, encoding="utf-8")
        print(f"[reliefs] wrote: {path} | rows: {len(new_df)}")
        return

    old = read_csv_safe(path)
    for c in CSV_COLS:
        if c not in old.columns:
            old[c] = ""

    def _key(df: pd.DataFrame) -> pd.Series:
        i = df.get("iso3", "").fillna("").astype(str).str.upper()
        q = df.get("feature_qid", "").fillna("").astype(str)
        return i + "||" + q

    old["_k"] = _key(old)
    new_df["_k"] = _key(new_df)
    new_df = new_df[~new_df["_k"].isin(set(old["_k"]))].copy()

    merged = pd.concat(
        [old.drop(columns=["_k"], errors="ignore"), new_df.drop(columns=["_k"], errors="ignore")],
        ignore_index=True
    )
    merged.to_csv(path, sep=";", index=False, encoding="utf-8")
    print(f"[reliefs] upsert: +{len(new_df)} (total {len(merged)}) → {path}")


def country_qid_map(profiles_csv: Path) -> Dict[str, str]:
    df = read_csv_safe(profiles_csv)
    if df.empty:
        return {}
    col_iso3 = "iso3" if "iso3" in df.columns else ("country_iso3" if "country_iso3" in df.columns else None)
    col_qid  = "qid"  if "qid"  in df.columns else ("country_qid"  if "country_qid"  in df.columns else None)
    if not col_iso3 or not col_qid:
        print("[reliefs] ERRO: countries_profiles.csv sem colunas iso3/qid")
        return {}
    df["iso3"] = df[col_iso3].astype(str).str.upper().str.strip()
    df["qid"]  = df[col_qid].astype(str).str.strip()
    df = df[(df["iso3"] != "") & (df["qid"] != "")]
    return dict(zip(df["iso3"], df["qid"]))


# -------------------- WDQS client (POST + backoff) --------------------
def sparql(query: str, timeout_s: int = REQUEST_TIMEOUT) -> dict:
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": USER_AGENT,
    }
    sleep = 1.0
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(WDQS_URL, data={"query": query}, headers=headers, timeout=timeout_s)
            if r.status_code in (429, 500, 502, 503, 504):
                # respeitar Retry-After se existir
                ra = r.headers.get("Retry-After")
                if ra:
                    try:
                        wait = float(ra)
                    except Exception:
                        wait = sleep
                else:
                    wait = sleep
                time.sleep(wait + random.uniform(0.2, 0.9))
                sleep = min(sleep * 1.8, 12.0)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            time.sleep(sleep + random.uniform(0.2, 0.9))
            sleep = min(sleep * 1.8, 15.0)
            if attempt == MAX_RETRIES:
                raise RuntimeError(f"WDQS falhou após múltiplas tentativas: {e}")
    raise RuntimeError("WDQS falhou")


# -------------------- Queries (A: IDs / B: Props) --------------------
def q_ids_country_branch(country_qid: str, class_qid: str, depth: int, limit: int) -> str:
    """
    Fase A: só QIDs, por ramos P131 até 'depth'.
      depth=0 → ?feat wdt:P17 country
      depth=1 → ?feat wdt:P131 ?a1 . ?a1 wdt:P17 country
      depth=2 → ?feat wdt:P131 ?a2 . ?a2 wdt:P131 ?a1 . ?a1 wdt:P17 country
    """
    if depth <= 0:
        p131_block = f"?feat wdt:P17 wd:{country_qid} ."
    elif depth == 1:
        p131_block = f"""
          ?feat wdt:P131 ?a1 .
          ?a1  wdt:P17 wd:{country_qid} .
        """
    else:  # depth >= 2
        p131_block = f"""
          ?feat wdt:P131 ?a2 .
          ?a2  wdt:P131 ?a1 .
          ?a1  wdt:P17 wd:{country_qid} .
        """
    return f"""
SELECT DISTINCT ?feat WHERE {{
  {p131_block}
  ?feat wdt:P31/wdt:P279* wd:{class_qid} .
}}
LIMIT {limit}
"""


def q_props_by_ids_with_kind(kind_map: Dict[str, str]) -> str:
    # construir pares (feat, kind)
    values = " ".join(f"(wd:{qid} wd:{k})" for qid, k in kind_map.items())
    return f"""
PREFIX wikibase: <http://wikiba.se/ontology#>
SELECT DISTINCT ?feat ?featLabel ?featDescription ?kind ?kindLabel ?type ?typeLabel ?coords ?elev
WHERE {{
  VALUES (?feat ?kind) {{ {values} }}
  OPTIONAL {{ ?feat wdt:P31   ?type . }}
  OPTIONAL {{ ?feat wdt:P2044 ?elev . }}
  OPTIONAL {{ ?feat wdt:P625  ?coords . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],pt,en". }}
}}
"""


# -------------------- Helpers props --------------------
def _to_float(x: str):
    if x is None or x == "":
        return ""
    try:
        return float(x)
    except Exception:
        return ""


def _coords_to_latlon(wkt: str):
    if not wkt:
        return "", ""
    s = wkt
    if "^^" in s:
        s = s.split("^^", 1)[0]
    if "Point(" in s and ")" in s:
        inner = s.split("Point(", 1)[1].split(")", 1)[0]
        parts = inner.strip().split()
        if len(parts) == 2:
            lon, lat = parts
            return _to_float(lat), _to_float(lon)
    return "", ""


# -------------------- Pipeline --------------------
from typing import Dict, List

def fetch_ids_for_country(country_qid: str, depth: int, limit_per_branch: int) -> Dict[str, str]:
    """
    Devolve dict: {feature_qid: kind_qid_da_classe_que_encontrou}
    Se o mesmo QID aparecer em várias classes, fica a primeira encontrada (ordem de RELIEF_CLASSES).
    """
    out: Dict[str, str] = {}
    for cls in RELIEF_CLASSES:
        tried_depths = [min(depth, 2), 1, 0] if depth > 0 else [0]
        for d in tried_depths:
            try:
                q = q_ids_country_branch(country_qid, cls, d, limit_per_branch)
                data = sparql(q, timeout_s=45 if d == 0 else 60)
                ids = [
                    b["feat"]["value"].rpartition("/")[-1]
                    for b in data.get("results", {}).get("bindings", [])
                    if b.get("feat", {}).get("value")
                ]
                for qid in ids:
                    out.setdefault(qid, cls)  # não sobrescrever se já existir
                time.sleep(0.6 + random.uniform(0.2, 0.6))
            except Exception as e:
                print(f"[ids] aviso: classe {cls} depth {d} falhou ({e}) — recuar/tentar próximo.")
                time.sleep(1.2 + random.uniform(0.2, 0.8))
    return out


def fetch_props_for_ids_with_kind(kind_map: Dict[str, str], batch_size: int) -> List[dict]:
    rows: List[dict] = []
    qids = list(kind_map.keys())
    for i in range(0, len(qids), batch_size):
        chunk = qids[i:i+batch_size]
        # mapa reduzido para o lote
        submap = {q: kind_map[q] for q in chunk}
        try:
            data = sparql(q_props_by_ids_with_kind(submap))
            rows.extend(data.get("results", {}).get("bindings", []))
        except Exception as e:
            print(f"[props] lote {i//batch_size+1} falhou: {e} — a dividir…")
            for q in chunk:
                try:
                    solo = sparql(q_props_by_ids_with_kind({q: kind_map[q]}))
                    rows.extend(solo.get("results", {}).get("bindings", []))
                    time.sleep(0.25)
                except Exception as e2:
                    print(f"[props] item {q} falhou: {e2}")
        time.sleep(0.3 + random.uniform(0.2, 0.4))
    return rows

def run_for_country(iso3: str, country_qid: str, depth: int, limit_per_branch: int, batch_size: int) -> List[dict]:
    kind_map = fetch_ids_for_country(country_qid, depth=depth, limit_per_branch=limit_per_branch)
    if not kind_map:
        print("0 QIDs")
        return []

    bindings = fetch_props_for_ids_with_kind(kind_map, batch_size=batch_size)

    out: List[dict] = []
    for b in bindings:
        get = lambda k: b.get(k, {}).get("value", "")
        feat_iri = get("feat")
        feat_qid = feat_iri.rpartition("/")[-1] if feat_iri else ""
        type_iri = get("type");   type_qid = type_iri.rpartition("/")[-1] if type_iri else ""
        kind_iri = get("kind");   kind_qid = kind_iri.rpartition("/")[-1] if kind_iri else ""
        lat, lon = _coords_to_latlon(get("coords"))
        out.append({
            "iso3": iso3,
            "feature_qid": feat_qid,
            "feature_label": get("featLabel"),
            "feature_description": get("featDescription"),
            "kind_qid": kind_qid,
            "kind_label": b.get("kindLabel", {}).get("value", ""),
            "type_qid": type_qid,
            "type_label": b.get("typeLabel", {}).get("value", ""),
            "elevation_m": _to_float(get("elev")),
            "lat": lat,
            "lon": lon,
            "source": "Wikidata",
        })
    return out



# -------------------- Main --------------------
def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch reliefs/plateaus by country (2-phase, WDQS-resilient)")
    ap.add_argument("--only", default="", help="ISO3 separados por vírgula (ex.: PRT,ESP)")
    ap.add_argument("--depth", type=int, default=1, help="Profundidade P131 (0..2). 0=apenas P17.")
    ap.add_argument("--limit-per-branch", type=int, default=1500, help="IDs por ramo (classe × depth).")
    ap.add_argument("--batch-size", type=int, default=25, help="Batch para props (fase B).")
    ap.add_argument("--overwrite", action="store_true", help="Recria data/reliefs.csv.")
    args = ap.parse_args()

    iso_map = country_qid_map(PROFILES)
    if not iso_map:
        print(f"[reliefs] ERRO: {PROFILES} vazio/inválido (precisa de iso3/qid).")
        sys.exit(1)

    if args.only:
        only = {x.strip().upper() for x in args.only.split(",") if x.strip()}
        iso_list = [i for i in only if i in iso_map]
        missing  = [i for i in only if i not in iso_map]
        if missing:
            print(f"[reliefs] aviso: sem QID para: {', '.join(missing)}")
    else:
        iso_list = sorted(iso_map.keys())

    if args.overwrite and OUT_CSV.exists():
        OUT_CSV.unlink(missing_ok=True)

    total = 0
    for idx, iso3 in enumerate(iso_list, start=1):
        qid = iso_map[iso3]
        print(f"[{idx}/{len(iso_list)}] {iso3} ({qid}) …", end=" ", flush=True)
        try:
            rows = run_for_country(
                iso3=iso3,
                country_qid=qid,
                depth=args.depth,
                limit_per_branch=args.limit_per_branch,
                batch_size=args.batch_size,
            )
            if not rows:
                print("0 resultados")
            else:
                upsert_rows(OUT_CSV, rows)
                total += len(rows)
                print(f"{len(rows)} linhas")
        except Exception as e:
            print(f"ERRO: {e}")
        # pausa entre países (respeitar WDQS)
        time.sleep(2.0 + random.uniform(0.5, 1.5))

    if not OUT_CSV.exists():
        pd.DataFrame(columns=CSV_COLS).to_csv(OUT_CSV, sep=";", index=False, encoding="utf-8")

    print(f"[reliefs] concluído. +{total} → {OUT_CSV}")


if __name__ == "__main__":
    main()
