# scripts/fetch_lakes_by_country.py
from __future__ import annotations
import argparse
import random
import sys
import time
from pathlib import Path
from typing import Iterable, List, Dict

import requests
import pandas as pd

# -------------------------
# Config
# -------------------------
DATA_DIR = Path("data")
PROFILES = DATA_DIR / "countries_profiles.csv"
OUT_CSV  = DATA_DIR / "lakes.csv"

WDQS_URL   = "https://query.wikidata.org/sparql"
USER_AGENT = "GeoFetch/1.1 (+https://example.local)"

REQUEST_TIMEOUT = 60
RETRY_SLEEP     = 2.0
MAX_RETRIES     = 4

CSV_COLS = [
    "iso3", "lake_qid", "lake_label", "lake_description",
    "type_qid", "type_label",
    "area_km2", "depth_m", "lat", "lon",
    "source",
]

LAKE_CLASSES = [
    "Q23397",      # lake
    "Q100900880",  # crater lake (class)
    "Q204324",     # volcanic crater lake
    "Q11726988",   # impact crater lake
]

# -------------------------
# Utils
# -------------------------
def read_csv_safe(path: Path, sep: str = ";") -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, sep=sep, dtype=str, keep_default_na=False, encoding="utf-8")
    except Exception:
        return pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8")


def write_csv_safe(path: Path, rows: Iterable[dict], overwrite: bool = False, sep: str = ";") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=CSV_COLS)
    if overwrite or (not path.exists()):
        df.to_csv(path, sep=sep, index=False, encoding="utf-8")
        print(f"[lakes] wrote: {path} | rows: {len(df)}")
        return
    df.to_csv(path, sep=sep, index=False, encoding="utf-8", mode="a", header=False)
    print(f"[lakes] appended: {path} | rows: {len(df)}")


def upsert_rows(path: Path, rows: List[dict]) -> None:
    if not rows:
        return
    new_df = pd.DataFrame(rows, columns=CSV_COLS)
    if not path.exists():
        write_csv_safe(path, rows, overwrite=True)
        return

    old = read_csv_safe(path)
    for c in CSV_COLS:
        if c not in old.columns:
            old[c] = ""

    def keyify(df: pd.DataFrame) -> pd.Series:
        q = df.get("lake_qid", "").fillna("").astype(str)
        iso = df.get("iso3", "").fillna("").astype(str).str.upper()
        return iso + "||" + q

    old["_key"] = keyify(old)
    new_df["_key"] = keyify(new_df)
    new_df = new_df[~new_df["_key"].isin(set(old["_key"]))].copy()

    merged = pd.concat([old.drop(columns=["_key"], errors="ignore"),
                        new_df.drop(columns=["_key"], errors="ignore")],
                       ignore_index=True)
    merged.to_csv(path, sep=";", index=False, encoding="utf-8")
    print(f"[lakes] upsert: +{len(new_df)} (total {len(merged)}) → {path}")


def country_qid_map(profiles_csv: Path) -> Dict[str, str]:
    df = read_csv_safe(profiles_csv)
    if df.empty:
        return {}
    col_iso3 = "iso3" if "iso3" in df.columns else ("country_iso3" if "country_iso3" in df.columns else None)
    col_qid  = "qid"  if "qid"  in df.columns else ("country_qid"  if "country_qid"  in df.columns else None)
    if not col_iso3 or not col_qid:
        print("[lakes] ERRO: countries_profiles.csv sem colunas iso3/qid")
        return {}
    df["iso3"] = df[col_iso3].astype(str).str.upper().str.strip()
    df["qid"]  = df[col_qid].astype(str).str.strip()
    df = df[(df["iso3"] != "") & (df["qid"] != "")]
    return dict(zip(df["iso3"], df["qid"]))


# -------------------------
# WDQS client (POST + backoff/jitter)
# -------------------------
def sparql(query: str, timeout_s: int = 60) -> dict:
    headers = {
        "Accept": "application/sparql-results+json",
        # Usa um UA identificável (o teu domínio/email ajuda a reduzir throttling)
        "User-Agent": "GeografiaReliefs/1.0 (+https://cfmessias.pt; contact: cfmessias@gmail.com)",
    }
    sleep = 1.0
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(WDQS_URL, data={"query": query}, headers=headers, timeout=timeout_s)
            # Se o WDQS sinaliza sobrecarga, respeita e aguarda
            if r.status_code in (429, 500, 502, 503, 504):
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
            # backoff exponencial com jitter
            time.sleep(sleep + random.uniform(0.2, 0.9))
            sleep = min(sleep * 1.8, 15.0)
            if attempt == MAX_RETRIES:
                raise RuntimeError(f"WDQS falhou após múltiplas tentativas: {e}")
    # nunca chega aqui
    raise RuntimeError("WDQS falhou")

def q_ids_country_branch(country_qid: str, class_qid: str, depth: int, limit: int) -> str:
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

def fetch_ids_for_country(country_qid: str, depth: int, limit_per_branch: int) -> List[str]:
    out: List[str] = []
    seen = set()
    for cls in RELIEF_CLASSES:
        # tenta depth, se falhar recua
        tried_depths = [min(depth, 2), 1, 0] if depth > 0 else [0]
        for d in tried_depths:
            try:
                q = q_ids_country_branch(country_qid, cls, d, limit_per_branch)
                data = sparql(q, timeout_s=45 if d == 0 else 60)
                ids = [b["feat"]["value"].rpartition("/")[-1]
                       for b in data.get("results", {}).get("bindings", [])
                       if b.get("feat", {}).get("value")]
                for qid in ids:
                    if qid not in seen:
                        seen.add(qid)
                        out.append(qid)
                # pausa pequena entre ramos bem sucedidos
                time.sleep(0.6 + random.uniform(0.2, 0.6))
            except Exception as e:
                print(f"[ids] aviso: classe {cls} depth {d} falhou ({e}) — recuar/tentar próximo.")
                # pequena pausa antes do próximo
                time.sleep(1.2 + random.uniform(0.2, 0.8))
    return out

# -------------------------
# Queries (fase A/B)
# -------------------------
def q_ids_by_country_and_class(country_qid: str, class_qid: str) -> str:

    LAKE_CLASSES = [
    "Q23397",      # lake
    "Q100900880",  # crater lake (class)
    "Q204324",     # volcanic crater lake
    "Q11726988",   # impact crater lake
]


def q_ids_by_country_and_class(country_qid: str, class_qid: str) -> str:
    # Apanha apenas instâncias diretas desta classe específica
    return f"""
SELECT DISTINCT ?lake WHERE {{
  VALUES ?country {{ wd:{country_qid} }}
  VALUES ?cls     {{ wd:{class_qid}  }}

  ?lake wdt:P17 ?country .
  ?lake wdt:P31 ?cls .              # Apenas instância DIRETA desta classe
  ?lake wdt:P625 ?coords .          # Requer coordenadas
}}
"""
def q_props_by_ids(qids: List[str]) -> str:
    values = " ".join(f"wd:{q}" for q in qids)
    return f"""
PREFIX wikibase: <http://wikiba.se/ontology#>

SELECT DISTINCT ?lake ?lakeLabel ?lakeDescription
                ?type ?typeLabel
                ?coords ?depth ?areaAmount ?areaUnit
WHERE {{
  VALUES ?lake {{ {values} }}

  OPTIONAL {{ ?lake wdt:P31 ?type . }}
  OPTIONAL {{ ?lake wdt:P625 ?coords . }}
  OPTIONAL {{ ?lake wdt:P4511 ?depth . }}
  OPTIONAL {{
    ?lake p:P2046/psv:P2046 ?areaNode .
    ?areaNode wikibase:quantityAmount ?areaAmount ;
              wikibase:quantityUnit   ?areaUnit .
  }}

  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],pt,en". }}
}}
"""


# -------------------------
# Helpers
# -------------------------
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


def normalize_area_km2(amount: str, unit_iri: str) -> str | float:
    # Q712226 = km², Q25343 = m²; senão devolve amount cru
    if amount == "" or unit_iri == "":
        return ""
    try:
        v = float(amount)
    except Exception:
        return ""
    if unit_iri.endswith("/Q712226"):
        return v
    if unit_iri.endswith("/Q25343"):
        return v / 1_000_000.0
    return v  # fallback (pouco comum, mas evita perder o valor)


# -------------------------
# Pipeline
# -------------------------
def fetch_ids_for_country(country_qid: str) -> List[str]:
    """Fase A: agrega QIDs unindo resultados por classe, com tentativas por classe."""
    out: List[str] = []
    seen = set()
    for cls in LAKE_CLASSES:
        q = q_ids_by_country_and_class(country_qid, cls)
        try:
            data = sparql(q)
            ids = [b["lake"]["value"].rpartition("/")[-1]
                   for b in data.get("results", {}).get("bindings", [])
                   if "lake" in b and b["lake"]["value"]]
            for qid in ids:
                if qid not in seen:
                    seen.add(qid)
                    out.append(qid)
        except Exception as e:
            print(f"[ids] aviso: classe {cls} falhou ({e}) — continuo nas restantes.")
            continue
        # cortesia: pequena pausa
        time.sleep(0.2 + random.uniform(0, 0.2))
    return out


def fetch_props_for_ids(qids: List[str], batch_size: int = 40) -> List[dict]:
    """Fase B: busca propriedades em lotes."""
    rows: List[dict] = []
    for i in range(0, len(qids), batch_size):
        chunk = qids[i:i+batch_size]
        q = q_props_by_ids(chunk)
        try:
            data = sparql(q)
        except Exception as e:
            print(f"[props] lote {i//batch_size+1} falhou: {e} — a dividir…")
            # fallback: tentar um a um este lote
            for qid in chunk:
                try:
                    solo = sparql(q_props_by_ids([qid]))
                    for b in solo.get("results", {}).get("bindings", []):
                        rows.append(b)
                except Exception as e2:
                    print(f"[props] item {qid} falhou: {e2}")
            continue

        rows.extend(data.get("results", {}).get("bindings", []))
        time.sleep(0.3 + random.uniform(0, 0.3))
    return rows


def run_for_country(iso3: str, country_qid: str, min_area_km2: float, batch_size: int) -> List[dict]:
    ids = fetch_ids_for_country(country_qid)
    if not ids:
        print("0 QIDs")
        return []

    bindings = fetch_props_for_ids(ids, batch_size=batch_size)
    out_rows: List[dict] = []
    for b in bindings:
        get = lambda k: b.get(k, {}).get("value", "")
        lake_iri = get("lake")
        lake_qid = lake_iri.rpartition("/")[-1] if lake_iri else ""
        type_iri = get("type")
        type_qid = type_iri.rpartition("/")[-1] if type_iri else ""

        lat, lon = _coords_to_latlon(get("coords"))
        area_km2 = normalize_area_km2(get("areaAmount"), get("areaUnit"))
        if isinstance(area_km2, float) and min_area_km2 > 0 and area_km2 < min_area_km2:
            continue  # aplica filtro min-area

        out_rows.append({
            "iso3": iso3,
            "lake_qid": lake_qid,
            "lake_label": get("lakeLabel"),
            "lake_description": get("lakeDescription"),
            "type_qid": type_qid,
            "type_label": b.get("typeLabel", {}).get("value", ""),
            "area_km2": area_km2 if area_km2 != "" else "",
            "depth_m": _to_float(get("depth")),
            "lat": lat,
            "lon": lon,
            "source": "Wikidata",
        })
    return out_rows


# -------------------------
# Main
# -------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch lakes per country (2-phase WDQS, resilient)")
    ap.add_argument("--only", default="", help="ISO3 separados por vírgula (ex.: PRT,ESP)")
    ap.add_argument("--min-area-km2", type=float, default=0.0, help="Filtra por área mínima (km²). 0 = sem filtro")
    ap.add_argument("--batch-size", type=int, default=40, help="Tamanho do lote para props (fase B).")
    ap.add_argument("--overwrite", action="store_true", help="Recria data/lakes.csv")
    args = ap.parse_args()

    iso_map = country_qid_map(PROFILES)
    if not iso_map:
        print(f"[lakes] ERRO: não encontrei {PROFILES} com colunas iso3/qid.")
        sys.exit(1)

    if args.only:
        only = {x.strip().upper() for x in args.only.split(",") if x.strip()}
        iso_list = [iso for iso in only if iso in iso_map]
        missing  = [iso for iso in only if iso not in iso_map]
        if missing:
            print(f"[lakes] Aviso: sem QID em profiles para: {', '.join(missing)}")
    else:
        iso_list = sorted(iso_map.keys())

    if args.overwrite and OUT_CSV.exists():
        OUT_CSV.unlink(missing_ok=True)

    total_new = 0
    for idx, iso3 in enumerate(iso_list, start=1):
        qid = iso_map.get(iso3)
        print(f"[{idx}/{len(iso_list)}] {iso3} ({qid}) …", end=" ", flush=True)
        try:
            rows = run_for_country(iso3, qid, min_area_km2=args.min_area_km2, batch_size=args.batch_size)
            if not rows:
                print("0 resultados")
                continue
            upsert_rows(OUT_CSV, rows)
            total_new += len(rows)
            print(f"{len(rows)} linhas")
        except Exception as e:
            print(f"ERRO: {e}")
            continue

    if not OUT_CSV.exists():
        write_csv_safe(OUT_CSV, [], overwrite=True)

    print(f"[lakes] concluído. novos+inseridos: {total_new} → {OUT_CSV}")


if __name__ == "__main__":
    main()
