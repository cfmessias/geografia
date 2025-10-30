# scripts/fetch_geography_all.py
# -*- coding: utf-8 -*-
"""
Pipeline único para extrair Geografia por país (Wikidata), lendo a lista
de países atuais de data/countries_profiles.csv (coluna obrigatória: iso3).

Gera:
  • data/borders.csv
  • data/timezones.csv
  • data/geografia_paises.csv

Principais opções:
  --batch N          : tamanho do lote para países/borders (default: 60)
  --sleep S          : pausa entre lotes países/borders (default: 0.25s)
  --sleep-regions S  : pausa entre países nas queries de regiões (default: 0.5s)

Notas técnicas:
- Queries SEM labels (rápidas); labels resolvidos no fim (bateladas de 200 QIDs).
- Fusos regionais: query simples por país (itens com P17 = país), sem property paths.
- Fronteiras: só vizinhos que também existam na lista do countries_profiles.csv.
"""

from __future__ import annotations
import argparse, time, random
from pathlib import Path
from typing import Dict, Any, List, Iterable, Tuple, Set
import pandas as pd
import requests

DATA = Path("data")
IN_PROFILES = DATA / "countries_profiles.csv"
OUT_BORDERS = DATA / "borders.csv"
OUT_TZ      = DATA / "timezonesWD.csv"
OUT_GEO     = DATA / "geografia_paises.csv"

WDQS = "https://query.wikidata.org/sparql"
UA   = "GeografiaApp/1.6 (cfmessias.pt)"

# ---------------------- util ----------------------

def read_iso3_list() -> List[str]:
    if not IN_PROFILES.exists():
        raise SystemExit(f"[erro] Ficheiro não encontrado: {IN_PROFILES}")
    df = pd.read_csv(IN_PROFILES, sep=";", dtype=str, encoding="utf-8", keep_default_na=False)
    if "iso3" not in df.columns:
        raise SystemExit("[erro] countries_profiles.csv deve conter a coluna 'iso3'")
    iso = sorted({str(x).strip().upper() for x in df["iso3"].tolist() if str(x).strip()})
    if not iso:
        raise SystemExit("[erro] Nenhum ISO3 válido encontrado em countries_profiles.csv")
    return iso

def load_iso3_name_map() -> dict[str, str]:
    df = pd.read_csv(IN_PROFILES, sep=";", dtype=str, encoding="utf-8", keep_default_na=False)
    # tenta várias colunas usuais
    name_col = next((c for c in ["country","Country","nome","Nome","country_name"] if c in df.columns), None)
    if not name_col:
        return {}
    m = {}
    for _, r in df.iterrows():
        iso = str(r.get("iso3","")).strip().upper()
        nm  = str(r.get(name_col,"")).strip()
        if iso and nm:
            m[iso] = nm
    return m

def chunks(lst: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def run_wdqs(query: str, retries: int = 6, timeout: int = 120) -> Dict[str, Any]:
    headers = {"Accept": "application/sparql-results+json", "User-Agent": UA}
    data = {"query": query}
    last = None
    for i in range(1, retries + 1):
        try:
            r = requests.post(WDQS, headers=headers, data=data, timeout=timeout)
            if r.status_code in (429, 503, 504):
                retry_after = r.headers.get("Retry-After")
                wait = float(retry_after) if (retry_after and retry_after.isdigit()) else (1.2 * i)
                print(f"[wdqs] {r.status_code} — backoff {wait:.2f}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            wait = 1.0 * i + random.uniform(0.0, 0.7)
            print(f"[wdqs] err try {i}/{retries}: {e} — backoff {wait:.2f}s")
            time.sleep(wait)
    print(f"[wdqs] falhou após {retries} tentativas: {last}")
    return {"results": {"bindings": []}}

def qid(uri: str) -> str:
    return uri.rsplit("/", 1)[-1] if uri else ""

def wkt_to_latlon(wkt_point: str) -> Tuple[float | None, float | None]:
    try:
        if not wkt_point:
            return None, None
        inside = wkt_point.split("(")[1].split(")")[0].strip()
        lon_str, lat_str = inside.split()
        return float(lat_str), float(lon_str)
    except Exception:
        return None, None

def estimate_seasons_from_lat(lat: float | None) -> int | None:
    if lat is None:
        return None
    a = abs(float(lat))
    if a < 10:  # tropicais
        return 2
    if a <= 66:  # médias
        return 4
    return 2  # polares

def values_iso3_block(iso3_list: List[str]) -> str:
    vals = " ".join(f'"{x}"' for x in iso3_list)
    return f"VALUES ?iso3 {{ {vals} }}"

# ---------------------- queries SEM labels ----------------------

def q_countries_capital_coords(iso3_list: List[str]) -> str:
    vals = values_iso3_block(iso3_list)
    return f"""
SELECT ?country ?iso3 ?cap ?coord ?ccoord WHERE {{
  {vals}
  ?country wdt:P298 ?iso3 .
  FILTER (STRLEN(?iso3)=3)
  ?country wdt:P31/wdt:P279* ?cls .
  VALUES ?cls {{ wd:Q3624078 wd:Q6256 }}
  FILTER NOT EXISTS {{ ?country wdt:P31/wdt:P279* wd:Q3024240 }}
  FILTER NOT EXISTS {{ ?country wdt:P576 ?diss }}

  OPTIONAL {{ ?country wdt:P36 ?cap .
             OPTIONAL {{ ?cap wdt:P625 ?coord }} }}
  OPTIONAL {{ ?country wdt:P625 ?ccoord }}
}}
"""

def q_borders(iso3_list: List[str]) -> str:
    vals = values_iso3_block(iso3_list)
    return f"""
SELECT ?country ?iso3 ?neighbor ?niso3 WHERE {{
  {vals}
  ?country wdt:P298 ?iso3 .
  FILTER (STRLEN(?iso3)=3)
  ?country wdt:P31/wdt:P279* ?cls .
  VALUES ?cls {{ wd:Q3624078 wd:Q6256 }}
  FILTER NOT EXISTS {{ ?country wdt:P31/wdt:P279* wd:Q3024240 }}
  FILTER NOT EXISTS {{ ?country wdt:P576 ?d1 }}

  ?country wdt:P47 ?neighbor .
  ?neighbor wdt:P298 ?niso3 .
  FILTER (STRLEN(?niso3)=3)
  ?neighbor wdt:P31/wdt:P279* ?ncls .
  VALUES ?ncls {{ wd:Q3624078 wd:Q6256 }}
  FILTER NOT EXISTS {{ ?neighbor wdt:P31/wdt:P279* wd:Q3024240 }}
  FILTER NOT EXISTS {{ ?neighbor wdt:P576 ?d2 }}
}}
"""

def q_timezones_direct(iso3_list: List[str]) -> str:
    vals = values_iso3_block(iso3_list)
    return f"""
SELECT DISTINCT ?country ?iso3 ?tz WHERE {{
  {vals}
  ?country wdt:P298 ?iso3 .
  FILTER (STRLEN(?iso3)=3)
  ?country wdt:P31/wdt:P279* ?cls .
  VALUES ?cls {{ wd:Q3624078 wd:Q6256 }}
  FILTER NOT EXISTS {{ ?country wdt:P31/wdt:P279* wd:Q3024240 }}
  FILTER NOT EXISTS {{ ?country wdt:P576 ?diss }}

  ?country wdt:P421 ?tz .
}}
"""

def q_timezones_regions_simple(qid_country: str) -> str:
    # Regiões com P17 = país; cada uma com P421
    return f"""
SELECT DISTINCT ?tz WHERE {{
  ?place wdt:P17 wd:{qid_country} .
  ?place wdt:P421 ?tz .
}}
"""

# ---------------------- resol. labels ----------------------

def q_labels(qids: List[str]) -> str:
    values = " ".join(f"wd:{q}" for q in qids)
    return f"""
SELECT ?item ?itemLabel WHERE {{
  VALUES ?item {{ {values} }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
}}
"""

def resolve_labels(qids: Set[str]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    qlist = sorted(q for q in qids if q and q.startswith("Q"))
    for i in range(0, len(qlist), 200):
        lot = qlist[i:i+200]
        js = run_wdqs(q_labels(lot))
        for b in js.get("results", {}).get("bindings", []):
            q = b["item"]["value"].rsplit("/", 1)[-1]
            lbl = b.get("itemLabel", {}).get("value", "")
            if lbl:
                mapping[q] = lbl
        time.sleep(0.3 + random.random() * 0.3)
    return mapping

# ---------------------- builders ----------------------

def build_countries(iso3_all: List[str], batch: int, sleep: float) -> pd.DataFrame:
    rows = []
    done = 0; total = len(iso3_all)
    for lot in chunks(iso3_all, max(1, batch)):
        done += len(lot)
        print(f"[countries] {done - len(lot) + 1}..{done} / {total}")
        js = run_wdqs(q_countries_capital_coords(lot))
        for b in js.get("results", {}).get("bindings", []):
            iso3 = b.get("iso3", {}).get("value", "").upper()
            c_uri = b.get("country", {}).get("value", "")
            cap_uri = b.get("cap", {}).get("value", "")
            coord = b.get("coord", {}).get("value", "")
            ccoord = b.get("ccoord", {}).get("value", "")
            lat, lon = wkt_to_latlon(coord)
            if lat is None or lon is None:
                lat, lon = wkt_to_latlon(ccoord)
            rows.append({
                "iso3": iso3,
                "country_qid": qid(c_uri),
                "capital_qid": qid(cap_uri) if cap_uri else "",
                "capital_lat": lat if lat is not None else "",
                "capital_lon": lon if lon is not None else "",
            })
        time.sleep(max(0.0, sleep))

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["iso3","country_qid","capital_qid","capital_lat","capital_lon"])

    # preferir linhas com coords da capital
    def prefer_key(r):
        return int(str(r.get("capital_lat","")).strip() != "" and str(r.get("capital_lon","")).strip() != "")
    df["_p"] = df.apply(prefer_key, axis=1)
    df = (df.sort_values(["iso3","_p"], ascending=[True, False])
            .drop_duplicates(subset=["iso3"], keep="first")
            .drop(columns=["_p"])
            .sort_values("iso3")
            .reset_index(drop=True))

    # aviso de ISO3 não encontrados
    got = set(df["iso3"])
    missing = [x for x in iso3_all if x not in got]
    if missing:
        print(f"[aviso] ISO3 não resolvidos no WDQS (ignorado nas próximas fases): {', '.join(missing)}")
    return df

def build_borders(iso3_all: List[str], iso3_set: Set[str], batch: int, sleep: float) -> pd.DataFrame:
    rows = []
    done = 0; total = len(iso3_all)
    for lot in chunks(iso3_all, max(1, batch)):
        done += len(lot)
        print(f"[borders] {done - len(lot) + 1}..{done} / {total}")
        js = run_wdqs(q_borders(lot))
        for b in js.get("results", {}).get("bindings", []):
            iso3  = b.get("iso3", {}).get("value", "").upper()
            niso3 = b.get("niso3", {}).get("value", "").upper()
            if not (iso3 and niso3):
                continue
            if niso3 not in iso3_set:
                continue  # só vizinhos presentes na tua lista atual
            rows.append({
                "country_iso3": iso3,
                "neighbor_iso3": niso3,
                "country_qid": qid(b.get("country", {}).get("value","")),
                "neighbor_qid": qid(b.get("neighbor", {}).get("value","")),
                "land_km": "",  # sem fonte fiável no WDQS
            })
        time.sleep(max(0.0, sleep))
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["country_iso3","neighbor_iso3","country_qid","neighbor_qid","land_km"])
    df = (df.drop_duplicates(subset=["country_iso3","neighbor_iso3"])
            .sort_values(["country_iso3","neighbor_iso3"])
            .reset_index(drop=True))
    return df

def build_timezones(iso3_all: List[str], df_countries: pd.DataFrame,
                    batch: int, sleep: float, sleep_regions: float) -> pd.DataFrame:
    rows = []

    # 1) Diretos (P421 no item do país)
    done = 0; total = len(iso3_all)
    for lot in chunks(iso3_all, max(1, batch)):
        done += len(lot)
        print(f"[tz-direct] {done - len(lot) + 1}..{done} / {total}")
        js = run_wdqs(q_timezones_direct(lot))
        for b in js.get("results", {}).get("bindings", []):
            iso3 = b.get("iso3", {}).get("value","").upper()
            rows.append({
                "country_iso3": iso3,
                "country_qid": qid(b.get("country", {}).get("value","")),
                "tz_qid": qid(b.get("tz", {}).get("value","")),
            })
        time.sleep(max(0.0, sleep))

    # 2) Regionais (por país — leve, sem property paths)
    print("[tz-regions] países um a um (queries leves)…")
    by_iso = df_countries.set_index("iso3")
    for i, iso in enumerate(iso3_all, start=1):
        if iso not in by_iso.index:
            continue
        c_qid = by_iso.loc[iso, "country_qid"]
        if not c_qid:
            continue
        js = run_wdqs(q_timezones_regions_simple(c_qid))
        for b in js.get("results", {}).get("bindings", []):
            rows.append({
                "country_iso3": iso,
                "country_qid": c_qid,
                "tz_qid": qid(b.get("tz", {}).get("value","")),
            })
        if i % 10 == 0:
            print(f"   → {i}/{len(iso3_all)} países processados")
        time.sleep(max(0.0, sleep_regions + random.uniform(0.0, 0.4)))

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["country_iso3","country_qid","tz_qid"])
    df = (df.drop_duplicates(subset=["country_iso3","tz_qid"])
            .sort_values(["country_iso3","tz_qid"])
            .reset_index(drop=True))
    return df

# ---------------------- resumo + labels ----------------------

def summarize_geografia(df_c: pd.DataFrame, df_b: pd.DataFrame, df_tz: pd.DataFrame,
                        labels_map: Dict[str,str]) -> pd.DataFrame:
    neigh_map: Dict[str, List[str]] = {}
    if not df_b.empty:
        for iso, sub in df_b.groupby("country_iso3"):
            neigh_map[iso] = sorted({x for x in sub["neighbor_iso3"].astype(str) if x})
    tz_map: Dict[str, List[str]] = {}
    if not df_tz.empty:
        names = df_tz["tz_qid"].map(labels_map).fillna(df_tz["tz_qid"])
        tmp = df_tz.assign(tz_label=names)
        for iso, sub in tmp.groupby("country_iso3"):
            tz_map[iso] = sorted({x for x in sub["tz_label"].astype(str) if x})

    rows = []
    for _, r in df_c.iterrows():
        iso3 = r["iso3"]
        lat  = r.get("capital_lat", "")
        lon  = r.get("capital_lon", "")
        seasons = estimate_seasons_from_lat(float(lat)) if str(lat).strip() not in ("","None") else None
        neighs = neigh_map.get(iso3, [])
        tzs    = tz_map.get(iso3, [])
        rows.append({
            "iso3": iso3,
            "country": labels_map.get(r.get("country_qid",""), ""),
            "neighbors_iso3": ",".join(neighs),
            "neighbors_count": len(neighs),
            "border_km_total": "",
            "timezones": ",".join(tzs),
            "timezones_count": len(tzs),
            "capital_lat": lat,
            "capital_lon": lon,
            "seasons_estimate": seasons if seasons is not None else "",
        })
    return pd.DataFrame(rows).sort_values("iso3").reset_index(drop=True)

# ---------------------- CLI/Main ----------------------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Extrai países, fronteiras e fusos (via countries_profiles.csv)")
    ap.add_argument("--batch", type=int, default=60, help="Tamanho do lote de ISO3 para países/borders (default: 60)")
    ap.add_argument("--sleep", type=float, default=0.25, help="Pausa entre lotes (países/borders) (default: 0.25)")
    ap.add_argument("--sleep-regions", type=float, default=0.5, help="Pausa entre países nas queries regionais (default: 0.5)")
    return ap.parse_args()

def main() -> None:
    args = parse_args()
    DATA.mkdir(parents=True, exist_ok=True)

    iso3_all = read_iso3_list()
    name_map = load_iso3_name_map()   # iso3 -> nome do país (dos profiles)

    iso3_set = set(iso3_all)

    print("[1/5] Países (capital + coordenadas)…")
    df_c = build_countries(iso3_all, args.batch, args.sleep)
    print(f"   países: {len(df_c)}")

    print("[2/5] Fronteiras (apenas vizinhos atuais)…")
    df_b = build_borders(iso3_all, iso3_set, args.batch, args.sleep)
    print(f"   fronteiras: {len(df_b)}")

    print("[3/5] Fusos horários (país + regiões)…")
    df_tz = build_timezones(iso3_all, df_c, args.batch, args.sleep, args.sleep_regions)
    print(f"   timezones: {len(df_tz)}")

    # -------- resolver labels --------
    print("[4/5] Resolver labels (QIDs → nomes)…")
    # QIDs a resolver (países, vizinhos e fusos)
    qids: Set[str] = set()
    if not df_c.empty:
        qids |= set(df_c.get("country_qid", []))
    if not df_b.empty:
        qids |= set(df_b.get("neighbor_qid", [])) | set(df_b.get("country_qid", []))
    if not df_tz.empty:
        qids |= set(df_tz.get("tz_qid", []))
    qids = {q for q in qids if isinstance(q, str) and q}

    labels_map = resolve_labels(qids)

    # --------------------------
    # Aplicar nomes/labels
    # --------------------------
    # 1) Fronteiras: prioriza nomes vindos do countries_profiles (name_map), depois fallback WDQS
    if not df_b.empty:
        # neighbor_name
        df_b["neighbor_name"] = df_b.get("neighbor_name", "")
        df_b["neighbor_name"] = df_b["neighbor_iso3"].map(name_map).fillna(df_b["neighbor_name"])
        mask_missing = df_b["neighbor_name"].astype(str).eq("")
        if mask_missing.any():
            df_b.loc[mask_missing, "neighbor_name"] = df_b.loc[mask_missing, "neighbor_qid"].map(labels_map).fillna("")

        # country (nome do próprio país) – também pelos profiles, fallback WDQS
        df_b["country"] = df_b["country_iso3"].map(name_map).fillna(
            df_b.get("country_qid", "").map(labels_map).fillna("")
        )

    # 2) Fusos horários: resolver labels e depois manter apenas os que começam por "UTC"
    df_tz_out = df_tz.copy()
    if not df_tz_out.empty:
        df_tz_out["country"] = df_tz_out.get("country", "")
        df_tz_out["country"] = df_tz_out.get("country_qid", "").map(labels_map).fillna(df_tz_out["country"])
        df_tz_out["tz_label"] = df_tz_out.get("tz_qid", "").map(labels_map).fillna(df_tz_out.get("tz_label", ""))

        # Apenas UTC (ex.: "UTC−1", "UTC+0", "UTC+1", …)
        df_tz_out["tz_label"] = df_tz_out["tz_label"].astype(str)
        df_tz_out = df_tz_out[df_tz_out["tz_label"].str.startswith("UTC")].copy()

    print("[5/5] Resumo por país (geografia_paises.csv)…")
    # O resumo usa os labels resolvidos (e já filtrados para UTC)
    df_geo = summarize_geografia(df_c, df_b, df_tz_out, labels_map)

    # --------------------------
    # Escritas finais (colunas estáveis)
    # --------------------------
    borders_cols = [
        "country_iso3", "neighbor_iso3", "neighbor_qid", "neighbor_name",
        "country_qid", "country", "land_km"
    ]
    tz_cols = ["country_iso3", "country_qid", "country", "tz_qid", "tz_label"]
    geo_cols = [
        "iso3", "country", "neighbors_iso3", "neighbors_count",
        "border_km_total", "timezones", "timezones_count",
        "capital_lat", "capital_lon", "seasons_estimate"
    ]

    df_b = df_b.reindex(columns=borders_cols, fill_value="") if not df_b.empty else pd.DataFrame(columns=borders_cols)
    df_tz_out = df_tz_out.reindex(columns=tz_cols, fill_value="") if not df_tz_out.empty else pd.DataFrame(columns=tz_cols)
    df_geo = df_geo.reindex(columns=geo_cols, fill_value="") if not df_geo.empty else pd.DataFrame(columns=geo_cols)

    df_b.to_csv(OUT_BORDERS, sep=";", index=False, encoding="utf-8")
    df_tz_out.to_csv(OUT_TZ, sep=";", index=False, encoding="utf-8")
    df_geo.to_csv(OUT_GEO, sep=";", index=False, encoding="utf-8")

    print(f"OK: {OUT_BORDERS} ({len(df_b)})")
    print(f"OK: {OUT_TZ} ({len(df_tz_out)})")
    print(f"OK: {OUT_GEO} ({len(df_geo)})")

if __name__ == "__main__":
    main()
