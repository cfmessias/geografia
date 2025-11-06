# scripts/enrich_rivers_wikidata.py
# -*- coding: utf-8 -*-
"""
Enriquece data/rivers.csv com informação da Wikidata:
 - nascente (P4080)
 - desaguamento (P403)
 - bacia hidrográfica / país (P205)
 - comprimento (P2043)

Output incremental: data/rivers_enriched.csv

Uso:
    python -u scripts/enrich_rivers_wikidata.py
"""

from __future__ import annotations
import requests
import pandas as pd
from pathlib import Path
import time, csv, sys

# ──────────────────────────────────────────────────────────────
DATA_DIR   = Path(__file__).resolve().parents[1] / "data"
IN_CSV     = DATA_DIR / "rivers.csv"
OUT_CSV    = DATA_DIR / "rivers_enriched.csv"
BATCH_SIZE = 50
SLEEP_BETWEEN = 1.2  # segundos entre requests (respeitar rate-limit)
USER_AGENT = "GeoDataBot/1.0 (cfmessias.pt)"

# colunas finais
OUT_COLS = [
    "iso3","river_name","length_km",
    "source_label","source_qid",
    "mouth_label","mouth_qid",
    "basin_label","basin_qid",
    "length_wd"
]

# ──────────────────────────────────────────────────────────────
def read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, sep=";", dtype=str, keep_default_na=False, encoding="utf-8")
    except Exception:
        return pd.DataFrame()

def save_incremental(df_new: pd.DataFrame, path: Path):
    """Grava incrementalmente (append, sem duplicar cabeçalho)."""
    mode = "a" if path.exists() else "w"
    header = not path.exists()
    df_new.to_csv(path, sep=";", index=False, mode=mode, header=header, encoding="utf-8")

def get_existing_keys(path: Path) -> set[tuple[str, str]]:
    if not path.exists():
        return set()
    try:
        df = pd.read_csv(path, sep=";", dtype=str, keep_default_na=False, encoding="utf-8")
        return set((row["iso3"], row["river_name"]) for _, row in df.iterrows())
    except Exception:
        return set()

# ──────────────────────────────────────────────────────────────
def query_wikidata_for_river(name: str) -> dict[str, str]:
    """
    Procura rio por label e devolve:
    source, mouth, basin, length.
    """
    q = f"""
    SELECT DISTINCT ?river ?source ?sourceLabel ?mouth ?mouthLabel ?basin ?basinLabel ?length
    WHERE {{
      ?river wdt:P31/wdt:P279* wd:Q4022 .       # instancia de rio
      ?river rdfs:label "{name}"@en .
      OPTIONAL {{ ?river wdt:P4080 ?source . ?source rdfs:label ?sourceLabel FILTER(LANG(?sourceLabel)="en") }}
      OPTIONAL {{ ?river wdt:P403 ?mouth . ?mouth rdfs:label ?mouthLabel FILTER(LANG(?mouthLabel)="en") }}
      OPTIONAL {{ ?river wdt:P205 ?basin . ?basin rdfs:label ?basinLabel FILTER(LANG(?basinLabel)="en") }}
      OPTIONAL {{ ?river wdt:P2043 ?length . }}
    }}
    LIMIT 1
    """
    url = "https://query.wikidata.org/sparql"
    headers = {"User-Agent": USER_AGENT, "Accept": "application/sparql-results+json"}
    try:
        r = requests.get(url, params={"query": q}, headers=headers, timeout=60)
        if r.status_code != 200:
            return {}
        data = r.json()
        if not data.get("results", {}).get("bindings"):
            return {}
        b = data["results"]["bindings"][0]
        def g(x): return b[x]["value"] if x in b else ""
        return {
            "source_label": g("sourceLabel"),
            "source_qid": g("source").split("/")[-1] if g("source") else "",
            "mouth_label": g("mouthLabel"),
            "mouth_qid": g("mouth").split("/")[-1] if g("mouth") else "",
            "basin_label": g("basinLabel"),
            "basin_qid": g("basin").split("/")[-1] if g("basin") else "",
            "length_wd": g("length")
        }
    except Exception as e:
        print(f"[wikidata] ERRO para '{name}': {e}")
        return {}

# ──────────────────────────────────────────────────────────────
def main():
    df_in = read_csv_safe(IN_CSV)
    if df_in.empty:
        print("[rivers-enrich] ERRO: não encontrei data/rivers.csv ou está vazio.")
        sys.exit(1)

    done_keys = get_existing_keys(OUT_CSV)
    df_in = df_in[df_in["river_name"].astype(str).str.strip() != ""]
    to_do = [(r.iso3, r.river_name, r.length_km) for r in df_in.itertuples() if (r.iso3, r.river_name) not in done_keys]

    print(f"[rivers-enrich] A processar {len(to_do)} rios (já existentes: {len(done_keys)})")

    rows_new = []
    for idx, (iso3, river_name, length_km) in enumerate(to_do, start=1):
        print(f"[{idx}/{len(to_do)}] {iso3} {river_name} …", end=" ")
        info = query_wikidata_for_river(river_name)
        if info:
            print("ok")
        else:
            print("sem dados")

        row = {
            "iso3": iso3,
            "river_name": river_name,
            "length_km": length_km,
            "source_label": info.get("source_label", ""),
            "source_qid": info.get("source_qid", ""),
            "mouth_label": info.get("mouth_label", ""),
            "mouth_qid": info.get("mouth_qid", ""),
            "basin_label": info.get("basin_label", ""),
            "basin_qid": info.get("basin_qid", ""),
            "length_wd": info.get("length_wd", "")
        }
        rows_new.append(row)

        # grava incrementalmente a cada 50 registos
        if len(rows_new) >= BATCH_SIZE:
            df_new = pd.DataFrame(rows_new)
            save_incremental(df_new, OUT_CSV)
            rows_new.clear()
            print(f"  → gravados {BATCH_SIZE} parciais")
        time.sleep(SLEEP_BETWEEN)

    if rows_new:
        df_new = pd.DataFrame(rows_new)
        save_incremental(df_new, OUT_CSV)

    print(f"[rivers-enrich] concluído → {OUT_CSV}")

# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
