# -*- coding: utf-8 -*-
"""
scripts/fetch_cities.py

Extrai cidades/municípios por país a partir da Wikidata (WDQS), incluindo lat/lon.
Para cada país:
  1) escreve bruto em data/tmp_cities/<ISO3>_raw.csv
  2) resolve 1 linha por cidade (por QID), escolhe a com maior população (em empate, ano mais recente)
  3) ordena por população desc e escreve o Top-N no ficheiro final data/cities_all.csv
  4) limpa temporários

Colunas do output final:
  iso3,country,city,city_qid,admin,is_capital,population,year,lat,lon
"""

from __future__ import annotations
from pathlib import Path
import csv
import os
import sys
import time
from typing import Dict, Iterable, Optional, Tuple

import pandas as pd
import requests

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────
PROJECT_ROOT   = Path(__file__).resolve().parents[1]
DATA_DIR       = PROJECT_ROOT / "data"
SEED_PATH      = DATA_DIR / "countries_seed.csv"   # precisa pelo menos: iso3,name_pt|name_en
OUT_FINAL      = DATA_DIR / "cities_all.csv"
TMP_DIR        = DATA_DIR / "tmp_cities"

# Quantos registos manter por país no ficheiro final
TOP_N          = 30

# Que países refazer agora
REFRESH_ALL    = False                 # True = ignora o que já existe e refaz tudo
REFRESH_ISO3   = set()                 # ex.: {"PRT","ESP"}
SKIP_DONE      = True                  # ignora países já presentes no OUT_FINAL (se REFRESH_ALL=False)

# WDQS
WDQS_URL       = "https://query.wikidata.org/sparql"
UA             = "GeoCities/1.0 (streamlit demo; contact: example@example.com)"
TIMEOUT        = 90
MAX_RETRIES    = 5
SLEEP_BASE     = 0.6                   # backoff exponencial
COOLDOWN_EVERY = 25                    # descanso a cada N países
COOLDOWN_SEC   = 3.0

# Para reduzir 504s, evitamos ORDER BY na query; resolvemos localmente em pandas
# Lista de classes aceitáveis p/ cidade/município/assentamento
CLASS_QIDS = [
    "Q515",      # city
    "Q15284",    # municipality
    "Q486972",   # human settlement
    "Q7930989",  # municipality of Portugal
    "Q2039348",  # municipality of Brazil
    "Q1549591",  # commune of France (exemplo útil)
]

# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _log(*a): print(*a, flush=True)

def _read_seed(path: Path) -> pd.DataFrame:
    if not path.exists():
        _log(f"❌ Falta {path}")
        sys.exit(1)
    # tolera ; ou ,
    try:
        df = pd.read_csv(path, sep=None, engine="python")
    except Exception:
        df = pd.read_csv(path)  # fallback
    # normaliza
    df.columns = [c.strip().lower() for c in df.columns]
    need = {"iso3"}
    if not need.issubset(df.columns):
        _log(f"❌ Seed sem coluna iso3. Colunas: {list(df.columns)}")
        sys.exit(1)
    return df

def _countries_to_process(seed: pd.DataFrame) -> Iterable[Tuple[str, str]]:
    """Devolve (iso3, label) conforme flags REFRESH_*."""
    for _, r in seed.iterrows():
        iso3 = str(r["iso3"]).upper()
        name = str(r.get("name_pt") or r.get("name_en") or iso3)
        if REFRESH_ALL:
            yield iso3, name
        else:
            if REFRESH_ISO3 and iso3 not in REFRESH_ISO3:
                continue
            yield iso3, name

def _already_done_iso3s(out_path: Path) -> set:
    if not out_path.exists():
        return set()
    try:
        df = pd.read_csv(out_path, usecols=["iso3"])
        return set(df["iso3"].astype(str).str.upper().unique())
    except Exception:
        return set()

def _wdqs_get(query: str) -> dict | None:
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": UA,
    }
    data = {"query": query}
    for i in range(MAX_RETRIES):
        try:
            r = requests.post(WDQS_URL, data=data, headers=headers, timeout=TIMEOUT)
            r.raise_for_status()
            ctype = r.headers.get("Content-Type", "")
            if "application/sparql-results+json" not in ctype and "application/json" not in ctype:
                raise ValueError(f"conteúdo não-JSON ({ctype})")
            return r.json()
        except Exception as e:
            wait = SLEEP_BASE * (2 ** i)
            _log(f"  … SPARQL falhou ({e}); retry em {wait:.1f}s")
            time.sleep(wait)
    return None


def _q_cities_for_iso3(iso3: str) -> str:
    classes_values = " ".join(f"wd:{q}" for q in CLASS_QIDS)
    return f"""
SELECT ?city ?cityLabel ?city_qid ?admin ?adminLabel ?is_cap ?pop ?year ?lat ?lon WHERE {{
  ?country wdt:P298 "{iso3}" .

  ?city wdt:P17 ?country ;
        wdt:P131 ?admin ;
        wdt:P31/wdt:P279* ?class .
  VALUES ?class {{ {classes_values} }}

  OPTIONAL {{
    {{ ?country wdt:P36 ?city }} UNION {{ ?admin wdt:P36 ?city }}
    BIND(1 AS ?is_cap)
  }}

  # POPULAÇÃO **DA CIDADE**
  OPTIONAL {{
    ?city p:P1082 ?popStmt .
    ?popStmt ps:P1082 ?pop .
    OPTIONAL {{ ?popStmt pq:P585 ?year }}
  }}

  # COORDENADAS **DA CIDADE**
  OPTIONAL {{
    ?city p:P625 ?c .
    ?c ps:P625 ?coord .
    BIND(geof:latitude(?coord)  AS ?lat)
    BIND(geof:longitude(?coord) AS ?lon)
  }}

  BIND(STRAFTER(STR(?city), "entity/") AS ?city_qid)
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
}}
"""


# ──────────────────────────────────────────────────────────────────────────────
# PROCESSAMENTO
# ──────────────────────────────────────────────────────────────────────────────

def _json_to_df(js: dict) -> pd.DataFrame:
    rows = []
    for b in js.get("results", {}).get("bindings", []):
        g = lambda k: b.get(k, {}).get("value")
        rows.append({
            "city":       g("cityLabel") or "",
            "city_qid":   g("city_qid") or "",
            "admin":      g("adminLabel") or "",
            "is_capital": 1 if g("is_cap") else 0,
            "population": g("pop"),
            "year":       g("popYear"),
            "lat":        g("lat"),
            "lon":        g("lon"),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # normaliza tipos
    df["population"] = pd.to_numeric(df["population"], errors="coerce")
    df["year"]       = pd.to_numeric(df["year"], errors="coerce")
    df["lat"]        = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"]        = pd.to_numeric(df["lon"], errors="coerce")
    df["is_capital"] = pd.to_numeric(df["is_capital"], downcast="integer", errors="coerce").fillna(0).astype(int)
    df["city_qid"]   = df["city_qid"].astype(str)
    df["city"]       = df["city"].astype(str)
    df["admin"]      = df["admin"].astype(str)
    return df

def _pick_best_per_city(df: pd.DataFrame) -> pd.DataFrame:
    """
    Dedup por city_qid:
      1) maior população
      2) em empate, ano mais recente
      3) se sem população, mantém a com ano mais recente (ou a primeira disponível)
      4) se qualquer linha tiver is_capital=1, a final fica 1
    """
    if df.empty:
        return df

    # marca capital por cidade (se em alguma linha aparecer 1)
    cap_map = (
        df.groupby("city_qid", observed=False)["is_capital"]
          .max()
          .astype(int)
    )

    # ordenar por (pop desc, ano desc)
    tmp = df.copy()
    tmp["_pop"] = tmp["population"].fillna(-1)
    tmp["_yr"]  = tmp["year"].fillna(-1)
    tmp = tmp.sort_values(["city_qid", "_pop", "_yr"], ascending=[True, False, False])

    # manter primeira de cada cidade (após ordenação)
    keep_idx = tmp.groupby("city_qid", observed=False).head(1).index
    best = tmp.loc[keep_idx, ["city_qid","city","admin","population","year","lat","lon"]].copy()
    best["is_capital"] = best["city_qid"].map(cap_map).fillna(0).astype(int)

    return best

def _write_csv(path: Path, rows: Iterable[Iterable], header: Optional[Iterable[str]]=None, append: bool=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if append and path.exists() else "w"
    with path.open(mode, newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if mode == "w" and header:
            w.writerow(header)
        for r in rows:
            w.writerow(r)
        f.flush()
        os.fsync(f.fileno())

# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    seed = _read_seed(SEED_PATH)
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    # países já no OUT_FINAL (para SKIP_DONE)
    done = _already_done_iso3s(OUT_FINAL) if SKIP_DONE and not REFRESH_ALL else set()

    # header final
    header = ["iso3","country","city","city_qid","admin","is_capital","population","year","lat","lon"]
    if REFRESH_ALL or not OUT_FINAL.exists():
        _write_csv(OUT_FINAL, [], header=header, append=False)

    for i, (iso3, country) in enumerate(_countries_to_process(seed), start=1):
        if SKIP_DONE and not REFRESH_ALL and REFRESH_ISO3 == set() and iso3 in done:
            _log(f"[cities] {iso3} {country} — já no final; a ignorar (SKIP_DONE)")
            continue

        _log(f"[cities] {iso3} {country}")
        # 1) query
        q = _q_cities_for_iso3(iso3)
        js = _wdqs_get(q)
        if js is None:
            _log("  … falhou WDQS (sem dados)")
            continue

        # 2) bruto → tmp
        raw_df = _json_to_df(js)
        tmp_raw = TMP_DIR / f"{iso3}_raw.csv"
        raw_df.to_csv(tmp_raw, index=False, encoding="utf-8")

        if raw_df.empty:
            _log(f"  … 0 linhas brutas → {tmp_raw.name}")
            try:
                os.remove(tmp_raw)
            except Exception:
                pass
            continue

        # 3) resolver melhor linha por cidade
        best = _pick_best_per_city(raw_df)

        if best.empty:
            _log("  … sem cidades após dedupe")
            try:
                os.remove(tmp_raw)
            except Exception:
                pass
            continue

        # 4) ordenar por população desc (NaN ao fim) e escolher TOP_N
        best["_pop"] = best["population"].fillna(-1)
        best = best.sort_values(["_pop","city"], ascending=[False, True]).drop(columns="_pop")
        top = best.head(TOP_N).copy()

        # 5) escrever final (append)
        rows = (
            (iso3, country, r.city, r.city_qid, r.admin, int(r.is_capital or 0),
             (None if pd.isna(r.population) else float(r.population)),
             (None if pd.isna(r.year) else int(r.year)),
             (None if pd.isna(r.lat) else float(r.lat)),
             (None if pd.isna(r.lon) else float(r.lon)))
            for r in top.itertuples(index=False)
        )
        _write_csv(OUT_FINAL, rows, header=header, append=True)

        #6) limpar temporários
        try:
            os.remove(tmp_raw)
        except Exception:
            pass

        # descanso periódico
        if i % COOLDOWN_EVERY == 0:
            time.sleep(COOLDOWN_SEC)

    _log(f"✔️ Terminado. Output: {OUT_FINAL}")

# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    main()
