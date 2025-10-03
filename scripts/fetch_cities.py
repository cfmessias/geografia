# scripts/fetch_cities.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import csv
import os
import sys
import time
import random
import heapq
from typing import Dict, List, Tuple, Optional

import pandas as pd
import requests
import json
from json import JSONDecodeError

# ──────────────────────────────────────────────────────────────────────────────
# Caminhos
# ──────────────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SEED_PATH    = PROJECT_ROOT / "data" / "countries_seed.csv"
OUT_PATH     = PROJECT_ROOT / "data" / "cities_all.csv"
TMP_DIR      = PROJECT_ROOT / "data" / "tmp_cities"

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
TOP_N = 20                        # nº final de cidades por país
RAW_LIMIT = 4000                  # nº bruto por país (para queries sem ORDER BY)
REFRESH_ALL: bool = False         # True: recria ficheiro de saída do zero
REFRESH_ISO3: set[str] = set()    # ex.: {"PRT","ESP"} para reprocessar só estes
SKIP_DONE: bool = False           # False para garantir execução de todos os países

# Ritmo / tolerância
TIMEOUT = 90
BASE_PAUSE = 0.35
COOLDOWN_EVERY = 20
COOLDOWN_SECS  = 8

# Comportamento de queries
PREFER_ORDERED = False            # evita queries agregadas pesadas por defeito
STABLE_MIN_POP: Optional[int] = None  # ex.: 1000 para filtrar micro-povoados

# HTTP
WDQS = "https://query.wikidata.org/sparql"
UA   = "GeografiaApp/1.0 (+coloca-o-teu-email-ou-site; contacto WDQS)"  # ← TROCAR POR CONTACTO REAL
WD_API = "https://www.wikidata.org/w/api.php"

# Cabeçalho do CSV final
HEAD = ["iso3","country","city","city_qid","admin","is_capital","population","year","lat","lon"]

# ──────────────────────────────────────────────────────────────────────────────
# Util
# ──────────────────────────────────────────────────────────────────────────────
def load_seed() -> pd.DataFrame:
    if not SEED_PATH.exists():
        print(f"❌ Falta {SEED_PATH}.", file=sys.stderr)
        sys.exit(1)
    df = pd.read_csv(SEED_PATH)
    for c in ("name_pt","name_en"):
        if c not in df.columns:
            df[c] = ""
    return df

def remove_iso3_from_csv(path: Path, iso3s: set[str]) -> None:
    if not path.exists() or not iso3s:
        return
    df = pd.read_csv(path)
    keep = ~df["iso3"].astype(str).str.upper().isin({i.upper() for i in iso3s})
    if not keep.all():
        df[keep].to_csv(path, index=False, encoding="utf-8")

def read_done_iso3() -> set[str]:
    if not OUT_PATH.exists():
        return set()
    try:
        return set(pd.read_csv(OUT_PATH, usecols=["iso3"])["iso3"].astype(str).str.upper().unique())
    except Exception:
        return set()

# ──────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ──────────────────────────────────────────────────────────────────────────────
SQS = requests.Session()
SQS.headers.update({"User-Agent": UA})
SWD = requests.Session()
SWD.headers.update({"User-Agent": UA})

def _tsv_to_bindings(tsv_text: str) -> dict:
    """
    Converte resposta TSV da WDQS num "pseudo-JSON" compatível com parse_rows().
    Assume cabeçalhos alinhados com as variáveis SPARQL (city, admin, pop, yr, lat, lon, isCap).
    """
    lines = [ln for ln in tsv_text.strip().splitlines() if ln.strip() != ""]
    if not lines:
        return {"results": {"bindings": []}}
    headers = lines[0].split("\t")
    bindings = []
    for ln in lines[1:]:
        cols = ln.split("\t")
        row = {}
        for h, v in zip(headers, cols):
            if v == "":
                continue
            row[h] = {"type": "literal", "value": v}
        bindings.append(row)
    return {"results": {"bindings": bindings}}

def sparql_post(q: str) -> dict | None:
    """
    Estratégia robusta:
      1) POST (form) JSON
      2) GET JSON
      3) POST (form) TSV → converter para 'bindings'
    Retorna dict estilo WDQS (com 'results'/'bindings') ou None.
    """
    headers_json = {
        "User-Agent": UA,
        "Accept": "application/sparql-results+json; charset=utf-8",
    }
    headers_tsv = {
        "User-Agent": UA,
        "Accept": "text/tab-separated-values; charset=utf-8",
    }

    for attempt in range(4):
        try:
            r = requests.post(WDQS, data={"query": q, "format": "json"},
                              headers=headers_json, timeout=TIMEOUT)
            r.raise_for_status()
            try:
                return r.json()
            except JSONDecodeError:
                r2 = requests.get(WDQS, params={"query": q, "format": "json"},
                                  headers=headers_json, timeout=TIMEOUT)
                r2.raise_for_status()
                try:
                    return r2.json()
                except JSONDecodeError:
                    r3 = requests.post(WDQS, data={"query": q}, headers=headers_tsv, timeout=TIMEOUT)
                    r3.raise_for_status()
                    return _tsv_to_bindings(r3.text)

        except Exception as e:
            wait = BASE_PAUSE * (2 ** attempt) + random.uniform(0, 0.4)
            print(f"  … SPARQL falhou ({e}); retry em {wait:.1f}s", file=sys.stderr)
            time.sleep(wait)

    return None

def wd_get_labels(qids: List[str], lang="pt", fallbacks=("pt-br","en")) -> Dict[str, str]:
    """
    Devolve labels priorizando português; fallbacks pt-br → en.
    """
    out: Dict[str,str] = {}
    if not qids:
        return out

    langs = [lang] + [l for l in fallbacks if l != lang]
    lang_param = "|".join(langs)

    ids = [q for q in dict.fromkeys([q for q in qids if q])]
    for i in range(0, len(ids), 50):
        chunk = ids[i:i+50]
        for attempt in range(3):
            try:
                r = SWD.get(WD_API, params={
                    "action":"wbgetentities", "ids":"|".join(chunk),
                    "props":"labels", "languages": lang_param, "format":"json"
                }, timeout=20)
                r.raise_for_status()
                ents = r.json().get("entities", {})
                for q, e in ents.items():
                    lab = e.get("labels", {})
                    chosen = None
                    for L in langs:
                        if L in lab:
                            chosen = lab[L].get("value")
                            break
                    out[q] = chosen or q
                break
            except Exception:
                time.sleep(0.4 * (attempt+1))
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Queries SPARQL
# ──────────────────────────────────────────────────────────────────────────────
def q_chn_user(limit: int = 200, min_pop: int = 1000000) -> str:
    """
    China — exatamente a query fornecida pelo utilizador (funciona no WDQS dele):
    - Capital via P36 garantida.
    - Cidades = P31/P279* Q515 com P17=Q148 e P1082 >= min_pop.
    - Sem ORDER BY. Devolve múltiplas linhas por cidade (uma por P1082).
    """
    return f"""
PREFIX wd:   <http://www.wikidata.org/entity/>
PREFIX wdt:  <http://www.wikidata.org/prop/direct/>
PREFIX p:    <http://www.wikidata.org/prop/>
PREFIX ps:   <http://www.wikidata.org/prop/statement/>
PREFIX pq:   <http://www.wikidata.org/prop/qualifier/>
PREFIX psv:  <http://www.wikidata.org/prop/statement/value/>
PREFIX wikibase: <http://wikiba.se/ontology#>

SELECT ?city ?admin ?pop ?yr ?lat ?lon ?isCap
WHERE {{
  # Capital
  {{
    wd:Q148 wdt:P36 ?city .
    BIND(1 AS ?isCap)
  }}
  UNION
  # Cidades com população >= min_pop
  {{
    ?city wdt:P31/wdt:P279* wd:Q515 ;
          wdt:P17 wd:Q148 ;
          wdt:P1082 ?pop .
    FILTER(?pop >= {min_pop})
    BIND(0 AS ?isCap)
  }}
  
  OPTIONAL {{ ?city wdt:P131 ?admin }}
  
  OPTIONAL {{ 
    ?city p:P625 ?coordStmt .
    ?coordStmt psv:P625 ?coordValue .
    ?coordValue wikibase:geoLatitude ?lat ;
                wikibase:geoLongitude ?lon .
  }}
  
  OPTIONAL {{
    ?city p:P1082 ?popStmt .
    ?popStmt ps:P1082 ?pop .
    OPTIONAL {{ ?popStmt pq:P585 ?date . BIND(YEAR(?date) AS ?yr) }}
  }}
}}
LIMIT {limit}
"""

def q_fra_wide(limit: int = 300, min_pop: int = 50000) -> str:
    """
    França — padrão “largo” alinhado com o da China:
    - Capital via P36 garantida.
    - Cidades = P31/P279* (Q515 ou Q484170) com P17=Q142 e P1082 >= min_pop.
    - Sem ORDER BY. Devolve múltiplas linhas por cidade (uma por P1082).
    """
    return f"""
PREFIX wd:   <http://www.wikidata.org/entity/>
PREFIX wdt:  <http://www.wikidata.org/prop/direct/>
PREFIX p:    <http://www.wikidata.org/prop/>
PREFIX ps:   <http://www.wikidata.org/prop/statement/>
PREFIX pq:   <http://www.wikidata.org/prop/qualifier/>
PREFIX psv:  <http://www.wikidata.org/prop/statement/value/>
PREFIX wikibase: <http://wikiba.se/ontology#>

SELECT ?city ?admin ?pop ?yr ?lat ?lon ?isCap
WHERE {{
  # A) Capital — entra sempre e não depende de população
  {{
    SELECT ?city (1 AS ?isCap) WHERE {{
      wd:Q142 wdt:P36 ?city .
    }}
  }}
  UNION
  # B) Cidades/communes com população >= min_pop (com LIMIT local)
  {{
    SELECT ?city (0 AS ?isCap) WHERE {{
      {{
        ?city wdt:P31/wdt:P279* wd:Q515
      }} UNION {{
        ?city wdt:P31/wdt:P279* wd:Q484170
      }}
      ?city wdt:P17 wd:Q142 ;
            wdt:P1082 ?popFilter .

      # Exclusões (evita métropoles, departamentos, regiões, etc.)
      MINUS {{ ?city wdt:P31/wdt:P279* wd:Q3333855 }}   # métropole
      MINUS {{ ?city wdt:P31/wdt:P279* wd:Q1907114 }}   # área metropolitana
      MINUS {{ ?city wdt:P31/wdt:P279* wd:Q6465 }}      # département
      MINUS {{ ?city wdt:P31/wdt:P279* wd:Q36784 }}     # região de França
      MINUS {{ ?city wdt:P31/wdt:P279* wd:Q22923920 }}  # collectivité à statut particulier

      FILTER(?popFilter >= {min_pop})
    }} LIMIT {limit}
  }}

  # Enriquecimento (opcional) — corre para ambos os ramos
  OPTIONAL {{ ?city wdt:P131 ?admin }}

  OPTIONAL {{
    ?city p:P625 ?coordStmt .
    ?coordStmt psv:P625 ?coordValue .
    ?coordValue wikibase:geoLatitude ?lat ;
                wikibase:geoLongitude ?lon .
  }}

  OPTIONAL {{
    ?city p:P1082 ?popStmt .
    ?popStmt ps:P1082 ?pop .
    OPTIONAL {{ ?popStmt pq:P585 ?date . BIND(YEAR(?date) AS ?yr) }}
  }}
}}


"""

def q_block_ordered(iso3: str, limit: int = 4000, min_pop: int = 1000) -> str:
    """
    Variante agregada + ORDER BY (fallback; pode ser pesada).
    """
    return f"""
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX p: <http://www.wikidata.org/prop/>
PREFIX ps: <http://www.wikidata.org/prop/statement/>
PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
PREFIX psv: <http://www.wikidata.org/prop/statement/value/>
PREFIX wikibase: <http://wikiba.se/ontology#>

SELECT
  ?city
  (SAMPLE(?admin) AS ?admin)
  (MAX(?pop)      AS ?pop)
  (MAX(?yr)       AS ?yr)
  (SAMPLE(?lat)   AS ?lat)
  (SAMPLE(?lon)   AS ?lon)
  (MAX(?isCap)    AS ?isCap)
WHERE {{
  ?country wdt:P298 "{iso3}" .
  OPTIONAL {{ ?country wdt:P36 ?capital . }}

  VALUES ?cls {{ wd:Q515 wd:Q15284 wd:Q486972 }}
  ?city wdt:P31/wdt:P279* ?cls .

  {{
    ?city wdt:P17 ?country .
  }} UNION {{
    ?city (wdt:P131)+ ?admUnit .
    ?admUnit wdt:P17 ?country .
  }}

  OPTIONAL {{ ?city wdt:P131 ?admin }}

  ?city p:P1082 ?stpop .
  ?stpop ps:P1082 ?pop .
  OPTIONAL {{ ?stpop pq:P585 ?yr }}

  OPTIONAL {{
    ?city p:P625 ?st .
    ?st psv:P625 ?coord .
    ?coord wikibase:geoLatitude ?lat ;
           wikibase:geoLongitude ?lon .
  }}

  BIND(IF(BOUND(?capital) && ?city = ?capital, 1, 0) AS ?isCap)
  FILTER(?pop >= {min_pop})
}}
GROUP BY ?city
ORDER BY DESC(?pop)
LIMIT {limit}
"""

def q_block_no_order(iso3: str, raw_limit: int) -> str:
    """
    Fallback "bruto" clássico (sem ORDER BY), amplo — usado para o resto dos países.
    """
    return f"""
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX p: <http://www.wikidata.org/prop/>
PREFIX ps: <http://www.wikidata.org/prop/statement/>
PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
PREFIX psv: <http://www.wikidata.org/prop/statement/value/>
PREFIX wikibase: <http://wikiba.se/ontology#>

SELECT ?city ?admin ?pop ?yr ?isCap ?lat ?lon WHERE {{
  ?country wdt:P298 "{iso3}" .
  OPTIONAL {{ ?country wdt:P36 ?capital . }}

  VALUES ?cls {{ wd:Q515 wd:Q15284 wd:Q486972 }}
  ?city wdt:P31/wdt:P279* ?cls .

  {{
    ?city wdt:P17 ?country .
  }} UNION {{
    ?city (wdt:P131)+ ?admUnit .
    ?admUnit wdt:P17 ?country .
  }}

  OPTIONAL {{ ?city wdt:P131 ?admin }}

  ?city p:P1082 ?stpop .
  ?stpop ps:P1082 ?pop .
  OPTIONAL {{ ?stpop pq:P585 ?yr }}

  OPTIONAL {{
    ?city p:P625 ?st .
    ?st psv:P625 ?coord .
    ?coord wikibase:geoLatitude ?lat ;
           wikibase:geoLongitude ?lon .
  }}

  BIND(IF(BOUND(?capital) && ?city = ?capital, 1, 0) AS ?isCap)
}}
LIMIT {raw_limit}
"""

def q_block_stable(iso3: str, limit: int = RAW_LIMIT, min_pop: Optional[int] = STABLE_MIN_POP) -> str:
    """
    Genérica e estável (sem recursões ilimitadas), SEM ORDER BY:
    - Garante capital (P36).
    - Aceita Q515, subclasses (1–3) de municipality (Q15284) / commune (Q203934) e fallback Q486972.
    - País por P17 direto OU P131 (1–2 saltos) para o território ISO-3; suporta territórios com soberano.
    - População: TODAS as P1082 (Python escolhe o máx. por QID no Top-20).
    """
    pop_filter = f"\n    FILTER(?pop >= {min_pop})" if isinstance(min_pop, int) else ""
    return f"""
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX p:   <http://www.wikidata.org/prop/>
PREFIX ps:  <http://www.wikidata.org/prop/statement/>
PREFIX pq:  <http://www.wikidata.org/prop/qualifier/>
PREFIX psv: <http://www.wikidata.org/prop/statement/value/>
PREFIX wikibase: <http://wikiba.se/ontology#>

SELECT ?city ?admin ?pop ?yr ?lat ?lon ?isCap
WHERE {{
  # Território por ISO-3 (e seu soberano, se existir)
  ?territory wdt:P298 "{iso3}" .
  OPTIONAL {{ ?territory wdt:P17 ?sovereign }}
  BIND(COALESCE(?sovereign, ?territory) AS ?country_like)

  # A) CAPITAL garantida
  {{
    OPTIONAL {{ ?territory wdt:P36 ?capital }}
    BIND(?capital AS ?city)
    FILTER(BOUND(?city))
    BIND(1 AS ?isCap)
  }}
  UNION
  # B) Universo estável
  {{
    {{
      ?city wdt:P31 wd:Q515
    }} UNION {{
      ?city wdt:P31 ?inst1 .
      {{ ?inst1 wdt:P279 wd:Q15284 }}
      UNION {{ ?inst1 wdt:P279/wdt:P279 wd:Q15284 }}
      UNION {{ ?inst1 wdt:P279/wdt:P279/wdt:P279 wd:Q15284 }}
    }} UNION {{
      ?city wdt:P31 ?inst2 .
      {{ ?inst2 wdt:P279 wd:Q203934 }}
      UNION {{ ?inst2 wdt:P279/wdt:P279 wd:Q203934 }}
      UNION {{ ?inst2 wdt:P279/wdt:P279/wdt:P279 wd:Q203934 }}
    }} UNION {{
      ?city wdt:P31 wd:Q486972
    }}

    {{
      ?city wdt:P17 ?territory
    }} UNION {{
      ?city wdt:P131 ?territory
    }} UNION {{
      ?city wdt:P131/wdt:P131 ?territory
    }} UNION {{
      ?city wdt:P17 ?country_like .
      {{ ?city wdt:P131 ?territory }} UNION {{ ?city wdt:P131/wdt:P131 ?territory }}
    }}

    OPTIONAL {{ ?city wdt:P131 ?admin }}

    OPTIONAL {{
      ?city p:P1082 ?stpop .
      ?stpop ps:P1082 ?pop .
      OPTIONAL {{ ?stpop pq:P585 ?date . BIND(YEAR(?date) AS ?yr) }}
    }}{pop_filter}

    OPTIONAL {{
      ?city p:P625 ?stc .
      ?stc psv:P625 ?coord .
      ?coord wikibase:geoLatitude ?lat ;
             wikibase:geoLongitude ?lon .
    }}

    BIND(0 AS ?isCap)
  }}
}}
LIMIT {limit}
"""

# ──────────────────────────────────────────────────────────────────────────────
# Parse bruto
# ──────────────────────────────────────────────────────────────────────────────
def _parse_int_or_none(x: Optional[str]) -> Optional[int]:
    if x is None or x == "":
        return None
    try:
        if len(x) >= 4 and x[0:4].isdigit():
            return int(x[0:4])
        return int(x)
    except Exception:
        return None

def parse_rows(js: Optional[dict]) -> List[Tuple]:
    """Tuplos: (city_qid, admin_q, is_cap, pop, yr, lat, lon)."""
    out: List[Tuple] = []
    if not js:
        return out
    for r in js.get("results", {}).get("bindings", []):
        g = lambda k: r.get(k, {}).get("value")
        city_qid = (g("city") or "").split("/")[-1]
        if not city_qid:
            continue
        admin_q  = (g("admin") or "").split("/")[-1] if g("admin") else ""
        cap      = 1 if g("isCap") in ("1","true","True") else 0

        pop = g("pop"); yr = g("yr"); lat = g("lat"); lon = g("lon")

        try: pop_v = int(float(pop)) if pop not in (None, "") else None
        except: pop_v = None
        yr_v  = _parse_int_or_none(yr)
        try: lat_v = float(lat) if lat not in (None, "") else None
        except: lat_v = None
        try: lon_v = float(lon) if lon not in (None, "") else None
        except: lon_v = None

        out.append((city_qid, admin_q, cap, pop_v, yr_v, lat_v, lon_v))
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Pipeline por país: query -> tmp -> dedupe Top-N -> final (mantendo tmp)
# ──────────────────────────────────────────────────────────────────────────────
def process_iso3(iso3: str, country_name: str, writer: csv.writer) -> bool:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    raw: List[Tuple] = []
    try:
        js = None

        # 1) Casos específicos primeiro
        if iso3 == "CHN":
            js = sparql_post(q_chn_user(limit=200, min_pop=1000000))
        elif iso3 == "FRA":
            js = sparql_post(q_fra_wide(limit=200, min_pop=50000))

        # 2) Restante mundo → “como estava”: BRUTA sem ORDER BY
        if not js or not js.get("results", {}).get("bindings"):
            js = sparql_post(q_block_no_order(iso3, raw_limit=RAW_LIMIT))

        # 3) Fallbacks só se necessário
        if (not js or not js.get("results", {}).get("bindings")) and PREFER_ORDERED:
            print("  … no_order vazio; tentar ordered", file=sys.stderr)
            js = sparql_post(q_block_ordered(iso3))
        if not js or not js.get("results", {}).get("bindings"):
            print("  … ordered vazio; tentar stable genérica", file=sys.stderr)
            js = sparql_post(q_block_stable(iso3, limit=RAW_LIMIT, min_pop=STABLE_MIN_POP))

        raw = parse_rows(js) if js else []
    except Exception as e:
        print(f"  … erro inesperado a consultar WDQS: {e}", file=sys.stderr)
        raw = []

    # 2) Guardar RAW em tmp SEMPRE (mesmo vazio)
    tmp_path = (TMP_DIR / f"{iso3}.csv").resolve()
    df_raw = pd.DataFrame(raw, columns=["city_qid","admin_q","is_cap","pop","yr","lat","lon"])
    df_raw.insert(0, "iso3", iso3)
    df_raw.insert(1, "country", country_name)
    df_raw.to_csv(tmp_path, index=False, encoding="utf-8")
    print(f"[debug] {iso3}: {len(df_raw)} linhas brutas → tmp: {tmp_path}")

    # 3) Se vazio, não prossegue; fica para retry
    if df_raw.empty:
        return False

    # 4) Deduplicação on-the-fly + Top-N (sem ordenar tudo)
    by_city: Dict[str, Dict] = {}
    for (cq, aq, cap, pop, yr, lat, lon) in raw:
        if not cq:
            continue
        cur = by_city.get(cq)
        if cur is None or (pop or -1) > (cur.get("pop") or -1):
            by_city[cq] = {
                "admin_q": (aq or (cur["admin_q"] if cur else "")),
                "is_cap": int(bool(cap)) or (cur["is_cap"] if cur else 0),
                "pop": pop, "yr": yr, "lat": lat, "lon": lon
            }
        else:
            if cap:
                cur["is_cap"] = 1
            if not cur.get("admin_q") and aq:
                cur["admin_q"] = aq
            if cur.get("lat") is None and lat is not None:
                cur["lat"] = lat
            if cur.get("lon") is None and lon is not None:
                cur["lon"] = lon
            if cur.get("yr") is None and yr is not None:
                cur["yr"] = yr

    top_items = heapq.nlargest(
        TOP_N,
        by_city.items(),
        key=lambda kv: ((kv[1].get("pop") if kv[1].get("pop") is not None else -1), kv[0])
    )

    rows_top: List[List] = []
    for cq, d in top_items:
        rows_top.append([
            cq, d.get("admin_q",""), d.get("is_cap",0), d.get("pop"),
            d.get("yr"), d.get("lat"), d.get("lon")
        ])
    df_top = pd.DataFrame(rows_top, columns=["city_qid","admin_q","is_cap","pop","yr","lat","lon"])

    # 5) Guardar preview Top-N ao lado do tmp (mantendo temporários)
    top_path = (TMP_DIR / f"{iso3}_top.csv").resolve()
    df_prev = df_top.copy()
    df_prev.insert(0, "iso3", iso3)
    df_prev.insert(1, "country", country_name)
    df_prev.to_csv(top_path, index=False, encoding="utf-8")
    print(f"[debug] {iso3}: Top {len(df_top)} preview → {top_path}")

    # 6) Labels e escrita final (OVERWRITE já garantido no main)
    lbl_city   = wd_get_labels(df_top["city_qid"].tolist(), "pt")
    admin_qids = [x for x in df_top["admin_q"].tolist() if x]
    lbl_admin  = wd_get_labels(admin_qids, "pt") if admin_qids else {}

    wrote = 0
    for _, r in df_top.iterrows():
        cq = r["city_qid"]; aq = r["admin_q"] or ""
        cap = int(r["is_cap"] or 0)
        pop = int(r["pop"]) if pd.notna(r["pop"]) else None
        yr  = int(r["yr"]) if pd.notna(r["yr"]) else None
        lat = float(r["lat"]) if pd.notna(r["lat"]) else None
        lon = float(r["lon"]) if pd.notna(r["lon"]) else None
        writer.writerow([
            iso3, country_name,
            lbl_city.get(cq, cq), cq,
            lbl_admin.get(aq, ""), cap, pop, yr, lat, lon
        ])
        wrote += 1

    print(f"[ok] {iso3}: Top {len(df_top)} escritas")
    return wrote > 0

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def main():
    seed = load_seed()

    if REFRESH_ISO3:
        only = {i.upper() for i in REFRESH_ISO3}
        before = len(seed)
        seed = seed[seed["iso3"].astype(str).str.upper().isin(only)].copy()
        print(f"[debug] seed filtrada: {len(seed)}/{before} países -> {sorted(only)}")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    # Overwrite total do ficheiro final
    if OUT_PATH.exists():
        OUT_PATH.unlink()

    done = set()
    f = OUT_PATH.open("w", newline="", encoding="utf-8")
    w = csv.writer(f)
    w.writerow(HEAD); f.flush(); os.fsync(f.fileno())

    processed = 0
    failed: List[Tuple[str,str]] = []

    for _, r in seed.iterrows():
        iso3 = str(r["iso3"]).upper().strip()
        if not iso3:
            continue
        country = r.get("name_pt") or r.get("name_en") or iso3

        if SKIP_DONE and iso3 in read_done_iso3():
            print(f"[skip] {iso3} já presente")
            continue

        print(f"[cities] {iso3} {country}")
        ok = process_iso3(iso3, country, w)
        f.flush(); os.fsync(f.fileno())
        if ok:
            done.add(iso3)
        else:
            failed.append((iso3, country))

        processed += 1
        time.sleep(BASE_PAUSE + random.uniform(0,0.2))
        if processed % COOLDOWN_EVERY == 0:
            print(f"  … cooldown {COOLDOWN_SECS}s", file=sys.stderr)
            time.sleep(COOLDOWN_SECS)

    if failed:
        print(f"\n↻ Repetir países que falharam: {len(failed)}")
        time.sleep(3)
        for iso3, country in failed:
            print(f"[retry] {iso3} {country}")
            ok = process_iso3(iso3, country, w)
            f.flush(); os.fsync(f.fileno())
            time.sleep(BASE_PAUSE * 1.8 + random.uniform(0, 0.2))

    f.close()
    print(f"✔️ Atualizado {OUT_PATH}")

if __name__ == "__main__":
    main()
