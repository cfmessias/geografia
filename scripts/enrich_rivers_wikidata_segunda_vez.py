# -*- coding: utf-8 -*-
"""
Enriquece data/rivers.csv com informação da Wikidata:
 - nascente (P4080), foz (P403), bacia/país (P205), comprimento (P2043)
 - matching robusto (label/altLabel en/pt/es/fr) + preferência por país (P17/P706/P205)
 - fallback por wbsearchentities quando WDQS textual falha
 - incremental com opções:
     --skip-empty         → não grava rios sem dados adicionais
     --reprocess-missing  → reprocessa rios já gravados mas sem dados adicionais
     --force-all          → recalcula tudo (ignora cache)
     --only PRT,ESP       → restringe a países
Saída: data/rivers_enriched.csv (sep=";")
"""

from __future__ import annotations
from pathlib import Path
import requests, time, sys, argparse
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR     = PROJECT_ROOT / "data"
IN_CSV       = DATA_DIR / "rivers.csv"
OUT_CSV      = DATA_DIR / "rivers_enriched.csv"
PROFILES     = DATA_DIR / "countries_profiles.csv"

USER_AGENT   = "GeoDataBot/1.0 (cfmessias.pt)"
WDQS_URL     = "https://query.wikidata.org/sparql"
WB_API       = "https://www.wikidata.org/w/api.php"

BATCH_SIZE   = 50
SLEEP_SEC    = 1.2
RETRY_MAX    = 3
RETRY_BACKOFF= 2.0

OUT_COLS = [
    "iso3","river_name","length_km",
    "source_label","source_qid",
    "mouth_label","mouth_qid",
    "basin_label","basin_qid",
    "length_wd"
]

# ──────────────────────────────────────────────────────────────
def read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.exists(): return pd.DataFrame()
    try:
        return pd.read_csv(path, sep=";", dtype=str, keep_default_na=False, encoding="utf-8")
    except Exception:
        return pd.DataFrame()

def country_qid_map(profiles: Path) -> dict[str,str]:
    df = read_csv_safe(profiles)
    if df.empty: return {}
    cols = {c.lower(): c for c in df.columns}
    iso_col = cols.get("iso3") or cols.get("country_iso3") or "iso3"
    qid_col = cols.get("qid") or cols.get("country_qid") or "qid"
    out = {}
    for r in df.itertuples():
        iso3 = str(getattr(r, iso_col, "")).upper().strip()
        qid  = str(getattr(r, qid_col, "")).strip()
        if iso3 and qid:
            out[iso3] = qid if qid.startswith("Q") else f"Q{qid}"
    return out

def existing_status(path: Path) -> dict[tuple[str, str], bool]:
    """
    Devolve {(iso3, river_name): has_data_bool}, onde has_data=True
    se existir pelo menos UM dos campos enriquecidos NÃO vazio.
    Considera vazios: None, NaN, "", "nan", "none" (case-insensitive).
    """
    df = read_csv_safe(path)
    status = {}
    if df.empty:
        return status

    enr_cols = ["source_qid", "mouth_qid", "basin_qid", "length_wd"]

    def _norm_empty(v) -> str:
        if v is None:
            return ""
        s = str(v).strip()
        return "" if s.lower() in ("", "nan", "none", "na") else s

    for r in df.itertuples(index=False):
        iso3 = _norm_empty(getattr(r, "iso3", "")).upper()
        name = _norm_empty(getattr(r, "river_name", ""))
        key = (iso3, name)
        has = False
        for c in enr_cols:
            if c in df.columns:
                val = _norm_empty(getattr(r, c, ""))
                if val:
                    has = True
                    break
        status[key] = has
    return status


def save_upsert(rows: list[dict], out_csv: Path):
    """
    UPSERT: lê CSV existente, substitui entradas com a mesma (iso3,river_name),
    e escreve de volta ordenado.
    """
    df_new = pd.DataFrame(rows, columns=OUT_COLS)
    if out_csv.exists():
        df = read_csv_safe(out_csv)
        if df.empty:
            base = pd.DataFrame(columns=OUT_COLS)
        else:
            base = df
        # remover chaves a substituir
        keys = set((r["iso3"], r["river_name"]) for r in rows)
        mask = ~base.apply(lambda x: (x["iso3"], x["river_name"]) in keys, axis=1)
        base = base[mask]
        out = pd.concat([base, df_new], ignore_index=True)
    else:
        out = df_new

    out = out.sort_values(["iso3","river_name"], kind="stable")
    out.to_csv(out_csv, sep=";", index=False, encoding="utf-8")

# ──────────────────────────────────────────────────────────────
def escape_lit(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')

def sparql(query: str) -> dict:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/sparql-results+json"}
    last_err = None
    for i in range(RETRY_MAX):
        try:
            r = requests.get(WDQS_URL, params={"query": query}, headers=headers, timeout=60)
            if r.status_code == 200:
                return r.json()
            last_err = f"HTTP {r.status_code}: {r.text[:200]}"
        except Exception as e:
            last_err = str(e)
        time.sleep((RETRY_BACKOFF ** i))
    raise RuntimeError(f"WDQS erro após {RETRY_MAX} tentativas: {last_err}")

def build_query_by_name(name: str, country_qid: str | None) -> str:
    lit = escape_lit(name)
    langs = '("en","pt","es","fr")'
    blocks = f"""
      {{ ?river rdfs:label ?label .
         FILTER(LANG(?label) IN {langs})
         FILTER(LCASE(STR(?label)) = LCASE("{lit}"))
      }}
      UNION
      {{ ?river skos:altLabel ?label .
         FILTER(LANG(?label) IN {langs})
         FILTER(LCASE(STR(?label)) = LCASE("{lit}"))
      }}
    """
    country_blocks = ""
    if country_qid:
        country_blocks = f"""
          OPTIONAL {{ ?river wdt:P17  wd:{country_qid} . BIND(true AS ?m1) }}
          OPTIONAL {{ ?river wdt:P706 wd:{country_qid} . BIND(true AS ?m2) }}
          OPTIONAL {{ ?river wdt:P205 wd:{country_qid} . BIND(true AS ?m3) }}
        """
    return f"""
SELECT DISTINCT ?river ?label ?length ?source ?sourceLabel ?mouth ?mouthLabel ?basin ?basinLabel ?sitelinks
       (COALESCE(?m1, ?m2, ?m3, false) AS ?inCountry)
       (IF(LCASE(STR(?label)) = LCASE("{lit}"), true, false) AS ?exactMatch)
WHERE {{
  ?river wdt:P31/wdt:P279* wd:Q4022 .
  {blocks}
  OPTIONAL {{ ?river wdt:P2043 ?length . }}
  OPTIONAL {{ ?river wikibase:sitelinks ?sitelinks . }}

  OPTIONAL {{ ?river wdt:P4080 ?source . }}
  OPTIONAL {{ ?source rdfs:label ?sourceLabel FILTER(LANG(?sourceLabel) IN {langs}) }}
  OPTIONAL {{ ?river wdt:P403 ?mouth . }}
  OPTIONAL {{ ?mouth rdfs:label ?mouthLabel FILTER(LANG(?mouthLabel) IN {langs}) }}
  OPTIONAL {{ ?river wdt:P205 ?basin . }}
  OPTIONAL {{ ?basin rdfs:label ?basinLabel FILTER(LANG(?basinLabel) IN {langs}) }}

  BIND(false AS ?m1) BIND(false AS ?m2) BIND(false AS ?m3)
  {country_blocks}
}}
LIMIT 25
"""

def build_query_by_qid(qid: str) -> str:
    return f"""
SELECT DISTINCT ?river ?length ?source ?sourceLabel ?mouth ?mouthLabel ?basin ?basinLabel
WHERE {{
  VALUES ?river {{ wd:{qid} }}
  OPTIONAL {{ ?river wdt:P2043 ?length . }}
  OPTIONAL {{ ?river wdt:P4080 ?source . }}
  OPTIONAL {{ ?source rdfs:label ?sourceLabel FILTER(LANG(?sourceLabel) IN ("pt","en","es","fr")) }}
  OPTIONAL {{ ?river wdt:P403 ?mouth . }}
  OPTIONAL {{ ?mouth rdfs:label ?mouthLabel FILTER(LANG(?mouthLabel) IN ("pt","en","es","fr")) }}
  OPTIONAL {{ ?river wdt:P205 ?basin . }}
  OPTIONAL {{ ?basin rdfs:label ?basinLabel FILTER(LANG(?basinLabel) IN ("pt","en","es","fr")) }}
}}
LIMIT 1
"""

def pick_best(binding_list: list[dict]) -> dict | None:
    if not binding_list:
        return None
    def getv(b,k): return b[k]["value"] if k in b else ""
    scored = []
    for b in binding_list:
        score = 0.0
        if getv(b,"inCountry") in ("true","1"): score += 3.0
        if getv(b,"exactMatch") in ("true","1"): score += 2.0
        if getv(b,"length"): score += 1.0
        try:
            sl = int(getv(b,"sitelinks")) if getv(b,"sitelinks") else 0
            score += min(sl, 120)/60.0
        except: pass
        scored.append((score, b))
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1]

def extract(binding: dict) -> dict[str,str]:
    def getv(k): return binding[k]["value"] if k in binding else ""
    def q(uri):  return uri.split("/")[-1] if uri else ""
    return {
        "source_label": getv("sourceLabel"),
        "source_qid":   q(getv("source")),
        "mouth_label":  getv("mouthLabel"),
        "mouth_qid":    q(getv("mouth")),
        "basin_label":  getv("basinLabel"),
        "basin_qid":    q(getv("basin")),
        "length_wd":    getv("length"),
    }

def wb_search(name: str, languages=("en","pt","es","fr"), limit=10) -> list[dict]:
    headers = {"User-Agent": USER_AGENT}
    results = []
    for lang in languages:
        params = {
            "action": "wbsearchentities",
            "format": "json",
            "language": lang,
            "search": name,
            "type": "item",
            "limit": str(limit),
        }
        try:
            r = requests.get(WB_API, params=params, headers=headers, timeout=30)
            if r.status_code != 200:
                continue
            data = r.json()
            results.extend(data.get("search", []))
        except Exception:
            continue
        time.sleep(0.3)
    return results

def is_river_qid(qid: str) -> bool:
    q = f"""
SELECT (COUNT(*) AS ?n) WHERE {{
  VALUES ?river {{ wd:{qid} }}
  ?river wdt:P31/wdt:P279* wd:Q4022 .
}}
"""
    try:
        data = sparql(q)
        b = data.get("results", {}).get("bindings", [])
        return bool(b and int(b[0]["n"]["value"]) > 0)
    except Exception:
        return False

def fetch_props_by_qid(qid: str) -> dict[str,str]:
    try:
        data = sparql(build_query_by_qid(qid))
        b = data.get("results", {}).get("bindings", [])
        if not b:
            return {k:"" for k in ("source_label","source_qid","mouth_label","mouth_qid","basin_label","basin_qid","length_wd")}
        binding = b[0]
        def getv(k): return binding[k]["value"] if k in binding else ""
        def q(uri):  return uri.split("/")[-1] if uri else ""
        return {
            "source_label": getv("sourceLabel"),
            "source_qid":   q(getv("source")),
            "mouth_label":  getv("mouthLabel"),
            "mouth_qid":    q(getv("mouth")),
            "basin_label":  getv("basinLabel"),
            "basin_qid":    q(getv("basin")),
            "length_wd":    getv("length"),
        }
    except Exception:
        return {k:"" for k in ("source_label","source_qid","mouth_label","mouth_qid","basin_label","basin_qid","length_wd")}

# ──────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Enrich rivers with Wikidata")
    ap.add_argument("--only", default="", help="Lista ISO3 separados por vírgula (ex.: PRT,ESP)")
    ap.add_argument("--skip-empty", action="store_true",
                    help="Não grava rios sem dados adicionais (primeira execução)")
    ap.add_argument("--reprocess-missing", action="store_true",
                    help="Reprocessa rios já gravados mas sem dados adicionais")
    ap.add_argument("--force-all", action="store_true",
                    help="Ignora cache e recalcula todos os rios")
    args = ap.parse_args()

    df_riv = read_csv_safe(IN_CSV)
    if df_riv.empty:
        print("[rivers-enrich] ERRO: data/rivers.csv vazio/inexistente.")
        sys.exit(1)

    # Normalização básica
    df_riv["iso3"]       = df_riv["iso3"].astype(str).str.upper().str.strip()
    df_riv["river_name"] = df_riv["river_name"].astype(str).str.strip()
    df_riv["length_km"]  = df_riv["length_km"].astype(str)
    df_riv = df_riv[(df_riv["iso3"] != "") & (df_riv["river_name"] != "")]

    # Filtro por --only
    if args.only:
        only_set = {x.strip().upper() for x in args.only.split(",") if x.strip()}
        df_riv = df_riv[df_riv["iso3"].isin(only_set)]

    iso3_to_qid = country_qid_map(PROFILES)
    status = existing_status(OUT_CSV)  # {(iso3,river_name): has_data_bool}
    

    # Conjuntos de chaves
    keys_in  = set((r.iso3, r.river_name) for r in df_riv.itertuples())
    keys_enr = set(status.keys())

    # A) não existentes no enriched
    missing = keys_in - keys_enr
    # B) existentes no enriched mas sem dados adicionais
    present_empty = {k for k, has in status.items() if not has} & keys_in

    # Diagnóstico
    print(f"[rivers-enrich] rivers.csv total: {len(keys_in)}")
    print(f"[rivers-enrich] enriched keys:   {len(keys_enr)}")
    print(f"[rivers-enrich] missing:         {len(missing)}")
    print(f"[rivers-enrich] present-empty:   {len(present_empty)}")

    # Definir o conjunto a processar conforme flags
    if args.force_all:
        to_process = keys_in
        print("[rivers-enrich] ⚠️  --force-all ativo: vai recalcular todos os rios listados em rivers.csv")
    elif args.reprocess_missing:
        to_process = missing | present_empty
        print("[rivers-enrich] ↻ --reprocess-missing ativo: vai processar missing ∪ present-empty")
    else:
        to_process = missing
        print("[rivers-enrich] modo normal: só missing (não existentes no enriched)")

    # Ordenação opcional: maiores primeiro (ajuda a ver progresso com “grandes” rios)
    def _len_km_for(key):
        iso3, name = key
        row = df_riv[(df_riv["iso3"] == iso3) & (df_riv["river_name"] == name)]
        try:
            return float(str(row["length_km"].iloc[0]).replace(",", "."))
        except Exception:
            return -1.0

    ordered = sorted(to_process, key=_len_km_for, reverse=True)
    # Construir a lista (iso3, river_name, length_km) para o loop principal
    todo = []
    for iso3, name in ordered:
        lk = df_riv.loc[(df_riv["iso3"] == iso3) & (df_riv["river_name"] == name), "length_km"]
        todo.append((iso3, name, (str(lk.iloc[0]) if not lk.empty else "")))

    print(f"[rivers-enrich] a processar: {len(todo)} rios | já existentes: {len(status)}")
    buf = []

    for idx, (iso3, name, length_km) in enumerate(todo, start=1):
        country_qid = iso3_to_qid.get(iso3, "")
        print(f"[{idx}/{len(todo)}] {iso3} | {name} … ", end="", flush=True)

        info = None

        # 1) WDQS por nome
        try:
            q1 = build_query_by_name(name, country_qid if country_qid else None)
            data = sparql(q1)
            bindings = data.get("results", {}).get("bindings", [])
            pick = pick_best(bindings)
            if pick:
                info = extract(pick)
        except Exception as e:
            print(f"WDQS-erro: {e}; ", end="")

        # 2) fallback wbsearch → valida QID de rio → propriedades
        if info is None or all(v == "" for v in info.values()):
            hits = wb_search(name)
            qid_found = ""
            for h in hits:
                cand_qid = h.get("id", "")
                if not cand_qid:
                    continue
                if is_river_qid(cand_qid):
                    qid_found = cand_qid
                    break
            if qid_found:
                info = fetch_props_by_qid(qid_found)

        ok = info is not None and any(v for v in info.values())

        # — lógica: --skip-empty → não gravar quando não há dados novos —
        if args.skip_empty and not ok:
            print("sem dados (ignorado)")
            continue

        print("ok" if ok else "sem dados")
        row = {
            "iso3": iso3,
            "river_name": name,
            "length_km": length_km,
            **(info if info else {
                "source_label": "", "source_qid": "",
                "mouth_label":  "", "mouth_qid":  "",
                "basin_label":  "", "basin_qid":  "",
                "length_wd":    ""
            })
        }
        buf.append(row)

        if len(buf) >= BATCH_SIZE:
            save_upsert(buf, OUT_CSV)   # UPSERT batch
            buf.clear()
            print(f"  ↳ upsert {BATCH_SIZE} registos")
        time.sleep(SLEEP_SEC)

    if buf:
        save_upsert(buf, OUT_CSV)

    print(f"[rivers-enrich] concluído → {OUT_CSV}")


if __name__ == "__main__":
    main()
