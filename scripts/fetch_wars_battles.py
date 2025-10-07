# scripts/fetch_wars.py
# -*- coding: utf-8 -*-
"""
Wikidata → Guerras e Batalhas por país (wars/battles).
- Sem argumentos: varre todos os ISO3 do ficheiro data/countries_seed.csv (ou country_seed.csv).
- Com argumentos: um país (por ISO3 ou QID), com opções.

Saídas:
  data/history/wars_battles.csv                         (agregado)
  data/history/wars_battles_by_country/PRT.csv, ...     (por país)

Exemplos:
  python -u scripts/fetch_wars.py
  python -u scripts/fetch_wars.py --iso3 PRT --since-year 1800
  python -u scripts/fetch_wars.py --country-qid Q45 --types battles --limit 500
"""
from __future__ import annotations
import argparse
import sys
import time
from pathlib import Path
from typing import Optional, Literal

import requests
import pandas as pd

WDQS_URL = "https://query.wikidata.org/sparql"
# 👉 IMPORTANTE: coloca um contacto teu (email/site) para o WDQS ser simpático.
USER_AGENT = "Good2Know-WarsFetcher/1.3 (+https://example.com; contact@example.com)"


# -------------------------- Utilitários de FS --------------------------
def project_root() -> Path:
    here = Path(__file__).resolve()
    for p in [here, *here.parents]:
        if (p / "data").exists():
            return p
    return Path.cwd()

def ensure_dirs(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


# -------------------------- Leitura de seed ----------------------------
def read_seed_countries(base: Path) -> pd.DataFrame:
    """
    Lê data/countries_seed.csv (ou data/country_seed.csv) e devolve col. iso3 (uppercase).
    """
    data_dir = base / "data"
    candidates = [data_dir / "countries_seed.csv", data_dir / "country_seed.csv"]
    path = next((p for p in candidates if p.exists()), None)
    if not path:
        raise FileNotFoundError("Não encontrei countries_seed.csv em data/")

    for sep in (",", ";"):
        try:
            df = pd.read_csv(path, sep=sep, dtype="string")
            if "iso3" in df.columns:
                df["iso3"] = df["iso3"].str.upper().str.strip()
                return df[["iso3"]].dropna().drop_duplicates().reset_index(drop=True)
        except Exception:
            pass
    raise RuntimeError(f"Não consegui ler {path} com separador ',' nem ';'.")


# -------------------------- SPARQL helpers ----------------------------
def run_sparql(query: str, max_retries: int = 6, timeout: int = 60) -> dict:
    """
    Envia query por POST ao WDQS com backoff e mensagens úteis em caso de erro.
    """
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": USER_AGENT,
    }
    data = {"query": query, "format": "json"}
    last_text = None
    for attempt in range(max_retries):
        try:
            r = requests.post(WDQS_URL, headers=headers, data=data, timeout=timeout)
            last_text = r.text
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            # 429/5xx → tenta outra vez com backoff exponencial
            if r is not None and r.status_code in (429, 500, 502, 503):
                time.sleep(2 ** attempt)
                continue
            # 400 → mostra corpo da resposta para debugging
            msg = f"HTTP {r.status_code}: {e} — body: {str(last_text)[:500]}..."
            raise requests.HTTPError(msg) from e
        except requests.RequestException:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)
    raise RuntimeError("Falha ao obter resposta do WDQS após várias tentativas.")


def resolve_country_qid_by_iso3(iso3: str) -> Optional[str]:
    """
    Resolve QID via ISO3 (P298). Retorna ex.: 'Q45' ou None.
    """
    iso3 = str(iso3).upper().strip()
    query = f"""
    SELECT ?c WHERE {{
      ?c wdt:P31/wdt:P279* wd:Q6256 ;
         wdt:P298 "{iso3}" .
    }} LIMIT 1
    """
    js = run_sparql(query)
    b = js.get("results", {}).get("bindings", [])
    if not b:
        return None
    uri = b[0]["c"]["value"]  # "http://www.wikidata.org/entity/Q45"
    return uri.rsplit("/", 1)[-1]


# -------------------------- Queries (2 modos) --------------------------
def _root_types_clause(types: Literal["wars", "battles", "both"]) -> str:
    Q_WAR    = "wd:Q198"
    Q_BATTLE = "wd:Q178561"
    return {
        "wars":   f"VALUES ?rootType {{ {Q_WAR} }}",
        "battles":f"VALUES ?rootType {{ {Q_BATTLE} }}",
        "both":   f"VALUES ?rootType {{ {Q_WAR} {Q_BATTLE} }}"
    }[types]

def _date_filter_clause(since_year: Optional[int]) -> str:
    if since_year is None:
        return ""
    y = int(since_year)
    return f"""
    FILTER(
      (!BOUND(?startYear) && !BOUND(?pointYear)) ||
      (BOUND(?startYear)  && ?startYear >= {y}) ||
      (BOUND(?pointYear)  && ?pointYear >= {y})
    )
    """

def build_query_participant(
    country_qid: str,
    *,
    types: Literal["wars", "battles", "both"] = "both",
    since_year: Optional[int] = None,
    languages: str = "pt,en",
    limit: Optional[int] = None,
) -> str:
    """
    Conflitos em que o país foi PARTICIPANTE (P710).
    """
    return f"""
SELECT ?country ?countryLabel ?conflict ?conflictLabel ?kindLabel
       ?start ?end ?pointInTime ?startYear ?endYear ?pointYear
       ?result ?resultLabel ?partOf ?partOfLabel
       ?context ?contextLabel ?place ?placeLabel ?coords ?deaths
WHERE {{
  {_root_types_clause(types)}
  VALUES ?country {{ wd:{country_qid.strip()} }}

  ?conflict wdt:P31/wdt:P279* ?rootType .
  ?conflict wdt:P710 ?country .

  OPTIONAL {{ ?conflict wdt:P580 ?start . }}
  OPTIONAL {{ ?conflict wdt:P582 ?end . }}
  OPTIONAL {{ ?conflict wdt:P585 ?pointInTime . }}
  OPTIONAL {{ ?conflict wdt:P1346 ?result . }}
  OPTIONAL {{ ?conflict wdt:P361 ?partOf . }}
  OPTIONAL {{ ?conflict wdt:P607 ?context . }}
  OPTIONAL {{ ?conflict wdt:P276 ?place . }}
  OPTIONAL {{ ?conflict wdt:P625 ?coords . }}
  OPTIONAL {{ ?conflict wdt:P1120 ?deaths . }}
  OPTIONAL {{ ?conflict wdt:P31 ?kind . }}

  BIND(year(?start) AS ?startYear)
  BIND(year(?end) AS ?endYear)
  BIND(year(?pointInTime) AS ?pointYear)

  {_date_filter_clause(since_year)}

  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{languages}" . }}
}}
ORDER BY ?countryLabel ?start ?pointInTime
{('LIMIT ' + str(int(limit))) if limit else ''}""".strip()

def build_query_location(
    country_qid: str,
    *,
    types: Literal["wars", "battles", "both"] = "both",
    since_year: Optional[int] = None,
    languages: str = "pt,en",
    limit: Optional[int] = None,
) -> str:
    """
    Conflitos que ocorreram EM território do país:
      ?conflict P276 ?place .
      ?place (P17|P131*) ?country .
    Útil quando não há participante-país explícito.
    """
    return f"""
SELECT ?country ?countryLabel ?conflict ?conflictLabel ?kindLabel
       ?start ?end ?pointInTime ?startYear ?endYear ?pointYear
       ?result ?resultLabel ?partOf ?partOfLabel
       ?context ?contextLabel ?place ?placeLabel ?coords ?deaths
WHERE {{
  {_root_types_clause(types)}
  VALUES ?country {{ wd:{country_qid.strip()} }}

  ?conflict wdt:P31/wdt:P279* ?rootType .
  ?conflict wdt:P276 ?place .
  {{ ?place wdt:P17 ?country }} UNION {{ ?place wdt:P131* ?country }}

  OPTIONAL {{ ?conflict wdt:P580 ?start . }}
  OPTIONAL {{ ?conflict wdt:P582 ?end . }}
  OPTIONAL {{ ?conflict wdt:P585 ?pointInTime . }}
  OPTIONAL {{ ?conflict wdt:P1346 ?result . }}
  OPTIONAL {{ ?conflict wdt:P361 ?partOf . }}
  OPTIONAL {{ ?conflict wdt:P607 ?context . }}
  OPTIONAL {{ ?conflict wdt:P625 ?coords . }}
  OPTIONAL {{ ?conflict wdt:P1120 ?deaths . }}
  OPTIONAL {{ ?conflict wdt:P31 ?kind . }}

  BIND(year(?start) AS ?startYear)
  BIND(year(?end) AS ?endYear)
  BIND(year(?pointInTime) AS ?pointYear)

  {_date_filter_clause(since_year)}

  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{languages}" . }}
}}
ORDER BY ?countryLabel ?start ?pointInTime
{('LIMIT ' + str(int(limit))) if limit else ''}""".strip()


# -------------------------- Normalização de resultados ------------------------
def bindings_to_df(data: dict, source: str) -> pd.DataFrame:
    """
    Converte JSON do WDQS em DataFrame; acrescenta coluna 'Source' (participant/location).
    """
    rows = []
    for b in data.get("results", {}).get("bindings", []):
        def get(k, field="value"): return b.get(k, {}).get(field)
        rows.append({
            "Country_ID":        get("country"),
            "Country":           get("countryLabel"),
            "Conflict_ID":       get("conflict"),
            "Conflict":          get("conflictLabel"),
            "Conflict_Type":     get("kindLabel"),
            "Start":             get("start"),
            "End":               get("end"),
            "PointInTime":       get("pointInTime"),
            "Start_Year":        get("startYear"),
            "End_Year":          get("endYear"),
            "Point_Year":        get("pointYear"),
            "Winner_ID":         get("result"),
            "Winner":            get("resultLabel"),
            "PartOf_ID":         get("partOf"),
            "PartOf":            get("partOfLabel"),
            "Context_ID":        get("context"),
            "Context":           get("contextLabel"),
            "Place_ID":          get("place"),
            "Place":             get("placeLabel"),
            "Coordinates":       get("coords"),
            "Deaths":            get("deaths"),
            "Source":            source,
        })

    df = pd.DataFrame(rows)

    def to_int(x):
        try: return int(x)
        except (TypeError, ValueError): return None

    def to_num(x):
        try: return int(float(x))
        except (TypeError, ValueError): return None

    if df.empty:
        return df

    df["Start_Year_i"] = df["Start_Year"].map(to_int)
    df["Point_Year_i"] = df["Point_Year"].map(to_int)
    df["End_Year_i"]   = df["End_Year"].map(to_int)
    df["Deaths_Num"]   = df["Deaths"].map(to_num)

    df = df.sort_values(
        ["Country", "Start_Year_i", "Point_Year_i", "End_Year_i", "Conflict"],
        na_position="last"
    ).drop(columns=["Start_Year_i", "Point_Year_i", "End_Year_i"]).reset_index(drop=True)

    return df


# -------------------------- Fetch (com fallback) ------------------------------
def fetch_wars_battles_for_qid(
    country_qid: str,
    *,
    types: Literal["wars", "battles", "both"] = "both",
    since_year: Optional[int] = None,
    languages: str = "pt,en",
    limit: Optional[int] = None,
    fallback_by_location: bool = True,
) -> pd.DataFrame:
    """
    Tenta participante (P710); se vazio e fallback=True, tenta localização (P276).
    Une resultados e deduplica por Conflict_ID + Source.
    """
    # 1) participante
    q1 = build_query_participant(country_qid, types=types, since_year=since_year, languages=languages, limit=limit)
    js1 = run_sparql(q1)
    df1 = bindings_to_df(js1, source="participant")

    # 2) fallback por localização
    if fallback_by_location and (df1 is None or df1.empty):
        q2 = build_query_location(country_qid, types=types, since_year=since_year, languages=languages, limit=limit)
        js2 = run_sparql(q2)
        df2 = bindings_to_df(js2, source="location")
        df = df2
    else:
        # podemos unir ambos se quiseres sempre os dois:
        # q2 = build_query_location(...); df = pd.concat([df1, df2])...
        df = df1

    if df is None or df.empty:
        return pd.DataFrame()

    # deduplicar por conflito (se vier repetido) preservando Source
    df = df.drop_duplicates(subset=["Conflict_ID", "Source"]).reset_index(drop=True)
    return df


# -------------------------- Bulk mode -----------------------------------------
def bulk_fetch_all_countries(
    *,
    since_year: int = 1500,
    types: Literal["wars", "battles", "both"] = "both",
    languages: str = "pt,en",
    per_country_delay: float = 0.8,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """
    Varre todos os ISO3 do seed, resolve QID e agrega tudo num DF.
    Grava também CSV por país.
    """
    base = project_root()
    seed = read_seed_countries(base)
    out_dir = base / "data" / "history" / "wars_battles_by_country"
    out_dir.mkdir(parents=True, exist_ok=True)

    all_rows: list[pd.DataFrame] = []

    for i, row in seed.iterrows():
        iso3 = str(row["iso3"]).upper()
        try:
            qid = resolve_country_qid_by_iso3(iso3)
            if not qid:
                print(f"[warn] {iso3}: sem QID (P298) — a saltar…")
                continue

            df = fetch_wars_battles_for_qid(
                qid, types=types, since_year=since_year,
                languages=languages, limit=limit, fallback_by_location=True
            )
            if df is None or df.empty:
                print(f"[info] {iso3}: 0 linhas")
                time.sleep(per_country_delay)
                continue

            df.insert(0, "ISO3", iso3)
            all_rows.append(df)

            # CSV por país (; ; utf-8)
            out_country = out_dir / f"{iso3}.csv"
            ensure_dirs(out_country)
            df.to_csv(out_country, sep=";", index=False, encoding="utf-8")
            print(f"[ok] {iso3}: {len(df)} linhas → {out_country.name}")

            time.sleep(per_country_delay)  # respeitar o endpoint
        except Exception as e:
            print(f"[err] {iso3}: {e}")

    if not all_rows:
        return pd.DataFrame()
    return pd.concat(all_rows, ignore_index=True)


# -------------------------- CLI -----------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Wikidata → Guerras/Batalhas por país → CSV (;).")
    ap.add_argument("--iso3", help="Código ISO3 do país (ex.: PRT). Opcional se usares --country-qid.")
    ap.add_argument("--country-qid", help="QID do país (ex.: Q45). Se omitido e sem --iso3, corre em modo bulk.")
    ap.add_argument("--types", choices=["wars", "battles", "both"], default="both")
    ap.add_argument("--since-year", type=int, default=None, help="Ano mínimo (início ou data pontual).")
    ap.add_argument("--limit", type=int, default=None, help="LIMIT (testes/depuração).")
    ap.add_argument("--languages", default="pt,en", help="Idiomas para labels (ex.: 'pt,en').")
    ap.add_argument("--out", help="CSV de saída (single). Se omitido no bulk, escreve para data/history/wars_battles.csv.")
    args = ap.parse_args()

    # Sem args relevantes → bulk
    if len(sys.argv) == 1 or (not args.iso3 and not args.country_qid and not args.out and args.types == "both" and args.since_year is None and args.limit is None):
        since = 1500
        print(f"== Wikidata wars/battles (bulk via seed) — desde {since} ==")
        df = bulk_fetch_all_countries(since_year=since, types="both", languages="pt,en")
        base = project_root()
        out_agg = base / "data" / "history" / "wars_battles.csv"
        ensure_dirs(out_agg)
        if df is None or df.empty:
            print("[warn] sem dados agregados — nada gravado.")
            sys.exit(0)
        df.to_csv(out_agg, sep=";", index=False, encoding="utf-8")
        print(f"[done] agregado: {len(df)} linhas → {out_agg}")
        sys.exit(0)

    # Single
    try:
        qid = args.country_qid
        if not qid:
            if not args.iso3:
                raise SystemExit("Fornece --iso3 ou --country-qid (ou não passes argumentos para modo bulk).")
            qid = resolve_country_qid_by_iso3(args.iso3)
            if not qid:
                raise SystemExit(f"Não consegui obter QID via ISO3={args.iso3}.")

        df = fetch_wars_battles_for_qid(
            qid,
            types=args.types,
            since_year=args.since_year,
            languages=args.languages,
            limit=args.limit,
            fallback_by_location=True,
        )
        if df is None:
            df = pd.DataFrame()

        out = Path(args.out) if args.out else (project_root() / "data" / "history" / "wars_battles.csv")
        ensure_dirs(out)
        df.to_csv(out, sep=";", index=False, encoding="utf-8")
        print(f"✅ Registos: {len(df)}  →  {out}")
    except Exception as e:
        print(f"❌ Erro: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
