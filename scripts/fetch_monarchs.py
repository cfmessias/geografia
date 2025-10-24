# scripts/fetch_monarchs.py
from __future__ import annotations
import time
import csv
from pathlib import Path
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

WD_SPARQL = "https://query.wikidata.org/sparql"
UA = "Good2Know/0.1 (mailto:you@example.com) Python-requests"

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
OUTDIR = DATA / "history"
OUTDIR.mkdir(parents=True, exist_ok=True)

SEED_CANDIDATES = [
    DATA / "countries_seed.csv",
    DATA / "countries.csv",
]
CACHE_ISO3_QID = OUTDIR / "_iso3_qid_cache.csv"

_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    """Cria session HTTP com connection pooling e retry strategy."""
    global _session
    if _session is None:
        _session = requests.Session()
        retry_strategy = Retry(
            total=2,  # Reduzido para falhar mais rápido
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
        _session.mount("https://", adapter)
        _session.mount("http://", adapter)
    return _session


def _read_seed() -> pd.DataFrame:
    """Lê arquivo seed com lista de países."""
    for p in SEED_CANDIDATES:
        if p.exists():
            df = pd.read_csv(p, dtype=str, sep=";")
            cols = {c.lower(): c for c in df.columns}
            if "iso3" in cols:
                return df.rename(columns={cols["iso3"]: "iso3"})
    raise FileNotFoundError("Não encontrei countries_seed.csv (com coluna iso3) em data/.")


def _load_cache() -> Dict[str, str]:
    """Carrega cache iso3->qid."""
    if not CACHE_ISO3_QID.exists():
        return {}
    try:
        rows = pd.read_csv(CACHE_ISO3_QID, dtype=str)
        return {r["iso3"]: r["qid"] for _, r in rows.iterrows()}
    except Exception:
        return {}


def _save_cache(d: Dict[str, str]) -> None:
    """Salva cache iso3->qid."""
    if not d:
        return
    items = sorted(d.items())
    with open(CACHE_ISO3_QID, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["iso3", "qid"])
        w.writerows(items)


def sparql(query: str, timeout: int = 90, retries: int = 2) -> Optional[dict]:
    """Executa query SPARQL com retry manual para timeouts."""
    headers = {"User-Agent": UA, "Accept": "application/sparql-results+json"}
    session = _get_session()
    
    for attempt in range(retries):
        try:
            r = session.get(
                WD_SPARQL,
                params={"query": query, "format": "json"},
                headers=headers,
                timeout=timeout
            )
            r.raise_for_status()
            return r.json()
        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                wait = 2 ** attempt  # Exponential backoff
                print(f"    [timeout] Tentando novamente em {wait}s...")
                time.sleep(wait)
            else:
                print(f"    [timeout] Query excedeu {timeout}s após {retries} tentativas")
                return None
        except requests.exceptions.RequestException as e:
            print(f"    [erro] Request falhou: {e}")
            return None
    
    return None


def qid_for_iso3_batch(iso3_list: List[str], cache: Dict[str, str]) -> Dict[str, Optional[str]]:
    """Busca QIDs para múltiplos ISO3 em uma única query."""
    iso3_upper = [iso.upper() for iso in iso3_list if iso]
    uncached = [iso for iso in iso3_upper if iso not in cache]
    
    if not uncached:
        return {iso: cache.get(iso) for iso in iso3_upper}
    
    values = " ".join(f'"{iso}"' for iso in uncached)
    q = f"""
    SELECT ?iso ?c WHERE {{
      VALUES ?iso {{ {values} }}
      ?c wdt:P298 ?iso .
      ?c wdt:P31/wdt:P279* wd:Q6256 .
    }}
    """
    
    js = sparql(q, timeout=60, retries=2)
    if js:
        for b in js["results"]["bindings"]:
            iso = b["iso"]["value"]
            qid = b["c"]["value"].rsplit("/", 1)[-1]
            cache[iso] = qid
        _save_cache(cache)
    
    return {iso: cache.get(iso) for iso in iso3_upper}


def fetch_monarchs_for(qid: str, iso3: str) -> pd.DataFrame:
    """
    Lista monarcas para um país. Usa múltiplas estratégias para capturar
    monarquias históricas que podem estar modeladas de formas diferentes.
    """
    all_results = []
    
    # Estratégia 1: Query via posições de monarca relacionadas ao país
    q1 = f"""
    SELECT DISTINCT ?monarch ?position ?house ?start ?end
    WHERE {{
      ?monarch p:P39 ?st .
      ?st ps:P39 ?position .
      ?position wdt:P279* wd:Q116 .
      
      {{ ?position wdt:P1001 wd:{qid} }} UNION
      {{ ?position wdt:P17 wd:{qid} }} UNION
      {{ ?st pq:P1001 wd:{qid} }}
      
      OPTIONAL {{ ?st pq:P580 ?start . }}
      OPTIONAL {{ ?st pq:P582 ?end . }}
      OPTIONAL {{ ?monarch wdt:P53 ?house . }}
    }}
    ORDER BY ?start
    """
    
    js1 = sparql(q1, timeout=90, retries=2)
    if js1 and js1["results"]["bindings"]:
        all_results.extend(js1["results"]["bindings"])
    
    # Estratégia 2: Via chefe de estado (P35) - captura monarcas históricos
    q2 = f"""
    SELECT DISTINCT ?monarch ?position ?house ?start ?end
    WHERE {{
      wd:{qid} p:P35 ?st .
      ?st ps:P35 ?monarch .
      
      OPTIONAL {{ ?st pq:P580 ?start . }}
      OPTIONAL {{ ?st pq:P582 ?end . }}
      OPTIONAL {{ ?monarch wdt:P53 ?house . }}
      
      # Tenta pegar a posição de monarca
      OPTIONAL {{
        ?monarch p:P39 ?st2 .
        ?st2 ps:P39 ?position .
        ?position wdt:P279* wd:Q116 .
        FILTER(
          BOUND(?start) && BOUND(?position) &&
          EXISTS {{ ?st2 pq:P580 ?start }}
        )
      }}
    }}
    ORDER BY ?start
    """
    
    js2 = sparql(q2, timeout=60, retries=2)
    if js2 and js2["results"]["bindings"]:
        all_results.extend(js2["results"]["bindings"])
    
    # Estratégia 3: Monarcas que têm cidadania/nacionalidade do país
    q3 = f"""
    SELECT DISTINCT ?monarch ?position ?house ?start ?end
    WHERE {{
      ?monarch wdt:P27 wd:{qid} .
      ?monarch p:P39 ?st .
      ?st ps:P39 ?position .
      ?position wdt:P279* wd:Q116 .
      
      OPTIONAL {{ ?st pq:P580 ?start . }}
      OPTIONAL {{ ?st pq:P582 ?end . }}
      OPTIONAL {{ ?monarch wdt:P53 ?house . }}
    }}
    ORDER BY ?start
    LIMIT 500
    """
    
    js3 = sparql(q3, timeout=60, retries=1)
    if js3 and js3["results"]["bindings"]:
        all_results.extend(js3["results"]["bindings"])
    
    if not all_results:
        return pd.DataFrame()
    if not all_results:
        return pd.DataFrame()
    
    # Processar e deduplicar resultados
    seen = set()
    rows = []
    for b in all_results:
        g = lambda k: b.get(k, {}).get("value")
        
        # Chave única para deduplicação
        key = (g("monarch"), g("start"), g("end"))
        if key in seen:
            continue
        seen.add(key)
        
        rows.append({
            "country_qid": f"http://www.wikidata.org/entity/{qid}",
            "monarch_qid": g("monarch"),
            "position_qid": g("position"),
            "house_qid": g("house"),
            "start": g("start"),
            "end": g("end"),
        })
    
    if not rows:
        return pd.DataFrame()
    
    df = pd.DataFrame(rows)
    
    # Buscar labels em separado
    df = _enrich_labels(df, ["country_qid", "monarch_qid", "position_qid", "house_qid"])
    
    for c in ("start", "end"):
        if c in df.columns:
            df[c + "_year"] = pd.to_numeric(df[c].str[:4], errors="coerce").astype("Int64")
    
    # Ordenar por data de início
    if "start_year" in df.columns:
        df = df.sort_values("start_year")
    
    return df


def fetch_government_forms_for(qid: str, iso3: str) -> pd.DataFrame:
    """Evolução da forma de governo com query otimizada."""
    q = f"""
    SELECT DISTINCT ?form ?start ?end
           (EXISTS {{ ?form wdt:P279* wd:Q4327889 }} AS ?is_mon)
    WHERE {{
      wd:{qid} p:P122 ?st .
      ?st ps:P122 ?form .
      OPTIONAL {{ ?st pq:P580 ?start }}
      OPTIONAL {{ ?st pq:P582 ?end }}
    }}
    ORDER BY ?start
    LIMIT 100
    """
    
    js = sparql(q, timeout=60, retries=2)
    if not js:
        return pd.DataFrame()
    
    rows = []
    for b in js["results"]["bindings"]:
        g = lambda k: b.get(k, {}).get("value")
        is_mon = g("is_mon")
        rows.append({
            "country_qid": f"http://www.wikidata.org/entity/{qid}",
            "form_qid": g("form"),
            "start": g("start"),
            "end": g("end"),
            "is_monarchy": 1 if is_mon == "true" else 0,
        })
    
    if not rows:
        return pd.DataFrame()
    
    df = pd.DataFrame(rows)
    df = _enrich_labels(df, ["country_qid", "form_qid"])
    
    for c in ("start", "end"):
        if c in df.columns:
            df[c + "_year"] = pd.to_numeric(df[c].str[:4], errors="coerce").astype("Int64")
    
    return df


def _enrich_labels(df: pd.DataFrame, qid_cols: List[str]) -> pd.DataFrame:
    """Busca labels em batch para otimizar performance."""
    all_qids = set()
    for col in qid_cols:
        if col in df.columns:
            all_qids.update(df[col].dropna().unique())
    
    if not all_qids:
        return df
    
    # Limitar a 50 QIDs por vez para evitar timeout
    qid_list = list(all_qids)[:50]
    values = " ".join(f"wd:{q.rsplit('/', 1)[-1]}" if "/" in q else f"wd:{q}" 
                      for q in qid_list if q)
    
    q = f"""
    SELECT ?item ?label_pt ?label_en WHERE {{
      VALUES ?item {{ {values} }}
      OPTIONAL {{ ?item rdfs:label ?label_pt FILTER(LANG(?label_pt)='pt') }}
      OPTIONAL {{ ?item rdfs:label ?label_en FILTER(LANG(?label_en)='en') }}
    }}
    """
    
    js = sparql(q, timeout=30, retries=1)
    if not js:
        return df
    
    label_map = {}
    for b in js["results"]["bindings"]:
        item = b["item"]["value"]
        label_map[item] = {
            "pt": b.get("label_pt", {}).get("value"),
            "en": b.get("label_en", {}).get("value"),
        }
    
    # Adicionar labels ao DataFrame
    for col in qid_cols:
        if col in df.columns:
            base = col.replace("_qid", "")
            df[f"{base}_pt"] = df[col].map(lambda x: label_map.get(x, {}).get("pt"))
            df[f"{base}_en"] = df[col].map(lambda x: label_map.get(x, {}).get("en"))
    
    return df


def process_country(iso3: str, qid: str) -> tuple:
    """Processa um país (ambas queries)."""
    print(f"  Processando {iso3}...", flush=True)
    monarchs = pd.DataFrame()
    forms = pd.DataFrame()
    
    try:
        monarchs = fetch_monarchs_for(qid, iso3)
        if not monarchs.empty:
            monarchs.insert(0, "iso3", iso3)
    except Exception as e:
        print(f"    [err] {iso3} monarchs: {e}")
    
    time.sleep(0.3)
    
    try:
        forms = fetch_government_forms_for(qid, iso3)
        if not forms.empty:
            forms.insert(0, "iso3", iso3)
    except Exception as e:
        print(f"    [err] {iso3} forms: {e}")
    
    return iso3, monarchs, forms


def main():
    seed = _read_seed()
    iso3_list = seed["iso3"].astype(str).str.upper().tolist()
    
    cache = _load_cache()
    
    print("== Monarchy data from Wikidata ==")
    print(f"Resolvendo QIDs para {len(iso3_list)} países...")
    
    iso3_to_qid = qid_for_iso3_batch(iso3_list, cache)
    valid_pairs = [(iso, qid) for iso, qid in iso3_to_qid.items() if qid]
    
    print(f"QIDs encontrados: {len(valid_pairs)}/{len(iso3_list)}\n")
    
    all_monarchs = []
    all_forms = []
    
    # Reduzir workers para evitar sobrecarregar API
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(process_country, iso, qid): iso
            for iso, qid in valid_pairs
        }
        
        completed = 0
        for future in as_completed(futures):
            completed += 1
            try:
                iso3, monarchs, forms = future.result()
                if not monarchs.empty:
                    all_monarchs.append(monarchs)
                    print(f"    ✓ {iso3}: {len(monarchs)} monarcas")
                if not forms.empty:
                    all_forms.append(forms)
                    print(f"    ✓ {iso3}: {len(forms)} formas de governo")
                
                if completed % 10 == 0:
                    print(f"\n[{completed}/{len(valid_pairs)}] países processados\n")
            except Exception as e:
                print(f"    [err] Erro ao processar: {e}")
    
    # Salvar resultados
    out_mon = pd.concat(all_monarchs, ignore_index=True) if all_monarchs else pd.DataFrame()
    out_forms = pd.concat(all_forms, ignore_index=True) if all_forms else pd.DataFrame()
    
    if not out_mon.empty:
        out_mon.to_csv(OUTDIR / "monarchs.enriched.csv", index=False)
        print(f"\n[ok] Gravado: {OUTDIR / 'monarchs.enriched.csv'} ({len(out_mon)} linhas)")
    else:
        print("\n[info] Sem linhas de monarcas.")
    
    if not out_forms.empty:
        out_forms.to_csv(OUTDIR / "government_forms.enriched.csv", index=False)
        print(f"[ok] Gravado: {OUTDIR / 'government_forms.enriched.csv'} ({len(out_forms)} linhas)")
    else:
        print("[info] Sem linhas de formas de governo.")


if __name__ == "__main__":
    main()