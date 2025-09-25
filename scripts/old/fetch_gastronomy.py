# scripts/fetch_gastronomy.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import csv, io, os, sys, time
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------
# Paths do projeto
# ---------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SEED_PATH    = PROJECT_ROOT / "data" / "countries_seed.csv"
OUT_PATH     = PROJECT_ROOT / "data" / "gastronomy_all.csv"

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
WDQS     = "https://query.wikidata.org/sparql"
UA       = "GeoGastronomy/1.2 (+https://github.com/)"
TIMEOUT  = 25
SLEEP    = 0.25
RETRIES  = 3

CSV_DELIM = ";"  # alinhado com o projeto

HEAD = [
    "iso3","country","kind","item","item_qid","description","admin",
    "instance_of","image","wikipedia_pt","wikipedia_en","commons","website"
]

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _seed_df() -> pd.DataFrame:
    if not SEED_PATH.exists():
        print(f"❌ Falta {SEED_PATH}. Corre scripts/build_country_seed.py")
        sys.exit(1)

    df = pd.read_csv(SEED_PATH)
    if "name_pt" not in df.columns: df["name_pt"] = ""
    if "name_en" not in df.columns: df["name_en"] = ""
    if "iso3" not in df.columns:
        # tenta recuperar iso3 de colunas semelhantes
        cand = [c for c in df.columns if str(c).strip().lower() in {"country_code","code3","alpha3","iso_3","iso"}]
        if cand:
            df = df.rename(columns={cand[0]: "iso3"})
        else:
            raise RuntimeError("countries_seed.csv não tem coluna 'iso3'.")
    return df

def _sparql(query: str) -> dict | None:
    for i in range(RETRIES):
        try:
            r = requests.get(
                WDQS,
                params={"query": query, "format": "json"},
                headers={"User-Agent": UA},
                timeout=TIMEOUT,
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if i == RETRIES - 1:
                print(f"  … SPARQL falhou em definitivo: {e}")
                return None
            time.sleep(SLEEP * (2 ** i))
    return None

def _q(iso3: str) -> str:
    """
    Itens de comida/bebida do país (P495), com fallback via cozinhas do país.
    Para bebidas, exclui itens com origem múltipla (mantém os de origem exclusiva do país).
    """
    return f"""
SELECT DISTINCT ?item ?itemLabel ?desc ?adminLabel ?kind ?image ?wp_pt ?wp_en ?commons ?website ?instLabel WHERE {{
  # país via ISO3
  ?country wdt:P298 "{iso3}" .

  # --- principal: P495 país ---
  {{
    ?item wdt:P495 ?country .
  }}
  UNION
  # --- fallback: itens ligados a cozinhas do país (cuisine Q1778821) ---
  {{
    ?cuisine wdt:P495 ?country ;
             wdt:P31/wdt:P279* wd:Q1778821 .
    ?item (wdt:P361|wdt:P279|wdt:P31/wdt:P279*) ?cuisine .
  }}

  # classificar como comida ou bebida
  VALUES ?k {{ wd:Q2095 wd:Q40050 }}            # food, drink
  ?item wdt:P31/wdt:P279* ?k .
  BIND(IF(?k=wd:Q40050,"beverage","dish") AS ?kind)

  # ⚠️ Para BEBIDAS: excluir origem múltipla (ex.: sangria ES+PT)
  FILTER( ?k = wd:Q2095 || NOT EXISTS {{ ?item wdt:P495 ?other . FILTER(?other != ?country) }} )

  # contexto e extras
  OPTIONAL {{ ?item wdt:P131 ?admin . ?admin rdfs:label ?adminLabel FILTER(LANG(?adminLabel)="pt") }}
  OPTIONAL {{ ?item wdt:P18  ?image }}
  OPTIONAL {{ ?item wdt:P856 ?website }}
  OPTIONAL {{ ?item wdt:P31  ?inst . ?inst rdfs:label ?instLabel FILTER(LANG(?instLabel)="pt") }}

  OPTIONAL {{ ?item schema:description ?desc FILTER(LANG(?desc)="pt") }}
  OPTIONAL {{ ?wp_pt schema:about ?item ; schema:isPartOf <https://pt.wikipedia.org/> . }}
  OPTIONAL {{ ?wp_en schema:about ?item ; schema:isPartOf <https://en.wikipedia.org/> . }}
  OPTIONAL {{ ?commons schema:about ?item ; schema:isPartOf <https://commons.wikimedia.org/> . }}

  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
}}
"""

def _read_existing_keys() -> set[tuple[str, str]]:
    """
    Lê o ficheiro de saída (se existir) e devolve um set de chaves para deduplicação:
      (iso3, item_qid) se QID existir,
      caso contrário (iso3, item_lower)
    """
    keys: set[tuple[str,str]] = set()
    if not OUT_PATH.exists() or OUT_PATH.stat().st_size == 0:
        return keys

    df = None
    # tenta com ; e encodings comuns
    for kwargs in (
        {"sep": CSV_DELIM, "encoding": "utf-8"},
        {"sep": CSV_DELIM, "encoding": "utf-8-sig"},
        {"engine": "python", "sep": None, "encoding": "utf-8"},
    ):
        try:
            df = pd.read_csv(OUT_PATH, **kwargs)
            df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]
            break
        except Exception:
            df = None
    if df is None or df.empty:
        return keys

    def _norm(s): return (str(s) if pd.notna(s) else "").strip()
    if "iso3" not in df.columns: return keys
    if "item" not in df.columns: return keys
    if "item_qid" not in df.columns: df["item_qid"] = ""

    for _, r in df.iterrows():
        iso3 = _norm(r.get("iso3")).upper()
        qid  = _norm(r.get("item_qid"))
        item = _norm(r.get("item")).lower()
        if iso3:
            if qid:
                keys.add((iso3, qid))
            elif item:
                keys.add((iso3, item))
    return keys

def _prepare_writer() -> tuple[csv.writer, object]:
    """
    Prepara writer CSV com validação/recriação do cabeçalho.
    - Se o cabeçalho atual não coincidir com HEAD (e com o mesmo delimitador),
      faz backup .bak e recria o ficheiro.
    """
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    write_mode = "a"
    need_header = True
    if OUT_PATH.exists() and OUT_PATH.stat().st_size > 0:
        with OUT_PATH.open("r", encoding="utf-8") as fh:
            first_line = fh.readline()
        try:
            first_cols = next(csv.reader(io.StringIO(first_line), delimiter=CSV_DELIM))
        except Exception:
            first_cols = []
        # normalizar espaços
        first_cols = [c.strip() for c in first_cols]
        if len(first_cols) == len(HEAD) and all(a == b for a, b in zip(first_cols, HEAD)):
            need_header = False
        else:
            # cabeçalho antigo ou delimiter errado → backup e recriar
            backup = OUT_PATH.with_suffix(".bak")
            try:
                if backup.exists():
                    backup.unlink()
            except Exception:
                pass
            OUT_PATH.rename(backup)
            write_mode = "w"
            need_header = True
    else:
        write_mode = "w"
        need_header = True

    f = OUT_PATH.open(write_mode, newline="", encoding="utf-8")
    w = csv.writer(f, delimiter=CSV_DELIM, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
    if need_header:
        w.writerow(HEAD)
        f.flush(); os.fsync(f.fileno())
    return w, f

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    seed = _seed_df()
    writer, handle = _prepare_writer()
    seen = _read_existing_keys()  # para deduplicar incrementalmente

    try:
        for _, r in seed.iterrows():
            iso3 = str(r["iso3"]).upper()
            country = (r.get("name_pt") or r.get("name_en") or iso3)
            print(f"[gastronomy] {iso3} {country}")

            js = _sparql(_q(iso3))
            if not js:
                print("  … falhou SPARQL")
                continue

            bindings = js.get("results", {}).get("bindings", [])
            new_rows = 0

            for b in bindings:
                g = lambda k: b.get(k, {}).get("value")
                iri  = g("item") or ""
                qid  = iri.split("/")[-1] if iri else ""
                kind = g("kind") or ""
                item = g("itemLabel") or ""
                desc = g("desc") or ""
                adm  = g("adminLabel") or ""
                inst = g("instLabel") or ""
                img  = g("image") or ""
                wppt = g("wp_pt") or ""
                wpen = g("wp_en") or ""
                cmns = g("commons") or ""
                web  = g("website") or ""

                # deduplicação incremental
                key = (iso3, qid) if qid else (iso3, item.lower().strip())
                if key in seen:
                    continue
                seen.add(key)

                row = [
                    iso3, country, kind, item, qid, desc, adm,
                    inst, img, wppt, wpen, cmns, web
                ]
                writer.writerow([x if x is not None else "" for x in row])
                new_rows += 1

                # flush frequente para robustez em interrupções
                handle.flush(); os.fsync(handle.fileno())

            print(f"  +{new_rows} registo(s) novos")
            time.sleep(SLEEP)

    finally:
        try:
            handle.close()
        except Exception:
            pass

    print(f"✔️ Atualizado {OUT_PATH}")

# ---------------------------------------------------------------------
if __name__ == "__main__":
    main()
