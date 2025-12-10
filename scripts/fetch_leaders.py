# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import csv
import os
import sys
import time
from typing import List, Tuple

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROFILES_PATH = PROJECT_ROOT / "data" / "countries_profiles.csv"
OUT_CURR     = PROJECT_ROOT / "data" / "leaders_current.csv"
OUT_HIST     = PROJECT_ROOT / "data" / "leaders_history.csv"

WDQS   = "https://query.wikidata.org/sparql"
UA     = "GeoLeaders/1.0 (+streamlit demo)"
TIMEOUT = 20
SLEEP   = 0.8   # pausa entre pedidos
MAX_TRIES = 3


def _load_profiles() -> pd.DataFrame:
    if not PROFILES_PATH.exists():
        print(f"[erro] Ficheiro {PROFILES_PATH} não existe.", file=sys.stderr)
        sys.exit(1)
    df = pd.read_csv(PROFILES_PATH, sep=";", dtype=str)
    if "iso3" not in df.columns:
        raise SystemExit("[erro] countries_profiles.csv não tem coluna 'iso3'")
    return df


def _sparql(query: str) -> dict | None:
    """Faz pedido ao WDQS com algumas tentativas e espera entre falhas."""
    for i in range(MAX_TRIES):
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
            wait = SLEEP * (i + 1)
            print(f"[warn] WDQS falhou (tentativa {i+1}/{MAX_TRIES}): {e} — a esperar {wait:.1f}s")
            time.sleep(wait)
    return None


def _q_role(iso3: str, prop: str) -> str:
    """
    Query simples: usa P6 (head of government) ou P35 (head of state)
    diretamente nas declarações do país.

    NOTA: aqui NÃO pedimos partido, só pessoa + datas.
    """
    return f"""
SELECT ?person ?personLabel ?start ?end ?causeEndLabel
WHERE {{
  ?country wdt:P298 "{iso3}" .
  ?country p:{prop} ?st .
  ?st ps:{prop} ?person .

  OPTIONAL {{ ?st pq:P580 ?start }}
  OPTIONAL {{ ?st pq:P582 ?end }}
  OPTIONAL {{ ?st pq:P1534 ?causeEnd }}

  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
}}
"""


def _parse_bindings(bindings: List[dict]) -> List[Tuple[str,str,str,str,str,str,str]]:
    rows: List[Tuple[str,str,str,str,str,str,str]] = []
    seen = set()
    for b in bindings:
        get = lambda k: b.get(k, {}).get("value")
        person_iri = get("person") or ""
        qid = person_iri.rsplit("/", 1)[-1] if person_iri else ""
        person = get("personLabel") or ""
        start  = get("start") or ""
        end    = get("end") or ""
        cause  = get("causeEndLabel") or ""
        key = (qid, start, end, cause)
        if key in seen:
            continue
        seen.add(key)
        # party fica vazio por enquanto
        party = ""
        rows.append((person, qid, start, end, party, cause))
    return rows


def main(overwrite: bool = True) -> None:
    """
    Extrai líderes por país e grava:
      - data/leaders_history.csv  (todas as passagens)
      - data/leaders_current.csv  (um por função)
    Campos: iso3,country,role,person,person_qid,start,end,party,end_cause

    NOTA: a coluna 'party' fica vazia por design; será preenchida
    num passo posterior apenas para chefes de governo.
    """
    df = _load_profiles()

    OUT_CURR.parent.mkdir(parents=True, exist_ok=True)

    mode = "w" if overwrite or not OUT_CURR.exists() else "a"
    fc = OUT_CURR.open(mode, newline="", encoding="utf-8")
    fh = OUT_HIST.open(mode, newline="", encoding="utf-8")

    headers = ["iso3","country","role","person","person_qid","start","end","party","end_cause"]
    wcsv = csv.writer(fc)
    hcsv = csv.writer(fh)
    if mode == "w":
        wcsv.writerow(headers)
        hcsv.writerow(headers)

    total_hist = total_curr = 0

    for _, row in df.iterrows():
        iso3 = (row.get("iso3") or "").upper()
        if not iso3:
            continue
        country = row.get("name") or row.get("country") or iso3

        print(f"[leaders] {iso3} {country}")

        all_rows: list[tuple] = []

        for role, prop in (
            ("head_of_government", "P6"),
            ("head_of_state",     "P35"),
        ):
            js = _sparql(_q_role(iso3, prop))
            if not js:
                print(f"  … falha no WDQS para {role}")
                continue
            bindings = js.get("results", {}).get("bindings", [])
            if not bindings:
                print(f"  … sem resultados para {role}")
                continue
            parsed = _parse_bindings(bindings)
            # escrever histórico
            for person, qid, start, end, party, cause in parsed:
                hcsv.writerow([iso3, country, role, person, qid, start, end, party, cause])
                fh.flush(); os.fsync(fh.fileno())
                total_hist += 1
                all_rows.append((role, person, qid, start, end, party, cause))

        # escolher "atuais" por função
        for role in ("head_of_government","head_of_state"):
            subset = [t for t in all_rows if t[0] == role]
            if not subset:
                continue
            open_terms = [t for t in subset if not t[4]]  # end vazio
            if open_terms:
                current = max(open_terms, key=lambda t: (t[3] or ""))  # start
            else:
                current = max(subset, key=lambda t: (t[3] or ""))
            _, person, qid, start, end, party, cause = current
            wcsv.writerow([iso3, country, role, person, qid, start, end, party, cause])
            fc.flush(); os.fsync(fc.fileno())
            total_curr += 1

    fc.close(); fh.close()
    print(f"✔️ Atualizado {OUT_CURR} (atual: {total_curr}) e {OUT_HIST} (histórico: {total_hist})")


if __name__ == "__main__":
    main()
