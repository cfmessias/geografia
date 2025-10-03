# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import csv, os, sys, time
import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SEED_PATH    = PROJECT_ROOT / "data" / "countries_seed.csv"
OUT_CURR     = PROJECT_ROOT / "data" / "leaders_current.csv"
OUT_HIST     = PROJECT_ROOT / "data" / "leaders_history.csv"

WDQS = "https://query.wikidata.org/sparql"
UA   = "GeoLeaders/1.0 (+streamlit demo)"
TIMEOUT = 15
SLEEP = 0.6  # entre queries
MAX_TRIES = 2

# + duas colunas no fim: party, end_cause
HEAD_CURR = ["iso3","country","role","person","person_qid","start","end","party","end_cause"]
HEAD_HIST = HEAD_CURR

import re

_SIGLA_RE = re.compile(r'^[A-ZÀ-Ü0-9]{2,}([-/][A-ZÀ-Ü0-9]{2,})*$')

def _only_siglas(party_text: str) -> str:
    """Devolve apenas siglas ('PS', 'PSD', 'CDS-PP', 'PCTP/MRPP'...). 
    Se não houver siglas, devolve o 1º nome (que já vem priorizado p/ PT depois EN)."""
    if not party_text:
        return ""
    parts = [p.strip() for p in party_text.split("|") if p.strip()]
    siglas = []
    for p in parts:
        for t in re.split(r'[/,;]|(?<!-)\s+(?!-)', p.strip()):
            t = t.strip()
            if _SIGLA_RE.match(t):
                siglas.append(t)
    # dedup preservando ordem
    siglas = list(dict.fromkeys(siglas))
    return " | ".join(siglas) if siglas else (parts[0] if parts else "")

def _to_ts(s: str):
    try:
        return pd.to_datetime(s, errors="coerce")
    except Exception:
        return pd.NaT
    
def _seed():
    if not SEED_PATH.exists():
        print(f"❌ Falta {SEED_PATH}. Corre scripts/build_country_seed.py")
        sys.exit(1)
    return pd.read_csv(SEED_PATH)

def _sparql(q):
    for i in range(3):
        try:
            r = requests.get(WDQS, params={"query": q, "format":"json"},
                             headers={"User-Agent": UA}, timeout=TIMEOUT)
            r.raise_for_status(); return r.json()
        except Exception:
            time.sleep(SLEEP * (2**i))
    return None

def _q_role_office(iso3: str, office_prop: str) -> str:
    """
    Busca o histórico via 'cargo' do país.
    office_prop: P1313 (chefe de governo) ou P1308 (chefe de Estado)
    Devolve: person, personLabel, start, end, causeEndLabel, party (siglas/nome)
    """
    return f"""
SELECT ?person ?personLabel ?start ?end ?causeEndLabel
       (GROUP_CONCAT(DISTINCT COALESCE(?partyShortPT, ?partyShortEN, ?partyLabel); separator=" | ") AS ?party)
WHERE {{
  ?country wdt:P298 "{iso3}" .
  ?country wdt:{office_prop} ?office .

  ?person p:P39 ?st .
  ?st ps:P39 ?office .

  OPTIONAL {{ ?st pq:P580  ?start }}
  OPTIONAL {{ ?st pq:P582  ?end   }}
  OPTIONAL {{ ?st pq:P1534 ?causeEnd }}

  OPTIONAL {{
    ?person wdt:P102 ?partyEnt .
    OPTIONAL {{ ?partyEnt wdt:P1813 ?partyShortPT . FILTER(LANG(?partyShortPT)="pt") }}
    OPTIONAL {{ ?partyEnt wdt:P1813 ?partyShortEN . FILTER(LANG(?partyShortEN)="en") }}
    OPTIONAL {{ ?partyEnt rdfs:label ?partyLabel . FILTER(LANG(?partyLabel)="pt" || LANG(?partyLabel)="en") }}
  }}

  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
}}
GROUP BY ?person ?personLabel ?start ?end ?causeEndLabel
ORDER BY ?start
"""


def _q_role_direct(iso3: str, prop: str) -> str:
    """
    Fallback: histórico via declarações no item do país (P6/P35).
    Mantém as colunas extra party / end_cause.
    """
    return f"""
SELECT ?person ?personLabel ?start ?end ?causeEndLabel
       (GROUP_CONCAT(DISTINCT COALESCE(?partyShortPT, ?partyShortEN, ?partyLabel); separator=" | ") AS ?party)
WHERE {{
  ?country wdt:P298 "{iso3}" .
  ?country p:{prop} ?st .
  ?st ps:{prop} ?person .

  OPTIONAL {{ ?st pq:P580  ?start }}
  OPTIONAL {{ ?st pq:P582  ?end   }}
  OPTIONAL {{ ?st pq:P1534 ?causeEnd }}

  OPTIONAL {{
    ?person wdt:P102 ?partyEnt .
    OPTIONAL {{ ?partyEnt wdt:P1813 ?partyShortPT . FILTER(LANG(?partyShortPT)="pt") }}
    OPTIONAL {{ ?partyEnt wdt:P1813 ?partyShortEN . FILTER(LANG(?partyShortEN)="en") }}
    OPTIONAL {{ ?partyEnt rdfs:label ?partyLabel . FILTER(LANG(?partyLabel)="pt" || LANG(?partyLabel)="en") }}
  }}

  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
}}
GROUP BY ?person ?personLabel ?start ?end ?causeEndLabel
ORDER BY ?start
"""

def _q_role(iso3: str, prop: str) -> str:
    # prop = "P6" (head of government) ou "P35" (head of state)
    # Acrescenta:
    #  - causa do fim (P1534) -> ?causeEndLabel
    #  - partido(s) (P102) agregados -> ?party
    return f"""
        SELECT ?person ?personLabel ?start ?end ?causeEndLabel
            (GROUP_CONCAT(DISTINCT ?partyLabel; separator=" | ") AS ?party)
        WHERE {{
        ?country wdt:P298 "{iso3}" .
        ?country p:{prop} ?st .
        ?st ps:{prop} ?person .

        OPTIONAL {{ ?st pq:P580 ?start }}
        OPTIONAL {{ ?st pq:P582 ?end }}
        OPTIONAL {{ ?st pq:P1534 ?causeEnd }}

        OPTIONAL {{ ?person wdt:P102 ?partyEnt . }}

        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}

        # As labels acima criam ?partyEntLabel e ?causeEndLabel
        BIND(?partyEntLabel AS ?partyLabel)
        }}
        GROUP BY ?person ?personLabel ?start ?end ?causeEndLabel
        ORDER BY ?start
    """
def _q_role_office_by_class(iso3: str, class_qid: str) -> str:
    """
    Puxa pessoas com P39=algum cargo cuja posição é subclasse de `class_qid`
    (ex.: Q48352 head of state) e cujo cargo aplica-se ao país (P17/P1001).
    """
    return f"""
SELECT ?person ?personLabel ?start ?end ?causeEndLabel
       (GROUP_CONCAT(DISTINCT COALESCE(?partyShortPT, ?partyShortEN, ?partyLabel); separator=" | ") AS ?party)
WHERE {{
  ?country wdt:P298 "{iso3}" .

  ?person p:P39 ?st .
  ?st ps:P39 ?office .
  ?office wdt:P279* wd:{class_qid} .
  ?office (wdt:P17|wdt:P1001) ?country .

  OPTIONAL {{ ?st pq:P580  ?start }}
  OPTIONAL {{ ?st pq:P582  ?end   }}
  OPTIONAL {{ ?st pq:P1534 ?causeEnd }}

  OPTIONAL {{
    ?person wdt:P102 ?partyEnt .
    OPTIONAL {{ ?partyEnt wdt:P1813 ?partyShortPT . FILTER(LANG(?partyShortPT)="pt") }}
    OPTIONAL {{ ?partyEnt wdt:P1813 ?partyShortEN . FILTER(LANG(?partyShortEN)="en") }}
    OPTIONAL {{ ?partyEnt rdfs:label ?partyLabel . FILTER(LANG(?partyLabel)="pt" || LANG(?partyLabel)="en") }}
  }}

  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
}}
GROUP BY ?person ?personLabel ?start ?end ?causeEndLabel
ORDER BY ?start
"""

def _count_rows(path: Path) -> int:
    try:
        with path.open("r", encoding="utf-8") as f:
            # conta linhas úteis (desconta o cabeçalho se existir)
            n = sum(1 for _ in f)
        return max(0, n - 1)
    except FileNotFoundError:
        return 0

def _read_header(path: Path) -> list[str] | None:
    try:
        with path.open("r", encoding="utf-8") as f:
            first = f.readline().strip()
        return [c.strip() for c in first.split(",")] if first else None
    except FileNotFoundError:
        return None

def main(overwrite: bool = True) -> None:
    """
    Extrai líderes por país e grava:
      - data/leaders_history.csv  (todas as passagens)
      - data/leaders_current.csv  (um por função)
    Campos: iso3,country,role,person,person_qid,start,end,party,end_cause
    """
    seed = _seed()

    OUT_CURR.parent.mkdir(parents=True, exist_ok=True)

    # — ficheiros de saída
    mode = "w" if overwrite else ("a" if OUT_CURR.exists() else "w")
    fc = OUT_CURR.open(mode, newline="", encoding="utf-8")
    fh = OUT_HIST.open(mode, newline="", encoding="utf-8")

    wcsv = csv.writer(fc)
    hcsv = csv.writer(fh)

    headers = ["iso3","country","role","person","person_qid","start","end","party","end_cause"]
    if mode == "w":
        wcsv.writerow(headers)
        hcsv.writerow(headers)

    total_hist = total_curr = 0

    # thresholds para evitar fallbacks caros quando já temos bastante histórico
    MIN_OK_GOV  = 8
    MIN_OK_HEAD = 8

    for _, r in seed.iterrows():
        iso3 = str(r["iso3"]).upper()
        country = (r.get("name_pt") or r.get("name_en") or iso3)

        print(f"[leaders] {iso3} {country}")

        all_rows: list[tuple] = []  # (role, person, qid, start, end, party, cause)

        # — duas funções: chefe de governo e chefe de estado
        
        for role, office_prop, direct_prop in (
            ("head_of_government", "P1313", "P6"),
            ("head_of_state",     "P1906", "P35"),
        ):
            bindings = []

            # 1) primeiro o "direct" (rápido): P6/P35 no item do país
            js = _sparql(_q_role_direct(iso3, direct_prop))
            if js:
                bindings += js.get("results", {}).get("bindings", [])

            # 2) se pouco histórico, tenta "office" (cargo do país: P1313/P1308)
            limiar = MIN_OK_HEAD if role == "head_of_state" else MIN_OK_GOV
            if len(bindings) < limiar:
                js2 = _sparql(_q_role_office(iso3, office_prop))
                if js2:
                    bindings += js2.get("results", {}).get("bindings", [])

            # 3) fallback extra só p/ presidentes: cargos subclasse de "head of state" (Q48352) que se aplicam ao país
            if role == "head_of_state" and len(bindings) < limiar:
                js3 = _sparql(_q_role_office_by_class(iso3, "Q48352"))
                if js3:
                    bindings += js3.get("results", {}).get("bindings", [])

            if not bindings:
                print(f"  … sem resultados para {role}")
                continue

            # — dedup por (qid,start,end)
            seen = set()
            rows = []
            for b in bindings:
                g = lambda k: b.get(k, {}).get("value")
                qid    = (g("person") or "").split("/")[-1]
                person = g("personLabel") or ""
                start  = g("start") or ""
                end    = g("end") or ""
                party  = _only_siglas(g("party") or "")
                cause  = g("causeEndLabel") or ""

                key = (qid, start, end)
                if key in seen:
                    continue
                seen.add(key)

                rows.append((role, person, qid, start, end, party, cause))

            # — escreve histórico + acumula
            for role_, person, qid, start, end, party, cause in rows:
                hcsv.writerow([iso3, country, role_, person, qid, start, end, party, cause])
                fh.flush(); os.fsync(fh.fileno())
                total_hist += 1
                all_rows.append((role_, person, qid, start, end, party, cause))

        # — escolher "atuais" por função (sem fim; em empate, início mais recente; senão, maior início)
        for role in ("head_of_government","head_of_state"):
            subset = [t for t in all_rows if t[0] == role]
            if not subset:
                continue

            # preferir “sem fim”
            open_terms = [t for t in subset if not t[4]]  # end vazio
            if open_terms:
                current = max(open_terms, key=lambda t: (t[3] or ""))  # start ISO ordena lexicograficamente
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
