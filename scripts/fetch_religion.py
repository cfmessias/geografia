# scripts/fetch_religion.py
# -*- coding: utf-8 -*-
"""
Extrai composição religiosa por país e grava em data/religion.csv (ISO3 + %).
- Fonte principal: tabela Pew (2012) "Religious composition by country"
- Robustez: deteta colunas por regex e agrega denominações para famílias
- Fallback: se existir data/religion_manual.csv, usa esse ficheiro

Saída (data/religion.csv):
  iso3, country, christian, muslim, unaffiliated, hindu, buddhist,
  folk_religions, other_religions, jewish, source_year
"""

from __future__ import annotations
from pathlib import Path
from typing import List, Dict, Optional
import io, re, time, random, sys, unicodedata

import requests
import pandas as pd
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = PROJECT_ROOT / "data"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH = OUT_DIR / "religion.csv"

UA = "GeoProject/2.0 (+contact: you@example.com)"
S = requests.Session()
S.headers.update({"User-Agent": UA})

PEW_URL = "https://www.pewresearch.org/religion/2012/12/18/table-religious-composition-by-country-in-percentages/"
SOURCE_YEAR = 2010  # Pew 2012 reporta composição em ~2010

# Correções de nomes para pycountry
NAME_FIX = {
    "Congo (Brazzaville)": "Congo",
    "Republic of the Congo": "Congo",
    "Congo (Kinshasa)": "Congo, The Democratic Republic of the",
    "Democratic Republic of Congo": "Congo, The Democratic Republic of the",
    "Cote d’Ivoire": "Côte d'Ivoire",
    "Côte d’Ivoire": "Côte d'Ivoire",
    "Ivory Coast": "Côte d'Ivoire",
    "Czech Republic": "Czechia",
    "Eswatini (Swaziland)": "Eswatini",
    "Burma (Myanmar)": "Myanmar",
    "Kyrgyz Republic": "Kyrgyzstan",
    "Macau": "Macao",
    "Micronesia": "Micronesia (Federated States of)",
    "Moldova": "Moldova, Republic of",
    "North Macedonia": "Macedonia, Republic of",
    "Russia": "Russian Federation",
    "Syria": "Syrian Arab Republic",
    "Vietnam": "Viet Nam",
    "Laos": "Lao People's Democratic Republic",
    "Cape Verde": "Cabo Verde",
    "Bahamas": "Bahamas, The",
    "Gambia": "Gambia, The",
    "Hong Kong": "Hong Kong, SAR China",
    "Palestinian territories": "Palestine, State of",
    "United States": "United States of America",
    "United Kingdom": "United Kingdom of Great Britain and Northern Ireland",
}

def _norm(s: str) -> str:
    """Normaliza texto de cabeçalhos para matching por regex."""
    t = str(s)
    t = unicodedata.normalize("NFKD", t)
    t = "".join(ch for ch in t if not unicodedata.combining(ch))
    t = t.lower().strip()
    t = re.sub(r"[\s\-/]+", " ", t)
    t = re.sub(r"[^\w %]", "", t)
    return t

def _pct_to_float(x) -> float:
    s = str(x).strip()
    if s == "" or s in {"—", "-", "…", "na", "n/a"}:
        return 0.0
    s = s.replace("\u2009", "")  # thin space
    s = s.replace(",", ".")
    if re.match(r"^<\s*0\.?1%?$", s):
        return 0.05
    s = s.replace("%", "")
    try:
        return float(s)
    except Exception:
        # às vezes vêm valores já numéricos
        try:
            return float(re.sub(r"[^\d\.]", "", s) or "0")
        except Exception:
            return 0.0

def _get_html(url: str) -> str:
    for attempt in range(5):
        try:
            r = S.get(url, timeout=40)
            r.raise_for_status()
            return r.text
        except Exception:
            time.sleep(0.8 + random.random())
    raise RuntimeError(f"Falhou GET {url}")

def _iso3_from_name(name: str) -> Optional[str]:
    nm = name.strip()
    nm = NAME_FIX.get(nm, nm)
    try:
        import pycountry
    except Exception:
        print("❌ Falta pycountry. Instala: pip install pycountry", file=sys.stderr)
        sys.exit(1)
    # lookup direto
    try:
        c = pycountry.countries.lookup(nm)
        return c.alpha_3
    except Exception:
        pass
    # remover parênteses
    alt = re.sub(r"\s*\(.*?\)\s*", "", nm).strip()
    if alt != nm:
        try:
            c = pycountry.countries.lookup(alt)
            return c.alpha_3
        except Exception:
            pass
    return None

# Padrões por família (qualquer coluna que dê match entra na soma)
RX = {
    "christian": [
        r"\bchristian", r"\bcatholic", r"\broman catholic", r"\bprotestant", r"\banglican",
        r"\borthodox", r"\bother christian", r"\bother protestant", r"\bchristianity"
    ],
    "muslim": [
        r"\bmuslim", r"\bislam", r"\bsunni", r"\bshi[ai]", r"\bshia", r"\bshii", r"\bibadi", r"\bother muslim"
    ],
    "buddhist": [r"\bbuddh"],
    "hindu": [r"\bhindu"],
    "jewish": [r"\bjew"],
    "folk_religions": [r"\bfolk\b", r"\btraditional\b", r"\bethnic"],
    "unaffiliated": [r"\bunaffiliated", r"\bno relig", r"\bnone", r"\bsecular"],
    "other_religions": [r"\bother relig", r"\bother$", r"\bothers$"],
}

def _score_columns(columns: List[str]) -> Dict[str, List[str]]:
    """
    Atribui colunas (normalizadas) às famílias por regex; devolve mapping:
      família -> [nomes originais que pertencem a essa família]
    """
    mapped: Dict[str, List[str]] = {k: [] for k in RX.keys()}
    norm_map = {c: _norm(c) for c in columns}
    for orig, n in norm_map.items():
        for fam, patterns in RX.items():
            for pat in patterns:
                if re.search(pat, n):
                    mapped[fam].append(orig)
                    break
    return mapped

def _best_table_from_html(html: str) -> pd.DataFrame:
    """
    Escolhe a melhor tabela: a que tiver mais colunas mapeáveis a famílias.
    """
    # tentar via pandas primeiro (rápido)
    try:
        tables = pd.read_html(io.StringIO(html))
    except Exception:
        tables = []

    # também coletar tabelas BS4 'table'
    soup = BeautifulSoup(html, "html.parser")
    for tbl in soup.find_all("table"):
        try:
            df = pd.read_html(io.StringIO(str(tbl)))[0]
            tables.append(df)
        except Exception:
            continue

    best, best_score = None, -1
    for df in tables:
        if df.empty:
            continue
        # normalizar cabeçalhos longos
        df = df.copy()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [" ".join([str(x) for x in tup if str(x) != "nan"]).strip()
                          for tup in df.columns.values]
        cols = [str(c) for c in df.columns]
        fam_map = _score_columns(cols)
        hits = sum(len(v) > 0 for v in fam_map.values())
        # preferir tabelas que também tenham coluna "country"
        bonus = 2 if any(_norm(c).startswith("country") or _norm(c).startswith("pais") for c in cols) else 0
        score = hits * 10 + bonus
        if score > best_score:
            best, best_score = df, score

    if best is None:
        raise RuntimeError("Nenhuma tabela adequada encontrada na página da Pew.")

    return best

def _aggregate_table(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()
    # normalizar cabeçalhos
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [" ".join([str(x) for x in tup if str(x) != "nan"]).strip()
                      for tup in df.columns.values]
    # descobrir coluna país
    country_col = None
    for c in df.columns:
        nc = _norm(c)
        if nc.startswith("country") or nc.startswith("pais"):
            country_col = c
            break
    if country_col is None:
        country_col = df.columns[0]

    # mapear famílias
    fam_map = _score_columns([str(c) for c in df.columns])

    # converter tudo o que parece percentagem para float
    numeric_df = {}
    for c in df.columns:
        if c == country_col:
            continue
        numeric_df[c] = df[c].map(_pct_to_float)
    num = pd.DataFrame(numeric_df)

    # somas por família
    out = pd.DataFrame({"country": df[country_col].astype(str).str.strip()})
    for fam in RX.keys():
        cols = fam_map.get(fam, [])
        if cols:
            out[fam] = num[cols].sum(axis=1, skipna=True).fillna(0.0)
        else:
            out[fam] = 0.0

    # ISO3
    out["iso3"] = out["country"].apply(_iso3_from_name)
    out = out[out["iso3"].notna()].copy()

    # ano de referência
    out["source_year"] = SOURCE_YEAR

    # ordenar colunas
    ordered = ["iso3","country","christian","muslim","unaffiliated","hindu",
               "buddhist","folk_religions","other_religions","jewish","source_year"]
    for c in ordered:
        if c not in out.columns:
            out[c] = 0.0 if c not in ("iso3","country","source_year") else ("" if c in ("iso3","country") else SOURCE_YEAR)
    out = out[ordered]

    # limpar outliers (>100 devido a somas duplicadas)
    for c in ["christian","muslim","unaffiliated","hindu","buddhist","folk_religions","other_religions","jewish"]:
        out[c] = out[c].clip(lower=0, upper=100)

    return out.sort_values("country").reset_index(drop=True)

def main() -> None:
    manual = OUT_DIR / "religion_manual.csv"
    if manual.exists():
        print(f"ℹ️ A usar ficheiro manual: {manual}")
        df = pd.read_csv(manual)
        # garantir colunas e escrever normalizado
        needed = {"iso3","country","christian","muslim","unaffiliated","hindu",
                  "buddhist","folk_religions","other_religions","jewish"}
        missing = needed - set(map(str.lower, df.columns))
        if missing:
            print(f"❌ Faltam colunas em religion_manual.csv: {missing}", file=sys.stderr)
            sys.exit(2)
        df["source_year"] = df.get("source_year", SOURCE_YEAR)
        df = df[[ "iso3","country","christian","muslim","unaffiliated","hindu",
                  "buddhist","folk_religions","other_religions","jewish","source_year"]]
        df.to_csv(OUT_PATH, index=False, encoding="utf-8")
        print(f"✔️ Escrevi {OUT_PATH} ({len(df)} países, fonte=manual)")
        return

    print("[PEW] a obter página…")
    html = _get_html(PEW_URL)
    print("[PEW] a detectar tabela…")
    table = _best_table_from_html(html)
    print(f"[PEW] tabela encontrada com {table.shape[0]} linhas e {table.shape[1]} colunas")
    out = _aggregate_table(table)
    print(f"[PEW] mapeados {len(out)} países com ISO3")

    out.to_csv(OUT_PATH, index=False, encoding="utf-8")
    print(f"✔️ Escrevi {OUT_PATH} ({len(out)} países)")

if __name__ == "__main__":
    main()
