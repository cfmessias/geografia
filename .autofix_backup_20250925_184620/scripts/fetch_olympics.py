# scripts/fetch_olympics.py
# -*- coding: utf-8 -*-
"""
Extrai medalhas olímpicas por edição (ano x verão/inverno) e agrega por país.
Robusto em anos antigos (1896–1936) e recentes (2020, 2022, 2024):
- Vai à secção 'Medal table' / 'Quadro de medalhas' via MediaWiki API (EN/PT).
- Fallback: varre a página completa e escolhe a 'wikitable' correta.
Saídas:
  data/olympics_medals_by_event.csv
  data/olympics_medals.csv
"""

from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import re, time, random, datetime as dt, html as ihtml
import urllib.parse

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ------------------------- Config -------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUT_EVENTS   = PROJECT_ROOT / "data" / "olympics_medals_by_event.csv"
OUT_TOTALS   = PROJECT_ROOT / "data" / "olympics_medals.csv"

UA      = "GeoOlympics/9.0 (+contact: you@example.com)"
TIMEOUT = 30
VERBOSE = True

S = requests.Session()
S.headers.update({
    "User-Agent": UA,
    "Accept-Language": "en,pt;q=0.8",
})

# urls base
EN_LIST = "https://en.wikipedia.org/wiki/List_of_Olympic_Games"
API_EN  = "https://en.wikipedia.org/w/api.php"
API_PT  = "https://pt.wikipedia.org/w/api.php"

URL_CANDIDATES = {
    "summer": [
        # 1) página dedicada (se existir)
        "https://en.wikipedia.org/wiki/{year}_Summer_Olympics_medal_table",
        # 2) página principal EN
        "https://en.wikipedia.org/wiki/{year}_Summer_Olympics",
        # 3) fallbacks PT: quadro e página principal
        "https://pt.wikipedia.org/wiki/Quadro_de_medalhas_dos_Jogos_Ol%C3%ADmpicos_de_Ver%C3%A3o_de_{year}",
        "https://pt.wikipedia.org/wiki/Jogos_Ol%C3%ADmpicos_de_Ver%C3%A3o_de_{year}",
    ],
    "winter": [
        "https://en.wikipedia.org/wiki/{year}_Winter_Olympics_medal_table",
        "https://en.wikipedia.org/wiki/{year}_Winter_Olympics",
        "https://pt.wikipedia.org/wiki/Quadro_de_medalhas_dos_Jogos_Ol%C3%ADmpicos_de_Inverno_de_{year}",
        "https://pt.wikipedia.org/wiki/Jogos_Ol%C3%ADmpicos_de_Inverno_de_{year}",
    ],
}

# Ignorar equipas históricas/mistas
SKIP_NOC = {
    "EUA","EUN","IOA","OAR","ROC","URS","GDR","FRG","YUG","ANZ","SCG","BOH","RHO",
    "TCH","WIF","UAR","ZZZ"
}
# Overrides NOC->ISO3
NOC_TO_ISO3_FIX = {
    "GER":"DEU","SUI":"CHE","NED":"NLD","TPE":"TWN","KOR":"KOR","PRK":"PRK",
    "HKG":"HKG","MAC":"MAC","CIV":"CIV","ROU":"ROU","UAE":"ARE","GBR":"GBR","UK":"GBR",
}

# ------------------------- HTTP helpers -------------------------
def _get(url: str) -> str:
    for attempt in range(4):
        try:
            r = S.get(url, timeout=TIMEOUT)
            if r.status_code in (429, 403):
                time.sleep(1.0 + attempt)
                continue
            r.raise_for_status()
            return r.text
        except Exception:
            time.sleep(0.6 + random.random()*0.8)
    raise RuntimeError(f"GET falhou: {url}")

def _api_call(api: str, **params) -> dict:
    # MediaWiki API GET
    q = params.copy()
    q.setdefault("format", "json")
    q.setdefault("formatversion", "2")
    for attempt in range(4):
        try:
            r = S.get(api, params=q, timeout=TIMEOUT)
            if r.status_code in (429, 403):
                time.sleep(1.0 + attempt)
                continue
            r.raise_for_status()
            return r.json()
        except Exception:
            time.sleep(0.6 + random.random()*0.8)
    return {}

# ------------------------- Utils -------------------------
def clean_text(x: str) -> str:
    t = re.sub(r"\[[^\]]*\]", "", str(x))  # remove footnotes [1]
    t = re.sub(r"\s+", " ", t).strip()
    return t

def to_int(cell: str) -> int:
    s = clean_text(cell)
    s = re.sub(r"[^\d\-]", "", s)
    if s == "" or s == "-": return 0
    try:
        return int(s)
    except Exception:
        return 0

def extract_noc(text: str) -> Optional[str]:
    t = clean_text(text).upper()
    m = re.match(r"^\s*([A-Z]{3})\b", t)
    if m: return m.group(1)
    m = re.search(r"\(([A-Z]{3})\)\s*$", t)
    if m: return m.group(1)
    if re.fullmatch(r"[A-Z]{3}", t): return t
    return None

# ------------------------- Table parsing via BS4 -------------------------
# Regex p/ cabeçalhos
RX_COUNTRY = re.compile(r"(pa[ií]s|country|nation|noc|team|delegat|committee)", re.I)
RX_GOLD    = re.compile(r"(gold|ouro)", re.I)
RX_SILVER  = re.compile(r"(silver|prata)", re.I)
RX_BRONZE  = re.compile(r"(bronze)", re.I)
RX_TOTAL   = re.compile(r"(total|medals?)", re.I)

def _header_text(th) -> str:
    # tenta <img alt>, <abbr title> e text
    img = th.find("img")
    if img and img.get("alt"): return img["alt"]
    abbr = th.find("abbr")
    if abbr and abbr.get("title"): return abbr["title"]
    return clean_text(th.get_text(" "))

def parse_wikitable(tbl) -> Optional[pd.DataFrame]:
    """
    Faz parse de uma <table> wiki, devolvendo DataFrame com
    ['NOC','country_raw','Gold','Silver','Bronze','Total'] se possível.
    """
    # caption “Medal table” ajuda a priorizar, mas não é obrigatório
    # Cabeçalhos:
    thead = tbl.find("thead")
    headers: List[str] = []
    if thead:
        ths = thead.find_all("th")
        headers = [_header_text(th) for th in ths]

    if not headers:
        first_tr = tbl.find("tr")
        if first_tr:
            headers = [_header_text(th) for th in first_tr.find_all(["th","td"])]

    if len(headers) < 3:
        return None

    # mapear colunas (primeira ocorrência)
    def find_idx(rx: re.Pattern) -> Optional[int]:
        for i, h in enumerate(headers):
            if rx.search(h): return i
        return None

    name_idx  = find_idx(RX_COUNTRY)
    gold_idx  = find_idx(RX_GOLD)
    silver_idx= find_idx(RX_SILVER)
    bronze_idx= find_idx(RX_BRONZE)
    total_idx = find_idx(RX_TOTAL)
    medal_hits = sum(x is not None for x in (gold_idx, silver_idx, bronze_idx, total_idx))

    if name_idx is None or medal_hits < 3:
        # heurística: se segunda coluna parecer país
        if name_idx is None and len(headers) >= 2:
            name_idx = 1

    if name_idx is None or medal_hits < 3:
        return None

    body = tbl.find("tbody") or tbl
    rows = []
    for tr in body.find_all("tr"):
        cells = tr.find_all(["td","th"])
        if len(cells) < 3: 
            continue
        vals = [clean_text(c.get_text(" ")) for c in cells]
        # alinhar ao nº de headers (heurística básica para colspan/rowspan comuns)
        if len(vals) < len(headers):
            vals += [""]*(len(headers)-len(vals))

        name = vals[name_idx] if name_idx < len(vals) else ""
        if not name:
            continue
        # descartar linhas-resumo/cabeçalho repetido
        lower = name.lower()
        if lower in ("rank","ordem","posição","total","totais","overall","grand total"):
            continue

        gold   = to_int(vals[gold_idx])   if gold_idx   is not None and gold_idx   < len(vals) else 0
        silver = to_int(vals[silver_idx]) if silver_idx is not None and silver_idx < len(vals) else 0
        bronze = to_int(vals[bronze_idx]) if bronze_idx is not None and bronze_idx < len(vals) else 0
        total  = to_int(vals[total_idx])  if total_idx  is not None and total_idx  < len(vals) else (gold+silver+bronze)

        noc = extract_noc(name)
        # se houver coluna “NOC” explícita noutro sítio, usa-a
        if noc is None:
            # tentar colunas com 'NOC'
            for i, h in enumerate(headers):
                if re.search(r"\bNOC\b", h, re.I):
                    cval = clean_text(vals[i]) if i < len(vals) else ""
                    if re.fullmatch(r"[A-Z]{3}", cval.strip().upper()):
                        noc = cval.strip().upper()
                        break

        if noc:
            rows.append({
                "NOC": noc,
                "country_raw": name,
                "Gold": gold, "Silver": silver, "Bronze": bronze, "Total": total
            })

    if not rows:
        return None

    df = pd.DataFrame(rows)
    df["NOC"] = df["NOC"].astype(str).str.upper().str.strip()
    df = df[~df["NOC"].isin(SKIP_NOC)]
    if df.empty:
        return None
    # garantir ints
    for c in ("Gold","Silver","Bronze","Total"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    return df[["NOC","country_raw","Gold","Silver","Bronze","Total"]]

def find_medal_table_in_html(html: str) -> Optional[pd.DataFrame]:
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table", class_=lambda c: c and "wikitable" in c)
    best, best_rows, best_bonus = None, 0, 0
    for tbl in tables:
        df = parse_wikitable(tbl)
        if df is None or df.empty:
            continue
        # bónus se a caption mencionar medalhas
        caption = tbl.find("caption")
        bonus = 1 if (caption and re.search(r"(medal|medalhas)", caption.get_text(" "), re.I)) else 0
        rows = len(df) + bonus*1000
        if rows > (best_rows + best_bonus*1000):
            best, best_rows, best_bonus = df, len(df), bonus
    return best

# ------------------------- MediaWiki API section finder -------------------------
SEC_EN = re.compile(r"\bMedal table\b", re.I)
SEC_PT = re.compile(r"\bQuadro de medalhas\b", re.I)

def _page_title_from_url(url: str) -> Optional[Tuple[str,str]]:
    """
    Devolve (lang, title) para URLs /wiki/..., ou None se não for Wikipedia.
    """
    try:
        u = urllib.parse.urlparse(url)
        if u.netloc.endswith("wikipedia.org") and u.path.startswith("/wiki/"):
            lang = u.netloc.split(".")[0]  # en, pt, etc.
            title = urllib.parse.unquote(u.path.split("/wiki/")[1])
            return lang, title
    except Exception:
        pass
    return None

def get_section_html_via_api(url: str) -> Optional[str]:
    """
    Usa a API para obter o HTML da secção 'Medal table'/'Quadro de medalhas' da página.
    """
    parsed = _page_title_from_url(url)
    if not parsed:
        return None
    lang, title = parsed
    api = API_EN if lang == "en" else API_PT if lang == "pt" else None
    if api is None:
        return None

    # 1) listar secções
    js = _api_call(api, action="parse", page=title, prop="sections")
    sections = js.get("parse", {}).get("sections", []) if js else []
    if not sections:
        return None

    rx = SEC_EN if lang == "en" else SEC_PT
    sec_id = None
    for sec in sections:
        line = sec.get("line") or ""
        if rx.search(line):
            sec_id = sec.get("index")
            break
    if sec_id is None:
        return None

    # 2) obter HTML dessa secção
    js2 = _api_call(api, action="parse", page=title, prop="text", section=sec_id)
    html = js2.get("parse", {}).get("text", "")
    if not html:
        return None
    return html

# ------------------------- Edições -------------------------
def list_editions() -> List[Tuple[int,str]]:
    """
    Extrai anos/estação a partir de links EN. Fallback canónico se pouco vier.
    """
    now_year = dt.datetime.now(dt.timezone.utc).year
    editions: set[Tuple[int,str]] = set()
    try:
        html = _get(EN_LIST)
        # /wiki/1908_Summer_Olympics  |  /wiki/1924_Winter_Olympics
        for m in re.finditer(r'/wiki/(\d{4})_(Summer|Winter)_Olympics(?!_Youth)', html):
            y  = int(m.group(1))
            ss = m.group(2).lower()
            if y in (1916, 1940, 1944):
                continue
            if y <= now_year:
                editions.add((y, ss))
    except Exception:
        editions = set()

    if len(editions) < 40:
        summer = [1896,1900,1904,1908,1912,1920,1924,1928,1932,1936,1948,1952,1956,
                  1960,1964,1968,1972,1976,1980,1984,1988,1992,1996,2000,2004,2008,
                  2012,2016,2020,2024]
        winter = [1924,1928,1932,1936,1948,1952,1956,1960,1964,1968,1972,1976,1980,
                  1984,1988,1992,1994,1998,2002,2006,2010,2014,2018,2022]
        editions = {(y,"summer") for y in summer} | {(y,"winter") for y in winter}
    return sorted(editions)

# ------------------------- NOC -> ISO3 -------------------------
def noc_map_with_pt_labels() -> Dict[str, Tuple[str, Optional[str]]]:
    q = """
    SELECT ?noc ?iso3 ?countryLabel WHERE {
      ?c wdt:P984 ?noc ; wdt:P298 ?iso3 .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "pt". ?c rdfs:label ?countryLabel }
    }
    """
    hdr = {
        "User-Agent": UA,
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/sparql-query",
    }
    for _ in range(4):
        try:
            r = requests.post("https://query.wikidata.org/sparql", data=q.encode("utf-8"), headers=hdr, timeout=20)
            if r.status_code == 400:
                r = requests.post("https://query.wikidata.org/sparql",
                                  data={"query": q, "format": "json"},
                                  headers={"User-Agent": UA, "Accept":"application/sparql-results+json"},
                                  timeout=20)
            r.raise_for_status()
            js = r.json()
            out: Dict[str, Tuple[str, Optional[str]]] = {}
            for b in js.get("results",{}).get("bindings",[]):
                noc = b.get("noc",{}).get("value","").upper()
                iso = b.get("iso3",{}).get("value","").upper()
                lab = b.get("countryLabel",{}).get("value")
                if noc and iso:
                    out[noc] = (iso, lab)
            # overrides e cortes
            for k,v in NOC_TO_ISO3_FIX.items():
                out[k] = (v, out.get(k,(None,None))[1])
            for k in SKIP_NOC:
                out.pop(k, None)
            return out
        except Exception:
            time.sleep(0.6 + random.random()*0.8)
    return {}

# ------------------------- Fetch por edição -------------------------
def fetch_medal_table_for(year: int, season: str) -> Optional[pd.DataFrame]:
    tried = []
    for tmpl in URL_CANDIDATES[season]:
        url = tmpl.format(year=year)
        tried.append(url)
        # 1) tentar via API a secção (se for Wikipedia)
        sec_html = get_section_html_via_api(url)
        if sec_html:
            df = find_medal_table_in_html(sec_html)
            if df is not None and not df.empty:
                if VERBOSE: print(f"[{season.title()}] {year}  ✓ (API sec) {url}")
                return df
        # 2) fallback: página inteira
        try:
            html = _get(url)
        except Exception:
            continue
        df2 = find_medal_table_in_html(html)
        if df2 is not None and not df2.empty:
            if VERBOSE: print(f"[{season.title()}] {year}  ✓ {url}")
            return df2
    if VERBOSE:
        print(f"[{season.title()}] {year}  … medal table não encontrada — skip")
    return None

# ------------------------- Pipeline -------------------------
def main() -> None:
    OUT_EVENTS.parent.mkdir(parents=True, exist_ok=True)
    OUT_TOTALS.parent.mkdir(parents=True, exist_ok=True)

    editions = list_editions()
    if not editions:
        print("❌ Não consegui obter edições.")
        return

    noc_map = noc_map_with_pt_labels()
    if not noc_map:
        print("❌ Falha no mapa NOC→ISO3.")
        return

    rows: List[dict] = []
    for (year, season) in editions:
        if year in (1916, 1940, 1944):
            continue
        df = fetch_medal_table_for(year, season)
        if df is None:
            continue

        for _, r in df.iterrows():
            noc = str(r["NOC"]).upper()
            if noc not in noc_map:
                continue
            iso3, pt_label = noc_map[noc]
            rows.append({
                "year": year, "season": season, "noc": noc, "iso3": iso3,
                "country_pt": pt_label or "",
                "gold": int(r["Gold"]), "silver": int(r["Silver"]),
                "bronze": int(r["Bronze"]), "total": int(r["Total"]),
            })
        time.sleep(0.25 + random.random()*0.5)

    if not rows:
        print("❌ Não foi possível recolher medal tables.")
        return

    events = pd.DataFrame(rows).sort_values(["year","season","iso3"]).reset_index(drop=True)
    events.to_csv(OUT_EVENTS, index=False, encoding="utf-8")
    print(f"✔️ Atualizado {OUT_EVENTS} ({len(events)} linhas)")

    # Agregados
    def agg(season: str) -> pd.DataFrame:
        part = events[events["season"]==season]
        if part.empty:
            return pd.DataFrame(columns=["iso3","country_pt",f"{season}_gold",f"{season}_silver",f"{season}_bronze",f"{season}_total"])
        g = (part.groupby(["iso3","country_pt"], as_index=False)[["gold","silver","bronze","total"]].sum()
             .rename(columns={"gold":f"{season}_gold","silver":f"{season}_silver","bronze":f"{season}_bronze","total":f"{season}_total"}))
        return g

    summer = agg("summer")
    winter = agg("winter")
    totals = pd.merge(summer, winter, on=["iso3","country_pt"], how="outer").fillna(0)

    for c in ("summer_gold","summer_silver","summer_bronze","summer_total",
              "winter_gold","winter_silver","winter_bronze","winter_total"):
        totals[c] = pd.to_numeric(totals[c], errors="coerce").fillna(0).astype(int)

    totals["total_gold"]   = totals["summer_gold"]  + totals["winter_gold"]
    totals["total_silver"] = totals["summer_silver"]+ totals["winter_silver"]
    totals["total_bronze"] = totals["summer_bronze"]+ totals["winter_bronze"]
    totals["total_total"]  = totals["summer_total"] + totals["winter_total"]

    totals = totals.sort_values("total_total", ascending=False).reset_index(drop=True)
    totals.to_csv(OUT_TOTALS, index=False, encoding="utf-8")
    print(f"✔️ Atualizado {OUT_TOTALS} ({len(totals)} países)")

    # debug rápido
    for iso in ("PRT","BRA","ESP","FRA","USA","ITA"):
        r = totals[totals["iso3"]==iso]
        if not r.empty:
            d = r.iloc[0].to_dict()
            print(f"{iso} → verão={d.get('summer_total',0)} inverno={d.get('winter_total',0)} total={d.get('total_total',0)}")

if __name__ == "__main__":
    main()
