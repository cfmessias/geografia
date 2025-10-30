# scripts/fetch_coastlines.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import csv, time, re, unicodedata
import requests

IN_PROFILES = Path("data/countries_profiles.csv")
OUT_CSV     = Path("data/coastlines.csv")

UA = "GeografiaApp/1.0 (cfmessias.pt)"
WDQS = "https://query.wikidata.org/sparql"

FACTBOOK_BASES = [
    # mirrors conhecidos; mantemos vários para aumentar a taxa de acerto
    "https://raw.githubusercontent.com/ianozsval/factbook.json/master/factbook",
    "https://raw.githubusercontent.com/oatmealine/factbook.json/master/factbook",
    "https://raw.githubusercontent.com/factbook/factbook.json/master/factbook",  # pode 404
]

def _norm(s: str) -> str:
    s = s.strip()
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")

def _req_json(url: str, timeout=25):
    r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
    if r.status_code != 200:
        return None
    try:
        return r.json()
    except Exception:
        return None

def fetch_labels_and_enwiki(qids: list[str]) -> dict[str, dict]:
    """
    Para cada QID devolve:
      { qid: {"pt": <label_pt>, "en": <label_en>, "enwiki": <titulo_enwiki> } }
    """
    out = {}
    CHUNK = 20
    for i in range(0, len(qids), CHUNK):
        part = qids[i:i+CHUNK]
        values = " ".join(f"wd:{q}" for q in part)
        query = f"""
        SELECT ?c ?pt ?en ?enwiki WHERE {{
          VALUES ?c {{ {values} }}
          OPTIONAL {{ ?c rdfs:label ?pt . FILTER(LANG(?pt) = "pt") }}
          OPTIONAL {{ ?c rdfs:label ?en . FILTER(LANG(?en) = "en") }}
          OPTIONAL {{
            ?enwiki_sitelink schema:about ?c ;
                              schema:isPartOf <https://en.wikipedia.org/> ;
                              schema:name ?enwiki .
          }}
        }}
        """
        try:
            r = requests.get(WDQS, params={"query": query, "format": "json"},
                             headers={"User-Agent": UA}, timeout=60)
            if r.status_code == 429:
                print("[warn] WDQS rate-limit — a aguardar 20s…")
                time.sleep(20)
                r = requests.get(WDQS, params={"query": query, "format": "json"},
                                 headers={"User-Agent": UA}, timeout=60)
            r.raise_for_status()
            for b in r.json().get("results", {}).get("bindings", []):
                qid = b["c"]["value"].split("/")[-1]
                pt = b.get("pt", {}).get("value", "")
                en = b.get("en", {}).get("value", "") or pt
                enwiki = b.get("enwiki", {}).get("value", "")
                out[qid] = {"pt": pt, "en": en, "enwiki": enwiki}
        except Exception as e:
            print(f"[warn] labels batch falhou ({i//CHUNK+1}): {e}")
            time.sleep(10)
    return out

def fetch_adjacent_waters(qids: list[str]) -> dict[str, dict]:
    """
    Recolhe massas de água adjacentes (mar/oceano/estreito/golfo/baía/canal)
    Devolve { qid: {"pt": "…", "en": "…"} } com listas separadas por ", "
    """
    out = {}
    CHUNK = 40
    classes = " ".join(["wd:Q165",  # ocean
                        "wd:Q9430",  # sea
                        "wd:Q2592810", # strait
                        "wd:Q1496967", # gulf
                        "wd:Q39594",   # bay
                        "wd:Q12284",   # channel
                        ])
    for i in range(0, len(qids), CHUNK):
        part = qids[i:i+CHUNK]
        values = " ".join(f"wd:{q}" for q in part)
        query = f"""
        SELECT ?c
               (GROUP_CONCAT(DISTINCT ?w_pt; separator=", ") AS ?waters_pt)
               (GROUP_CONCAT(DISTINCT ?w_en; separator=", ") AS ?waters_en)
        WHERE {{
          VALUES ?c {{ {values} }}
          OPTIONAL {{
            ?c wdt:P206 ?w .
            ?w wdt:P31/wdt:P279* ?cls .
            VALUES ?cls {{ {classes} }}
            OPTIONAL {{ ?w rdfs:label ?w_pt FILTER(LANG(?w_pt)="pt") }}
            OPTIONAL {{ ?w rdfs:label ?w_en FILTER(LANG(?w_en)="en") }}
          }}
        }}
        GROUP BY ?c
        """
        try:
            r = requests.get(WDQS, params={"query": query, "format": "json"},
                             headers={"User-Agent": UA}, timeout=60)
            if r.status_code == 429:
                print("[warn] WDQS rate-limit (waters) — pausa 20s…")
                time.sleep(20); r = requests.get(WDQS, params={"query": query, "format": "json"},
                                                 headers={"User-Agent": UA}, timeout=60)
            r.raise_for_status()
            for b in r.json().get("results", {}).get("bindings", []):
                qid = b["c"]["value"].split("/")[-1]
                wpt = b.get("waters_pt", {}).get("value", "")
                wen = b.get("waters_en", {}).get("value", "")
                out[qid] = {"pt": wpt, "en": wen}
        except Exception as e:
            print(f"[warn] waters batch falhou ({i//CHUNK+1}): {e}")
            time.sleep(10)
    return out

def fetch_has_coast_wikidata(qids: list[str]) -> dict[str, bool]:
    """
    Determina se o país tem costa via existência de P206 para massas de água marinhas.
    """
    out = {q: False for q in qids}
    CHUNK = 50
    classes = " ".join(["wd:Q165","wd:Q9430","wd:Q2592810","wd:Q1496967","wd:Q39594","wd:Q12284"])
    for i in range(0, len(qids), CHUNK):
        part = qids[i:i+CHUNK]
        values = " ".join(f"wd:{q}" for q in part)
        query = f"""
        SELECT DISTINCT ?c WHERE {{
          VALUES ?c {{ {values} }}
          ?c wdt:P206 ?w .
          ?w wdt:P31/wdt:P279* ?cls .
          VALUES ?cls {{ {classes} }}
        }}
        """
        try:
            r = requests.get(WDQS, params={"query": query, "format": "json"},
                             headers={"User-Agent": UA}, timeout=60)
            if r.status_code == 429:
                time.sleep(20); r = requests.get(WDQS, params={"query": query, "format": "json"},
                                                 headers={"User-Agent": UA}, timeout=60)
            r.raise_for_status()
            for b in r.json().get("results", {}).get("bindings", []):
                qid = b["c"]["value"].split("/")[-1]
                out[qid] = True
        except Exception as e:
            print(f"[warn] has_coast batch falhou ({i//CHUNK+1}): {e}")
            time.sleep(10)
    return out

def coastline_from_factbook(pt: str, en: str, enwiki: str, iso3: str) -> float | None:
    """
    Tenta extrair 'Coastline: #### km' de mirrors do Factbook.
    Gera vários candidatos de nome de ficheiro. Devolve km (float) ou None.
    """
    names = []
    # preferências: enwiki title; EN; PT; variações normalizadas; iso3
    for base in [enwiki, en, pt]:
        if base:
            s = base
            names += [
                s,
                s.replace(" ", "_"),
                s.replace(" ", "-"),
                _norm(s),
                _norm(s).replace(" ", "_"),
                _norm(s).replace(" ", "-"),
            ]
    if iso3:
        names += [iso3.upper(), iso3.lower()]
    # remover duplicados mantendo ordem
    seen = set(); cand = []
    for n in names:
        n2 = n.strip()
        if n2 and n2 not in seen:
            seen.add(n2); cand.append(n2)

    # tentar em todos os mirrors + candidatos
    for base_url in FACTBOOK_BASES:
        for name in cand:
            url = f"{base_url}/{name}.json"
            data = _req_json(url)
            if not data:
                continue
            # chaves variam conforme o mirror
            geo = data.get("Geography") or data.get("geography") or {}
            cstr = geo.get("Coastline") or geo.get("coastline")
            if not cstr:
                # alguns mirrors aninham em "geography": {"Coastline": {"text": "### km"}}
                if isinstance(geo.get("Coastline"), dict):
                    cstr = geo["Coastline"].get("text")
                elif isinstance(geo.get("coastline"), dict):
                    cstr = geo["coastline"].get("text")
            if not cstr:
                continue
            # extrair número (permite "1,793 km", "1 793 km", "1793 km")
            m = re.search(r"([\d][\d\s,\.]*)\s*km\b", cstr, flags=re.I)
            if not m:
                continue
            num = m.group(1).replace(" ", "").replace(",", "")
            try:
                return float(num)
            except Exception:
                pass
    return None

def main():
    print("[fetch_coastlines] A recolher dados em modo batch…")
    if not IN_PROFILES.exists():
        print(f"[erro] {IN_PROFILES} não existe.")
        return

    rows = list(csv.DictReader(open(IN_PROFILES, encoding="utf-8"), delimiter=";"))
    qids = [r["qid"] for r in rows if r.get("qid")]
    idx = {r["qid"]: r for r in rows}

    # 1) Labels PT/EN + título enwiki
    meta = fetch_labels_and_enwiki(qids)

    # 2) Águas adjacentes (PT/EN)
    waters = fetch_adjacent_waters(qids)

    # 3) Tem costa (via Wikidata P206)
    has_coast_map = fetch_has_coast_wikidata(qids)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8", newline="") as fo:
        w = csv.writer(fo, delimiter=";")
        w.writerow([
            "country_iso2","country_iso3","country_qid",
            "country_pt","country_en",
            "coast_km","has_coast",
            "adjacent_seas_pt","adjacent_seas_en"
        ])

        total = 0
        for qid in qids:
            r = idx[qid]
            iso2 = r.get("iso2","")
            iso3 = r.get("iso3","")
            m = meta.get(qid, {})
            pt = m.get("pt") or r.get("name","")
            en = m.get("en") or pt
            enwiki = m.get("enwiki","")

            # coastline (km) — tentar factbook mirrors com vários nomes
            km = coastline_from_factbook(pt, en, enwiki, iso3)

            # has_coast: preferir WD P206 (robusto); se km>0 também valida
            has_coast = has_coast_map.get(qid, False) or (km is not None and km > 0)

            wpt = waters.get(qid, {}).get("pt","")
            wen = waters.get(qid, {}).get("en","")

            w.writerow([
                iso2, iso3, qid,
                pt, en,
                f"{km:.0f}" if isinstance(km,(int,float)) else "",
                "Sim" if has_coast else "Não",
                wpt, wen
            ])
            total += 1
            # pequena pausa apenas para gentileza com mirrors (não crítico)
            time.sleep(0.1)

    print(f"[ok] Escrito {OUT_CSV} | total: {total} linhas")

if __name__ == "__main__":
    main()
