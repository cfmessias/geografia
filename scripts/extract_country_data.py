# scripts/extract_country_data.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import csv, os, re, sys, time, gc
from pathlib import Path
from typing import Dict, Any, List, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ───────── paths ─────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SEED_PATH    = PROJECT_ROOT / "data" / "countries_seed.csv"
OUT_PATH     = PROJECT_ROOT / "data" / "countries_profiles.csv"

# ───────── http ─────────
WIKIDATA_API  = "https://www.wikidata.org/w/api.php"
UA = "GeoProfiles/1.0 (+streamlit demo)"

def session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    s.mount("https://", HTTPAdapter(max_retries=Retry(
        total=3, backoff_factor=0.4,
        status_forcelist=[429,500,502,503,504],
        allowed_methods=["GET","POST"],
    )))
    return s

SESSION = session()
HTTP_SLEEP_SECONDS = 0.15

def _sleep():
    if HTTP_SLEEP_SECONDS: time.sleep(HTTP_SLEEP_SECONDS)

def http_get(url, params=None, timeout=12):
    _sleep()
    r = SESSION.get(url, params=params or {}, timeout=timeout)
    r.raise_for_status()
    return r

# ───────── utils ─────────
def slugify(s: str) -> str:
    s = re.sub(r"[^\w\-]+", "_", s, flags=re.U)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "pais"

def claim_quantity(claim: dict) -> Optional[float]:
    try:
        v = claim["mainsnak"]["datavalue"]["value"]
        s = str(v["amount"]) if isinstance(v, dict) and "amount" in v else str(v)
        return float(s.lstrip("+"))
    except Exception:
        return None

def claim_time_year(claim: dict) -> Optional[int]:
    try:
        q = claim.get("qualifiers", {}).get("P585", [])
        if not q: return None
        t = q[0]["datavalue"]["value"]["time"]  # +2020-00-00T...
        return int(t[1:5])
    except Exception:
        return None

# ───────── Wikidata helpers ─────────
def wd_search_qid_by_name(name_pt: str, name_en: str) -> Optional[str]:
    for lang, name in (("pt", name_pt), ("en", name_en)):
        if not name: continue
        try:
            r = http_get(WIKIDATA_API, {
                "action":"wbsearchentities","search":name,"language":lang,
                "type":"item","limit":1,"format":"json"
            }, timeout=10)
            arr = r.json().get("search", [])
            if arr: return arr[0]["id"]
        except Exception:
            continue
    return None

def wd_getentities(ids: List[str], props="labels|claims|sitelinks", languages="pt|en") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for i in range(0, len(ids), 50):
        chunk = ids[i:i+50]
        try:
            r = http_get(WIKIDATA_API, {
                "action":"wbgetentities","ids":"|".join(chunk),
                "props":props,"languages":languages,"format":"json"
            }, timeout=12)
            out.update(r.json().get("entities", {}))
        except Exception as e:
            print(f"[wbgetentities] {e}", file=sys.stderr)
    return out

def wd_label(qid: str, lang="pt") -> Optional[str]:
    ents = wd_getentities([qid], props="labels", languages=f"{lang}|en")
    e = ents.get(qid, {})
    return e.get("labels", {}).get(lang, {}).get("value") or e.get("labels", {}).get("en", {}).get("value")

# ───────── seed helpers ─────────
def ensure_seed() -> pd.DataFrame:
    if SEED_PATH.exists():
        return pd.read_csv(SEED_PATH)
    # criar seed com pycountry (sem Babel para não exigir deps)
    try:
        import pycountry
    except Exception:
        print("❌ Falta 'pycountry'. Instala com: pip install pycountry", file=sys.stderr)
        sys.exit(1)
    rows = []
    for c in pycountry.countries:
        iso2 = getattr(c, "alpha_2", "").upper()
        iso3 = getattr(c, "alpha_3", "").upper()
        if not iso2: continue
        name_en = getattr(c, "common_name", None) or getattr(c, "official_name", None) or getattr(c, "name", "")
        rows.append({"iso2": iso2, "iso3": iso3, "name_en": name_en, "name_pt": "", "slug": slugify(name_en or iso2)})
    df = pd.DataFrame(rows, columns=["iso2","iso3","name_en","name_pt","slug"])
    SEED_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(SEED_PATH, index=False, encoding="utf-8")
    print(f"[seed] criado {SEED_PATH} ({len(df)} países)", file=sys.stderr)
    return df

# ───────── perfil por QID (sem SPARQL) ─────────
def profile_from_qid(qid: str, fallback_name: str) -> Dict[str, Any]:
    prof = {
        "qid": qid, "name": fallback_name, "capital": "", "capital_qid": "",
        "inception": "", "area_km2": "", "head_of_government": "", "hog_party": "",
        "population": "", "population_year": ""
    }
    try:
        ent = wd_getentities([qid], props="labels|claims|sitelinks", languages="pt|en").get(qid, {}) or {}
        # nome
        lbl = ent.get("labels", {})
        prof["name"] = (lbl.get("pt", {}) or {}).get("value") or (lbl.get("en", {}) or {}).get("value") or fallback_name
        # capital (P36)
        cap = (ent.get("claims", {}).get("P36") or [])
        if cap:
            try:
                cap_qid = cap[0]["mainsnak"]["datavalue"]["value"]["id"]
                prof["capital_qid"] = cap_qid
                prof["capital"] = wd_label(cap_qid, "pt") or wd_label(cap_qid, "en") or ""
            except Exception: pass
        # área (P2046)
        ar = (ent.get("claims", {}).get("P2046") or [])
        if ar:
            v = claim_quantity(ar[0]); prof["area_km2"] = int(v) if v else ""
        # população (P1082) mais recente
        best = None
        for c in (ent.get("claims", {}).get("P1082") or []):
            v = claim_quantity(c); y = claim_time_year(c)
            if v is None: continue
            key = (y if y is not None else -1, v)
            if best is None or key > best[0]: best = (key, int(round(v)), y)
        if best:
            prof["population"] = best[1]; prof["population_year"] = best[2] or ""
        # independência (P730) ou P571
        indep = ent.get("claims", {}).get("P730") or []
        if indep:
            try: prof["inception"] = indep[0]["mainsnak"]["datavalue"]["value"]["time"][1:11]
            except Exception: pass
        else:
            p571 = ent.get("claims", {}).get("P571") or []
            if p571:
                try: prof["inception"] = p571[0]["mainsnak"]["datavalue"]["value"]["time"][1:11]
                except Exception: pass
        # chefe de governo (P6)
        hog_claims = ent.get("claims", {}).get("P6") or []
        def _claim_year(cl):
            try:
                q = cl.get("qualifiers", {}).get("P580", [])
                if q:
                    return int(q[0]["datavalue"]["value"]["time"][1:5])
            except Exception:
                pass
            return -1
        current = None
        for cl in hog_claims:
            ended = cl.get("qualifiers", {}).get("P582", [])
            if not ended:
                current = cl; break
        if current is None and hog_claims:
            current = max(hog_claims, key=_claim_year)
        if current:
            try:
                hog_q = current["mainsnak"]["datavalue"]["value"]["id"]
                prof["head_of_government"] = wd_label(hog_q, "pt") or wd_label(hog_q, "en") or ""
                ent_hog = wd_getentities([hog_q], props="claims", languages="en").get(hog_q, {}) or {}
                party = ent_hog.get("claims", {}).get("P102") or []
                if party:
                    party_q = party[0]["mainsnak"]["datavalue"]["value"]["id"]
                    prof["hog_party"] = wd_label(party_q, "pt") or wd_label(party_q, "en") or ""
            except Exception:
                pass

    except Exception as e:
        print(f"[enrich] {qid}: {e}", file=sys.stderr)
    return prof

def profile_min(name: str) -> Dict[str, Any]:
    return {
        "qid": "", "name": name, "capital": "", "capital_qid": "",
        "inception": "", "area_km2": "", "head_of_government": "", "hog_party": "",
        "population": "", "population_year": ""
    }

# ───────── main ─────────
FIELDS = ["qid","name","capital","capital_qid","inception","area_km2",
          "head_of_government","hog_party","population","population_year",
          "iso2","iso3","slug"]

def main() -> None:
    seed = ensure_seed()
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    header = not OUT_PATH.exists()
    written = 0

    with OUT_PATH.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if header: w.writeheader()

        for _, r in seed.iterrows():
            iso2 = str(r["iso2"]).upper()
            iso3 = str(r["iso3"]).upper()
            name = str(r.get("name_pt") or r.get("name_en") or iso3 or iso2)
            slug = str(r.get("slug") or slugify(name))

            # skip se já existir linha para este iso3+name (resume simples)
            if not header:
                try:
                    # leitura rápida (~O(n)): ok p/ uma vez; para produção usar índice.
                    df_prev = pd.read_csv(OUT_PATH, usecols=["iso3","name"])
                    if ((df_prev["iso3"].astype(str).str.upper()==iso3) &
                        (df_prev["name"].astype(str)==name)).any():
                        print(f"[SKIP] {name}")
                        continue
                except Exception:
                    pass

            print(f"[START] {name}")
            qid = wd_search_qid_by_name(str(r.get("name_pt","")), str(r.get("name_en","")))
            prof = profile_from_qid(qid, name) if qid else profile_min(name)

            row = {**prof, "iso2": iso2, "iso3": iso3, "slug": slug}
            w.writerow(row); f.flush(); os.fsync(f.fileno())
            written += 1
            gc.collect()

    print(f"✔️ Escrevi/atualizei {OUT_PATH} (linhas novas: {written})")

if __name__ == "__main__":
    main()

