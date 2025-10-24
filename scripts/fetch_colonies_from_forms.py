# scripts/fetch_colonies_from_forms.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import time, random, json, csv, tempfile, hashlib
from pathlib import Path
from typing import Dict, List
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlencode

# ------------------------ FICHEIROS ------------------------
FORMS_DETAILS_CSV = Path("data/state_lineage_level2_details.csv")  # entrada principal (tem iso3start)
PROFILES_CSV      = Path("data/countries_profiles.csv")            # opcional p/ mapear colony_qid→iso3
OUT_CSV           = Path("data/colonies_all.csv")                  # saída detalhada por colónia
SPAN_CSV          = Path("data/colonies_span_by_iso3.csv")         # novo resumo: intervalo [min,max] por iso3

# ------------------------ WDQS ------------------------
WDQS_URL = "https://query.wikidata.org/sparql"
HEADERS = {
    "User-Agent": "GeoColonies/1.1 (+streamlit; contact: user@example)",
    "Accept": "application/sparql-results+json",
}

_session = requests.Session()
_retry = Retry(
    total=8, connect=5, read=5,
    status_forcelist=(429, 500, 502, 503, 504),
    backoff_factor=0.7,
    allowed_methods=frozenset(["GET", "POST"]),
    raise_on_status=False,
)
_session.mount("https://", HTTPAdapter(max_retries=_retry))

CACHE_DIR = Path(tempfile.gettempdir()) / "wdqs_cache_colonies"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_TTL = 24 * 3600  # 24h

def _cache_key(q: str) -> Path:
    return CACHE_DIR / (hashlib.sha1(q.encode("utf-8")).hexdigest() + ".json")

def wdqs(query: str, attempts: int = 3, timeout: int = 60):
    ck = _cache_key(query)
    now = time.time()
    if ck.exists() and now - ck.stat().st_mtime < CACHE_TTL:
        try:
            return json.loads(ck.read_text(encoding="utf-8"))
        except Exception:
            pass

    def _parse(r: requests.Response):
        r.raise_for_status()
        js = r.json()
        return js.get("results", {}).get("bindings", [])

    delay = 1.0
    for i in range(1, attempts + 1):
        try:
            if len(query) < 7500:
                url = f"{WDQS_URL}?{urlencode({'query': query})}"
                r = _session.get(url, headers=HEADERS, timeout=timeout)
                if r.status_code == 200:
                    rows = _parse(r)
                    ck.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
                    return rows
            r = _session.post(WDQS_URL, data={"query": query}, headers=HEADERS, timeout=timeout)
            rows = _parse(r)
            ck.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
            return rows
        except Exception as e:
            sleep_s = delay + random.uniform(0, 0.6)
            print(f"[wdqs] tentativa {i}/{attempts} falhou: {e} -> dormir {sleep_s:.1f}s", flush=True)
            time.sleep(sleep_s)
            delay = min(delay * 1.9, 10.0)

    print("[wdqs] erro definitivo; a devolver lista vazia", flush=True)
    return []

# ------------------------ SPARQL TEMPLATES ------------------------
# A) Unidade colonial (?unit) com P17 = colonizador (?who) e P3842 → país/estado moderno (?colony)
#    Agora devolvemos também ?unit e as suas P571 (inception) / P576 (dissolved)
A_TEMPLATE = """
SELECT DISTINCT ?unit ?colony ?start ?end ?incep ?diss WHERE {
  VALUES ?who { %WHO% }
  ?unit p:P17 ?pSt .
  ?pSt  ps:P17 ?who .
  OPTIONAL { ?pSt pq:P580 ?start }
  OPTIONAL { ?pSt pq:P582 ?end   }
  OPTIONAL { ?unit wdt:P571 ?incep }
  OPTIONAL { ?unit wdt:P576 ?diss  }
  ?unit p:P3842 ?uSt .
  ?uSt  ps:P3842 ?colony .
  VALUES ?countryClass { wd:Q6256 wd:Q3624078 }
  ?colony wdt:P31/wdt:P279* ?countryClass .
}
"""

# B) Unidade colonial (?unit) com P17 = colonizador (?who) e sucessor (P1366) = país moderno (?colony)
B_TEMPLATE = """
SELECT DISTINCT ?unit ?colony ?start ?end ?incep ?diss WHERE {
  VALUES ?who { %WHO% }
  ?unit  p:P17 ?pSt .
  ?pSt   ps:P17 ?who .
  OPTIONAL { ?pSt pq:P580 ?start }
  OPTIONAL { ?pSt pq:P582 ?end   }
  OPTIONAL { ?unit wdt:P571 ?incep }
  OPTIONAL { ?unit wdt:P576 ?diss  }
  ?unit  wdt:P1366 ?colony .
  VALUES ?countryClass { wd:Q6256 wd:Q3624078 }
  ?colony wdt:P31/wdt:P279* ?countryClass .
}
"""

# ------------------------ UTIL ------------------------
def sniff_sep(p: Path) -> str:
    sample = p.read_text(encoding="utf-8", errors="ignore")[:4096]
    try:
        return csv.Sniffer().sniff(sample, delimiters=";,|\t,").delimiter
    except Exception:
        return ";" if sample.count(";") >= sample.count(",") else ","

def write_csv(df: pd.DataFrame, path: Path, header=True, mode="w"):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, sep=";", index=False, encoding="utf-8", mode=mode, header=header)

def _qid(uri: str) -> str:
    return uri.rpartition("/")[-1] if uri else ""

def _year4(x: str) -> str:
    if not x:
        return ""
    s = str(x)
    if s and s[0] in "+-":
        return s[1:5]
    return s[0:4]

# ---------- Deteção de colunas (ISO3 e QID) no ficheiro lineage ----------
ISO3_CANDIDATES = [
    "iso3start", "iso3_start", "iso3_colonizer", "iso3", "ISO3", "country_iso3", "mapped_iso3"
]
QID_CANDIDATES = [
    "qid","QID","form_qid","lineage_qid","level2_qid","state_qid","entity_qid","Form_QID","State_QID"
]

def _guess_qid_col(df: pd.DataFrame) -> str | None:
    best_col, best_hits = None, 0
    for c in df.columns:
        try:
            s = df[c].astype(str).str.fullmatch(r"Q\d+").sum()
        except Exception:
            s = 0
        if s > best_hits:
            best_hits, best_col = s, c
    return best_col if best_hits > 0 else None

def extract_iso3_qids_from_details(df: pd.DataFrame) -> pd.DataFrame:
    cols_lower = {c.lower(): c for c in df.columns}
    def pick(cands: List[str]) -> str | None:
        for c in cands:
            lc = c.lower()
            if lc in cols_lower:
                return cols_lower[lc]
        return None
    iso_col = pick(ISO3_CANDIDATES)
    qid_col = pick(QID_CANDIDATES) or _guess_qid_col(df)
    if not iso_col or not qid_col:
        raise RuntimeError(
            "state_lineage_level2_details.csv precisa de colunas para ISO3 (ex.: 'iso3start') e QID. "
            f"Detetado ISO3: {bool(iso_col)} | QID: {bool(qid_col)} | colunas={list(df.columns)}"
        )
    tmp = (df[[iso_col, qid_col]]
           .rename(columns={iso_col: "iso3", qid_col: "qid"})
           .astype(str))
    tmp["iso3"] = tmp["iso3"].str.upper().str.strip()
    tmp["qid"]  = tmp["qid"].str.strip()
    tmp = tmp[tmp["iso3"].ne("") & tmp["qid"].str.match(r"^Q\d+$", na=False)]
    return tmp.drop_duplicates()

# ------------------------ LÓGICA DE OBTENÇÃO ------------------------
def fetch_colonies_for_forms(iso3: str, forms: List[str], batch: int = 25) -> pd.DataFrame:
    out_cols = ["iso3_colonizer","colony_qid","start_year","end_year","source"]
    if not forms:
        return pd.DataFrame(columns=out_cols)

    rows_all = []
    for i in range(0, len(forms), batch):
        chunk = [q for q in forms[i:i+batch] if q and q.startswith("Q")]
        if not chunk:
            continue
        who_vals = " ".join(f"wd:{q}" for q in chunk)

        for source, templ in (("A", A_TEMPLATE), ("B", B_TEMPLATE)):
            q = templ.replace("%WHO%", who_vals).strip()
            rows = wdqs(q)
            for r in rows:
                col  = _qid(r.get("colony", {}).get("value", ""))
                st   = r.get("start", {}).get("value", "")
                en   = r.get("end",   {}).get("value", "")
                incp = r.get("incep", {}).get("value", "")
                diss = r.get("diss",  {}).get("value", "")

                # Preferir datas do próprio item (?unit): COALESCE(inception, start_qualifier) / COALESCE(dissolved, end_qualifier)
                y_start = _year4(incp or st)
                y_end   = _year4(diss or en)

                if col.startswith("Q"):
                    rows_all.append({
                        "iso3_colonizer": iso3,
                        "colony_qid": col,
                        "start_year": y_start,
                        "end_year":   y_end,
                        "source": source,
                    })
        time.sleep(0.25 + random.uniform(0, 0.25))

    if not rows_all:
        return pd.DataFrame(columns=out_cols)

    df = pd.DataFrame(rows_all).drop_duplicates()
    df["_s"] = pd.to_numeric(df["start_year"], errors="coerce")
    df["_e"] = pd.to_numeric(df["end_year"], errors="coerce")
    agg = (
        df.sort_values(["_s","_e"])
          .groupby(["iso3_colonizer","colony_qid"], as_index=False)
          .agg({"start_year":"min","end_year":"max","source":"first"})
    )
    return agg.drop(columns=[c for c in agg.columns if c.startswith("_")], errors="ignore")

# ------------------------ MAIN ------------------------
def main():
    if not FORMS_DETAILS_CSV.exists():
        raise FileNotFoundError(f"Ficheiro não encontrado: {FORMS_DETAILS_CSV}")

    # 1) Ler state_lineage_level2_details.csv
    sep_forms = sniff_sep(FORMS_DETAILS_CSV)
    df_details = pd.read_csv(FORMS_DETAILS_CSV, sep=sep_forms, dtype=str, keep_default_na=False, encoding="utf-8")
    df_forms = extract_iso3_qids_from_details(df_details)  # -> colunas: iso3, qid
    if df_forms.empty:
        raise RuntimeError("Não foram encontrados pares (iso3, qid) válidos em state_lineage_level2_details.csv")

    # 2) Ler profiles (opcional) para mapear colony_qid -> colony_iso3
    qid2iso: Dict[str, str] = {}
    if PROFILES_CSV.exists():
        try:
            sep_prof = sniff_sep(PROFILES_CSV)
            prof = pd.read_csv(PROFILES_CSV, sep=sep_prof, dtype=str, keep_default_na=False, encoding="utf-8")
            if {"qid", "iso3"} <= set(prof.columns):
                tmp = prof[["qid","iso3"]].dropna().astype(str)
                qid2iso = dict(zip(tmp["qid"].str.strip(), tmp["iso3"].str.upper().str.strip()))
        except Exception as e:
            print(f"[warn] a ler {PROFILES_CSV}: {e} — seguirei sem mapear colony_iso3", flush=True)

    # 3) Processar por colonizador (iso3)
    iso_list = sorted(df_forms["iso3"].unique().tolist())

    header_detail = not OUT_CSV.exists()
    total_rows = 0
    spans = []  # para o resumo por iso3

    for i, iso3 in enumerate(iso_list, start=1):
        forms = sorted(df_forms.loc[df_forms["iso3"].eq(iso3), "qid"].tolist())
        print(f"[{i}/{len(iso_list)}] {iso3} — {len(forms)} formas (state_lineage_level2_details.csv)", flush=True)

        try:
            df_col = fetch_colonies_for_forms(iso3, forms)
            if not df_col.empty:
                # mapear colony_qid -> colony_iso3 se possível
                if qid2iso:
                    df_col["colony_iso3"] = df_col["colony_qid"].map(qid2iso).fillna("")
                    df_col = df_col[["iso3_colonizer","colony_iso3","colony_qid","start_year","end_year","source"]]

                write_csv(df_col, OUT_CSV, header=header_detail, mode="a")
                header_detail = False
                total_rows += len(df_col)
                print(f"  └ +{len(df_col)} linhas (acumulado {total_rows})", flush=True)

                # acumular span por iso3 (min start / max end através de TODAS as colónias encontradas)
                s_min = pd.to_numeric(df_col["start_year"], errors="coerce").min()
                e_max = pd.to_numeric(df_col["end_year"],   errors="coerce").max()
                spans.append({"iso3_colonizer": iso3,
                              "span_start_min": int(s_min) if pd.notna(s_min) else "",
                              "span_end_max":   int(e_max) if pd.notna(e_max) else ""})
            else:
                print("  └ 0 linhas", flush=True)
                spans.append({"iso3_colonizer": iso3, "span_start_min": "", "span_end_max": ""})

        except KeyboardInterrupt:
            print("\n[warn] Interrompido pelo utilizador.", flush=True)
            break
        except Exception as e:
            print(f"[err] {iso3}: {e}", flush=True)
            spans.append({"iso3_colonizer": iso3, "span_start_min": "", "span_end_max": ""})

        time.sleep(0.4 + random.uniform(0, 0.5))

    print(f"[done] {total_rows} linhas → {OUT_CSV}", flush=True)

    # 4) Escrever o resumo por iso3 (min e max associados ao iso3, como pediste)
    if spans:
        df_span = pd.DataFrame(spans)
        # consolidar (no caso de múltiplos ciclos por iso3, ficamos com o min real e o max real)
        def _min_col(s): 
            v = pd.to_numeric(s, errors="coerce"); m = v.min()
            return int(m) if pd.notna(m) else ""
        def _max_col(s):
            v = pd.to_numeric(s, errors="coerce"); m = v.max()
            return int(m) if pd.notna(m) else ""

        df_span = (df_span.groupby("iso3_colonizer", as_index=False)
                          .agg(span_start_min=("span_start_min", _min_col),
                               span_end_max=("span_end_max", _max_col)))
        write_csv(df_span, SPAN_CSV, header=True, mode="w")
        print(f"[span] Intervalos por colonizador → {SPAN_CSV}", flush=True)

if __name__ == "__main__":
    main()
