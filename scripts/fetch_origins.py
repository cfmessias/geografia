# -*- coding: utf-8 -*-
"""
fetch_origins.py
Recolhe texto-base sobre as "Origens do país" (Wikidata + Wikipedia) e grava em data/history/origins.enriched.csv

Uso:
  - Todos os países (a partir de data/countries_seed.csv e/ou data/iso_m49_un_world/*.csv):
      python scripts/fetch_origins.py
  - Apenas alguns países:
      python scripts/fetch_origins.py PRT ESP FRA
"""

from __future__ import annotations

import csv
import json
import re
import sys
import time
import html
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import requests


# ----------------------------
# Configuração
# ----------------------------
ROOT   = Path(__file__).resolve().parents[1]
DATA   = ROOT / "data"
OUT_DIR = DATA / "history"
OUT_CSV = OUT_DIR / "origins.enriched.csv"
LOG_CSV = OUT_DIR / "origins.fetch.log.csv"

# Wikidata & Wikipedia
SPARQL_URL = "https://query.wikidata.org/sparql"
UA = {
    # Por favor personalize com um contacto teu (melhora fiabilidade)
    "User-Agent": "geografia-app/1.0 (contacto: email@dominio; repo: local)",
    "Accept": "application/json",
}

# ----------------------------
# Utilitários CSV
# ----------------------------
def norm_colnames(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).replace("\ufeff", "").strip().lower().replace(" ", "_") for c in df.columns]
    return df

def read_csv_semicolon(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(
        path, dtype=str, sep=";", engine="python",
        encoding="utf-8-sig", on_bad_lines="skip", quotechar='"'
    )
    return norm_colnames(df)

def write_csv_semicolon(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(
        path,
        index=False,
        sep=";",
        encoding="utf-8-sig",
        lineterminator="\n",
        quoting=csv.QUOTE_ALL,
        quotechar='"',
        escapechar="\\",
    )

# ----------------------------
# Descoberta de ISO3
# ----------------------------
def discover_iso3s(*, debug: bool = False) -> List[str]:
    iso3: set[str] = set()

    # 1) countries_seed.csv (assumido ';' conforme o projeto)
    seed = DATA / "countries_seed.csv"
    if seed.exists():
        df = read_csv_semicolon(seed)
        if "iso3" in df.columns:
            vals = df["iso3"].dropna().astype(str).str.upper().str.strip()
            iso3.update(v for v in vals if v)
            if debug:
                print(f"[DEBUG] countries_seed.csv (;) -> {len(vals)} registos lidos")

    # 2) iso_m49_un_world/*.csv (também ';')
    m49_dir = DATA / "iso_m49_un_world"
    if m49_dir.exists():
        for path in m49_dir.glob("*.csv"):
            df = read_csv_semicolon(path)
            if "iso3" in df.columns:
                vals = df["iso3"].dropna().astype(str).str.upper().str.strip()
                iso3.update(v for v in vals if v)
                if debug:
                    print(f"[DEBUG] {path.name} (;) -> {len(vals)} registos lidos")

    return sorted(iso3)

# ----------------------------
# Wikidata (SPARQL)
# ----------------------------
def sparql(query: str, *, debug: bool = False) -> Dict[str, Any]:
    headers = dict(UA)
    headers["Accept"] = "application/sparql-results+json"
    try:
        r = requests.get(SPARQL_URL, params={"query": query}, headers=headers, timeout=30)
        if r.status_code != 200:
            if debug:
                print(f"[DEBUG] SPARQL HTTP {r.status_code}: {r.text[:200]}")
            return {}
        return r.json()
    except Exception as e:
        if debug:
            print(f"[DEBUG] SPARQL falhou: {e}")
        return {}

def qid_from_iso3(iso3: str, *, debug: bool = False) -> Optional[str]:
    iso3s = str(iso3).upper().strip()
    q = f"""
    SELECT ?c WHERE {{
      ?c wdt:P298 "{iso3s}" .
    }} LIMIT 1
    """
    js = sparql(q, debug=debug)
    for b in js.get("results", {}).get("bindings", []):
        v = b.get("c", {}).get("value", "")
        if v:
            return v.rpartition("/")[2]
    return None

def labels_from_qids(qids: Iterable[str], lang: str, *, debug: bool = False) -> Dict[str, str]:
    qid_list = [q for q in set(qids) if q and isinstance(q, str)]
    if not qid_list:
        return {}
    values = " ".join(f"wd:{q}" for q in qid_list)
    q = f"""
    SELECT ?item ?itemLabel WHERE {{
      VALUES ?item {{ {values} }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang},en". }}
    }}
    """
    js = sparql(q, debug=debug)
    out: Dict[str, str] = {}
    for b in js.get("results", {}).get("bindings", []):
        qfull = b.get("item", {}).get("value", "")
        label = b.get("itemLabel", {}).get("value", "")
        if qfull:
            qid = qfull.rpartition("/")[2]
            out[qid] = label
    return out

def country_facts(qid: str, lang: str, *, debug: bool = False) -> Dict[str, Any]:
    """
    inception (P571),
    earliest (P1249 ou P1319),
    named after (P138),
    predecessors (P155, P1365),
    wikipedia title (schema:name)
    """
    wiki_host = "pt" if lang == "pt" else "en"
    q = f"""
    SELECT ?inception ?earliest ?namedAfter ?pred ?wptitle WHERE {{
      OPTIONAL {{ wd:{qid} wdt:P571 ?inception . }}
      OPTIONAL {{ wd:{qid} wdt:P1249 ?earliest . }}
      OPTIONAL {{ wd:{qid} wdt:P1319 ?earliest . }}
      OPTIONAL {{ wd:{qid} wdt:P138 ?namedAfter . }}
      OPTIONAL {{ wd:{qid} wdt:P155 ?pred . }}
      OPTIONAL {{ wd:{qid} wdt:P1365 ?pred . }}
      OPTIONAL {{
        ?sitelink schema:about wd:{qid} ;
                  schema:isPartOf <https://{wiki_host}.wikipedia.org/> ;
                  schema:name ?wptitle .
      }}
    }}
    """
    js = sparql(q, debug=debug)
    rows = js.get("results", {}).get("bindings", [])

    def _year(x: str | None) -> Optional[int]:
        if not x:
            return None
        m = re.match(r"^-?\d{1,4}", x)
        if not m:
            return None
        y = int(m.group(0))
        return y if 500 <= abs(y) <= datetime.now().year else None

    inception_year = None
    earliest_year  = None
    named_after    = None
    predecessors: set[str] = set()
    page_title     = None

    for b in rows:
        if "inception" in b:
            inception_year = inception_year or _year(b["inception"]["value"])
        if "earliest" in b:
            earliest_year  = earliest_year  or _year(b["earliest"]["value"])
        if "namedAfter" in b:
            named_after = b["namedAfter"]["value"].rpartition("/")[2]
        if "pred" in b:
            predecessors.add(b["pred"]["value"].rpartition("/")[2])
        if "wptitle" in b:
            page_title = page_title or b["wptitle"]["value"]

    return {
        "inception_year": inception_year,
        "earliest_year": earliest_year,
        "named_after_qid": named_after,
        "predecessor_qids": sorted(predecessors),
        "wp_title": page_title,
    }

# ----------------------------
# Wikipedia helpers
# ----------------------------
def wp_summary(title: str, lang: str = "pt", *, debug: bool = False) -> Optional[str]:
    if not title:
        return None
    base = f"https://{'pt' if lang=='pt' else 'en'}.wikipedia.org/api/rest_v1/page/summary/{requests.utils.quote(title)}"
    try:
        r = requests.get(base, headers=UA, timeout=25)
        if r.status_code != 200:
            if debug: print(f"[DEBUG] summary {lang} {title}: HTTP {r.status_code}")
            return None
        j = r.json()
        extract = (j.get("extract") or "").strip()
        return extract or None
    except Exception as e:
        if debug: print(f"[DEBUG] summary falhou: {e}")
        return None

def wp_origins_text(title: Optional[str], lang: str = "pt", *, debug: bool = False) -> Optional[str]:
    """
    1–3 frases de resumo (summary). Usado para páginas 'História de/History of' e fallback.
    """
    if not title:
        return None
    base = f"https://{'pt' if lang=='pt' else 'en'}.wikipedia.org/api/rest_v1/page/summary/{requests.utils.quote(title)}"
    try:
        r = requests.get(base, headers=UA, timeout=25)
        if r.status_code != 200:
            if debug: print(f"[DEBUG] summary {lang} {title}: HTTP {r.status_code}")
            return None
        j = r.json()
        extract = (j.get("extract") or "").strip()
        if not extract:
            return None
        sents = re.split(r"(?<=[.!?])\s+", extract)
        out = " ".join(sents[:3]).strip()
        if len(out) > 900:
            out = out[:900].rsplit(" ", 1)[0] + "…"
        return out
    except Exception as e:
        if debug: print(f"[DEBUG] summary falhou: {e}")
        return None

def _wp_search_title(query: str, lang: str, *, debug: bool=False) -> Optional[str]:
    base = f"https://{'pt' if lang=='pt' else 'en'}.wikipedia.org/api/rest_v1/search/title"
    try:
        r = requests.get(base, params={"q": query, "limit": 1}, headers=UA, timeout=25)
        if r.status_code == 200:
            j = r.json()
            pages = (j.get("pages") or [])
            if pages:
                return pages[0].get("title")
    except Exception as e:
        if debug: print(f"[DEBUG] _wp_search_title falhou: {e}")
    return None

def _wp_fetch_mobile_html(title: str, lang: str, *, debug: bool=False) -> str | None:
    if not title:
        return None
    url = f"https://{'pt' if lang=='pt' else 'en'}.wikipedia.org/api/rest_v1/page/mobile-html/{requests.utils.quote(title)}"
    try:
        r = requests.get(url, headers=UA, timeout=30)
        if r.status_code == 200 and r.text:
            return r.text
        if debug:
            print(f"[DEBUG] mobile-html {lang}/{title}: HTTP {r.status_code}")
    except Exception as e:
        if debug: print(f"[DEBUG] mobile-html falhou: {e}")
    return None

_ws_re = re.compile(r"\s+")
_ref_tag_re = re.compile(r"\[\d+\]")

def _html_to_text(html_str: str) -> str:
    if not html_str:
        return ""
    s = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html_str, flags=re.S | re.I)
    s = re.sub(r"<[^>]+>", " ", s)
    s = html.unescape(s)
    s = _ref_tag_re.sub("", s)
    s = _ws_re.sub(" ", s).strip()
    return s

def wp_section_text(title: str, lang: str, section_patterns: list[str], *, min_chars=250, max_chars=1400, debug: bool=False) -> str | None:
    html_str = _wp_fetch_mobile_html(title, lang, debug=debug)
    if not html_str:
        return None

    pattern = re.compile(r"(<h[1-6][^>]*>.*?</h[1-6]>)(.*?)(?=<h[1-6][^>]*>|\Z)", flags=re.S | re.I)
    blocks: list[tuple[str, str]] = []
    for m in pattern.finditer(html_str):
        htag = m.group(1)
        body = m.group(2) or ""
        htxt = _html_to_text(htag).lower()
        blocks.append((htxt, body))

    if not blocks:
        paras = re.findall(r"<p[^>]*>(.*?)</p>", html_str, flags=re.S | re.I)
        text = " ".join(_html_to_text(p) for p in paras[:3]).strip()
        return text if len(text) >= min_chars else None

    regs = [re.compile(pat, re.I) for pat in section_patterns]

    for htxt, body in blocks:
        if any(rgx.search(htxt) for rgx in regs):
            paras = re.findall(r"<p[^>]*>(.*?)</p>", body, flags=re.S | re.I)
            if not paras:
                continue
            out_texts = []
            total = 0
            for p in paras:
                t = _html_to_text(p)
                if len(t) < 40:
                    continue
                out_texts.append(t)
                total += len(t)
                if len(out_texts) >= 4 or total >= max_chars:
                    break
            text = " ".join(out_texts).strip()
            if len(text) >= min_chars:
                return text

    return None

def history_title_candidates(label_pt: str, label_en: str) -> Dict[str, List[str]]:
    lp = (label_pt or "").strip()
    le = (label_en or "").strip()
    cands_pt = [f"História de {lp}", f"História da {lp}", f"História do {lp}", f"História {lp}"]
    cands_en = [f"History of {le}", f"History {le}"]
    return {"pt": cands_pt, "en": cands_en}

def wp_best_origins_text(primary_title: Optional[str],
                         label_pt: str, label_en: str,
                         lang: str, *, debug: bool = False) -> Optional[str]:
    """
    Prioridade:
      1) Secções 'Etimologia/Toponímia/Nome' no artigo principal do país
      2) Secções em páginas 'História de/History of ...'
      3) Summary das páginas 'História de/History of ...'
      4) Summary do título principal
    """
    if lang == "pt":
        sec_pats = [r"\betimolog", r"\btopon", r"\bnome\b", r"\borigem"]
    else:
        sec_pats = [r"\betymolog", r"\btoponym", r"\bname\b", r"\borigin"]

    cands = history_title_candidates(label_pt, label_en).get("pt" if lang == "pt" else "en", [])

    if primary_title:
        txt = wp_section_text(primary_title, lang, sec_pats, debug=debug)
        if txt and len(txt) > 200:
            if debug: print(f"[DEBUG] origins via secção no principal: {primary_title}")
            return txt

    for t in cands:
        txt = wp_section_text(t, lang, sec_pats, debug=debug)
        if txt and len(txt) > 200:
            if debug: print(f"[DEBUG] origins via secção na página de história: {t}")
            return txt

    for t in cands:
        txt = wp_origins_text(t, lang=lang, debug=debug)
        if txt and len(txt) > 200:
            if debug: print(f"[DEBUG] origins via summary da página de história: {t}")
            return txt

    if primary_title:
        txt = wp_origins_text(primary_title, lang=lang, debug=debug)
        if txt and len(txt) > 120:
            if debug: print(f"[DEBUG] origins via summary principal: {primary_title}")
            return txt

    return None

# ----------------------------
# Montagem da linha
# ----------------------------
def now_utc_iso() -> str:
    # Evita DeprecationWarning do utcnow; string simples suficiente
    return datetime.now(timezone.utc).isoformat()

def row_for_iso3(iso3: str, *, debug: bool = False) -> Dict[str, Any]:
    iso3u = (iso3 or "").upper().strip()
    if not iso3u:
        raise ValueError("ISO3 vazio")

    qid = qid_from_iso3(iso3u, debug=debug)
    if not qid:
        raise RuntimeError(f"Sem QID (P298) para {iso3u}")

    # Labels do país
    labels_pt = labels_from_qids([qid], "pt", debug=debug)
    labels_en = labels_from_qids([qid], "en", debug=debug)
    country_pt = labels_pt.get(qid, "")
    country_en = labels_en.get(qid, "")

    # Fatos (PT/EN)
    facts_pt = country_facts(qid, "pt", debug=debug)
    facts_en = country_facts(qid, "en", debug=debug)

    inception_year = facts_pt.get("inception_year") or facts_en.get("inception_year")
    earliest_year  = facts_pt.get("earliest_year")  or facts_en.get("earliest_year")
    named_qid      = facts_pt.get("named_after_qid") or facts_en.get("named_after_qid")
    preds_qids     = facts_pt.get("predecessor_qids") or facts_en.get("predecessor_qids") or []

    # Labels de named-after & predecessores
    named_pt = named_en = ""
    if named_qid:
        named_pt = labels_from_qids([named_qid], "pt", debug=debug).get(named_qid, "")
        named_en = labels_from_qids([named_qid], "en", debug=debug).get(named_qid, "")

    preds_pt = preds_en = ""
    if preds_qids:
        lab_pt = labels_from_qids(preds_qids, "pt", debug=debug)
        lab_en = labels_from_qids(preds_qids, "en", debug=debug)
        preds_pt = " | ".join(lab_pt.get(q, q) for q in preds_qids)
        preds_en = " | ".join(lab_en.get(q, q) for q in preds_qids)

    # Títulos Wikipedia
    wp_title_pt = facts_pt.get("wp_title") or country_pt or ""
    wp_title_en = facts_en.get("wp_title") or country_en or ""

    # Textos: ORIGENS (melhor esforço) e SUMMARY (fallback visível no expander)
    wp_orig_pt = wp_best_origins_text(wp_title_pt, country_pt, country_en, "pt", debug=debug)
    wp_orig_en = wp_best_origins_text(wp_title_en, country_pt, country_en, "en", debug=debug)

    wp_sum_pt  = wp_summary(wp_title_pt, "pt", debug=debug) if wp_title_pt else ""
    wp_sum_en  = wp_summary(wp_title_en, "en", debug=debug) if wp_title_en else ""

    row = {
        "iso3": iso3u,
        "country_qid": qid,
        "country_label_pt": country_pt,
        "country_label_en": country_en,
        "inception_year": inception_year if inception_year is not None else "",
        "earliest_year":  earliest_year  if earliest_year  is not None else "",
        "named_after_qid": named_qid or "",
        "named_after_pt":  named_pt,
        "named_after_en":  named_en,
        "predecessor_qids": "|".join(preds_qids) if preds_qids else "",
        "predecessors_pt":  preds_pt,
        "predecessors_en":  preds_en,
        "wp_title_pt": wp_title_pt,
        "wp_title_en": wp_title_en,
        "wp_origins_pt": wp_orig_pt or "",
        "wp_origins_en": wp_orig_en or "",
        "wp_summary_pt": wp_sum_pt or "",
        "wp_summary_en": wp_sum_en or "",
        "fetched_at": now_utc_iso(),
    }
    return row

# ----------------------------
# Escrita e logging
# ----------------------------
def append_rows_to_csv(rows: List[Dict[str, Any]]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)

    if OUT_CSV.exists():
        old = read_csv_semicolon(OUT_CSV)
        all_cols = list(dict.fromkeys(list(old.columns) + list(df.columns)))
        old = old.reindex(columns=all_cols)
        df  = df.reindex(columns=all_cols)

        # chave iso3; evita duplicar as recém-geradas
        key = "iso3"
        if key in old.columns and key in df.columns:
            old = old[~old[key].isin(df[key])]
        out = pd.concat([old, df], ignore_index=True)
    else:
        out = df

    write_csv_semicolon(out, OUT_CSV)

def log_status(iso3: str, status: str, info: str = "") -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    new = pd.DataFrame([{
        "iso3": iso3,
        "status": status,
        "info": info,
        "ts": now_utc_iso(),
    }])
    if LOG_CSV.exists():
        old = read_csv_semicolon(LOG_CSV)
        out = pd.concat([old, new], ignore_index=True)
    else:
        out = new
    write_csv_semicolon(out, LOG_CSV)

# ----------------------------
# Main
# ----------------------------
def _brief_row_status(row: dict) -> str:
    pt = row.get("wp_origins_pt") or row.get("wp_summary_pt") or ""
    en = row.get("wp_origins_en") or row.get("wp_summary_en") or ""
    tpt = len(pt)
    ten = len(en)
    tptitle = row.get("wp_title_pt") or "-"
    entitle = row.get("wp_title_en") or "-"
    return f"PT:{tpt}ch ({tptitle}) | EN:{ten}ch ({entitle})"

def main(argv: List[str]) -> int:
    debug = "--debug" in argv
    iso_args = [a for a in argv if re.fullmatch(r"[A-Za-z]{3}", a)]

    if iso_args:
        iso3_list = [a.upper() for a in iso_args]
    else:
        print("[INFO] autodiscovery de ISO3 a partir do seed/ONU… (usa --debug para detalhes)")
        iso3_list = discover_iso3s(debug=debug)
        if not iso3_list:
            print("❌ Não encontrei ISO3 em nenhum dos fontes indicados.")
            return 2

    rows: List[Dict[str, Any]] = []
    total = len(iso3_list)

    for i, iso3 in enumerate(iso3_list, 1):
        t0 = time.perf_counter()
        print(f"[{i:>3}/{total}] ▶ {iso3} …", end="", flush=True)
        try:
            row = row_for_iso3(iso3, debug=debug)
            rows.append(row)
            log_status(iso3, "ok", "fetched")
            dt = time.perf_counter() - t0
            print(f"\r[{i:>3}/{total}] ✅ {iso3}  { _brief_row_status(row) }  ({dt:.1f}s)")
        except Exception as e:
            dt = time.perf_counter() - t0
            print(f"\r[{i:>3}/{total}] ❌ {iso3}  {e}  ({dt:.1f}s)")
            log_status(iso3, "error", str(e))

    if rows:
        append_rows_to_csv(rows)
        print(f"✅ Gravado: {OUT_CSV}  (+{len(rows)} linhas novas)")
    else:
        print("⚠️  Nenhuma linha nova para gravar.")

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
