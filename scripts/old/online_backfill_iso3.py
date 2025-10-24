# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import csv
import random
import time
import requests
import pandas as pd

# ===== Paths =====
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR     = PROJECT_ROOT / "data"

CANDIDATES = [
    DATA_DIR / "conflicts_long_for_ui.enriched.backfilled.csv",
    DATA_DIR / "conflicts_long_for_ui.enriched.cleaned.csv",
    DATA_DIR / "conflicts_long_for_ui.enriched.csv",
]
IN_CSV  = next((p for p in CANDIDATES if p.exists()), None)
OUT_CSV = DATA_DIR / "conflicts_long_for_ui.enriched.online_iso3.csv"
CACHE   = DATA_DIR / "tmp_iso3_wd_cache.csv"  # {entity_qid;iso3;source}

# ===== Columns =====
COL_ISO3_FILLED = "mapped_iso3_filled"
COL_ISO3        = "mapped_iso3"
COL_ISO3_ONLINE = "mapped_iso3_online"
COL_SRC_ONLINE  = "mapped_iso3_online_source"
COL_FINAL       = "mapped_iso3_final"

COL_ROLE = "role"
COL_EQID = "entity_qid"

# ===== WDQS =====
USER_AGENT   = "GeoMundi/online-iso3/1.0 (+contact)"
WDQS_URL     = "https://query.wikidata.org/sparql"
BATCH        = 60            # pequeno e estável
TIMEOUT_S    = 120
RETRIES      = 7

def _load_df(path: Path) -> pd.DataFrame:
    if path is None:
        raise SystemExit("[erro] não encontrei nenhum CSV de input (backfilled/cleaned/enriched).")
    df = pd.read_csv(path, sep=";", dtype=str, keep_default_na=False)
    for c in (COL_ROLE, COL_EQID, COL_ISO3, COL_ISO3_FILLED):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    if COL_ISO3 in df.columns:
        df[COL_ISO3] = df[COL_ISO3].str.upper()
    if COL_ISO3_FILLED in df.columns:
        df[COL_ISO3_FILLED] = df[COL_ISO3_FILLED].str.upper()
    return df

def _load_cache(path: Path) -> dict[str, tuple[str,str]]:
    if not path.exists():
        return {}
    out: dict[str, tuple[str,str]] = {}
    try:
        df = pd.read_csv(path, sep=";", dtype=str, keep_default_na=False)
        for _, r in df.iterrows():
            q = str(r.get("entity_qid","")).strip()
            iso = str(r.get("iso3","")).strip().upper()
            src = str(r.get("source","")).strip()
            if q.startswith("Q") and len(iso) == 3:
                out[q] = (iso, src)
    except Exception:
        pass
    return out

def _save_cache(path: Path, cache: dict[str, tuple[str,str]]) -> None:
    rows = [(q, iso, src) for q, (iso, src) in sorted(cache.items())]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["entity_qid","iso3","source"])
        w.writerows(rows)

def _http_post(query: str, accept: str):
    headers = {"User-Agent": USER_AGENT, "Accept": accept, "Connection": "close"}
    return requests.post(WDQS_URL, data={"query": query}, headers=headers, timeout=TIMEOUT_S)

def _with_retry_tsv(query: str) -> str:
    last = None
    for i in range(1, RETRIES+1):
        try:
            r = _http_post(query, "text/tab-separated-values; charset=utf-8")
            if r.status_code == 200 and r.text:
                return r.text
            raise RuntimeError(f"HTTP {r.status_code}")
        except Exception as e:
            last = e
            wait = min(60, 1.7**i) + random.uniform(0.0, 0.9)
            print(f"[warn] WDQS tentativa {i}/{RETRIES} falhou ({e}); a aguardar {wait:.1f}s…")
            time.sleep(wait)
    raise RuntimeError(f"WDQS falhou após {RETRIES} tentativas: {last}")

def _query_iso3_for_items(qids: list[str]) -> dict[str, tuple[str,str]]:
    """
    Devolve {item_qid: (ISO3, how)}.
    Estratégias:
      - self_p298: o próprio item é país/estado (P31/P279* em {Q6256,Q3624078}) e tem P298
      - p3842_present: P3842 → país atual
      - p17_or_p495: P17 (país) ou P495 (país de origem)
      - p131_to_p17: P131+ → P17
      - p159_to_p131_to_p17: P159 (sede) → P131+ → P17
    """
    values = " ".join(f"wd:{q}" for q in qids if q)
    query = f"""
SELECT ?item ?iso3 ?how WHERE {{
  VALUES ?item {{ {values} }}

  OPTIONAL {{
    {{
      ?item wdt:P31/wdt:P279* ?cls .
      VALUES ?cls {{ wd:Q6256 wd:Q3624078 }}
      ?item wdt:P298 ?iso3 .
      BIND("self_p298" AS ?how)
    }}
    UNION {{
      ?item wdt:P3842 ?c3842 .
      ?c3842 wdt:P298 ?iso3 .
      BIND("p3842_present" AS ?how)
    }}
    UNION {{
      ?item (wdt:P17|wdt:P495) ?c17 .
      ?c17 wdt:P298 ?iso3 .
      BIND("p17_or_p495" AS ?how)
    }}
    UNION {{
      ?item wdt:P131+ ?place .
      ?place wdt:P17 ?c131 .
      ?c131 wdt:P298 ?iso3 .
      BIND("p131_to_p17" AS ?how)
    }}
    UNION {{
      ?item wdt:P159 ?hq .
      ?hq wdt:P131+ ?mun .
      ?mun wdt:P17 ?c159 .
      ?c159 wdt:P298 ?iso3 .
      BIND("p159_to_p131_to_p17" AS ?how)
    }}
    FILTER(STRLEN(?iso3)=3)
  }}
}}
""".strip()

    tsv = _with_retry_tsv(query)
    lines = tsv.splitlines()
    if not lines:
        return {}
    head = [h.strip() for h in lines[0].split("\t")]
    idx = {h: i for i, h in enumerate(head)}
    out: dict[str, tuple[str,str]] = {}
    for ln in lines[1:]:
        if not ln.strip():
            continue
        cells = ln.split("\t")
        item = cells[idx.get("item", -1)].rsplit("/", 1)[-1] if "item" in idx else ""
        iso3 = cells[idx.get("iso3", -1)].strip().upper() if "iso3" in idx else ""
        how  = cells[idx.get("how",  -1)].strip() if "how" in idx else ""
        if item.startswith("Q") and len(iso3) == 3 and item not in out:
            out[item] = (iso3, how or "sparql")
    return out

def main():
    print(f"[in] {IN_CSV}")
    df = _load_df(IN_CSV)

    # coluna base para decidir “falta ISO3”
    iso_base = df.get(COL_ISO3_FILLED, df.get(COL_ISO3, pd.Series("", index=df.index))).astype(str).str.upper()

    # candidatos: participants sem ISO3
    mask_part = df.get(COL_ROLE, "").astype(str).str.lower().eq("participant")
    no_iso    = iso_base.str.len() != 3
    cand = df.loc[mask_part & no_iso, COL_EQID].astype(str).str.strip()
    qids = sorted({q for q in cand if q.startswith("Q")})
    print(f"[todo] participantes únicos sem ISO3: {len(qids)}")

    # resume: cache
    cache = _load_cache(CACHE)
    done = set(cache.keys())
    pend = [q for q in qids if q not in done]
    print(f"[cache] já mapeados: {len(done)} · por fazer agora: {len(pend)}")

    # WDQS por batches
    mapped_now = 0
    for i in range(0, len(pend), BATCH):
        blk = pend[i:i+BATCH]
        try:
            res = _query_iso3_for_items(blk)
        except Exception as e:
            print(f"[warn] bloco {i//BATCH+1} falhou: {e}")
            continue
        cache.update(res)
        mapped_now += len(res)
        print(f"[wdqs] bloco {i//BATCH+1}: +{len(res)} (acum {mapped_now})")
        # grava cache a cada bloco (resume)
        _save_cache(CACHE, cache)
        time.sleep(0.8 + random.random()*0.6)

    # aplicar ao DataFrame
    df[COL_ISO3_ONLINE] = ""
    df[COL_SRC_ONLINE]  = ""
    for idx, row in df.iterrows():
        if str(row.get(COL_ROLE,"")).lower() != "participant":
            continue
        if iso_base.iat[idx] and len(iso_base.iat[idx]) == 3:
            continue
        q = str(row.get(COL_EQID,""))
        if q in cache:
            df.at[idx, COL_ISO3_ONLINE] = cache[q][0]
            df.at[idx, COL_SRC_ONLINE]  = cache[q][1]

    # coluna final (prioridade: offline -> online -> original)
    def pick_final(i: int) -> str:
        a = str(df.at[i, COL_ISO3_FILLED]) if COL_ISO3_FILLED in df.columns else ""
        b = str(df.at[i, COL_ISO3_ONLINE]) if COL_ISO3_ONLINE in df.columns else ""
        c = str(df.at[i, COL_ISO3])        if COL_ISO3 in df.columns else ""
        for v in (a, b, c):
            if len(v) == 3:
                return v
        return ""
    df[COL_FINAL] = [pick_final(i) for i in df.index]

    # escreve output
    df.to_csv(OUT_CSV, sep=";", index=False, encoding="utf-8-sig")
    print(f"[ok] escrito → {OUT_CSV}")
    print(f"[sum] preenchidos agora via WDQS: {mapped_now} · total com ISO3_final: {(df[COL_FINAL].str.len()==3).sum()} / {len(df)}")

if __name__ == "__main__":
    main()
