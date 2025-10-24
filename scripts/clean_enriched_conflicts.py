# scripts/clean_enriched_conflicts.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import json
import random
import time
from pathlib import Path
from typing import Dict, Iterable, List, Set

import pandas as pd
import requests

# =============== CONFIG ===============
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR     = PROJECT_ROOT / "data"

IN_CSV       = DATA_DIR / "conflicts_long_for_ui.enriched.csv"
OUT_CLEAN    = DATA_DIR / "conflicts_long_for_ui.enriched.cleaned.csv"
OUT_EXCL     = DATA_DIR / "tmp_clean_exclusions.csv"
OUT_P31CACHE = DATA_DIR / "tmp_clean_p31_cache.csv"

USER_AGENT   = "GeoMundi/clean-enriched/1.0 (+contact)"
MW_API       = "https://www.wikidata.org/w/api.php"
TIMEOUT_S    = 45
BATCH        = 200

# Regras: o filtro aplica-se APENAS a linhas role=participant
EXCLUDE_HUMANS = True   # se quiseres manter humanos, põe False

# Tipos “state-like” (sinais positivos). Se uma entidade tiver algum destes P31, NUNCA é excluída pelo filtro de genericidade.
WHITELIST_STATE_LIKE: Set[str] = {
    "Q6256",    # sovereign state
    "Q3624078", # historical country
    "Q7275",    # state (polity)
    "Q46169",   # kingdom
    "Q22721",   # empire
    "Q7270",    # republic
    "Q43287",   # caliphate
    "Q43702",   # emirate
    "Q49893",   # county
    "Q215793",  # duchy
    "Q5119",    # city-state
    "Q184636",  # principality
    "Q2277",    # sultanate
    "Q1637706", # confederation
    "Q1349640", # protectorate
}

# Tipos genéricos/abstratos que DEVEM ser excluídos (se não forem state-like):
BLACKLIST_GENERIC: Set[str] = {
    "Q474717",  # military order (classe)
    "Q41710",   # chivalric order (classe)
    "Q82794",   # region
    "Q46831",   # historical region
    "Q3455524", # march (frontier)
    "Q185562",  # frontier
    "Q811430",  # class (metaclass)
    "Q35120",   # organization (genérico)
    "Q163740",  # religious order
    "Q7210356", # military unit (demasiado genérico)
    "Q1496967", # ecclesiastical province
    "Q180673",  # diocese
}

# … no topo, junto às listas:
SHIPLIKE: set[str] = {
    "Q11446",   # ship
    "Q1145924", # container ship
    "Q49848",   # cargo ship / freighter
    "Q170517",  # destroyer
    "Q170475",  # frigate
    "Q173623",  # battleship
    "Q622365",  # cruiser
    "Q174385",  # corvette
    "Q483251",  # submarine (militar ou civil)
    "Q1510",    # aircraft carrier
    "Q170474",  # patrol boat
    # acrescenta mais classes se encontrares casos
}


# Opcional: manter uma lista de QIDs que NUNCA queremos excluir (mesmo que o P31 seja “mau”)
HARDCODED_KEEP: Set[str] = set()

# =============== Helpers ===============
# ---------------- Datas canónicas por conflito (só com o CSV) ----------------

def _norm_date(s: str) -> str:
    """Aceita 'YYYY' | 'YYYY-MM' | 'YYYY-MM-DD' e devolve string aparada."""
    return (str(s) if s is not None else "").strip()

def _is_date(s: str) -> bool:
    s = _norm_date(s)
    if not s: return False
    y = s[:4]
    return len(y) == 4 and y.isdigit()

def _to_year(s: str) -> str:
    s = _norm_date(s)
    y = s[:4]
    return y if len(y) == 4 and y.isdigit() else ""

def _min_date(values: list[str]) -> str:
    vals = [v for v in map(_norm_date, values) if _is_date(v)]
    return min(vals) if vals else ""

def _max_date(values: list[str]) -> str:
    vals = [v for v in map(_norm_date, values) if _is_date(v)]
    return max(vals) if vals else ""

def build_conflict_date_index(df: pd.DataFrame) -> dict[str, dict]:
    """
    Cria um índice {conflict_qid: {'pit': ..., 'start': ..., 'end': ..., 'year': ...}}
    usando APENAS o que já vem no CSV (point_in_time/start_date/end_date/point_year).
    Regras:
      - pit_canónico = min(point_in_time, start_date, end_date) que existir
      - start_canónico = min(start_date)  (se vazio, pode ficar vazio)
      - end_canónico   = max(end_date)
      - year = point_year válido, senão ano do pit_canónico, senão start/end
    """
    have_cols = {c: (c in df.columns) for c in ["point_in_time","start_date","end_date","point_year"]}
    idx: dict[str, dict] = {}

    # agrupar apenas pelas colunas necessárias
    grp = df.groupby("conflict_qid", dropna=False, sort=False)

    for cq, g in grp:
        if not isinstance(cq, str) or not cq:
            continue
        pits   = g["point_in_time"].astype(str).tolist() if have_cols["point_in_time"] else []
        starts = g["start_date"].astype(str).tolist()     if have_cols["start_date"]     else []
        ends   = g["end_date"].astype(str).tolist()       if have_cols["end_date"]       else []
        years  = g["point_year"].astype(str).tolist()     if have_cols["point_year"]     else []

        pit_canon   = _min_date(pits + starts + ends)
        start_canon = _min_date(starts)
        end_canon   = _max_date(ends)

        # escolher o ano canónico
        year_candidates = [y for y in years if _to_year(y)]
        year = _to_year(pit_canon) or (year_candidates[0] if year_candidates else _to_year(start_canon) or _to_year(end_canon))

        idx[cq] = {
            "pit":   pit_canon,
            "start": start_canon,
            "end":   end_canon,
            "year":  year,
        }
    return idx

def harmonize_conflict_dates(df: pd.DataFrame, *, fill_point=True, fill_bounds=True, write_point_year=True) -> pd.DataFrame:
    """
    Preenche datas em falta por conflito:
      - se fill_point:   point_in_time := pit_canónico quando vazio
      - se fill_bounds:  start_date/end_date := canónicos quando vazios
      - se write_point_year: recalcula point_year (do point_in_time, senão start/end)
    Retorna um novo DataFrame.
    """
    df = df.copy()
    # garantir colunas
    for c in ("point_in_time","start_date","end_date","point_year"):
        if c not in df.columns:
            df[c] = ""

    idx = build_conflict_date_index(df)

    # métricas
    before_no_pit = (df["point_in_time"].astype(str).str.strip() == "").sum()
    before_no_year = (df["point_year"].astype(str).str.strip() == "").sum()

    # aplicar por linhas (rápido o suficiente)
    def _apply(row):
        cq = row.get("conflict_qid", "")
        meta = idx.get(cq, {})
        if fill_point and not _is_date(row["point_in_time"]):
            row["point_in_time"] = meta.get("pit", "") or row["point_in_time"]
        if fill_bounds:
            if not _is_date(row["start_date"]):
                row["start_date"] = meta.get("start", "") or row["start_date"]
            if not _is_date(row["end_date"]):
                row["end_date"] = meta.get("end", "") or row["end_date"]
        if write_point_year:
            y = _to_year(row["point_in_time"]) or _to_year(row["start_date"]) or _to_year(row["end_date"]) or meta.get("year","")
            row["point_year"] = y or row["point_year"]
        return row

    df = df.apply(_apply, axis=1)

    after_no_pit  = (df["point_in_time"].astype(str).str.strip() == "").sum()
    after_no_year = (df["point_year"].astype(str).str.strip() == "").sum()
    print(f"[dates] point_in_time vazias: {before_no_pit} → {after_no_pit} | point_year vazios: {before_no_year} → {after_no_year}")
    return df

def chunks(seq: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"[erro] não encontrei {path}")
    return pd.read_csv(path, sep=";", dtype=str, encoding="utf-8-sig").fillna("")



# =============== MediaWiki: obter P31 em batches ===============
def mw_get_entities_claims(sess: requests.Session, qids: List[str]) -> dict:
    params = {
        "action": "wbgetentities",
        "ids": "|".join(qids),
        "props": "claims",
        "format": "json",
    }
    headers = {"User-Agent": USER_AGENT}
    r = sess.get(MW_API, params=params, headers=headers, timeout=TIMEOUT_S)
    r.raise_for_status()
    return r.json()

def p31_from_claims(claims: dict) -> List[str]:
    out: List[str] = []
    arr = claims.get("P31", []) or []
    for snak in arr:
        dv = (snak.get("mainsnak") or {}).get("datavalue") or {}
        if dv.get("type") == "wikibase-entityid":
            q = (dv.get("value") or {}).get("id") or ""
            if q.startswith("Q"):
                out.append(q)
    return out

# =============== Caching ===============
def load_cache(path: Path) -> Dict[str, List[str]]:
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path, sep=";", dtype=str, encoding="utf-8-sig").fillna("")
        cache: Dict[str, List[str]] = {}
        for _, r in df.iterrows():
            q = str(r["qid"]).strip()
            p31s = [s for s in str(r["p31_list"]).split("|") if s]
            if q:
                cache[q] = p31s
        return cache
    except Exception:
        return {}

def save_cache(path: Path, cache: Dict[str, List[str]]) -> None:
    rows = [(q, "|".join(v)) for q, v in sorted(cache.items())]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["qid", "p31_list"])
        w.writerows(rows)

# =============== Classificador de qualidade ===============
def classify_participant(qid: str, p31_list: list[str]) -> tuple[bool, str]:
    if not qid or not qid.startswith("Q"):
        return (True, "skip-empty")

    if qid in HARDCODED_KEEP:
        return (True, "keep-forced")

    if EXCLUDE_HUMANS and "Q5" in p31_list:
        return (False, "human(Q5)")

    # mantém estados / reinos / etc.
    if any(t in WHITELIST_STATE_LIKE for t in p31_list):
        return (True, "state-like")

    # excluir navios (embarcações individuais)
    if any(t in SHIPLIKE for t in p31_list):
        return (False, "shiplike")

    # excluir genéricos (ordens, regiões, etc.)
    if any(t in BLACKLIST_GENERIC for t in p31_list):
        return (False, "generic-p31")

    # conservador: manter o resto
    return (True, "default-keep")

# =============== MAIN CLEANER ===============
def main():
    df = read_csv(IN_CSV)
    df = harmonize_conflict_dates(df, fill_point=True, fill_bounds=True, write_point_year=True)

    # garantir colunas
    for c in ("role","entity_qid","entity_label","is_human"):
        if c not in df.columns:
            df[c] = ""
    df["role"] = df["role"].astype(str).str.lower().str.strip()
    df["entity_qid"] = df["entity_qid"].astype(str).str.strip()
    df["entity_label"] = df["entity_label"].astype(str).str.strip()
    df["is_human"] = df["is_human"].astype(str).str.lower().str.strip()

    # candidatos a verificar (apenas participantes com QID)
    cand = df[(df["role"] == "participant") & (df["entity_qid"].str.startswith("Q"))][["entity_qid"]].drop_duplicates()
    qids: List[str] = cand["entity_qid"].tolist()
    print(f"[scan] participantes únicos a classificar: {len(qids)}")

    # cache de P31
    cache = load_cache(OUT_P31CACHE)
    missing = [q for q in qids if q not in cache]
    print(f"[cache] P31 em cache: {len(cache)} · por obter: {len(missing)}")

    if missing:
        sess = requests.Session()
        sess.headers.update({"User-Agent": USER_AGENT})
        for blk in chunks(missing, BATCH):
            try:
                js = mw_get_entities_claims(sess, blk)
                ents = js.get("entities", {}) or {}
                for q in blk:
                    claims = (ents.get(q) or {}).get("claims") or {}
                    cache[q] = p31_from_claims(claims)
            except Exception as e:
                print(f"[warn] batch P31 falhou: {e}")
            time.sleep(0.25 + random.random()*0.5)

        # persistir cache
        save_cache(OUT_P31CACHE, cache)
        print(f"[cache] atualizado: {OUT_P31CACHE}")

    # construir mapa qid -> decisão
    decisions: Dict[str, tuple[bool, str]] = {}
    for q in qids:
        p31s = cache.get(q, [])
        decisions[q] = classify_participant(q, p31s)

    # aplicar filtro
    keep_mask = df["role"] != "participant"
    excl_rows: List[List[str]] = []
    for idx, row in df[df["role"] == "participant"].iterrows():
        q = row["entity_qid"]
        keep, reason = decisions.get(q, (True, "unknown"))
        if keep:
            keep_mask.at[idx] = True
        else:
            keep_mask.at[idx] = False
            excl_rows.append([
                row.get("conflict_qid",""),
                row.get("conflict_label",""),
                row.get("entity_qid",""),
                row.get("entity_label",""),
                reason,
                "|".join(cache.get(q, []))
            ])

    cleaned = df[keep_mask].reset_index(drop=True)
    print(f"[result] mantidas {cleaned.shape[0]} / {df.shape[0]} linhas "
          f"({(cleaned.shape[0]/max(1,df.shape[0])):.1%}). "
          f"Excluídas: {len(excl_rows)}")

    # escrever outputs
    cleaned.to_csv(OUT_CLEAN, sep=";", index=False, encoding="utf-8-sig")
    print(f"[ok] escrito → {OUT_CLEAN}")

    with OUT_EXCL.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["conflict_qid","conflict_label","entity_qid","entity_label","reason","p31_list"])
        w.writerows(excl_rows)
    print(f"[ok] exclusões → {OUT_EXCL}")

if __name__ == "__main__":
    main()
