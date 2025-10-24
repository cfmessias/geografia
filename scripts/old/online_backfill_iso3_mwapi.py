# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import csv, time, random
from typing import Dict, List, Iterable, Tuple, Set
import requests
import pandas as pd
import json

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

FORMS_CSV          = DATA_DIR / "forms_all.csv"                # opcional: iso3;qid
CLAIMS_CACHE_CSV   = DATA_DIR / "tmp_mw_claims_cache.csv"      # qid;json
COUNTRY_ISO_CACHE  = DATA_DIR / "tmp_iso3_country_cache.csv"   # country_qid;iso3
RESOLVED_CACHE_CSV = DATA_DIR / "tmp_iso3_resolved_cache.csv"  # entity_qid;iso3;source

# ===== Columns =====
COL_ISO3_FILLED = "mapped_iso3_filled"
COL_ISO3        = "mapped_iso3"
COL_ISO3_ONLINE = "mapped_iso3_online"
COL_SRC_ONLINE  = "mapped_iso3_online_source"
COL_FINAL       = "mapped_iso3_final"
COL_ROLE        = "role"
COL_EQID        = "entity_qid"

# ===== API =====
MW_API     = "https://www.wikidata.org/w/api.php"
USER_AGENT = "GeoMundi/online-iso3-mwapi/1.0 (+contact)"
BATCH      = 50        # limite do wbgetentities
TIMEOUT    = 45
RETRIES    = 5

# ===== Utils =====
def chunks(seq: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def _sleep_backoff(attempt: int) -> None:
    wait = min(30, 1.7**attempt) + random.uniform(0.1, 0.5)
    time.sleep(wait)

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s

def _read_df(path: Path) -> pd.DataFrame:
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

# ===== Simple CSV caches =====
def _load_kv_cache(path: Path, k: str, v: str) -> Dict[str, str]:
    if not path.exists(): return {}
    out: Dict[str, str] = {}
    try:
        df = pd.read_csv(path, sep=";", dtype=str, keep_default_na=False)
        for _, r in df.iterrows():
            key = str(r.get(k,"")).strip()
            val = str(r.get(v,"")).strip()
            if key:
                out[key] = val
    except Exception:
        pass
    return out

def _save_kv_cache(path: Path, data: Dict[str, str], k: str, v: str) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow([k, v])
        for key, val in sorted(data.items()):
            w.writerow([key, val])

def _load_resolved_cache(path: Path) -> Dict[str, Tuple[str,str]]:
    if not path.exists(): return {}
    out: Dict[str, Tuple[str,str]] = {}
    try:
        df = pd.read_csv(path, sep=";", dtype=str, keep_default_na=False)
        for _, r in df.iterrows():
            q = str(r.get("entity_qid","")).strip()
            iso = str(r.get("iso3","")).strip().upper()
            src = str(r.get("source","")).strip()
            if q and iso and len(iso)==3:
                out[q] = (iso, src)
    except Exception:
        pass
    return out

def _save_resolved_cache(path: Path, data: Dict[str, Tuple[str,str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["entity_qid","iso3","source"])
        for q, (iso, src) in sorted(data.items()):
            w.writerow([q, iso, src])

# ===== Forms map (fallback país->ISO3) =====
def _load_forms_map() -> Dict[str, str]:
    m: Dict[str, str] = {}
    if not FORMS_CSV.exists(): return m
    try:
        df = pd.read_csv(FORMS_CSV, sep=";", dtype=str, keep_default_na=False)
        cols = {c.lower(): c for c in df.columns}
        qcol = cols.get("qid") or cols.get("form_qid")
        icol = cols.get("iso3")
        if not qcol or not icol: return m
        for _, r in df.iterrows():
            q = str(r[qcol]).strip()
            i = str(r[icol]).strip().upper()
            if q.startswith("Q") and len(i)==3:
                m[q] = i
    except Exception:
        pass
    return m

# ===== MW API: wbgetentities (claims) =====
def _fetch_entities_claims(sess: requests.Session, qids: List[str]) -> dict:
    params = {
        "action": "wbgetentities",
        "ids": "|".join(qids),
        "props": "claims",
        "format": "json",
    }
    for att in range(1, RETRIES+1):
        try:
            r = sess.get(MW_API, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json().get("entities", {}) or {}
        except Exception as e:
            print(f"[warn] claims batch falhou ({e}) → retry {att}/{RETRIES}")
            _sleep_backoff(att)
    return {}

def _extract_targets(claims: dict, pid: str) -> List[str]:
    out: List[str] = []
    for snak in claims.get(pid, []) or []:
        dv = (snak.get("mainsnak") or {}).get("datavalue") or {}
        if dv.get("type") == "wikibase-entityid":
            q = (dv.get("value") or {}).get("id") or ""
            if q.startswith("Q"):
                out.append(q)
    return out

def _claims_cache_load() -> Dict[str, str]:
    # qid -> json string
    if not CLAIMS_CACHE_CSV.exists(): return {}
    out: Dict[str, str] = {}
    try:
        df = pd.read_csv(CLAIMS_CACHE_CSV, sep=";", dtype=str, keep_default_na=False)
        for _, r in df.iterrows():
            q = str(r.get("qid","")).strip()
            js = str(r.get("claims_json","")).strip()
            if q and js:
                out[q] = js
    except Exception:
        pass
    return out

def _claims_cache_save(data: Dict[str, str]) -> None:
    """
    Guarda cache de claims em CSV (qid;claims_json), de forma atómica.
    Normaliza o JSON para linha única para evitar quebras de linha no CSV.
    """
    tmp = CLAIMS_CACHE_CSV.with_suffix(CLAIMS_CACHE_CSV.suffix + ".tmp")

    with tmp.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writerow(["qid", "claims_json"])
        for q in sorted(data.keys()):
            js = data[q]
            # normalizar para JSON compacto e monolinha
            try:
                js_obj = json.loads(js) if isinstance(js, str) else js
                js = json.dumps(js_obj, ensure_ascii=False, separators=(",", ":"))
            except Exception:
                # se não for JSON válido, grava como veio (o csv.writer trata das aspas)
                js = str(js)
            w.writerow([q, js])

    # overwrite atómico
    tmp.replace(CLAIMS_CACHE_CSV)

# ===== País -> ISO3 via P298 =====
def _fetch_country_iso3(sess: requests.Session, country_qids: List[str]) -> Dict[str, str]:
    if not country_qids: return {}
    params = {
        "action": "wbgetentities",
        "ids": "|".join(country_qids),
        "props": "claims",
        "format": "json",
    }
    out: Dict[str, str] = {}
    for att in range(1, RETRIES+1):
        try:
            r = sess.get(MW_API, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            ents = r.json().get("entities", {}) or {}
            for q, ent in ents.items():
                claims = ent.get("claims", {}) or {}
                # P298 → Padrão ISO 3166-1 alfa-3
                vals = _extract_targets(claims, "P298")  # P298 é string, não entidade
                # Como P298 é string, extrairemos pelo snakvalue:
                iso = ""
                for sn in claims.get("P298", []) or []:
                    dv = (sn.get("mainsnak") or {}).get("datavalue") or {}
                    v = dv.get("value")
                    if isinstance(v, str) and len(v.strip()) == 3:
                        iso = v.strip().upper()
                        break
                if iso:
                    out[q] = iso
            return out
        except Exception as e:
            print(f"[warn] country P298 batch falhou ({e}) → retry {att}/{RETRIES}")
            _sleep_backoff(att)
    return out

# ===== Resolver entidade -> país (cadeias) =====
def resolve_entity_country_qids(entity_claims: dict, claims_cache: Dict[str, dict], sess: requests.Session) -> Tuple[list[str], str]:
    """
    Tenta encontrar país(es) (QIDs) para uma entidade.
    Retorna ([country_qids], how). Pode devolver vários (ex.: humanos com múltiplos P27).
    Estratégia:
      1) Se P31 inclui Q5 (humano) → usar P27 (country of citizenship) [p27]
         (se vazio, tenta P19 -> P131+ -> P17 [p19_path])
      2) P3842 (present-day country) [p3842_present]
      3) P17 | P495 [p17 / p495]
      4) P131+ -> P17 [p131_to_p17]
      5) P159 -> P131+ -> P17 [p159_to_p131_to_p17]
    """
    def _has_p31_q5(claims: dict) -> bool:
        for snak in claims.get("P31", []) or []:
            dv = (snak.get("mainsnak") or {}).get("datavalue") or {}
            if dv.get("type") == "wikibase-entityid":
                if (dv.get("value") or {}).get("id") == "Q5":
                    return True
        return False

    # Humanos → P27
    if _has_p31_q5(entity_claims):
        p27 = _extract_targets(entity_claims, "P27")
        if p27:
            return (p27, "p27")
        # fallback humano sem P27: P19 -> P131+ -> P17
        p19 = _extract_targets(entity_claims, "P19")
        if p19:
            # caminhar P131 a partir do lugar de nascimento
            def p131_walk(qs: list[str], max_depth: int = 3) -> str:
                frontier = list(qs); seen: set[str] = set()
                for _ in range(max_depth):
                    if not frontier: break
                    need = [q for q in frontier if q not in claims_cache]
                    if need:
                        ents = _fetch_entities_claims(sess, need)
                        for q in need:
                            claims_cache[q] = (ents.get(q) or {}).get("claims", {}) or {}
                        time.sleep(0.2 + random.random()*0.3)
                    nxt = []
                    for q in frontier:
                        if q in seen: continue
                        seen.add(q)
                        c = claims_cache.get(q, {})
                        p17 = _extract_targets(c, "P17")
                        if p17:
                            return p17[0]
                        nxt.extend(_extract_targets(c, "P131"))
                    frontier = nxt
                return ""
            ctry = p131_walk(p19, 3)
            if ctry:
                return ([ctry], "p19_path")

    # 2) P3842
    p3842 = _extract_targets(entity_claims, "P3842")
    if p3842:
        return (p3842, "p3842_present")

    # 3) P17 / P495
    for pid, tag in (("P17","p17"), ("P495","p495")):
        tgt = _extract_targets(entity_claims, pid)
        if tgt:
            return (tgt, tag)

    # 4) P131+ → P17
    def p131_walk(qs: List[str], max_depth: int = 3) -> str:
        frontier = list(qs); seen: set[str] = set()
        for _ in range(max_depth):
            if not frontier: break
            need = [q for q in frontier if q not in claims_cache]
            if need:
                ents = _fetch_entities_claims(sess, need)
                for q in need:
                    claims_cache[q] = (ents.get(q) or {}).get("claims", {}) or {}
                time.sleep(0.2 + random.random()*0.3)
            nxt = []
            for q in frontier:
                if q in seen: continue
                seen.add(q)
                c = claims_cache.get(q, {})
                p17 = _extract_targets(c, "P17")
                if p17:
                    return p17[0]
                nxt.extend(_extract_targets(c, "P131"))
            frontier = nxt
        return ""

    p131s = _extract_targets(entity_claims, "P131")
    if p131s:
        ctry = p131_walk(p131s, 3)
        if ctry:
            return ([ctry], "p131_to_p17")

    # 5) P159 -> P131+ -> P17
    p159 = _extract_targets(entity_claims, "P159")
    if p159:
        ctry = p131_walk(p159, 3)
        if ctry:
            return ([ctry], "p159_to_p131_to_p17")

    return ([], "")


# ===== Main =====
def main():
    print(f"[in] {IN_CSV}")
    df = _read_df(IN_CSV)

    # base ISO3 (já preenchido offline, se existir)
    iso_base = df.get(COL_ISO3_FILLED, df.get(COL_ISO3, pd.Series("", index=df.index))).astype(str).str.upper()

    # candidatos: participants sem iso3
    mask_part = df.get(COL_ROLE, "").astype(str).str.lower().eq("participant")
    no_iso    = iso_base.str.len() != 3
    qids = sorted({q for q in df.loc[mask_part & no_iso, COL_EQID].astype(str) if q.startswith("Q")})
    print(f"[todo] participantes únicos sem ISO3: {len(qids)}")

    # caches
    claims_json_cache = _claims_cache_load()            # qid -> json str
    claims_cache: Dict[str, dict] = {}
    for q, js in claims_json_cache.items():
        try:
            claims_cache[q] = json.loads(js)   # antes: pd.io.json.loads
        except Exception:
            claims_cache[q] = {}

    country_iso_cache = _load_kv_cache(COUNTRY_ISO_CACHE, "country_qid", "iso3")  # país -> iso3
    resolved_cache    = _load_resolved_cache(RESOLVED_CACHE_CSV)                  # entity -> (iso3,source)
    forms_map         = _load_forms_map()

    done = set(resolved_cache.keys())
    pend = [q for q in qids if q not in done]
    print(f"[cache] resolvidos: {len(done)} · por fazer agora: {len(pend)}")

    sess = _session()

    # 1) obter claims das entidades pendentes
    for blk in chunks(pend, BATCH):
        ents = _fetch_entities_claims(sess, blk)
        for q in blk:
            claims = (ents.get(q) or {}).get("claims") or {}
            claims_cache[q] = claims
            claims_json_cache[q] = json.dumps(claims, ensure_ascii=False)
        _claims_cache_save(claims_json_cache)
        time.sleep(0.25 + random.random()*0.5)

    # 2) resolver país(es) e ISO3
    mapped_now = 0

    # resolved_cache: entity -> (iso3_list_str, source)
    # (para humanos com múltiplos P27, guardamos vários ISO3 separados por '|')
    for q in pend:
        claims = claims_cache.get(q, {})
        country_qids, how = resolve_entity_country_qids(claims, claims_cache, sess)
        if not country_qids:
            continue

        iso_list: list[str] = []
        need_iso = [c for c in country_qids if c not in country_iso_cache]
        if need_iso:
            iso_map = _fetch_country_iso3(sess, need_iso)
            if iso_map:
                for cqid, iso in iso_map.items():
                    if iso:
                        country_iso_cache[cqid] = iso
                _save_kv_cache(COUNTRY_ISO_CACHE, country_iso_cache, "country_qid", "iso3")
            time.sleep(0.2 + random.random()*0.3)

        for cqid in country_qids:
            iso = country_iso_cache.get(cqid, "")
            if not iso and forms_map:
                iso = forms_map.get(cqid, "")
            if iso and len(iso)==3:
                iso_list.append(iso)

        if iso_list:
            resolved_cache[q] = ("|".join(dict.fromkeys(iso_list)), how or "mwapi")
            mapped_now += 1
            if mapped_now % 200 == 0:
                _save_resolved_cache(RESOLVED_CACHE_CSV, resolved_cache)

    _save_resolved_cache(RESOLVED_CACHE_CSV, resolved_cache)
    print(f"[mwapi] preenchidos agora: {mapped_now} · resolvidos (cache total): {len(resolved_cache)}")

    # 3) aplicar no DataFrame
    conf_countries: dict[str, set[str]] = {}
    sub = df[(df.get(COL_ROLE,"").astype(str).str.lower()=="country") & (df.get(COL_ISO3,"").astype(str).str.len()==3)]
    for cq, g in sub.groupby("conflict_qid"):
        conf_countries[cq] = set(g[COL_ISO3].str.upper().unique())

    df[COL_ISO3_ONLINE] = ""
    df[COL_SRC_ONLINE]  = ""

    for idx, row in df.iterrows():
        if str(row.get(COL_ROLE,"")).lower() != "participant":
            continue
        # já tem ISO3 (offline ou original)?
        base = iso_base.iat[idx] if idx < len(iso_base) else ""
        if base and len(base)==3:
            continue

        eq = str(row.get(COL_EQID,""))
        if not eq or eq not in resolved_cache:
            continue

        iso_multi, how = resolved_cache[eq]
        cand_iso = [x.strip().upper() for x in str(iso_multi).split("|") if len(x.strip())==3]

        if not cand_iso:
            continue

        # se houver múltiplos, preferir o que consta como país do conflito desta linha
        cq = str(row.get("conflict_qid",""))
        match = ""
        if cq in conf_countries and conf_countries[cq]:
            for iso in cand_iso:
                if iso in conf_countries[cq]:
                    match = iso
                    break

        chosen = match or cand_iso[0]
        df.at[idx, COL_ISO3_ONLINE] = chosen
        df.at[idx, COL_SRC_ONLINE]  = how if how else "mwapi"

    # 4) coluna final
    def pick_final(i: int) -> str:
        a = str(df.at[i, COL_ISO3_FILLED]) if COL_ISO3_FILLED in df.columns else ""
        b = str(df.at[i, COL_ISO3_ONLINE]) if COL_ISO3_ONLINE in df.columns else ""
        c = str(df.at[i, COL_ISO3])        if COL_ISO3 in df.columns else ""
        for v in (a, b, c):
            if len(v) == 3:
                return v
        return ""
    df[COL_FINAL] = [pick_final(i) for i in df.index]

    df.to_csv(OUT_CSV, sep=";", index=False, encoding="utf-8-sig")
    print(f"[ok] escrito → {OUT_CSV}")
    print(f"[sum] total com ISO3_final: {(df[COL_FINAL].str.len()==3).sum()} / {len(df)}")

if __name__ == "__main__":
    main()
