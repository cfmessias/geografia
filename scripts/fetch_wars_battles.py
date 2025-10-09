# -*- coding: utf-8 -*-
# scripts/war2.py
from __future__ import annotations

import csv
import sys
import time
import random
from pathlib import Path
from typing import Dict, Any, List, Optional, Iterable, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache

import requests
import pandas as pd

# ==============================
# Constantes / Paths
# ==============================
ROOT = Path(__file__).resolve().parents[1]
SEED = ROOT / "data" / "countries_seed.csv"
OUT  = ROOT / "data" / "wars_battles.csv"

UA = {"User-Agent": "Good2Know/1.0 (wars/battles; contact: cfmessias@gmail.com)"}
API = "https://www.wikidata.org/w/api.php"
WDQS = "https://query.wikidata.org/sparql"

# Desempenho
MAX_WORKERS = 6
BATCH_SIZE  = 50
SLEEP_BETWEEN_COUNTRIES = (0.20, 0.45)

# Tipos de conflito (usados como envelope; extraímos via claims)
CONFLICT_CLASSES = [
    "Q198",      # war
    "Q178561",   # battle
    "Q188055",   # siege
    "Q2001676",  # military conflict
    "Q350604",   # armed conflict
    "Q190771",   # military operation
]

# Alianças “macro” (para WWI/WWII) — expandidas via P527
MAJOR_ALLIANCES = {
    "Q36120": ["Q484653", "Q171831"],  # WWI: Allies, Central Powers
    "Q362":   ["Q53698",  "Q42406"],   # WWII: Allies, Axis
}

# =====================================================
# Sessão HTTP
# =====================================================
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(UA)
    ad = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=40, max_retries=3)
    s.mount("https://", ad); s.mount("http://", ad)
    return s

SESSION = make_session()

def _api_call(params: Dict[str, Any], attempts: int = 3, base_backoff: float = 1.0) -> Dict[str, Any]:
    for k in range(attempts):
        try:
            r = SESSION.get(API, params=params, timeout=30)
            r.raise_for_status()
            js = r.json()
            if "error" in js:
                raise RuntimeError(js["error"])
            return js
        except Exception:
            if k == attempts - 1:
                raise
            time.sleep(base_backoff * (k + 1) + random.uniform(0.0, 0.4))
    return {}

def _wdqs(query: str, attempts: int = 3, timeout: int = 60) -> List[Dict[str, Any]]:
    for k in range(attempts):
        try:
            r = SESSION.get(
                WDQS,
                params={"query": query, "format": "json"},
                headers={"Accept": "application/sparql-results+json"},
                timeout=timeout,
            )
            r.raise_for_status()
            return r.json().get("results", {}).get("bindings", [])
        except Exception as e:
            if k == attempts - 1:
                print(f"[wdqs] {type(e).__name__}: giving up")
                return []
            back = 2.5 * (k + 1) + random.uniform(0, 0.8)
            print(f"[wdqs] {type(e).__name__}: backoff {back:.1f}s")
            time.sleep(back)
    return []

# =====================================================
# Leitura do seed
# =====================================================
def _read_seed_df(path: Path) -> pd.DataFrame:
    for sep in (";", ","):
        try:
            df = pd.read_csv(path, dtype=str, sep=sep)
            if df.shape[1] > 1:
                break
        except Exception:
            continue
    else:
        df = pd.read_csv(path, dtype=str, engine="python")
    df.columns = [c.replace("\ufeff", "").strip().lower().replace(" ", "_") for c in df.columns]
    # normalizar ISO3
    for cand in ("iso3", "iso_3", "alpha3", "alpha_3", "iso-3", "alpha-3"):
        if cand in df.columns:
            df["iso3"] = df[cand]; break
    if "iso3" not in df.columns:
        raise KeyError(f"Coluna 'iso3' não encontrada em {path}")
    df["iso3"] = df["iso3"].astype(str).str.upper().str.strip()
    return df[["iso3"]].dropna().drop_duplicates()

# =====================================================
# Cache Wikidata
# =====================================================
class WikidataCache:
    def __init__(self):
        self.entities: Dict[str, Any] = {}
        self.labels: Dict[str, str]   = {}
        self._fetch_lock: Set[str]    = set()

CACHE = WikidataCache()

def wbgetentities_batch(ids: List[str], props: str) -> None:
    new_ids = [q for q in ids if q and q not in CACHE.entities and q not in CACHE._fetch_lock]
    if not new_ids:
        return
    CACHE._fetch_lock.update(new_ids)
    for i in range(0, len(new_ids), BATCH_SIZE):
        chunk = new_ids[i:i+BATCH_SIZE]
        params = {
            "action": "wbgetentities",
            "ids": "|".join(chunk),
            "props": props,
            "languages": "pt|en",
            "format": "json",
        }
        try:
            js = _api_call(params, attempts=3)
            ents = js.get("entities", {})
            CACHE.entities.update(ents)
        except Exception as e:
            print(f"[warn] wbgetentities_batch: {e}")
        if i + BATCH_SIZE < len(new_ids):
            time.sleep(0.05)
    CACHE._fetch_lock.difference_update(new_ids)

def ensure_claims(qids: Iterable[str]) -> None:
    lst = [q for q in set(qids) if q and q not in CACHE.entities]
    if not lst: return
    for i in range(0, len(lst), BATCH_SIZE):
        wbgetentities_batch(lst[i:i+BATCH_SIZE], props="claims")

def ensure_labels(qids: Iterable[str]) -> None:
    to_get = [q for q in set(qids) if q and q not in CACHE.labels]
    for i in range(0, len(to_get), 50):
        chunk = to_get[i:i+50]
        # pt primeiro
        params_pt = {"action":"wbgetentities","ids":"|".join(chunk),"props":"labels","languages":"pt","format":"json"}
        params_en = {"action":"wbgetentities","ids":"|".join(chunk),"props":"labels","languages":"en","format":"json"}
        try:
            r = SESSION.get(API, params=params_pt, timeout=30); r.raise_for_status()
            ents = r.json().get("entities", {})
            for q in chunk:
                lbl = ents.get(q, {}).get("labels", {}).get("pt", {}).get("value")
                if lbl: CACHE.labels[q] = lbl
        except Exception: pass
        missing = [q for q in chunk if q not in CACHE.labels]
        if missing:
            try:
                r = SESSION.get(API, params=params_en, timeout=30); r.raise_for_status()
                ents = r.json().get("entities", {})
                for q in missing:
                    lbl = ents.get(q, {}).get("labels", {}).get("en", {}).get("value")
                    CACHE.labels[q] = lbl or q
            except Exception:
                for q in missing:
                    CACHE.labels[q] = q
        time.sleep(0.1)

# =====================================================
# Helpers de claims
# =====================================================
def _targets(qid: str, pid: str) -> List[str]:
    ent = CACHE.entities.get(qid, {})
    res = []
    for cl in ent.get("claims", {}).get(pid, []) or []:
        v = cl.get("mainsnak", {}).get("datavalue", {}).get("value")
        if isinstance(v, dict) and v.get("id"):
            res.append(v["id"])
    return res

def _literal_times(qid: str, pid: str) -> List[str]:
    ent = CACHE.entities.get(qid, {})
    out = []
    for cl in ent.get("claims", {}).get(pid, []) or []:
        v = cl.get("mainsnak", {}).get("datavalue", {}).get("value")
        if isinstance(v, dict) and "time" in v:
            out.append(v["time"])
    return out

def _quantity_or_str(qid: str, pid: str) -> List[str]:
    ent = CACHE.entities.get(qid, {})
    out = []
    for cl in ent.get("claims", {}).get(pid, []) or []:
        v = cl.get("mainsnak", {}).get("datavalue", {}).get("value")
        if isinstance(v, dict) and "amount" in v:
            try:
                out.append(str(int(float(v["amount"]))));  # "±12345" → 12345
            except Exception:
                out.append(str(v["amount"]))
        elif isinstance(v, (str, int, float)):
            out.append(str(v))
    return out

def _y4(v: Optional[str]) -> str:
    if not v: return ""
    # valores de tempo em claims costumam vir como +01939-...
    if len(v) >= 5 and v[0] in "+-":
        return v[1:5]
    return v[:4]

# =====================================================
# ISO3 <-> QID
# =====================================================
_iso3_to_qid: Dict[str, str] = {}
_qid_to_iso3: Dict[str, str] = {}

def warm_iso3_to_qid(iso3_list: List[str]) -> None:
    vals = " ".join(f'"{c}"' for c in set(i.upper() for i in iso3_list))
    q = f'SELECT ?iso ?c WHERE {{ VALUES ?iso {{ {vals} }} ?c wdt:P298 ?iso . }}'
    for row in _wdqs(q, attempts=2):
        iso = row["iso"]["value"].upper()
        qid = row["c"]["value"].rpartition("/")[-1]
        _iso3_to_qid[iso] = qid
        _qid_to_iso3[qid] = iso

@lru_cache(maxsize=2000)
def _iso3_from_qid(country_qid: str) -> Optional[str]:
    return _qid_to_iso3.get(country_qid)

def _qid_from_iso3(iso3: str) -> Optional[str]:
    return _iso3_to_qid.get(iso3.upper())

# =====================================================
# Resolver ator → ISO3
# =====================================================
SUCCESSION_PROPS = ("P1365","P1366","P155","P156")

@lru_cache(maxsize=2000)
def _get_iso3_on_entity(qid: str) -> Optional[str]:
    ent = CACHE.entities.get(qid, {})
    for cl in ent.get("claims", {}).get("P298", []) or []:
        v = cl.get("mainsnak", {}).get("datavalue", {}).get("value")
        if isinstance(v, str) and len(v) == 3:
            return v.upper()
    return None

def _resolve_actor_to_iso3(actor_qid: str, max_depth: int = 3) -> List[str]:
    # junta QIDs a verificar
    to_ensure: Set[str] = {actor_qid}
    if actor_qid in CACHE.entities:
        to_ensure.update(_targets(actor_qid, "P17"))
    ensure_claims(to_ensure)

    out: List[str] = []
    iso = _get_iso3_on_entity(actor_qid)
    if iso: out.append(iso)

    # país do ator
    for c in _targets(actor_qid, "P17"):
        ensure_claims([c])
        iso = _get_iso3_on_entity(c)
        if iso: out.append(iso)

    # expandir por cadeia de sucessões (curta)
    visited: Set[str] = {actor_qid}
    frontier: List[Tuple[str,int]] = [(actor_qid, 0)]
    while frontier:
        cur, d = frontier.pop(0)
        if d >= max_depth: continue
        ensure_claims([cur])
        for pid in SUCCESSION_PROPS:
            for nb in _targets(cur, pid):
                if nb in visited: continue
                visited.add(nb); ensure_claims([nb])
                iso = _get_iso3_on_entity(nb)
                if iso: out.append(iso)
                frontier.append((nb, d + 1))
    # único por ordem natural
    seen, uniq = set(), []
    for i in out:
        if i not in seen:
            seen.add(i); uniq.append(i.upper())
    return uniq

# =====================================================
# Localização (P276) → país por P17 / subida P131+
# =====================================================
@lru_cache(maxsize=10000)
def _place_country_qid(place_qid: str, climb_limit: int = 8) -> Optional[str]:
    # 1) P17 direto
    ensure_claims([place_qid])
    for c in _targets(place_qid, "P17"):
        return c
    # 2) subir P131 até encontrar um nó com P17
    curr = [place_qid]; visited = set(curr); steps = 0
    while curr and steps < climb_limit:
        nxt = []
        for q in curr:
            ensure_claims([q])
            for adm in _targets(q, "P131"):
                if adm in visited: continue
                visited.add(adm)
                # tem P17?
                ensure_claims([adm])
                cands = _targets(adm, "P17")
                if cands:
                    return cands[0]
                nxt.append(adm)
        curr = nxt; steps += 1
    return None

# =====================================================
# Processamento de conflitos → linhas (participant + location + from_battle)
# =====================================================
COLS = [
    "iso3","country_qid","country_label",
    "conflict_qid","conflict_label","kind_qid","kind_label",
    "source","start_year","end_year","point_year",
    "part_of_qid","part_of_label","place_qid","place_label",
    "result_qid","result_label","deaths"
]

def _make_rows_for_conflict(conflict_qid: str, wanted_iso3: Set[str]) -> List[Dict[str,str]]:
    ensure_claims([conflict_qid])
    rows: List[Dict[str,str]] = []

    # claims do conflito
    kind_q    = next(iter(_targets(conflict_qid, "P31")),  "")
    part_of_q = next(iter(_targets(conflict_qid, "P361")), "")
    place_q   = next(iter(_targets(conflict_qid, "P276")), "")
    result_q  = next(iter(_targets(conflict_qid, "P1346")), "")
    deaths    = next(iter(_quantity_or_str(conflict_qid, "P1120")), "")

    start_y   = _y4(next(iter(_literal_times(conflict_qid, "P580")), ""))
    end_y     = _y4(next(iter(_literal_times(conflict_qid, "P582")), ""))
    point_y   = _y4(next(iter(_literal_times(conflict_qid, "P585")), ""))

    # --- (A) participant ---
    actors = set(_targets(conflict_qid, "P710"))
    actors.update(MAJOR_ALLIANCES.get(conflict_qid, []))
    if actors:
        # expandir alianças via P527
        ensure_claims(list(actors))
        expanded: Set[str] = set()
        for a in list(actors):
            for p in _targets(a, "P527"):
                expanded.add(p)
        if expanded:
            ensure_claims(list(expanded))
            actors.update(expanded)

    actor_iso3s: Set[str] = set()
    for a in actors:
        for iso in _resolve_actor_to_iso3(a, max_depth=3):
            if iso in wanted_iso3:
                actor_iso3s.add(iso)

    for iso3 in actor_iso3s:
        rows.append({
            "iso3": iso3,
            "country_qid": _qid_from_iso3(iso3) or "",
            "country_label": "",
            "conflict_qid": conflict_qid,
            "conflict_label": "",
            "kind_qid": kind_q,
            "kind_label": "",
            "source": "participant",
            "start_year": start_y,
            "end_year": end_y,
            "point_year": point_y,
            "part_of_qid": part_of_q,
            "part_of_label": "",
            "place_qid": place_q,
            "place_label": "",
            "result_qid": result_q,
            "result_label": "",
            "deaths": deaths,
        })

    # --- (B) location ---
    if place_q:
        ctry_q = _place_country_qid(place_q)
        if ctry_q:
            iso3 = _iso3_from_qid(ctry_q)
            if iso3 and iso3 in wanted_iso3:
                rows.append({
                    "iso3": iso3,
                    "country_qid": ctry_q,
                    "country_label": "",
                    "conflict_qid": conflict_qid,
                    "conflict_label": "",
                    "kind_qid": kind_q,
                    "kind_label": "",
                    "source": "location",
                    "start_year": start_y,
                    "end_year": end_y,
                    "point_year": point_y,
                    "part_of_qid": part_of_q,
                    "part_of_label": "",
                    "place_qid": place_q,
                    "place_label": "",
                    "result_qid": result_q,
                    "result_label": "",
                    "deaths": deaths,
                })

    # --- (C) from_battle: propaga para guerra-mãe (P361) ---
    extra: List[Dict[str,str]] = []
    for r in rows:
        p = r.get("part_of_qid") or ""
        if p and p != r.get("conflict_qid"):
            extra.append({
                **r,
                "source": "from_battle",
                "conflict_qid": p,
                "kind_qid": "",
                "place_qid": "",
                "result_qid": "",
                "deaths": "",
            })
    rows.extend(extra)

    return rows

def _dedup_global(rows: List[Dict[str,str]]) -> List[Dict[str,str]]:
    seen, out = set(), []
    for r in rows:
        key = (r.get("iso3",""), r.get("conflict_qid",""), r.get("source",""))
        if not r.get("conflict_qid"): 
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

# =====================================================
# Query por país → lista de conflitos (participantes e predecessores)
# =====================================================
def _conflicts_for_country_iso3(iso3: str) -> List[str]:
    q = f"""
SELECT DISTINCT ?conflict WHERE {{
  VALUES ?iso3 {{ "{iso3}" }}
  ?country wdt:P298 ?iso3 .

  {{ ?conflict wdt:P710 ?country }}
  UNION {{
    ?actor wdt:P17 ?country .
    ?conflict wdt:P710 ?actor .
  }}
  VALUES ?type {{ {" ".join(f"wd:{c}" for c in CONFLICT_CLASSES)} }}
  ?conflict wdt:P31/wdt:P279* ?type .
}}
LIMIT 1000
""".strip()
    rows = _wdqs(q, attempts=2)
    out = []
    for r in rows:
        qid = r.get("conflict", {}).get("value", "").rpartition("/")[-1]
        if qid.startswith("Q"):
            out.append(qid)
    return out


# =====================================================
# Escrita incremental
# =====================================================
class IncrementalWriter:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.seen: Set[Tuple[str,str,str]] = set()
        self.fh = None
        self.wr = None
        self._load_existing()

    def _load_existing(self):
        if not self.path.exists(): return
        try:
            with self.path.open("r", encoding="utf-8") as f:
                rd = csv.DictReader(f, delimiter=";")
                for row in rd:
                    key = (row.get("iso3",""), row.get("conflict_qid",""), row.get("source",""))
                    if key[1]:
                        self.seen.add(key)
            print(f"[info] já no arquivo: {len(self.seen)} registos")
        except Exception as e:
            print(f"[warn] erro a ler existentes: {e}")

    def open(self):
        mode = "a" if self.path.exists() else "w"
        self.fh = self.path.open(mode, newline="", encoding="utf-8")
        self.wr = csv.DictWriter(self.fh, fieldnames=COLS, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        if mode == "w":
            self.wr.writeheader()
            self.fh.flush()

    def write_rows(self, rows: List[Dict[str,str]]) -> int:
        if not rows: return 0
        # labels primeiro
        label_qs: Set[str] = set()
        for r in rows:
            label_qs.update([r.get("country_qid",""), r.get("conflict_qid",""),
                             r.get("kind_qid",""), r.get("part_of_qid",""),
                             r.get("place_qid",""), r.get("result_qid","")])
        ensure_labels([q for q in label_qs if q])

        written = 0
        for r in rows:
            key = (r.get("iso3",""), r.get("conflict_qid",""), r.get("source",""))
            if not key[1] or key in self.seen:
                continue
            # preencher labels
            r["country_label"]  = CACHE.labels.get(r.get("country_qid",""), r.get("country_qid",""))
            r["conflict_label"] = CACHE.labels.get(r.get("conflict_qid",""), r.get("conflict_qid",""))
            r["kind_label"]     = CACHE.labels.get(r.get("kind_qid",""), r.get("kind_qid",""))
            r["part_of_label"]  = CACHE.labels.get(r.get("part_of_qid",""), r.get("part_of_qid",""))
            r["place_label"]    = CACHE.labels.get(r.get("place_qid",""), r.get("place_qid",""))
            r["result_label"]   = CACHE.labels.get(r.get("result_qid",""), r.get("result_qid",""))
            self.wr.writerow({c: r.get(c,"") for c in COLS})
            self.seen.add(key)
            written += 1
        if written:
            self.fh.flush()
        return written

    def close(self):
        if self.fh:
            self.fh.close()
            self.fh = None
            self.wr = None

# =====================================================
# Pipelines
# =====================================================
def process_conflicts_for_country(iso3: str, conflict_qids: List[str]) -> List[Dict[str,str]]:
    wanted = {iso3.upper()}
    all_rows: List[Dict[str,str]] = []

    # garantir claims base de todos os conflitos num bloco
    ensure_claims(conflict_qids)

    # (participant + location + from_battle)
    for cq in conflict_qids:
        rows = _make_rows_for_conflict(cq, wanted)
        all_rows.extend(rows)

    # dedup
    return _dedup_global(all_rows)

def discover_conflicts_by_country_incremental(iso3_codes: List[str], writer: IncrementalWriter) -> int:
    total_new = 0
    for idx, iso3 in enumerate(iso3_codes, 1):
        print(f"\n[{idx}/{len(iso3_codes)}] País {iso3}...")
        conflicts = _conflicts_for_country_iso3(iso3)
        if not conflicts:
            print("  └─ sem conflitos")
            continue
        print(f"  └─ {len(conflicts)} conflitos")
        rows = process_conflicts_for_country(iso3, conflicts)
        # garantir country_qid presente
        for r in rows:
            if not r.get("country_qid"):
                r["country_qid"] = _qid_from_iso3(r["iso3"]) or ""
        wrote = writer.write_rows(rows)
        total_new += wrote
        print(f"  └─ +{wrote} (total {total_new})")
        time.sleep(random.uniform(*SLEEP_BETWEEN_COUNTRIES))
    return total_new

# =====================================================
# Main
# =====================================================
def main(argv: List[str]):
    mode = "country-based"
    specific_conflicts: List[str] = []
    only_iso: List[str] = []

    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--country-based":
            mode = "country-based"; i += 1
        elif a == "--conflicts" and i+1 < len(argv):
            mode = "specific"; specific_conflicts = [x.strip().upper() for x in argv[i+1].split(",") if x.strip()]; i += 2
        elif len(a) == 3 and a.isalpha():
            only_iso.append(a.upper()); i += 1
        else:
            i += 1

    df = _read_seed_df(SEED)
    iso_list = only_iso or df["iso3"].tolist()

    # aquecer ISO3 <-> QID
    warm_iso3_to_qid(iso_list)

    print(f"[info] Modo: {mode} | Países: {len(iso_list)}")
    print(f"[info] Output: {OUT}")

    if mode == "country-based":
        wr = IncrementalWriter(OUT); wr.open()
        try:
            total_new = discover_conflicts_by_country_incremental(iso_list, wr)
            print(f"\n[✓] Concluído. Novos registos: {total_new} | Total acumulado: {len(wr.seen)}")
        except KeyboardInterrupt:
            print("\n[warn] Interrompido — progresso ficou gravado.")
        finally:
            wr.close()
        return

    # modo "specific": processar lista de conflitos para todos os países
    conflicts = specific_conflicts
    if not conflicts:
        print("[warn] Nenhum conflito especificado."); return

    ensure_claims(conflicts)
    all_rows: List[Dict[str,str]] = []
    for iso in iso_list:
        rows = process_conflicts_for_country(iso, conflicts)
        for r in rows:
            if not r.get("country_qid"):
                r["country_qid"] = _qid_from_iso3(r["iso3"]) or ""
        all_rows.extend(rows)

    all_rows = _dedup_global(all_rows)
    if all_rows:
        # labels e guardar uma vez
        label_qs: Set[str] = set()
        for r in all_rows:
            label_qs.update([r.get("country_qid",""), r.get("conflict_qid",""),
                             r.get("kind_qid",""), r.get("part_of_qid",""),
                             r.get("place_qid",""), r.get("result_qid","")])
        ensure_labels([q for q in label_qs if q])

        for r in all_rows:
            r["country_label"]  = CACHE.labels.get(r.get("country_qid",""), r.get("country_qid",""))
            r["conflict_label"] = CACHE.labels.get(r.get("conflict_qid",""), r.get("conflict_qid",""))
            r["kind_label"]     = CACHE.labels.get(r.get("kind_qid",""), r.get("kind_qid",""))
            r["part_of_label"]  = CACHE.labels.get(r.get("part_of_qid",""), r.get("part_of_qid",""))
            r["place_label"]    = CACHE.labels.get(r.get("place_qid",""), r.get("place_qid",""))
            r["result_label"]   = CACHE.labels.get(r.get("result_qid",""), r.get("result_qid",""))

        OUT.parent.mkdir(parents=True, exist_ok=True)
        with OUT.open("w", newline="", encoding="utf-8") as f:
            wr = csv.DictWriter(f, fieldnames=COLS, delimiter=";")
            wr.writeheader()
            for r in all_rows:
                wr.writerow({c: r.get(c, "") for c in COLS})
        print(f"[save] {OUT} ({len(all_rows)} linhas)")
    else:
        print("[info] Sem linhas para guardar.")

if __name__ == "__main__":
    main(sys.argv[1:])
