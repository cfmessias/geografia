# scripts/fetch_cities.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import csv, os, sys, time, random
import pandas as pd
import requests

# ──────────────────────────────────────────────────────────────────────────────
# Caminhos
# ──────────────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SEED_PATH    = PROJECT_ROOT / "data" / "countries_seed.csv"
OUT_PATH     = PROJECT_ROOT / "data" / "cities_all.csv"
TMP_DIR      = PROJECT_ROOT / "data" / "tmp_cities"

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
TOP_N = 20                       # nº final de cidades por país
RAW_LIMIT = 1200                 # nº bruto sem ORDER BY (LIMIT obrigatório para evitar 504)
REFRESH_ALL: bool = False        # True: recria ficheiro de saída
REFRESH_ISO3: set[str] = set()   # ex.: {"PRT","ESP"} para reprocessar só estes
SKIP_DONE: bool = True           # salta ISO3 já presentes quando não em refresh

# Ritmo / tolerância
TIMEOUT = 90
BASE_PAUSE = 0.35
COOLDOWN_EVERY = 20
COOLDOWN_SECS  = 8

# HTTP
WDQS = "https://query.wikidata.org/sparql"
UA   = "GeografiaApp/1.0 (+https://example.org; contact: you@example.org)"  # coloca contacto real
WD_API = "https://www.wikidata.org/w/api.php"

# Cabeçalho (agora com lat/lon)
HEAD = ["iso3","country","city","city_qid","admin","is_capital","population","year","lat","lon"]

# ──────────────────────────────────────────────────────────────────────────────
# Util
# ──────────────────────────────────────────────────────────────────────────────
def load_seed() -> pd.DataFrame:
    if not SEED_PATH.exists():
        print(f"❌ Falta {SEED_PATH}. Corre scripts/build_country_seed.py", file=sys.stderr)
        sys.exit(1)
    df = pd.read_csv(SEED_PATH)
    for c in ("name_pt","name_en"):
        if c not in df.columns:
            df[c] = ""
    return df

def remove_iso3_from_csv(path: Path, iso3s: set[str]) -> None:
    if not path.exists() or not iso3s:
        return
    df = pd.read_csv(path)
    keep = ~df["iso3"].astype(str).str.upper().isin({i.upper() for i in iso3s})
    if not keep.all():
        df[keep].to_csv(path, index=False, encoding="utf-8")

def read_done_iso3() -> set[str]:
    if not OUT_PATH.exists():
        return set()
    try:
        return set(pd.read_csv(OUT_PATH, usecols=["iso3"])["iso3"].astype(str).str.upper().unique())
    except Exception:
        return set()

# ──────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ──────────────────────────────────────────────────────────────────────────────
SQS = requests.Session()
SQS.headers.update({"User-Agent": UA, "Accept": "application/sparql-results+json"})
SWD = requests.Session()
SWD.headers.update({"User-Agent": UA})

def sparql_post(q: str) -> dict | None:
    headers_q = {
        "User-Agent": UA,
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/sparql-query",
    }
    headers_form = {
        "User-Agent": UA,
        "Accept": "application/sparql-results+json",
    }
    for attempt in range(4):
        try:
            r = requests.post(WDQS, data=q.encode("utf-8"), headers=headers_q, timeout=TIMEOUT)
            if r.status_code == 400:
                r = requests.post(WDQS, data={"query": q, "format": "json"}, headers=headers_form, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            wait = BASE_PAUSE * (2**attempt) + random.uniform(0, 0.4)
            print(f"  … SPARQL falhou ({e}); retry em {wait:.1f}s", file=sys.stderr)
            time.sleep(wait)
    return None

def wd_get_labels(qids: list[str], lang="pt") -> dict[str,str]:
    out: dict[str,str] = {}
    if not qids:
        return out
    ids = [q for q in dict.fromkeys([q for q in qids if q])]
    for i in range(0, len(ids), 50):
        chunk = ids[i:i+50]
        for attempt in range(3):
            try:
                r = SWD.get(WD_API, params={
                    "action":"wbgetentities", "ids":"|".join(chunk),
                    "props":"labels", "languages":f"{lang}|en", "format":"json"
                }, timeout=20)
                r.raise_for_status()
                ents = r.json().get("entities", {})
                for q, e in ents.items():
                    lab = e.get("labels", {})
                    out[q] = lab.get(lang, {}).get("value") or lab.get("en", {}).get("value") or q
                break
            except Exception:
                time.sleep(0.4 * (attempt+1))
    return out

# ──────────────────────────────────────────────────────────────────────────────
# SPARQL SEM ORDER BY (LIMIT obrigatório para evitar timeouts)
# ──────────────────────────────────────────────────────────────────────────────
def q_block_no_order(iso3: str, raw_limit: int) -> str:
    """
    Sem ORDER BY e sem filtro de população.
    Aceita cidade/município/assentamento humano.
    Liga ao território por P17 direto OU via cadeia administrativa P131 até ao próprio item.
    """
    return f"""
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX p: <http://www.wikidata.org/prop/>
PREFIX ps: <http://www.wikidata.org/prop/statement/>
PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
PREFIX psv: <http://www.wikidata.org/prop/statement/value/>
PREFIX wikibase: <http://wikiba.se/ontology#>

SELECT ?city ?admin ?pop ?yr ?isCap ?lat ?lon WHERE {{
  # território/país identificado por ISO3
  ?country wdt:P298 "{iso3}" .

  OPTIONAL {{ ?country wdt:P36 ?capital . }}

  # classes aceites (e subtipos)
  VALUES ?cls {{ wd:Q515 wd:Q15284 wd:Q486972 }}
  ?city wdt:P31/wdt:P279* ?cls .

  # pertença: P17 direto OU cadeia P131 até ao próprio ?country
  {{
    ?city wdt:P17 ?country .
  }} UNION {{
    ?city (wdt:P131)+ ?country .
  }}

  # admin imediato (opcional)
  OPTIONAL {{ ?city wdt:P131 ?admin }}

  # população (opcional; podem existir vários statements)
  OPTIONAL {{
    ?city p:P1082 ?stpop .
    ?stpop ps:P1082 ?pop .
    OPTIONAL {{ ?stpop pq:P585 ?yr }}
  }}

  # coordenadas (opcional)
  OPTIONAL {{
    ?city p:P625 ?st .
    ?st psv:P625 ?coord .
    ?coord wikibase:geoLatitude ?lat ;
           wikibase:geoLongitude ?lon .
  }}

  BIND(IF(BOUND(?capital) && ?city = ?capital, 1, 0) AS ?isCap)
}}
LIMIT {raw_limit}
"""




# ──────────────────────────────────────────────────────────────────────────────
# Parse bruto
# ──────────────────────────────────────────────────────────────────────────────
def parse_rows(js: dict) -> list[tuple]:
    """Tuplos: (city_qid, admin_q, is_cap, pop, yr, lat, lon)."""
    out = []
    for r in js.get("results", {}).get("bindings", []):
        g = lambda k: r.get(k, {}).get("value")
        city_qid = (g("city") or "").split("/")[-1]
        admin_q  = (g("admin") or "").split("/")[-1] if g("admin") else ""
        cap      = 1 if g("isCap") in ("1","true","True") else 0
        pop, yr, lat, lon = g("pop"), g("yr"), g("lat"), g("lon")

        try: pop = int(float(pop)) if pop else None
        except: pop = None
        try: yr = int(yr) if yr else None
        except: yr = None
        try: lat = float(lat) if lat else None
        except: lat = None
        try: lon = float(lon) if lon else None
        except: lon = None

        out.append((city_qid, admin_q, cap, pop, yr, lat, lon))
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Pipeline por país: query -> tmp -> dedupe+sort local -> Top-N -> final -> apaga tmp
# ──────────────────────────────────────────────────────────────────────────────
def process_iso3(iso3: str, country_name: str, writer: csv.writer) -> bool:
    # 1) Query SEM ORDER BY (LIMIT obrigatório)
    js = sparql_post(q_block_no_order(iso3, raw_limit=RAW_LIMIT))
    raw = parse_rows(js) if js else []

    # 2) Guardar RAW em tmp SEMPRE (mesmo vazio) e mostrar caminho absoluto
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = (TMP_DIR / f"{iso3}.csv").resolve()
    df_raw = pd.DataFrame(raw, columns=["city_qid","admin_q","is_cap","pop","yr","lat","lon"])
    df_raw.insert(0, "iso3", iso3)
    df_raw.insert(1, "country", country_name)
    df_raw_tmp = df_raw.copy()
    df_raw_tmp = df_raw.copy()
    # atribui (se já existirem, sobrescreve; se não existirem, cria)
    df_raw_tmp["iso3"] = iso3
    df_raw_tmp["country"] = country_name
    # reordena para ficarem no início
    first = ["iso3", "country"]
    rest = [c for c in df_raw_tmp.columns if c not in first]
    df_raw_tmp = df_raw_tmp[first + rest]

    df_raw_tmp.to_csv(tmp_path, index=False, encoding="utf-8")
    df_raw_tmp.to_csv(tmp_path, index=False, encoding="utf-8")

    # 3) ficar só com a primeira ocorrência por cidade
    df = df_raw.loc[:, ["city_qid","admin_q","is_cap","pop","yr","lat","lon"]].copy()
    df = df.drop_duplicates(subset=["city_qid"], keep="first")

    # 4) ordenar por população (desc) e, em empate, por QID (asc) — sort estável
    df["_pop_sort"] = pd.to_numeric(df["pop"], errors="coerce").fillna(-1)
    df = df.sort_values(["_pop_sort","city_qid"], ascending=[False, True], kind="mergesort") \
        .drop(columns=["_pop_sort"])

    # 5) Top-N
    df_top = df.head(TOP_N).copy()
    print(f"[debug] {iso3}: {len(df_raw)} linhas brutas → tmp: {tmp_path}")

    if df_raw.empty:
        # nada para processar; deixa o tmp para inspecionar
        return False  # marca como 'falhou' para reaparecer na lista de retries

    # 3) (mantém o teu dedupe + sort por população desc + Top-N) …
    by_city: dict[str, dict] = {}
    for (cq, aq, cap, pop, yr, lat, lon) in raw:
        if not cq:
            continue
        cur = by_city.get(cq)
        if cur is None or (pop or -1) > (cur.get("pop") or -1):
            by_city[cq] = {"admin_q": aq, "is_cap": int(bool(cap)), "pop": pop, "yr": yr, "lat": lat, "lon": lon}
        else:
            if cap:
                cur["is_cap"] = 1
            if not cur.get("admin_q") and aq:
                cur["admin_q"] = aq

    rows = []
    for cq, d in by_city.items():
        rows.append([cq, d.get("admin_q",""), d.get("is_cap",0), d.get("pop"), d.get("yr"), d.get("lat"), d.get("lon")])
    df = pd.DataFrame(rows, columns=["city_qid","admin_q","is_cap","pop","yr","lat","lon"])

    df["_pop_sort"] = df["pop"].fillna(-1)
    df = df.sort_values(["_pop_sort","city_qid"], ascending=[False, True]).drop(columns=["_pop_sort"])
    df_top = df.head(TOP_N).copy()

    # 4) Salva também um preview das Top-N ao lado do tmp
    top_path = (TMP_DIR / f"{iso3}_top.csv").resolve()
    df_prev = df_top.copy()
    df_prev.insert(0, "iso3", iso3)
    df_prev.insert(1, "country", country_name)
    df_prev.to_csv(top_path, index=False, encoding="utf-8")
    print(f"[debug] {iso3}: Top {len(df_top)} preview → {top_path}")

    # 5) Labels e escrita no ficheiro final (igual ao teu)
    lbl_city  = wd_get_labels(df_top["city_qid"].tolist(), "pt")
    admin_qids = [x for x in df_top["admin_q"].tolist() if x]
    lbl_admin = wd_get_labels(admin_qids, "pt") if admin_qids else {}

    wrote = 0
    for _, r in df_top.iterrows():
        cq = r["city_qid"]; aq = r["admin_q"] or ""
        cap = int(r["is_cap"] or 0)
        pop = int(r["pop"]) if pd.notna(r["pop"]) else None
        yr  = int(r["yr"]) if pd.notna(r["yr"]) else None
        lat = float(r["lat"]) if pd.notna(r["lat"]) else None
        lon = float(r["lon"]) if pd.notna(r["lon"]) else None
        writer.writerow([
            iso3, country_name,
            lbl_city.get(cq, cq), cq,
            lbl_admin.get(aq, ""), cap, pop, yr,
            lat, lon
        ])
        wrote += 1

    print(f"[ok] {iso3}: Top {len(df_top)} escritas")
    # 6) REMOVE os temporários (como pediste)
    try:
        os.remove(tmp_path)
        os.remove(top_path)
    except OSError:
        pass

    return wrote > 0

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def main():
    seed = load_seed()

    # Processar APENAS os ISO3 pedidos (se REFRESH_ISO3 definido)
    if REFRESH_ISO3:
        only = {i.upper() for i in REFRESH_ISO3}
        before = len(seed)
        seed = seed[seed["iso3"].astype(str).str.upper().isin(only)].copy()
        print(f"[debug] seed filtrada: {len(seed)}/{before} países -> {sorted(only)}")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    if REFRESH_ALL and OUT_PATH.exists():
        OUT_PATH.unlink()
    if REFRESH_ISO3:
        remove_iso3_from_csv(OUT_PATH, REFRESH_ISO3)

    write_head = not OUT_PATH.exists()
    done = read_done_iso3()

    f = OUT_PATH.open("a", newline="", encoding="utf-8")
    w = csv.writer(f)
    if write_head:
        w.writerow(HEAD); f.flush(); os.fsync(f.fileno())

    processed = 0
    failed: list[tuple[str,str]] = []

    for _, r in seed.iterrows():
        iso3 = str(r["iso3"]).upper().strip()
        country = r.get("name_pt") or r.get("name_en") or iso3
        if not iso3:
            continue

        if SKIP_DONE and not REFRESH_ALL and (not REFRESH_ISO3 or iso3 not in {i.upper() for i in REFRESH_ISO3}):
            if iso3 in done:
                continue

        print(f"[cities] {iso3} {country}")
        ok = process_iso3(iso3, country, w)
        f.flush(); os.fsync(f.fileno())
        if ok:
            done.add(iso3)
        else:
            failed.append((iso3, country))

        processed += 1
        time.sleep(BASE_PAUSE + random.uniform(0,0.2))
        if processed % COOLDOWN_EVERY == 0:
            print(f"  … cooldown {COOLDOWN_SECS}s", file=sys.stderr)
            time.sleep(COOLDOWN_SECS)

    # (opcional) segunda passada nos que falharam
    if failed:
        print(f"\n↻ Repetir países que falharam: {len(failed)}")
        time.sleep(3)
        for iso3, country in failed:
            print(f"[retry] {iso3} {country}")
            ok = process_iso3(iso3, country, w)
            f.flush(); os.fsync(f.fileno())
            time.sleep(BASE_PAUSE * 1.8 + random.uniform(0, 0.2))

    f.close()
    print(f"✔️ Atualizado {OUT_PATH}")

if __name__ == "__main__":
    main()
