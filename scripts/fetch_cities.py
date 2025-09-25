# scripts/fetch_cities.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import csv, os, sys, time, random
import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SEED_PATH    = PROJECT_ROOT / "data" / "countries_seed.csv"
OUT_PATH     = PROJECT_ROOT / "data" / "cities_all.csv"

# ====== CONFIG ======
TOP_N = 20
REFRESH_ALL: bool = False            # True: recria ficheiro
REFRESH_ISO3: set[str] = set()       # ex.: {"PRT","ESP"} para refazer só estes
SKIP_DONE: bool = True               # salta países já presentes quando não em refresh

# tempo/ritmo
TIMEOUT = 45
BASE_PAUSE = 0.35
COOLDOWN_EVERY = 20
COOLDOWN_SECS  = 8

# HTTP
WDQS = "https://query.wikidata.org/sparql"
UA   = "GeoCities/4.0 (contact: teu_email@exemplo)"  # põe um contacto real
WD_API = "https://www.wikidata.org/w/api.php"

HEAD = ["iso3","country","city","city_qid","admin","is_capital","population","year"]

# ---------- util ----------
def load_seed() -> pd.DataFrame:
    if not SEED_PATH.exists():
        print(f"❌ Falta {SEED_PATH}. Corre scripts/build_country_seed.py", file=sys.stderr)
        sys.exit(1)
    df = pd.read_csv(SEED_PATH)
    for c in ("name_pt","name_en"):
        if c not in df.columns: df[c] = ""
    return df

def remove_iso3_from_csv(path: Path, iso3s: set[str]) -> None:
    if not path.exists() or not iso3s: return
    df = pd.read_csv(path)
    keep = ~df["iso3"].astype(str).str.upper().isin({i.upper() for i in iso3s})
    if not keep.all(): df[keep].to_csv(path, index=False, encoding="utf-8")

def read_done_iso3() -> set[str]:
    if not OUT_PATH.exists(): return set()
    try:
        return set(pd.read_csv(OUT_PATH, usecols=["iso3"])["iso3"].astype(str).str.upper().unique())
    except Exception:
        return set()

# ---------- HTTP ----------
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
    """wbgetentities em batches para obter labels PT (fallback EN)."""
    out: dict[str,str] = {}
    if not qids: return out
    # remove vazios/duplicados
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

# ---------- SPARQL (sem labels!) ----------
def q_block(iso3: str) -> str:
    # captura cidades Q515 e municipalities Q15284 num só pedido, sem SERVICE label
    return f"""
SELECT ?city ?admin (MAX(?pop_) AS ?pop) (SAMPLE(?yr_) AS ?yr) ?isCap WHERE {{
  ?country wdt:P298 "{iso3}" .
  OPTIONAL {{ ?country wdt:P36 ?capital }}
  VALUES ?cls {{ wd:Q515 wd:Q15284 }}
  ?city wdt:P17 ?country ;
        wdt:P31/wdt:P279* ?cls ;
        wdt:P1082 ?pop_ .
  BIND(IF(BOUND(?capital) && ?city = ?capital, 1, 0) AS ?isCap)
  OPTIONAL {{ ?city p:P1082 ?st . OPTIONAL {{ ?st pq:P585 ?yr_ }} }}
  OPTIONAL {{ ?city wdt:P131 ?admin }}
}}
GROUP BY ?city ?admin ?isCap
ORDER BY DESC(?pop)
LIMIT {TOP_N}
"""

def parse_rows(js: dict) -> list[tuple]:
    out = []
    for r in js.get("results", {}).get("bindings", []):
        g = lambda k: r.get(k, {}).get("value")
        city_qid = (g("city") or "").split("/")[-1]
        admin_q  = (g("admin") or "").split("/")[-1] if g("admin") else ""
        cap      = 1 if (g("isCap") == "1") else 0
        pop      = g("pop")
        yr       = g("yr")
        try: pop = int(float(pop)) if pop else None
        except Exception: pop = None
        try: yr  = int(yr) if yr else None
        except Exception: yr = None
        out.append((city_qid, admin_q, cap, pop, yr))
    return out

# ---------- main ----------
def process_iso3(iso3: str, country_name: str, writer: csv.writer) -> bool:
    """Devolve True se escreveu linhas; False se falhou."""
    js = sparql_post(q_block(iso3))
    if not js:
        return False
    rows = parse_rows(js)
    if not rows:
        return True  # nada a gravar, mas não é falha
    # labels via API (muito mais leve)
    city_qids  = [q for q,_,_,_,_ in rows]
    admin_qids = [q for _,q,_,_,_ in rows if q]
    lbl_city   = wd_get_labels(city_qids, "pt")
    lbl_admin  = wd_get_labels(admin_qids, "pt") if admin_qids else {}

    # escrever (dedupe será feito depois por iso3+city_qid quando for lido)
    for cq, aq, cap, pop, yr in rows:
        writer.writerow([
            iso3, country_name,
            lbl_city.get(cq, cq), cq,
            lbl_admin.get(aq, ""), cap, pop, yr
        ])
    return True

def main():
    seed = load_seed()
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
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

    # segunda passada nos que falharam (timeout), com pausas maiores
        # segunda passada nos que falharam (timeout), com pausas maiores
        # segunda passada nos que falharam (timeout), com pausas maiores
    if failed:
        print(f"\n↻ Repetir países que falharam: {len(failed)}")
        time.sleep(3)
        retry_base_pause = BASE_PAUSE * 1.8  # usa variável local, evita 'global'
        for iso3, country in failed:
            print(f"[retry] {iso3} {country}")
            ok = process_iso3(iso3, country, w)
            f.flush(); os.fsync(f.fileno())
            # pequena pausa entre os retries
            time.sleep(retry_base_pause + random.uniform(0, 0.2))



    f.close()
    print(f"✔️ Atualizado {OUT_PATH}")

if __name__ == "__main__":
    main()
