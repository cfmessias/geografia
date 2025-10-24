# scripts/map_qids_to_iso3.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import csv
import json
import random
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Set

import pandas as pd
import requests

# =======================
# CONFIG (hardcoded)
# =======================
DATA_DIR = Path("data")
ENRICHED = DATA_DIR / "conflicts_long_for_ui.enriched.csv"

OUT_MISSING = DATA_DIR / "tmp_qids_missing_iso3.csv"
OUT_MAPPED  = DATA_DIR / "tmp_qids_iso3_mapped.csv"
OUT_FAILED  = DATA_DIR / "tmp_qids_retry_later.csv"

USER_AGENT = "GeoMundi/iso3-mapper/1.2 (+https://cfmessias.pt; contact: cfmessias@gmail.com)"
WDQS_URL   = "https://query.wikidata.org/sparql"

# WDQS robustness
BATCH   = 8     # pequenos: WDQS anda instável
RETRIES = 8
TIMEOUT = 90
DELAY   = 1.8   # base do backoff (com jitter)

# Atualiza enriched in-place
OVERWRITE_ENRICHED = True


# =======================
# Helpers
# =======================
def _sniff_delim(path: Path) -> str:
    if not path.exists():
        return ";"
    sample = path.read_text(encoding="utf-8", errors="ignore")[:4096]
    for d in (";", ",", "|", "\t"):
        if d in sample:
            return d
    return ";"


def chunks(seq: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def wdqs_post_json(query: str, session: requests.Session) -> dict:
    """
    POST robusto ao WDQS:
      - Accept JSON (sem brotli)
      - Gere 429 (Retry-After) e 5xx
      - Backoff exponencial com jitter
      - Valida Content-Type; se não for JSON tenta parse; se falhar, lança
    """
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/sparql-results+json",
        "Accept-Encoding": "gzip, deflate",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }
    last_err = None
    for i in range(1, RETRIES + 1):
        try:
            resp = session.post(
                WDQS_URL,
                data={"query": query},
                headers=headers,
                timeout=TIMEOUT,
            )
            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                wait = float(ra) if ra else (2 ** i) + random.uniform(0, 0.6)
                print(f"[rate] 429; aguardar {wait:.1f}s…", flush=True)
                time.sleep(wait)
                continue
            if 500 <= resp.status_code < 600:
                raise RuntimeError(f"HTTP {resp.status_code}")

            ctype = resp.headers.get("Content-Type", "")
            if "application/sparql-results+json" not in ctype:
                try:
                    return resp.json()
                except Exception:
                    snip = resp.text[:160].replace("\n", " ")
                    raise RuntimeError(f"Resposta não-JSON (Content-Type={ctype}): {snip}")

            return resp.json()
        except Exception as e:
            last_err = e
            pause = min((2 ** i) * DELAY + random.uniform(0, 0.6), 45.0)
            print(f"[warn] WDQS tentativa {i}/{RETRIES} falhou ({e}); a aguardar {pause:.1f}s…", flush=True)
            time.sleep(pause)
    raise RuntimeError(f"WDQS falhou após {RETRIES} tentativas: {last_err}")


def read_missing_qids(enriched: Path) -> List[str]:
    if not enriched.exists():
        print(f"[erro] não encontrei {enriched}", file=sys.stderr)
        return []
    sep = _sniff_delim(enriched)
    df = pd.read_csv(enriched, sep=sep, dtype=str, encoding="utf-8-sig").fillna("")
    for c in ("mapped_iso3", "entity_qid", "role"):
        if c not in df.columns:
            df[c] = ""
    df["mapped_iso3"] = df["mapped_iso3"].astype(str).str.upper().str.strip()
    df["entity_qid"]  = df["entity_qid"].astype(str).str.strip()
    df["role"]        = df["role"].astype(str).str.lower().str.strip()

    miss = df[
        (df["role"] == "participant") &                      # ← só participantes
        (df["entity_qid"].str.startswith("Q")) &
        (df["mapped_iso3"].str.len() != 3)
    ]
    qids = sorted({q for q in miss["entity_qid"].tolist() if q})
    return qids


def write_qids(path: Path, qids: List[str]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.Series(qids, name="entity_qid").to_csv(path, sep=";", index=False, encoding="utf-8")
    return len(qids)


def load_resume(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path, sep=";", dtype=str).fillna("")
        df.columns = [c.strip().lower() for c in df.columns]
        if "entity_qid" in df.columns and "iso3" in df.columns:
            m = {str(r["entity_qid"]).strip(): str(r["iso3"]).strip().upper()
                 for _, r in df.iterrows() if str(r["entity_qid"]).strip()}
            return m
    except Exception:
        pass
    return {}


def append_mapping(path: Path, newmap: Dict[str, str]) -> None:
    # Acrescenta linhas novas; não duplica
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [{"entity_qid": q, "iso3": iso} for q, iso in newmap.items()]
    mode = "a" if path.exists() else "w"
    header = not path.exists()
    with path.open(mode, encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        if header:
            w.writerow(["entity_qid", "iso3"])
        for r in rows:
            w.writerow([r["entity_qid"], r["iso3"]])

HINTS = '  hint:Query hint:timeout "60000".'  # 60s por query no servidor

def query_iso3_fast(session: requests.Session, qids: list[str]) -> dict[str,str]:
    values = " ".join(f"wd:{q}" for q in qids if q)
    sparql = f"""
SELECT ?item ?iso3 WHERE {{
  VALUES ?item {{ {values} }}
{HINTS}
  {{
    ?item wdt:P298 ?iso3 .
  }}
  UNION {{
    ?item wdt:P17|wdt:P495 ?country .
    ?country wdt:P298 ?iso3 .
  }}
  FILTER(STRLEN(?iso3)=3)
}}
"""
    data = wdqs_post_json(sparql, session)
    out = {}
    for b in data.get("results", {}).get("bindings", []):
        out[b["item"]["value"].rsplit("/",1)[-1]] = b["iso3"]["value"].strip().upper()
    return out

def query_iso3_heavy(session: requests.Session, qids: list[str]) -> dict[str,str]:
    values = " ".join(f"wd:{q}" for q in qids if q)
    sparql = f"""
SELECT ?item ?iso3 WHERE {{
  VALUES ?item {{ {values} }}
{HINTS}
  {{
    ?item wdt:P3842 ?present .
    ?present wdt:P298 ?iso3 .
  }}
  UNION {{
    ?item wdt:P131+ ?place .
    ?place wdt:P17 ?country2 .
    ?country2 wdt:P298 ?iso3 .
  }}
  FILTER(STRLEN(?iso3)=3)
}}
"""
    data = wdqs_post_json(sparql, session)
    out = {}
    for b in data.get("results", {}).get("bindings", []):
        out[b["item"]["value"].rsplit("/",1)[-1]] = b["iso3"]["value"].strip().upper()
    return out



def map_qids(session: requests.Session, qids: List[str]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    failed: List[str] = []
    total_batches = (len(qids) + BATCH - 1) // BATCH

    for bi, block in enumerate(chunks(qids, BATCH), 1):
        block = [q for q in block if q not in mapping]
        if not block:
            continue
        got = 0
        try:
            # 1º passe (rápido)
            res1 = query_iso3_fast(session, block)
            mapping.update(res1)
            got += len(res1)

            # 2º passe (pesado) só para os que faltam
            remain = [q for q in block if q not in mapping]
            if remain:
                res2 = query_iso3_heavy(session, remain)
                mapping.update(res2)
                got += len(res2)

            print(f"[batch {bi}/{total_batches}] +{got} (acum {len(mapping)}/{len(qids)})", flush=True)

        except Exception as e:
            print(f"[warn] bloco {bi} falhou: {e}", flush=True)
            # fallback 1-a-1 neste bloco
            for q in block:
                try:
                    r = query_iso3_fast(session, [q])
                    if not r:
                        r = query_iso3_heavy(session, [q])
                    if r and q in r:
                        mapping[q] = r[q]
                        got += 1
                    else:
                        failed.append(q)
                except Exception:
                    failed.append(q)

            print(f"[fallback] bloco {bi} 1-a-1: +{got} (acum {len(mapping)}/{len(qids)})", flush=True)

        # pausa aleatória
        time.sleep(random.uniform(DELAY * 0.6, DELAY * 1.2))
        # flush incremental a cada 5 batches
        if bi % 5 == 0 and mapping:
            append_mapping(OUT_MAPPED, mapping)

    if failed:
        pd.Series(sorted(set(failed)), name="entity_qid").to_csv(OUT_FAILED, sep=";", index=False, encoding="utf-8")
        print(f"[info] falhados guardados em {OUT_FAILED} ({len(set(failed))})")
    return mapping



def merge_into_enriched(enriched: Path, mapping: Dict[str, str]) -> int:
    if not OVERWRITE_ENRICHED or not mapping:
        return 0
    sep = _sniff_delim(enriched)
    df = pd.read_csv(enriched, sep=sep, dtype=str, encoding="utf-8-sig").fillna("")
    if "entity_qid" not in df.columns:
        print("[warn] enriched sem coluna entity_qid; não consigo aplicar merge.")
        return 0
    if "mapped_iso3" not in df.columns:
        df["mapped_iso3"] = ""

    # só preenche onde está vazio/incorrecto
    def need_iso(v: str) -> bool:
        s = (v or "").strip().upper()
        return len(s) != 3

    filled = 0
    iso_col = df["mapped_iso3"].astype(str)
    for idx, row in df.iterrows():
        if need_iso(row.get("mapped_iso3", "")):
            q = str(row.get("entity_qid", "")).strip()
            iso = mapping.get(q)
            if iso and len(iso) == 3:
                df.at[idx, "mapped_iso3"] = iso
                filled += 1
                # marca origem
                if "mapped_iso3_source" not in df.columns:
                    df["mapped_iso3_source"] = ""
                if not str(df.at[idx, "mapped_iso3_source"]).strip():
                    df.at[idx, "mapped_iso3_source"] = "wdqs_auto"

    # escrever de volta (sempre com ;)
    tmp = enriched.with_suffix(".tmp.csv")
    df.to_csv(tmp, sep=";", index=False, encoding="utf-8")
    tmp.replace(enriched)
    return filled


# =======================
# MAIN
# =======================
def main() -> None:
    print(f"[1/4] A ler QIDs em falta de {ENRICHED} …", flush=True)
    qids = read_missing_qids(ENRICHED)
    print(f"    → {len(qids)} QIDs sem ISO3", flush=True)
    nmiss = write_qids(OUT_MISSING, qids)
    print(f"    [ok] Guardado {OUT_MISSING} ({nmiss} linhas)", flush=True)

    print(f"[2/4] A preparar recomeço/‘resume’…", flush=True)
    mapped_so_far = load_resume(OUT_MAPPED)
    if mapped_so_far:
        print(f"    [resume] já mapeados: {len(mapped_so_far)}", flush=True)
    qids_todo = [q for q in qids if q not in mapped_so_far]

    if not qids_todo:
        print("[skip] nada por fazer (tudo mapeado).", flush=True)
        filled = merge_into_enriched(ENRICHED, mapped_so_far)
        print(f"[merge] preenchidos {filled} ISO3 no enriched.", flush=True)
        return

    print(f"[3/4] A mapear QIDs → ISO3 (batches de {BATCH}) …", flush=True)
    sess = requests.Session()
    sess.headers.update({"User-Agent": USER_AGENT})

    newmap = map_qids(sess, qids_todo)

    # junta com o que já existia
    allmap = dict(mapped_so_far)
    allmap.update(newmap)

    if newmap:
        append_mapping(OUT_MAPPED, {k: v for k, v in newmap.items() if k not in mapped_so_far})
        print(f"    [ok] Guardado {OUT_MAPPED} (total {len(allmap)} mapeados)", flush=True)
    else:
        print("    [warn] nenhum novo mapeamento obtido nesta execução.", flush=True)

    print(f"[4/4] A aplicar mapeamentos ao enriched …", flush=True)
    filled = merge_into_enriched(ENRICHED, allmap)
    print(f"    [ok] enriched atualizado; {filled} linhas preenchidas com ISO3", flush=True)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[stop] interrompido pelo utilizador.", file=sys.stderr)
