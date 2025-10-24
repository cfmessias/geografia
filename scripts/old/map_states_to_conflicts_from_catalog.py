# scripts/map_states_to_conflicts_from_catalog.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import csv, sys, time, argparse, requests, json
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional

# --- paths & consts ---
PROJECT = Path(__file__).resolve().parent.parent
DATA    = PROJECT / "data"

CATALOG = DATA / "conflict_catalog.csv"                 # precisa de: conflict_qid;conflict_label
STATES  = DATA / "state_lineage_level2_details.csv"     # precisa de: qid;label;iso3 (nomes flexíveis)
OUT     = DATA / "conflicts_by_state.csv"               # state_qid;state_label;iso3;relation;conflict_qid;conflict_label;role_label
EMPTY   = DATA / "conflicts_by_state.empty.csv"         # state_qid;state_label;iso3
FAILED  = DATA / "conflicts_by_state.failed"            # erros ocasionais

# caches (opcionais, para não reindexar sempre o catálogo)
CACHE_P17   = DATA / "conflict_index_p17.json"          # { country_qid: [conflict_qid, ...] }
CACHE_P710  = DATA / "conflict_index_p710.json"         # { participant_qid: [conflict_qid, ...] }
CACHE_ROLES = DATA / "conflict_role_map.json"           # { "conflict|participant": ["role_qid", ...] }

UA       = "GeoMundi-States→Conflicts/1.1 (+cfmessias@gmail.com)"
TIMEOUT  = 90
RETRIES  = 4
LABEL_BATCH = 50

# --- utils ---
def sniff_delim(p: Path) -> str:
    txt = p.read_text(encoding="utf-8", errors="ignore")[:4096]
    for d in (";", ",", "\t", "|"):
        if d in txt: return d
    return ";"

def get_json(url: str, params: dict | None = None) -> dict:
    h = {"User-Agent": UA}
    for a in range(1, RETRIES+1):
        try:
            r = requests.get(url, params=params, headers=h, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
        except requests.RequestException:
            pass
        time.sleep(1.0 * a)
    raise RuntimeError(f"HTTP falhou: {url}")

def load_catalog(limit: Optional[int], offset: int) -> List[Tuple[str,str]]:
    if not CATALOG.exists(): raise FileNotFoundError(f"Falta {CATALOG}")
    sep = sniff_delim(CATALOG)
    out: List[Tuple[str,str]] = []
    with CATALOG.open("r", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f, delimiter=sep)
        cols = {c.lower(): c for c in (r.fieldnames or [])}
        qcol = cols.get("conflict_qid") or cols.get("qid") or list(cols.values())[0]
        lcol = cols.get("conflict_label") or cols.get("label") or ""
        for i, row in enumerate(r):
            if i < offset: continue
            q = (row.get(qcol) or "").strip().upper()
            if not q.startswith("Q"): continue
            lbl = (row.get(lcol) or "").strip() if lcol else ""
            out.append((q, lbl))
            if limit and len(out) >= limit: break
    return out

def load_states(limit: Optional[int], offset: int) -> List[Tuple[str,str,str]]:
    if not STATES.exists(): raise FileNotFoundError(f"Falta {STATES}")
    sep = sniff_delim(STATES)
    rows: List[Tuple[str,str,str]] = []
    with STATES.open("r", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f, delimiter=sep)
        cols = {c.lower(): c for c in (r.fieldnames or [])}
        qcol = cols.get("qid") or cols.get("state_qid") or list(cols.values())[0]
        lcol = cols.get("label") or cols.get("state_label") or ""
        icol = cols.get("iso3") or ""
        for i, row in enumerate(r):
            if i < offset: continue
            q = (row.get(qcol) or "").strip().upper()
            if not q.startswith("Q"): continue
            lbl = (row.get(lcol) or "").strip() if lcol else ""
            iso = (row.get(icol) or "").strip().upper() if icol else ""
            rows.append((q, lbl, iso))
            if limit and len(rows) >= limit: break
    return rows

def batch_labels(qids: List[str], cache: Dict[str,str]) -> None:
    ids = [q for q in qids if q and q.startswith("Q") and q not in cache]
    if not ids: return
    for i in range(0, len(ids), LABEL_BATCH):
        chunk = ids[i:i+LABEL_BATCH]
        js = get_json("https://www.wikidata.org/w/api.php", {
            "action": "wbgetentities",
            "format": "json",
            "props": "labels",
            "languages": "pt|en",
            "ids": "|".join(chunk)
        })
        for q, ent in (js.get("entities") or {}).items():
            labs = ent.get("labels") or {}
            lbl = (labs.get("pt") or {}).get("value") or (labs.get("en") or {}).get("value") or ""
            cache[q.upper()] = lbl

def build_index_from_catalog(conflicts: List[Tuple[str,str]],
                             reuse_cache: bool = True) -> Tuple[Dict[str,List[str]], Dict[str,List[str]], Dict[str,List[str]]]:
    """
    Devolve (by_p17, by_p710, rolemap)
      by_p17[country_qid]   -> [conflict_qid, ...]
      by_p710[participant_qid] -> [conflict_qid, ...]
      rolemap["conflict|participant"] -> [role_qid, ...]
    """
    if reuse_cache and CACHE_P17.exists() and CACHE_P710.exists() and CACHE_ROLES.exists():
        by_p17  = json.loads(CACHE_P17.read_text(encoding="utf-8"))
        by_p710 = json.loads(CACHE_P710.read_text(encoding="utf-8"))
        rolemap = json.loads(CACHE_ROLES.read_text(encoding="utf-8"))
        return by_p17, by_p710, rolemap

    by_p17: Dict[str, List[str]] = {}
    by_p710: Dict[str, List[str]] = {}
    rolemap: Dict[str, List[str]] = {}

    for i, (cqid, _clabel) in enumerate(conflicts, 1):
        try:
            js = get_json(f"https://www.wikidata.org/wiki/Special:EntityData/{cqid}.json")
            ent = js["entities"][cqid]
            claims = ent.get("claims", {})

            for st in claims.get("P17", []):
                try:
                    tgt = st["mainsnak"]["datavalue"]["value"]["id"].upper()
                    if tgt.startswith("Q"):
                        by_p17.setdefault(tgt, []).append(cqid)
                except Exception:
                    pass

            for st in claims.get("P710", []):
                try:
                    tgt = st["mainsnak"]["datavalue"]["value"]["id"].upper()
                    if tgt.startswith("Q"):
                        by_p710.setdefault(tgt, []).append(cqid)
                        # papéis P3831 (guarda QIDs; labels serão resolvidos depois)
                        roles = []
                        for qf in st.get("qualifiers", {}).get("P3831", []):
                            try:
                                rid = qf["datavalue"]["value"]["id"].upper()
                                if rid.startswith("Q"): roles.append(rid)
                            except Exception:
                                pass
                        if roles:
                            rolemap[f"{cqid}|{tgt}"] = roles
                except Exception:
                    pass

        except Exception as e:
            FAILED.write_text(FAILED.read_text(encoding="utf-8") + f"{cqid} | {type(e).__name__}:{e}\n" if FAILED.exists() else f"{cqid} | {type(e).__name__}:{e}\n")

        # flush parcial a cada 2000 conflitos (segurança)
        if i % 2000 == 0:
            CACHE_P17.write_text(json.dumps(by_p17), encoding="utf-8")
            CACHE_P710.write_text(json.dumps(by_p710), encoding="utf-8")
            CACHE_ROLES.write_text(json.dumps(rolemap), encoding="utf-8")

    # flush final
    CACHE_P17.write_text(json.dumps(by_p17), encoding="utf-8")
    CACHE_P710.write_text(json.dumps(by_p710), encoding="utf-8")
    CACHE_ROLES.write_text(json.dumps(rolemap), encoding="utf-8")
    return by_p17, by_p710, rolemap

def progress_line(i, total, ok, empties, written, t0):
    pct = int(i * 100 / max(1, total))
    elapsed = time.perf_counter() - t0
    rate = i / max(elapsed, 1e-9)
    remain = total - i
    eta_s = int(remain / max(rate, 1e-9))
    m, s = divmod(eta_s, 60)
    return (f"[{i}/{total}] {pct:3d}%  ok:{ok} vazios:{empties} linhas:{written}  "
            f"ETA {m:02d}:{s:02d}")

# --- main ---
def main():
    ap = argparse.ArgumentParser(
        description="Para cada estado (state_lineage_level2_details.csv), verifica no conflict_catalog em que conflitos entra (P17/P710)."
    )
    ap.add_argument("--limit-states", type=int)
    ap.add_argument("--offset-states", type=int, default=0)
    ap.add_argument("--limit-conflicts", type=int)
    ap.add_argument("--offset-conflicts", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=0.2)
    ap.add_argument("--rebuild-index", action="store_true", help="reconstrói o índice dos conflitos (ignora caches)")
    ap.add_argument("--no-ticker", dest="no_ticker", action="store_true", help="não atualizar a mesma linha; imprime linhas normais")
    ap.add_argument("--report-every", type=int, default=200, help="imprime um resumo a cada N estados (default: 200)")
    args = ap.parse_args()

    DATA.mkdir(parents=True, exist_ok=True)
    if not OUT.exists():
        with OUT.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f, delimiter=";").writerow(
                ["state_qid","state_label","iso3","relation","conflict_qid","conflict_label","role_label"]
            )
    if not EMPTY.exists():
        with EMPTY.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f, delimiter=";").writerow(["state_qid","state_label","iso3"])

    # 1) carregar catálogo & estados
    conflicts = load_catalog(args.limit_conflicts, args.offset_conflicts)
    states    = load_states(args.limit_states, args.offset_states)
    if not conflicts:
        print("Catálogo de conflitos vazio."); return
    if not states:
        print("Lista de estados vazia."); return

    print(f"catalogo: {len(conflicts)} conflitos | estados: {len(states)}")

    # 2) construir (ou carregar) índice invertido
    by_p17, by_p710, rolemap = build_index_from_catalog(conflicts, reuse_cache=not args.rebuild_index)
    print(f"índice: P17→{sum(len(v) for v in by_p17.values())} ligações ; "
          f"P710→{sum(len(v) for v in by_p710.values())} ligações ; "
          f"papéis(P3831): {len(rolemap)}")

    # 3) resolver labels dos papéis (P3831) de uma vez
    role_label_cache: Dict[str,str] = {}
    all_role_ids: List[str] = sorted({rid for v in rolemap.values() for rid in v})
    batch_labels(all_role_ids, role_label_cache)

    # 4) índices para não duplicar linhas já existentes
    seen: Set[Tuple[str,str,str]] = set()
    if OUT.exists():
        with OUT.open("r", encoding="utf-8", errors="ignore") as f:
            next(f, None)
            for line in f:
                p = line.rstrip("\n").split(";")
                if len(p) >= 5:
                    seen.add((p[0], p[3], p[4]))  # (state_qid, relation, conflict_qid)

    # 5) loop de escrita
    total = len(states)
    ok = 0; empties = 0; written = 0
    t0 = time.perf_counter()

    with OUT.open("a", newline="", encoding="utf-8") as fo:
        w = csv.writer(fo, delimiter=";")
        for i, (sqid, slabel, iso3) in enumerate(states, 1):
            wrote = False

            # conflitos como país (P17)
            for cq in by_p17.get(sqid, []):
                clabel = next((l for (q,l) in conflicts if q == cq), "")
                key = (sqid, "P17", cq)
                if key in seen: continue
                w.writerow([sqid, slabel, iso3, "P17", cq, clabel, ""])
                seen.add(key); wrote = True; written += 1

            # conflitos como participante (P710)
            for cq in by_p710.get(sqid, []):
                clabel = next((l for (q,l) in conflicts if q == cq), "")
                roles = rolemap.get(f"{cq}|{sqid}", [])
                role_labels = [role_label_cache.get(r, "") for r in roles]
                role_labels = [x for x in role_labels if x]
                key = (sqid, "P710", cq)
                if key in seen: continue
                w.writerow([sqid, slabel, iso3, "P710", cq, clabel, "; ".join(role_labels)])
                seen.add(key); wrote = True; written += 1

            if not wrote:
                with EMPTY.open("a", newline="", encoding="utf-8") as fe:
                    csv.writer(fe, delimiter=";").writerow([sqid, slabel, iso3])
                empties += 1
            else:
                ok += 1

            # evolução
            if args.no_ticker:
                print(f"{progress_line(i, total, ok, empties, written, t0)} — {sqid} {'ok' if wrote else 'vazio'}")
            else:
                sys.stdout.write("\r" + progress_line(i, total, ok, empties, written, t0))
                sys.stdout.flush()
                if i % max(1, args.report_every) == 0:
                    sys.stdout.write("\n" + progress_line(i, total, ok, empties, written, t0) + "\n")
                    sys.stdout.flush()

            time.sleep(args.sleep)

    if not args.no_ticker:
        print()
    mm, ss = divmod(int(time.perf_counter() - t0), 60)
    print(f"✔️ escrito: {OUT} | linhas:{written} | estados com conflitos:{ok} | vazios:{empties} | tempo {mm:02d}:{ss:02d}")

if __name__ == "__main__":
    main()
