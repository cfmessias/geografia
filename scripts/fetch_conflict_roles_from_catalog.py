# scripts/fetch_conflict_roles_from_catalog.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import csv, sys, time, argparse, requests
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

CATALOG   = DATA_DIR / "conflict_catalog.csv"              # precisa de conflict_qid;conflict_label
OUT_K     = DATA_DIR / "conflict_countries.csv"            # conflict_qid;conflict_label;country_qid;country_label
OUT_P     = DATA_DIR / "conflict_participants.csv"         # conflict_qid;conflict_label;participant_qid;participant_label;role_label
EMPTY_CSV = DATA_DIR / "conflicts_from_catalog.empty.csv"  # conflito sem P17 e/ou P710
DONE      = DATA_DIR / "conflicts_from_catalog.done"
FAILED    = DATA_DIR / "conflicts_from_catalog.failed"

UA      = "GeoMundi-ConflictRoles/1.0 (+cfmessias@gmail.com)"
TIMEOUT = 90
RETRY   = 4
LABEL_BATCH = 50  # até 50 ids por chamada ao wbgetentities

def sniff_delim(p: Path) -> str:
    txt = p.read_text(encoding="utf-8", errors="ignore")[:4096]
    for d in (";", ",", "\t", "|"):
        if d in txt: return d
    return ";"

def ensure_outputs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not OUT_K.exists():
        with OUT_K.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f, delimiter=";").writerow(
                ["conflict_qid","conflict_label","country_qid","country_label"]
            )
    if not OUT_P.exists():
        with OUT_P.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f, delimiter=";").writerow(
                ["conflict_qid","conflict_label","participant_qid","participant_label","role_label"]
            )
    if not EMPTY_CSV.exists():
        with EMPTY_CSV.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f, delimiter=";").writerow(
                ["conflict_qid","conflict_label","empty_what"]  # none/p17/p710/both
            )
    for p in (DONE, FAILED):
        if not p.exists(): p.write_text("", encoding="utf-8")

def load_catalog(limit: Optional[int], offset: int) -> List[Tuple[str,str]]:
    if not CATALOG.exists(): raise FileNotFoundError(f"Falta {CATALOG}")
    sep = sniff_delim(CATALOG)
    rows: List[Tuple[str,str]] = []
    with CATALOG.open("r", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f, delimiter=sep)
        qcol = None; lcol = None
        for c in (r.fieldnames or []):
            n = c.lower()
            if n in ("conflict_qid","qid"): qcol = c
            if n in ("conflict_label","label"): lcol = c
        if not qcol: qcol = (r.fieldnames or ["conflict_qid"])[0]
        if not lcol: lcol = (r.fieldnames or ["conflict_label"])[1 if len(r.fieldnames or [])>1 else 0]
        for i,row in enumerate(r):
            if i < offset: continue
            q = (row.get(qcol) or "").strip().upper()
            l = (row.get(lcol) or "").strip()
            if q.startswith("Q"):
                rows.append((q,l))
                if limit and len(rows) >= limit: break
    return rows

def load_done() -> Set[str]:
    return {ln.strip().upper() for ln in DONE.read_text(encoding="utf-8").splitlines() if ln.strip()} if DONE.exists() else set()

def append_done(qid: str):
    with DONE.open("a", encoding="utf-8") as f: f.write(qid.upper()+"\n")

def append_failed(qid: str, msg: str=""):
    with FAILED.open("a", encoding="utf-8") as f: f.write(qid.upper()+(" | "+msg if msg else "")+"\n")

def append_empty_row(qid: str, label: str, what: str):
    with EMPTY_CSV.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f, delimiter=";").writerow([qid, label, what])

def http_get_json(url: str, params: dict=None) -> dict:
    headers = {"User-Agent": UA}
    for a in range(1, RETRY+1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
            time.sleep(1.2*a)
        except requests.RequestException:
            time.sleep(1.2*a)
    raise RuntimeError(f"HTTP falhou para {url}")

def get_entity_json(qid: str) -> dict:
    url = f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
    return http_get_json(url)

def batch_labels(qids: List[str], cache: Dict[str,str]) -> None:
    # preenche cache com labels (pt→en), ignora já resolvidos
    ids = [q for q in qids if q not in cache]
    if not ids: return
    for i in range(0, len(ids), LABEL_BATCH):
        chunk = ids[i:i+LABEL_BATCH]
        params = {
            "action":"wbgetentities",
            "format":"json",
            "props":"labels",
            "ids":"|".join(chunk),
            "languages":"pt|en",
        }
        js = http_get_json("https://www.wikidata.org/w/api.php", params=params)
        ents = js.get("entities", {})
        for q, ent in ents.items():
            labs = (ent.get("labels") or {})
            lbl = (labs.get("pt") or {}).get("value") or (labs.get("en") or {}).get("value") or ""
            cache[q.upper()] = lbl

def fmt_eta(done, total, t0):
    if done == 0: return "ETA --:--"
    rate = done / max(1e-9, (time.perf_counter() - t0))
    rem  = total - done
    s    = int(rem / max(rate, 1e-9))
    m, s = divmod(s, 60)
    return f"ETA {m:02d}:{s:02d}"

def main():
    ap = argparse.ArgumentParser(description="Extrai P17 e P710 para cada conflito do conflict_catalog.csv (labels em lote).")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--offset", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=0.3)
    ap.add_argument("--no-ticker", action="store_true")
    args = ap.parse_args()

    ensure_outputs()
    all_conflicts = load_catalog(args.limit, args.offset)
    done_set = load_done()
    todo = [(q,l) for (q,l) in all_conflicts if q not in done_set]
    total = len(todo)
    if total == 0:
        print("Nada a fazer (todos em .done)."); return

    # índices para evitar duplicados
    seen_k: Set[Tuple[str,str]] = set()  # (conflict_qid, country_qid)
    seen_p: Set[Tuple[str,str]] = set()  # (conflict_qid, participant_qid)
    if OUT_K.exists():
        with OUT_K.open("r", encoding="utf-8", errors="ignore") as f:
            next(f, None)
            for line in f:
                p = line.rstrip("\n").split(";")
                if len(p) >= 3: seen_k.add((p[0], p[2]))
    if OUT_P.exists():
        with OUT_P.open("r", encoding="utf-8", errors="ignore") as f:
            next(f, None)
            for line in f:
                p = line.rstrip("\n").split(";")
                if len(p) >= 3: seen_p.add((p[0], p[2]))

    label_cache: Dict[str,str] = {}
    ok = 0; empty = 0; fails = 0; written = 0
    t0 = time.perf_counter()

    with OUT_K.open("a", newline="", encoding="utf-8") as fk, OUT_P.open("a", newline="", encoding="utf-8") as fp:
        wk = csv.writer(fk, delimiter=";")
        wp = csv.writer(fp, delimiter=";")

        for i, (cqid, clabel) in enumerate(todo, 1):
            status = ""
            try:
                # 1) ir ao JSON do conflito
                js = get_entity_json(cqid)
                ent = js["entities"][cqid]
                claims = ent.get("claims", {})

                # 2) recolher QIDs alvo
                k_qids: List[str] = []
                p_qids: List[str] = []
                p_roles: Dict[str, str] = {}  # participant_qid -> role_label (quando existir)

                for st in claims.get("P17", []):
                    try:
                        tgt = st["mainsnak"]["datavalue"]["value"]["id"].upper()
                        if tgt.startswith("Q"): k_qids.append(tgt)
                    except Exception:
                        pass

                for st in claims.get("P710", []):
                    try:
                        tgt = st["mainsnak"]["datavalue"]["value"]["id"].upper()
                        if tgt.startswith("Q"):
                            p_qids.append(tgt)
                            # papel via P3831 (opcional, resolve label já na 2.ª fase)
                            roles = []
                            for qf in st.get("qualifiers", {}).get("P3831", []):
                                try:
                                    rid = qf["datavalue"]["value"]["id"].upper()
                                    roles.append(rid)
                                except Exception:
                                    pass
                            # se houver papéis, iremos buscar labels e concatenar
                            if roles:
                                p_roles[tgt] = "|".join(roles)  # temporariamente ids; depois viram labels
                    except Exception:
                        pass

                # 3) resolver labels PT→EN em lote (países, participantes e papéis)
                to_label = list(set(k_qids + p_qids))
                # incluir roles ids (se houver) para ficarem já resolvidos
                role_ids = []
                for ids in p_roles.values():
                    for rid in ids.split("|"):
                        if rid and rid.startswith("Q"):
                            role_ids.append(rid)
                to_label += role_ids
                to_label = list({x for x in to_label if x.startswith("Q")})

                if to_label:
                    batch_labels(to_label, label_cache)

                # 4) escrever países
                wrote_any = False
                for k in k_qids:
                    lbl = label_cache.get(k, "")
                    if not lbl or lbl.upper()==k:  # ignora sem label
                        continue
                    key = (cqid, k)
                    if key in seen_k: continue
                    wk.writerow([cqid, clabel, k, lbl])
                    seen_k.add(key)
                    written += 1
                    wrote_any = True

                # 5) escrever participantes (com role_label resolvido)
                for p in p_qids:
                    lbl = label_cache.get(p, "")
                    if not lbl or lbl.upper()==p:
                        continue
                    role_lbl = ""
                    ids = p_roles.get(p, "")
                    if ids:
                        parts = []
                        for rid in ids.split("|"):
                            rl = label_cache.get(rid, "")
                            if rl and rl.upper()!=rid:
                                parts.append(rl)
                        role_lbl = "; ".join(parts)
                    key = (cqid, p)
                    if key in seen_p: continue
                    wp.writerow([cqid, clabel, p, lbl, role_lbl])
                    seen_p.add(key)
                    written += 1
                    wrote_any = True

                # 6) empties para auditoria
                empty_what = []
                if not k_qids: empty_what.append("p17")
                if not p_qids: empty_what.append("p710")
                if empty_what:
                    append_empty_row(cqid, clabel, "both" if len(empty_what)==2 else empty_what[0])
                else:
                    # havia ambos, mas se nenhum escreveu (por falta de labels), regista como none->labels
                    if not wrote_any:
                        append_empty_row(cqid, clabel, "labels")

                append_done(cqid)
                ok += 1
                status = "ok"

            except Exception as e:
                fails += 1
                append_failed(cqid, f"{type(e).__name__}:{e}")
                status = "falha"

            # ticker
            if args.no_ticker:
                print(f"[{i}/{total}] {cqid} — {status}")
            else:
                pct = int(i * 100 / total)
                eta = fmt_eta(i, total, t0)
                sys.stdout.write(f"\r[{i}/{total}] {pct:3d}%  ok:{ok} vazios:{empty} falhas:{fails} linhas:{written}  {eta}")
                sys.stdout.flush()

            time.sleep(args.sleep)

    if not args.no_ticker:
        print()
    elapsed = int(time.perf_counter() - t0)
    mm, ss = divmod(elapsed, 60)
    print(f"✔️ países → {OUT_K}")
    print(f"✔️ participantes → {OUT_P}")
    print(f"✔️ vazios → {EMPTY_CSV} | linhas escritas: {written} | ok:{ok} falhas:{fails} | tempo {mm:02d}:{ss:02d}")

if __name__ == "__main__":
    main()
