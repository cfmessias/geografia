# scripts/build_lineage_level2_from_seed.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import csv, sys, time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
SEED_CSV     = DATA_DIR / "countries_profiles.csv"           # colunas: qid, iso3
OUT_MINIMAL  = DATA_DIR / "state_lineage_level2.csv"          # QID;Iso3
OUT_DETAILS  = DATA_DIR / "state_lineage_level2_details.csv"  # Iso3Start;Level;QID;Label;FormationYear;SourceFlag
DONE_FILE    = DATA_DIR / "state_lineage_level2.done"

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT      = "GeoMundi-LineageL2/1.4 (+cfmessias@gmail.com)"
REQUEST_TIMEOUT = 90
RETRY_MAX       = 4
BACKOFF_BASE_S  = 6
THROTTLE_S      = 0.8

CLS_VALUES = """
VALUES ?cls {
  wd:Q3624078  # sovereign state
  wd:Q6256     # country
  wd:Q417175   # kingdom
  wd:Q3024240  # former country
  wd:Q48349    # empire
  wd:Q7269     # monarchy
  wd:Q7270     # republic
  wd:Q41614    # caliphate
  wd:Q184558   # sultanate
  wd:Q133156   # colony
  wd:Q170156   # confederation
  wd:Q179164   # federation
  wd:Q28108    # commonwealth
}
""".strip()

# ------------- I/O helpers -------------
def ensure_outputs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not OUT_MINIMAL.exists():
        with OUT_MINIMAL.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f, delimiter=";").writerow(["QID", "Iso3"])
    if not OUT_DETAILS.exists():
        with OUT_DETAILS.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f, delimiter=";").writerow(
                ["Iso3Start","Level","QID","Label","FormationYear","SourceFlag"]
            )
    if not DONE_FILE.exists():
        DONE_FILE.write_text("", encoding="utf-8")

def read_seed() -> List[Tuple[str,str]]:
    def _read_with(p: Path, delim: str) -> List[Tuple[str,str]]:
        out: List[Tuple[str,str]] = []
        with p.open("r", encoding="utf-8", errors="ignore") as f:
            r = csv.DictReader(f, delimiter=delim)
            if not r.fieldnames: return []
            fields = { (n or "").strip().lower(): n for n in r.fieldnames }
            k_qid = fields.get("qid"); k_iso = fields.get("iso3") or fields.get("iso")
            if not k_qid or not k_iso: return []
            for row in r:
                qid  = str(row.get(k_qid,"")).strip().upper()
                iso3 = str(row.get(k_iso,"")).strip().upper()
                if qid.startswith("Q") and iso3:
                    out.append((qid, iso3))
        return out
    if not SEED_CSV.exists():
        raise FileNotFoundError(f"Seed CSV não encontrado: {SEED_CSV}")
    rows = _read_with(SEED_CSV, ";") or _read_with(SEED_CSV, ",")
    if not rows:
        raise ValueError("Não encontrei colunas 'qid' e 'iso3' em countries_profiles.csv.")
    # dedupe por iso3 (primeira ocorrência vence)
    seen: Set[str] = set(); dedup: List[Tuple[str,str]] = []
    for qid, iso in rows:
        if iso not in seen:
            seen.add(iso); dedup.append((qid, iso))
    return dedup

def load_done_iso3() -> Set[str]:
    try:
        return {ln.strip().upper() for ln in DONE_FILE.read_text(encoding="utf-8").splitlines() if ln.strip()}
    except FileNotFoundError:
        return set()

def append_done_iso3(iso3: str) -> None:
    with DONE_FILE.open("a", encoding="utf-8") as f:
        f.write(iso3.upper() + "\n")

def write_minimal(rows: Iterable[Tuple[str,str]]) -> int:
    cnt = 0
    with OUT_MINIMAL.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        for qid, iso3 in rows:
            w.writerow([qid, iso3.upper()]); cnt += 1
    return cnt

def write_details(rows: Iterable[Tuple[str,int,str,str,Optional[int],str]]) -> int:
    cnt = 0
    with OUT_DETAILS.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        for iso3, level, qid, label, year, flag in rows:
            w.writerow([iso3.upper(), level, qid, label, year if year is not None else "", flag]); cnt += 1
    return cnt

# ------------- WDQS helpers -------------
def run_sparql(query: str) -> Dict:
    headers = {"Accept":"application/sparql-results+json","User-Agent":USER_AGENT}
    for attempt in range(1, RETRY_MAX+1):
        try:
            resp = requests.post(SPARQL_ENDPOINT, data={"query": query}, headers=headers, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
            sys.stderr.write(f"[warn] HTTP {resp.status_code}: {resp.text[:200]}\n")
        except requests.RequestException as e:
            sys.stderr.write(f"[err] {type(e).__name__}: {e}\n")
        sleep_s = BACKOFF_BASE_S * (2 ** (attempt-1))
        sys.stderr.write(f"[info] retry {attempt}/{RETRY_MAX} em {sleep_s}s…\n")
        time.sleep(sleep_s)
    raise RuntimeError("Falha após retries no WDQS.")

def fetch_label(qid: str) -> str:
    try:
        js = run_sparql(f"""
SELECT ?itemLabel WHERE {{
  VALUES ?item {{ wd:{qid} }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
}}""")
        return js["results"]["bindings"][0]["itemLabel"]["value"] if js["results"]["bindings"] else qid
    except Exception:
        return qid

def fetch_predecessors(qid: str) -> List[Tuple[str,str,Optional[int]]]:
    q = f"""
SELECT DISTINCT ?rel ?relLabel ?relQID ?formation WHERE {{
  VALUES ?item {{ wd:{qid} }}
  {CLS_VALUES}
  {{ ?item wdt:P1365 ?rel }} UNION {{ ?item wdt:P155 ?rel }}
  ?rel wdt:P31/wdt:P279* ?cls .
  OPTIONAL {{ ?rel wdt:P571 ?formation }}
  BIND(STRAFTER(STR(?rel), "entity/") AS ?relQID)
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
}}
""".strip()
    js = run_sparql(q)
    out: List[Tuple[str,str,Optional[int]]] = []
    for b in js.get("results", {}).get("bindings", []):
        rq  = b.get("relQID", {}).get("value","")
        rl  = b.get("relLabel", {}).get("value","")
        fy  = b.get("formation", {}).get("value","")
        year: Optional[int] = None
        if fy:
            try: year = int(fy[:4])
            except Exception: year = None
        if rq:
            out.append((rq, rl, year))
    return out

def fetch_successors(qid: str) -> Set[str]:
    """
    Sucessores (P1366 ou P156) → devolve QIDs (string 'Qxxxx').
    """
    q = f"""
SELECT DISTINCT ?relQID WHERE {{
  VALUES ?item {{ wd:{qid} }}
  {{ ?item wdt:P1366 ?rel }} UNION {{ ?item wdt:P156 ?rel }}
  BIND(STRAFTER(STR(?rel), "entity/") AS ?relQID)
}}
""".strip()
    js = run_sparql(q)
    out: Set[str] = set()
    for b in js.get("results", {}).get("bindings", []):
        rq = b.get("relQID", {}).get("value","").strip()
        if rq: out.add(rq)
    return out

def fetch_p3842_iso3(qid: str) -> Set[str]:
    q = f"""
SELECT DISTINCT ?iso WHERE {{
  VALUES ?rel {{ wd:{qid} }}
  ?rel wdt:P3842 ?modern .
  ?modern wdt:P298 ?iso .
}}
"""
    js = run_sparql(q)
    out: Set[str] = set()
    for b in js.get("results", {}).get("bindings", []):
        iso = b.get("iso", {}).get("value","").strip().upper()
        if iso: out.add(iso)
    return out

# ----------- Fase B: resolver conflitos -----------
def resolve_iso_for_qid(
    qid: str,
    candidates: Set[str],
    country_qid_to_iso: Dict[str,str],
) -> Set[str]:
    """
    Resolve QIDs com >1 ISO3 candidatos usando SUCESSORES → ISO3 modernos.
    Retorna o(s) ISO3 final(is). Normalmente 0, 1 ou >1 (split real).
    """
    if len(candidates) <= 1:
        return candidates

    # 1) Tentar via sucessores → países modernos (present-day)
    succs = fetch_successors(qid)
    iso_by_successor = { country_qid_to_iso[sq] for sq in succs if sq in country_qid_to_iso }
    if len(iso_by_successor) == 1:
        # caso do Q200464: só aponta p/ Q45 → {PRT}
        return iso_by_successor

    if len(iso_by_successor) > 1:
        # divisão real via sucessores (raro): aceitar múltiplos
        return iso_by_successor

    # 2) Sem sucessores modernos → tentar P3842
    via_p3842 = fetch_p3842_iso3(qid)
    if len(via_p3842) == 1:
        return via_p3842
    if len(via_p3842) > 1:
        # split real via P3842
        return via_p3842

    # 3) Último recurso: manter “o primeiro observado” (estável) — evita desaparecer
    # (a ordem final é determinada pela inserção no set original; aqui devolvemos o próprio set)
    return candidates

# ---------------- Main ----------------
def main() -> None:
    ensure_outputs()

    # seed: [(country_qid, iso3)]
    seeds = read_seed()
    done_iso3 = load_done_iso3()

    # mapas úteis
    country_qid_to_iso: Dict[str,str] = {qid: iso for qid, iso in seeds}  # Q45 -> PRT, Q1011 -> CPV, ...
    label_cache: Dict[str,str] = {}

    # coletores Fase A (pré-resultado)
    prelim_min: Set[Tuple[str,str]] = set()  # (QID, ISO3)
    prelim_det: List[Tuple[str,int,str,str,Optional[int],str]] = []  # Iso3,Level,QID,Label,Year,Flag
    # índice temporário para contagem/ordem dos candidatos por QID
    qid_to_iso_candidates: Dict[str, List[str]] = {}

    for i, (seed_qid, seed_iso) in enumerate(seeds, 1):
        if seed_iso in done_iso3:
            continue

        print(f"[{i}/{len(seeds)}] {seed_iso} — {seed_qid}", flush=True)

        # nível 0
        if seed_qid not in label_cache:
            label_cache[seed_qid] = fetch_label(seed_qid)
        prelim_min.add((seed_qid, seed_iso))
        prelim_det.append((seed_iso, 0, seed_qid, label_cache[seed_qid], None, "seed"))

        # nível 1 (predecessores do seed)
        lvl1 = fetch_predecessors(seed_qid)
        seen1: Set[str] = set()
        for rel_qid, rel_label, rel_year in lvl1:
            if rel_qid in seen1: 
                continue
            seen1.add(rel_qid)
            label_cache.setdefault(rel_qid, rel_label or rel_qid)

            # adicionar candidato ISO3 = seed_iso (e marcar; desambiguação virá na Fase B)
            qid_to_iso_candidates.setdefault(rel_qid, []).append(seed_iso)
            prelim_min.add((rel_qid, seed_iso))
            prelim_det.append((seed_iso, 1, rel_qid, label_cache[rel_qid], rel_year, "seed_evidence"))
            time.sleep(THROTTLE_S)

        # nível 2 (predecessores dos predecessores)
        for rel_qid, _, _ in lvl1:
            lvl2 = fetch_predecessors(rel_qid)
            seen2: Set[str] = set()
            for r2_qid, r2_label, r2_year in lvl2:
                if r2_qid in seen2:
                    continue
                seen2.add(r2_qid)
                label_cache.setdefault(r2_qid, r2_label or r2_qid)

                qid_to_iso_candidates.setdefault(r2_qid, []).append(seed_iso)
                prelim_min.add((r2_qid, seed_iso))
                prelim_det.append((seed_iso, 2, r2_qid, label_cache[r2_qid], r2_year, "seed_evidence"))
                time.sleep(THROTTLE_S)

        append_done_iso3(seed_iso)
        time.sleep(THROTTLE_S)

    # --------- Fase B: resolver conflitos por SUCESSORES ---------
    # candidatos finais por QID
    final_qid_to_iso: Dict[str, Set[str]] = {}
    for qid, iso_list in qid_to_iso_candidates.items():
        cands: List[str] = []
        # preserva ordem de primeira observação
        for iso in iso_list:
            if iso not in cands:
                cands.append(iso)
        uniq: Set[str] = set(cands)
        if len(uniq) <= 1:
            final_qid_to_iso[qid] = uniq
        else:
            resolved = resolve_iso_for_qid(qid, uniq, country_qid_to_iso)
            final_qid_to_iso[qid] = resolved

    # Constrói resultados finais (filtrando os pares preliminares que foram descartados)
    final_min: Set[Tuple[str,str]] = set()
    final_det: List[Tuple[str,int,str,str,Optional[int],str]] = []

    # nível 0 (seeds) fica sempre
    final_min.update({ (qid, iso) for (qid, iso) in prelim_min if qid in country_qid_to_iso })

    # níveis 1 e 2 (predecessores), respeitando resolução
    for iso3, level, qid, label, year, flag in prelim_det:
        if flag == "seed":
            final_det.append((iso3, level, qid, label, year, flag))
            continue
        ok_iso = final_qid_to_iso.get(qid, {iso3})
        if iso3 in ok_iso:
            final_min.add((qid, iso3))
            # se houve conflito e ficou resolvido por sucessor/P3842, podemos refinar Flag
            if len(ok_iso) == 1 and len(set(qid_to_iso_candidates.get(qid, []))) > 1:
                # conflito resolvido → assinalar
                final_det.append((iso3, level, qid, label, year, "successor_resolved"))
            else:
                final_det.append((iso3, level, qid, label, year, flag))

    # Escrita
    # (limpa ficheiros de output para evitar resíduos; se preferires append, comenta estes dois blocos)
    OUT_MINIMAL.write_text("QID;Iso3\n", encoding="utf-8")
    OUT_DETAILS.write_text("Iso3Start;Level;QID;Label;FormationYear;SourceFlag\n", encoding="utf-8")

    write_minimal(sorted(final_min))
    write_details(final_det)

    print("✔️ Concluído.")
    print(f"   Minimal:  {OUT_MINIMAL}")
    print(f"   Detalhes: {OUT_DETAILS}")
    print(f"   Done:     {DONE_FILE}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[info] Interrompido. Progresso gravado.", file=sys.stderr)
        sys.exit(130)
