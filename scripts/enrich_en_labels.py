# scripts/enrich_en_labels.py
# Preenche labels EN nos CSV já extraídos em PT, usando Wikidata wbgetentities.
from __future__ import annotations
import argparse, time, math, re, csv, sys, random
from pathlib import Path
from typing import Iterable, Dict, List, Tuple, Set

import requests
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

# ficheiros alvo (podes ajustar)
FILES = [
    DATA / "country_languages_official.csv",
    DATA / "country_languages_used.csv",
    DATA / "colonization.csv",
    DATA / "wars_battles.csv",
    DATA / "wars_battles_comprehensive.csv",
]

API_URL = "https://www.wikidata.org/w/api.php"
HEADERS = {"User-Agent": "Good2Know/label-enricher/1.0 (contact@example.com)"}

QID_RE = re.compile(r"(?:^|/)(Q\d+)$", re.IGNORECASE)

def extract_qid(val: str | float | int) -> str | None:
    if pd.isna(val):
        return None
    s = str(val).strip()
    m = QID_RE.search(s)
    return m.group(1).upper() if m else None

def batched(iterable: Iterable[str], size: int) -> Iterable[List[str]]:
    batch: List[str] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch

def fetch_labels_en(qids: List[str], *, langs=("en",), max_retries=6, timeout=60) -> Dict[str, Dict[str,str]]:
    """Retorna {qid: {'en': 'Label'}} usando wbgetentities (em lotes ≤ 40)."""
    out: Dict[str, Dict[str, str]] = {}
    if not qids:
        return out
    for chunk in batched(qids, 40):
        params = {
            "action": "wbgetentities",
            "ids": "|".join(chunk),
            "props": "labels",
            "languages": "|".join(langs),
            "format": "json"
        }
        last_text = None
        for attempt in range(max_retries):
            try:
                r = requests.post(API_URL, data=params, headers=HEADERS, timeout=timeout)
                last_text = r.text
                r.raise_for_status()
                js = r.json()
                for qid, ent in js.get("entities", {}).items():
                    labels = ent.get("labels", {}) if isinstance(ent, dict) else {}
                    out[qid.upper()] = {lg: labels.get(lg, {}).get("value", "") for lg in langs}
                break
            except (requests.ReadTimeout, requests.ConnectTimeout):
                time.sleep((2 ** attempt) + random.random() * 0.5)
                continue
            except requests.HTTPError as e:
                if r is not None and r.status_code in (429, 500, 502, 503, 504):
                    time.sleep((2 ** attempt) + random.random() * 0.5)
                    continue
                body = (last_text or "")[:400]
                raise requests.HTTPError(f"API error {getattr(r,'status_code',None)}: {e} — {body}...") from e
    return out

def plan_columns(df: pd.DataFrame) -> List[Tuple[str, str]]:
    """
    Decide mapeamentos (col_qid -> col_label_en).
    Regras:
      - *_qid -> *_label_en   (ex.: lang_qid -> lang_label_en)
      - *_ID  -> base_en      (ex.: Conflict_ID -> Conflict_en)
      - qid_country -> country_label_en (caso comum)
    """
    cols: List[Tuple[str,str]] = []
    for c in df.columns:
        lc = c.lower()
        if lc.endswith("_qid"):
            base = c[:-4]
            target = f"{base}_label_en"
            cols.append((c, target))
        elif lc.endswith("_id"):
            base = c[:-3]
            target = f"{base}_en"
            cols.append((c, target))
        elif lc == "qid_country":
            cols.append((c, "country_label_en"))
    return cols

def enrich_file(path: Path, inplace: bool) -> Path | None:
    if not path.exists():
        print(f"[skip] não existe: {path}")
        return None

    # separador: muitos dos teus CSV usam ';'
    try:
        df = pd.read_csv(path, sep=";", dtype=str)
    except Exception:
        df = pd.read_csv(path, dtype=str)

    plan = plan_columns(df)
    if not plan:
        print(f"[info] {path.name}: sem colunas QID para traduzir.")
        return None

    # junta todas as QIDs a resolver
    qids: Set[str] = set()
    for qcol, _ in plan:
        if qcol not in df.columns:
            continue
        for v in df[qcol]:
            q = extract_qid(v)
            if q:
                qids.add(q)

    if not qids:
        print(f"[info] {path.name}: 0 QIDs detectadas.")
        return None

    # vai buscar labels EN
    labels = fetch_labels_en(sorted(qids))
    # preencher colunas alvo
    for qcol, tgt in plan:
        if qcol not in df.columns:
            continue
        # cria alvo se faltar
        if tgt not in df.columns:
            df[tgt] = ""
        # só preenche onde está vazio
        mask = df[tgt].isna() | (df[tgt].astype(str).str.strip() == "")
        if not mask.any():
            continue
        new_vals: List[str] = []
        for v in df[qcol]:
            q = extract_qid(v)
            lab = labels.get(q, {}).get("en", "") if q else ""
            new_vals.append(lab)
        df.loc[mask, tgt] = pd.Series(new_vals).where(mask, df[tgt])

    # decidir saída
    if inplace:
        out = path
    else:
        out = path.with_suffix(path.suffix.replace(".csv", "") + ".enriched.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, sep=";", index=False, encoding="utf-8")
    print(f"[save] {out}")
    return out

def main():
    ap = argparse.ArgumentParser(description="Enriquece CSVs com labels EN via Wikidata wbgetentities.")
    ap.add_argument("--inplace", action="store_true", help="Escrever por cima dos CSVs originais.")
    ap.add_argument("--files", nargs="*", help="Lista de CSVs (default: conhecidos).")
    args = ap.parse_args()

    files = [Path(f) for f in (args.files or FILES)]
    for p in files:
        try:
            enrich_file(p, inplace=args.inplace)
        except Exception as e:
            print(f"[err] {p.name}: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
