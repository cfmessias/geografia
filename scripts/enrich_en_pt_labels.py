# scripts/enrich_labels_pt_en.py
# Preenche labels PT/EN nos CSV já extraídos, usando Wikidata wbgetentities
from __future__ import annotations
import argparse, time, re, sys, random, json
from pathlib import Path
from typing import Iterable, Dict, List, Tuple, Set

import requests
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

# ficheiros alvo por defeito (podes ajustar)
FILES = [
    
    #DATA / "wars_battles.csv",
    #DATA / "country_languages_official.csv",
    #DATA / "country_languages_used.csv",
    ##DATA / "colonization.csv",
    DATA / "colonies_all.csv",
    #DATA / "conflict_catalog.csv",
]

API_URL = "https://www.wikidata.org/w/api.php"
HEADERS = {"User-Agent": "Good2Know/label-enricher/2.0 (+streamlit-ui)"}

QID_RE = re.compile(r"(?:^|/)(Q\d+)$", re.IGNORECASE)

def extract_qid(val) -> str | None:
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
            yield batch; batch = []
    if batch:
        yield batch

def fetch_labels_multi(qids: List[str], *, langs=("pt","en"), max_retries=6, timeout=60) -> Dict[str, Dict[str,str]]:
    """Retorna {qid: {'pt': '…','en':'…'}} via wbgetentities (lotes ≤ 40)."""
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
                ents = js.get("entities", {}) or {}
                for qid, ent in ents.items():
                    labels = ent.get("labels", {}) if isinstance(ent, dict) else {}
                    out[qid.upper()] = {lg: (labels.get(lg, {}) or {}).get("value", "") for lg in langs}
                break
            except (requests.ReadTimeout, requests.ConnectTimeout):
                time.sleep((2 ** attempt) + random.random() * 0.5); continue
            except requests.HTTPError as e:
                if r is not None and r.status_code in (429, 500, 502, 503, 504):
                    time.sleep((2 ** attempt) + random.random() * 0.5); continue
                body = (last_text or "")[:400]
                raise requests.HTTPError(f"API error {getattr(r,'status_code',None)}: {e} — {body}...") from e
    return out

def plan_columns(df: pd.DataFrame) -> List[Tuple[str, str, str]]:
    """
    Decide mapeamentos (col_qid -> col_label_pt, col_label_en).
    Regras:
      - *_qid -> *_label_pt / *_label_en
      - *_ID  -> base_pt / base_en
      - qid_country -> country_label_pt / country_label_en
    """
    cols: List[Tuple[str,str,str]] = []
    for c in df.columns:
        lc = c.lower()
        if lc.endswith("_qid"):
            base = c[:-4]
            cols.append((c, f"{base}_label_pt", f"{base}_label_en"))
        elif lc.endswith("_id"):
            base = c[:-3]
            cols.append((c, f"{base}_pt", f"{base}_en"))
        elif lc == "qid_country":
            cols.append((c, "country_label_pt", "country_label_en"))
    return cols

def enrich_file(path: Path, inplace: bool) -> Path | None:
    if not path.exists():
        print(f"[skip] não existe: {path}")
        return None

    # separador: muitos CSVs estão com ';'
    try:
        df = pd.read_csv(path, sep=";", dtype=str, encoding="utf-8", keep_default_na=False)
    except Exception:
        df = pd.read_csv(path, dtype=str, encoding="utf-8", keep_default_na=False)

    plan = plan_columns(df)
    if not plan:
        print(f"[info] {path.name}: sem colunas QID para rotular.")
        return None

    # junta todas as QIDs a resolver
    qids: Set[str] = set()
    for qcol, _, _ in plan:
        if qcol not in df.columns: 
            continue
        for v in df[qcol]:
            q = extract_qid(v)
            if q: qids.add(q)

    if not qids:
        print(f"[info] {path.name}: 0 QIDs detectadas.")
        return None

    labels = fetch_labels_multi(sorted(qids), langs=("pt","en"))

    # preenche colunas alvo (PT/EN) com fallback PT<-EN
    for qcol, tgt_pt, tgt_en in plan:
        if qcol not in df.columns:
            continue
        if tgt_pt not in df.columns: df[tgt_pt] = ""
        if tgt_en not in df.columns: df[tgt_en] = ""

        mask_pt = df[tgt_pt].isna() | (df[tgt_pt].astype(str).str.strip() == "")
        mask_en = df[tgt_en].isna() | (df[tgt_en].astype(str).str.strip() == "")

        new_pt, new_en = [], []
        for v in df[qcol]:
            q = extract_qid(v)
            lab_pt = labels.get(q, {}).get("pt", "") if q else ""
            lab_en = labels.get(q, {}).get("en", "") if q else ""
            new_pt.append(lab_pt)
            new_en.append(lab_en)

        # preenche onde está vazio
        df.loc[mask_en, tgt_en] = pd.Series(new_en).where(mask_en, df[tgt_en])
        df.loc[mask_pt, tgt_pt] = pd.Series(new_pt).where(mask_pt, df[tgt_pt])

        # fallback: se PT ficou vazio, copia EN
        still_empty = df[tgt_pt].astype(str).str.strip().eq("")
        if still_empty.any():
            df.loc[still_empty, tgt_pt] = df.loc[still_empty, tgt_en].fillna("").astype(str)

        # limpeza de literais "nan"/"None"/"null"
        for col in (tgt_pt, tgt_en):
            df[col] = df[col].fillna("").astype(str).str.strip()
            df[col] = df[col].replace({"nan":"", "NaN":"", "None":"", "null":"", "NULL":""})

    # saída
    out = path if inplace else path.with_suffix(path.suffix.replace(".csv","") + ".enriched.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    # manter separador ';'
    df.to_csv(out, sep=";", index=False, encoding="utf-8")
    print(f"[save] {out}")
    return out

def main():
    ap = argparse.ArgumentParser(description="Enriquece CSVs com labels PT/EN via Wikidata; PT herda EN se faltar.")
    ap.add_argument("--inplace", action="store_true", help="Escrever por cima dos CSVs originais.")
    ap.add_argument("files", nargs="*", help="Lista de CSVs (default: lista interna).")
    args = ap.parse_args()

    files = [Path(f) for f in (args.files or FILES)]
    for p in files:
        try:
            enrich_file(p, inplace=args.inplace)
        except Exception as e:
            print(f"[err] {p.name}: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
