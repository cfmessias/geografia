# utils/add_labels_to_csv.py
from __future__ import annotations
import argparse, csv, time, random, requests, pandas as pd
from pathlib import Path
from typing import Iterable, List, Dict

API = "https://www.wikidata.org/w/api.php"
HEADERS = {"User-Agent": "GeoWars/label-adder/1.0 (+streamlit)"}

def sniff_sep(p: Path) -> str:
    sample = p.read_text(encoding="utf-8", errors="ignore")[:4096]
    try:
        return csv.Sniffer().sniff(sample, delimiters=";,|\t,").delimiter
    except Exception:
        return ";" if sample.count(";") >= sample.count(",") else ","

def batched(it: Iterable[str], n: int) -> Iterable[List[str]]:
    buf: List[str] = []
    for x in it:
        if x: buf.append(x)
        if len(buf) >= n:
            yield buf; buf=[]
    if buf: yield buf

def fetch_labels(qids: List[str], langs=("pt","en")) -> Dict[str, Dict[str,str]]:
    out: Dict[str, Dict[str,str]] = {}
    if not qids: return out
    for chunk in batched(qids, 40):
        params = {
            "action": "wbgetentities",
            "ids": "|".join(chunk),
            "props": "labels",
            "languages": "|".join(langs),
            "format": "json",
        }
        # retry simples
        for attempt in range(6):
            try:
                r = requests.post(API, data=params, headers=HEADERS, timeout=60)
                r.raise_for_status()
                ents = (r.json().get("entities") or {})
                for qid, ent in ents.items():
                    labs = ent.get("labels", {}) if isinstance(ent, dict) else {}
                    out[qid.upper()] = {lg: (labs.get(lg, {}) or {}).get("value", "") for lg in langs}
                break
            except requests.RequestException:
                time.sleep((2**attempt) + random.random()*0.5)
        time.sleep(0.15 + random.random()*0.2)
    return out

def main():
    ap = argparse.ArgumentParser(description="Adiciona labels PT/EN a um CSV com QIDs (mantém o mesmo separador).")
    ap.add_argument("input", help="CSV de entrada (tem de conter uma coluna com QIDs)")
    ap.add_argument("--qid-col", default=None, help="Nome da coluna com QIDs (auto se não passar)")
    ap.add_argument("--out", default=None, help="CSV de saída; se omisso e --inplace não for usado, adiciona .labeled.csv")
    ap.add_argument("--inplace", action="store_true", help="Substituir ficheiro de entrada")
    args = ap.parse_args()

    inp = Path(args.input)
    if not inp.exists():
        raise SystemExit(f"Não encontrei: {inp}")

    sep = sniff_sep(inp)
    df = pd.read_csv(inp, sep=sep, dtype=str, keep_default_na=False, encoding="utf-8")

    # detetar coluna com QIDs
    qcol = args.qid_col
    if not qcol:
        cand = [c for c in df.columns if c.lower() in {"qid","id","who_qid","conflict_qid","country_qid"} or c.lower().endswith("_qid")]
        if not cand:
            raise SystemExit("Não encontrei coluna com QIDs (ex: 'qid' ou '*_qid'). Use --qid-col.")
        qcol = cand[0]

    df[qcol] = df[qcol].astype(str).str.strip()
    qids = sorted(set([x for x in df[qcol] if x.startswith("Q")]))
    if not qids:
        raise SystemExit(f"Nenhum QID válido na coluna {qcol}.")

    labels = fetch_labels(qids, langs=("pt","en"))

    # criar/atualizar colunas alvo
    pt_col = f"{qcol.rsplit('_qid',1)[0]}_label_pt" if qcol.endswith("_qid") else "label_pt"
    en_col = f"{qcol.rsplit('_qid',1)[0]}_label_en" if qcol.endswith("_qid") else "label_en"
    if pt_col not in df.columns: df[pt_col] = ""
    if en_col not in df.columns: df[en_col] = ""

    # preencher + fallback PT <- EN
    def _pt(q): return labels.get(q, {}).get("pt", "")
    def _en(q): return labels.get(q, {}).get("en", "")

    empty_en = df[en_col].astype(str).str.strip().eq("")
    df.loc[empty_en, en_col] = df.loc[empty_en, qcol].map(_en).fillna("")

    empty_pt = df[pt_col].astype(str).str.strip().eq("")
    df.loc[empty_pt, pt_col] = df.loc[empty_pt, qcol].map(_pt).fillna("")
    still = df[pt_col].astype(str).str.strip().eq("")
    df.loc[still, pt_col] = df.loc[still, en_col].fillna("")

    # limpar literais indesejados
    for c in (pt_col, en_col):
        df[c] = df[c].fillna("").astype(str).str.strip().replace({"nan":"", "NaN":"", "None":"", "null":"", "NULL":""})

    # saída
    if args.inplace:
        out = inp
    else:
        out = Path(args.out) if args.out else inp.with_name(inp.stem + ".labeled.csv")
    df.to_csv(out, sep=sep, index=False, encoding="utf-8")
    print(f"[save] {out}  (sep='{sep}', qcol='{qcol}', qids={len(qids)})")

if __name__ == "__main__":
    main()
