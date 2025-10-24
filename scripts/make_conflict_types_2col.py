# scripts/make_conflict_types_2col.py
# -*- coding: utf-8 -*-
from pathlib import Path
import csv

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
INP = DATA_DIR / "conflict_types.csv"              # colunas: type_qid;type_label;root_qid;root_label
OUT = DATA_DIR / "conflict_types.2col.csv"         # colunas: qid;label

def sniff_delim(path: Path) -> str:
    sample = path.read_text(encoding="utf-8", errors="ignore")[:4096]
    for d in (";", ",", "\t", "|"):
        if d in sample: return d
    return ";"

def main():
    if not INP.exists():
        raise FileNotFoundError(f"Falta {INP}")

    sep = sniff_delim(INP)
    label_by_qid = {}  # qid -> label (primeiro não-vazio que aparecer)
    order = []         # para manter ordem de 1ª aparição

    with INP.open("r", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f, delimiter=sep)
        # nomes de colunas tolerantes
        cols = {c.lower(): c for c in (r.fieldnames or [])}
        type_qid   = cols.get("type_qid")   or list(r.fieldnames)[0]
        type_label = cols.get("type_label") or list(r.fieldnames)[1]
        root_qid   = cols.get("root_qid")   or list(r.fieldnames)[2]
        root_label = cols.get("root_label") or list(r.fieldnames)[3]

        for row in r:
            tq = (row.get(type_qid,   "") or "").strip()
            tl = (row.get(type_label, "") or "").strip()
            rq = (row.get(root_qid,   "") or "").strip()
            rl = (row.get(root_label, "") or "").strip()

            # adiciona root
            if rq and rq not in label_by_qid:
                label_by_qid[rq] = rl or tl  # se root_label vazio, tenta reaproveitar type_label
                order.append(rq)

            # adiciona type
            if tq and tq not in label_by_qid:
                label_by_qid[tq] = tl or rl  # se type_label vazio, usa root_label
                order.append(tq)

    # grava 2-col sem duplicados
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["qid","label"])
        for qid in order:
            w.writerow([qid, label_by_qid.get(qid, "")])

    print(f"✔️ escrito: {OUT} (linhas: {len(order)})")

if __name__ == "__main__":
    main()
