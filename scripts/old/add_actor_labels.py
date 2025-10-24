# scripts/add_actor_labels.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import pandas as pd
from pathlib import Path

DATA_DIR   = Path(__file__).resolve().parent.parent / "data"
F_CONFLICT = DATA_DIR / "conflicts_direct.csv"      # Iso3;ActorQID;ConflictQID;ConflictLabel;...
F_STATE    = DATA_DIR / "state_forms_by_class.csv"  # cls_qid;cls_label;item_qid;item_label;iso3
F_OUT      = DATA_DIR / "conflicts_direct_with_actor_label.csv"

def sniff_sep(p: Path) -> str:
    txt = p.read_text(encoding="utf-8", errors="ignore")[:4096]
    for d in (";", ",", "\t", "|"):
        if d in txt:
            return d
    return ";"

def main() -> None:
    # lê conflitos
    sep_c = sniff_sep(F_CONFLICT)
    dfc = pd.read_csv(F_CONFLICT, sep=sep_c, dtype=str, keep_default_na=False)

    # garante colunas mínimas
    for col in ["Iso3","ActorQID","ConflictQID","ConflictLabel"]:
        if col not in dfc.columns:
            dfc[col] = ""

    # lê mapping de QID -> label
    sep_s = sniff_sep(F_STATE)
    dfs = pd.read_csv(F_STATE, sep=sep_s, dtype=str, keep_default_na=False)

    # normaliza nomes comuns
    if "item_qid" not in dfs.columns and "QID" in (c := [c.lower() for c in dfs.columns]):
        # nada fancy: se o ficheiro tiver "QID" maiúsculo, renomeia
        real = [col for col in dfs.columns if col.lower() == "qid"][0]
        dfs = dfs.rename(columns={real: "item_qid"})
    if "item_label" not in dfs.columns and any(col.lower()=="label" for col in dfs.columns):
        real = [col for col in dfs.columns if col.lower()=="label"][0]
        dfs = dfs.rename(columns={real: "item_label"})

    # reduz ao essencial
    mapping = dfs[["item_qid","item_label"]].drop_duplicates()
    mapping["item_qid"] = mapping["item_qid"].str.upper().str.strip()

    # prepara conflitos
    dfc["ActorQID"] = dfc["ActorQID"].str.upper().str.strip()

    # join para obter ActorLabel
    out = dfc.merge(mapping, how="left", left_on="ActorQID", right_on="item_qid")
    out = out.drop(columns=["item_qid"]).rename(columns={"item_label": "ActorLabel"})

    # grava
    F_OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(F_OUT, index=False, sep=";")
    print(f"✔️ escrito: {F_OUT} ({out.shape[0]} linhas)")

if __name__ == "__main__":
    main()
