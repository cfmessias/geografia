# scripts/append_conflicts_hardcoded.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import sys
import pandas as pd

# === PATHS HARDCODED ===
COUNTRIES_CSV    = Path("data/conflict_countries.csv")
PARTICIPANTS_CSV = Path("data/conflict_participants.csv")
OUT_CSV          = Path("data/conflicts_all.csv")

def read_csv_semicolon(path: Path) -> pd.DataFrame:
    """Lê CSV com ;, tudo como string, sem NA automáticos."""
    if not path.exists():
        raise FileNotFoundError(f"Ficheiro não encontrado: {path}")
    return pd.read_csv(path, sep=";", dtype=str, encoding="utf-8", keep_default_na=False)

def build_countries_df(df: pd.DataFrame) -> pd.DataFrame:
    required = {"conflict_qid","conflict_label","country_qid","country_label"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"[countries] Faltam colunas: {sorted(missing)}")
    out = pd.DataFrame({
        "conflict_qid": df["conflict_qid"].astype(str).str.strip(),
        "conflict_label": df["conflict_label"].astype(str).str.strip(),
        "role_label": "",  # países não têm role_label
        "entity_qid": df["country_qid"].astype(str).str.strip(),
        "entity_label": df["country_label"].astype(str).str.strip(),
        "entity_type": "country",
        "is_human": ""     # países → vazio
    })
    return out

def build_participants_df(df: pd.DataFrame) -> pd.DataFrame:
    required = {"conflict_qid","conflict_label","participant_qid","participant_label","role_label"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"[participants] Faltam colunas: {sorted(missing)}")

    # 'is_human' é opcional: se não existir, criamos vazia (compatibilidade)
    if "is_human" not in df.columns:
        df = df.copy()
        df["is_human"] = ""

    out = pd.DataFrame({
        "conflict_qid":  df["conflict_qid"].astype(str).str.strip(),
        "conflict_label": df["conflict_label"].astype(str).str.strip(),
        "role_label":    df["role_label"].astype(str).str.strip(),
        "entity_qid":    df["participant_qid"].astype(str).str.strip(),
        "entity_label":  df["participant_label"].astype(str).str.strip(),
        "entity_type":   "participant",
        "is_human":      df["is_human"].astype(str).str.strip().str.lower().map(lambda v: v if v in {"yes","no",""} else "")
    })
    return out

def main() -> None:
    try:
        df_c = read_csv_semicolon(COUNTRIES_CSV)
        df_p = read_csv_semicolon(PARTICIPANTS_CSV)

        uni_c = build_countries_df(df_c)
        uni_p = build_participants_df(df_p)

        cols = ["conflict_qid","conflict_label","role_label","entity_qid","entity_label","entity_type","is_human"]
        df_all = pd.concat([uni_c[cols], uni_p[cols]], ignore_index=True)

        # (Opcional) Normalização final simples
        for c in cols:
            df_all[c] = df_all[c].astype(str).str.strip()

        OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        df_all.to_csv(OUT_CSV, sep=";", index=False, encoding="utf-8")

        # estatísticas úteis
        part_mask = df_all["entity_type"] == "participant"
        yes = int((df_all.loc[part_mask, "is_human"] == "yes").sum())
        no  = int((df_all.loc[part_mask, "is_human"] == "no").sum())
        blank = int((df_all.loc[part_mask, "is_human"] == "").sum())

        print(
            f"✔️ Escrevi {OUT_CSV} | total={len(df_all)} "
            f"| countries={len(uni_c)} | participants={len(uni_p)} "
            f"| is_human(participants): yes={yes} no={no} vazios={blank}",
            flush=True
        )
    except Exception as e:
        print(f"Erro: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
