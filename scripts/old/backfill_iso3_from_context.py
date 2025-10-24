# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"

IN_CSV  = DATA_DIR / "conflicts_long_for_ui.enriched.csv"          # ou .cleaned, se preferires
OUT_CSV = DATA_DIR / "conflicts_long_for_ui.enriched.backfilled.csv"

COL_ISO3 = "mapped_iso3"     # nome no teu CSV
COL_ROLE = "role"
COL_CQID = "conflict_qid"
COL_CLBL = "conflict_label"
COL_EQID = "entity_qid"
COL_ELBL = "entity_label"

# parâmetros das heurísticas
MAJORITY_THRESHOLD = 0.60    # Regra C

def load_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"Ficheiro não encontrado: {path}")
    df = pd.read_csv(path, sep=";", dtype=str, keep_default_na=False)
    # normalizações
    for c in (COL_ISO3, COL_ROLE, COL_CQID, COL_EQID):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    df[COL_ISO3] = df[COL_ISO3].str.upper()
    return df

def most_frequent(series: pd.Series) -> str:
    s = series.dropna().astype(str)
    s = s[s != ""]
    if s.empty: return ""
    return s.value_counts().idxmax()

def build_entity_iso3_map(df: pd.DataFrame) -> dict[str, str]:
    """Regra A: ISO3 mais frequente por entity_qid (onde já existe ISO3)."""
    have = df[(df[COL_EQID] != "") & (df[COL_ISO3].str.len() == 3)]
    if have.empty: return {}
    grp = have.groupby(COL_EQID)[COL_ISO3].agg(most_frequent)
    return grp.to_dict()

def conflict_countries(df: pd.DataFrame) -> dict[str, set[str]]:
    """Para cada conflito, ISO3 que aparecem como role=country."""
    sub = df[(df[COL_ROLE].str.lower() == "country") & (df[COL_ISO3].str.len() == 3)]
    if sub.empty: return {}
    m: dict[str, set[str]] = {}
    for cq, g in sub.groupby(COL_CQID):
        m[cq] = set(g[COL_ISO3].unique())
    return m

def conflict_majority_iso3(df: pd.DataFrame) -> dict[str, str]:
    """
    Regra C: para cada conflito, se algum ISO3 for claramente dominante (>= threshold)
    entre os participantes com ISO3, devolve esse ISO3.
    """
    sub = df[(df[COL_ROLE].str.lower() == "participant") & (df[COL_ISO3].str.len() == 3)]
    if sub.empty: return {}
    out: dict[str, str] = {}
    for cq, g in sub.groupby(COL_CQID):
        counts = g[COL_ISO3].value_counts(normalize=True)
        if not counts.empty and counts.iloc[0] >= MAJORITY_THRESHOLD:
            out[cq] = counts.index[0]
    return out

def backfill_iso3(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # colunas de saída
    if "mapped_iso3_filled" not in df.columns:
        df["mapped_iso3_filled"] = df[COL_ISO3]
    if "mapped_iso3_fill_source" not in df.columns:
        df["mapped_iso3_fill_source"] = ""

    # índices auxiliares
    ent_map = build_entity_iso3_map(df)              # Regra A
    ctry_by_conf = conflict_countries(df)            # Regra B suporte
    majority_by_conf = conflict_majority_iso3(df)    # Regra C

    filled_A = filled_B = filled_C = 0

    # vamos preencher só PARTICIPANTS sem ISO3
    mask_target = (df[COL_ROLE].str.lower() == "participant") & (df["mapped_iso3_filled"].str.len() != 3)

    for idx in df[mask_target].index:
        cq = df.at[idx, COL_CQID]
        eq = df.at[idx, COL_EQID]
        iso = ""

        # A) herdado por entidade (aparece noutros conflitos com ISO3)
        iso = ent_map.get(eq, "")
        if iso:
            df.at[idx, "mapped_iso3_filled"] = iso
            df.at[idx, "mapped_iso3_fill_source"] = "entity_context"
            filled_A += 1
            continue

        # B) conflito com 1 país em role=country (conflito interno)
        cset = ctry_by_conf.get(cq, set())
        if len(cset) == 1:
            iso = next(iter(cset))
            df.at[idx, "mapped_iso3_filled"] = iso
            df.at[idx, "mapped_iso3_fill_source"] = "conflict_solo_country"
            filled_B += 1
            continue

        # C) maioria clara entre participantes com ISO3
        iso = majority_by_conf.get(cq, "")
        if iso:
            df.at[idx, "mapped_iso3_filled"] = iso
            df.at[idx, "mapped_iso3_fill_source"] = "conflict_majority"
            filled_C += 1
            continue

    total = int(mask_target.sum())
    print(f"[fill] alvo={total} · A(entity)={filled_A} · B(solo country)={filled_B} · C(majority)={filled_C} "
          f"· resolvidos={(filled_A+filled_B+filled_C)}")

    return df

def main():
    df = load_df(IN_CSV)
    df2 = backfill_iso3(df)
    df2.to_csv(OUT_CSV, sep=";", index=False, encoding="utf-8-sig")
    print(f"[ok] escrito → {OUT_CSV}")

if __name__ == "__main__":
    main()
