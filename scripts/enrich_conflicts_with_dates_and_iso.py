# scripts/enrich_conflicts_with_dates_and_iso.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import sys
import argparse
import pandas as pd
from collections import Counter

# === PATHS HARDCODED ===
CONF_ALL_CSV   = Path("data/conflicts_all.csv")
CATALOG_CSV    = Path("data/conflict_catalog.csv")
LINEAGE_CSV    = Path("data/state_lineage_level2_details.csv")
OUT_CSV        = Path("data/conflicts_all_enriched.csv")

def read_csv_semicolon(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Ficheiro não encontrado: {path}")
    return pd.read_csv(path, sep=";", dtype=str, encoding="utf-8", keep_default_na=False)

def _ci_pick(df: pd.DataFrame, names: list[str]) -> list[str]:
    low = {c.lower(): c for c in df.columns}
    out = []
    for n in names:
        c = low.get(n.lower())
        if c: out.append(c)
    return out

def _canon(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        df[c] = df[c].astype(str).fillna("").str.strip()
    return df

def prepare_lineage_unique(df_lin: pd.DataFrame) -> pd.DataFrame:
    c_qid  = _ci_pick(df_lin, ["QID"])[0]
    c_iso  = _ci_pick(df_lin, ["Iso3Start"])[0]
    c_form = _ci_pick(df_lin, ["FormationYear"])[0]

    def agg_iso(series: pd.Series) -> str:
        vals = [v for v in series.astype(str) if v.strip()]
        if not vals: return ""
        cnt = Counter(vals)
        return cnt.most_common(1)[0][0]

    def agg_year(series: pd.Series) -> str:
        vals = []
        for v in series.astype(str):
            v = v.strip()
            if not v:
                continue
            try:
                vals.append(int(v))
            except Exception:
                pass
        return str(min(vals)) if vals else ""

    grouped = (df_lin.groupby(c_qid, as_index=False)
                    .agg({c_iso: agg_iso, c_form: agg_year}))
    grouped = grouped.rename(columns={c_qid:"QID", c_iso:"Iso3Start", c_form:"FormationYear"})
    return _canon(grouped)

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Enriquece conflicts_all.csv com datas/tipos, is_military e Iso3Start. NÃO filtra non-military."
    )
    ap.add_argument("--dedup-key", type=str,
        default="conflict_qid,role_label,entity_qid,entity_type,type_qid,start,end,point_in_time",
        help="Chave de deduplicação final (após joins).")
    args = ap.parse_args()

    try:
        # 1) Ler
        df_all = _canon(read_csv_semicolon(CONF_ALL_CSV))
        df_cat = _canon(read_csv_semicolon(CATALOG_CSV))
        df_lin = _canon(read_csv_semicolon(LINEAGE_CSV))

        print(f"[debug] lidos: conflicts_all={len(df_all)} | conflict_catalog={len(df_cat)} | lineage_raw={len(df_lin)}", flush=True)

        # 2) Schemas
        req_all = {"conflict_qid","conflict_label","role_label","entity_qid","entity_label","entity_type"}
        req_cat = {"conflict_qid","conflict_label","type_qid","type_label","start","end","point_in_time","earliest_year","latest_year"}
        miss_all = req_all - set(df_all.columns)
        miss_cat = req_cat - set(df_cat.columns)
        if miss_all: raise ValueError(f"[conflicts_all] Faltam colunas: {sorted(miss_all)}")
        if miss_cat: raise ValueError(f"[conflict_catalog] Faltam colunas: {sorted(miss_cat)}")

        # 3) Dedup pré-join (participante idêntico)
        pre_key = ["conflict_qid","role_label","entity_qid","entity_type"]
        before_pre = len(df_all)
        df_all = df_all.drop_duplicates(subset=pre_key, keep="first")
        print(f"[debug] pre-join dedup {pre_key}: removidos={before_pre - len(df_all)} | restantes={len(df_all)}", flush=True)

        # 4) Join com catálogo — agora transporta SEMPRE is_military (se existir)
        has_is_military = "is_military" in df_cat.columns
        cat_cols_keep = ["conflict_qid","type_qid","type_label","start","end","point_in_time","earliest_year","latest_year"] \
                        + (["is_military"] if has_is_military else [])
        before_cat = len(df_all)
        df_all_cat = df_all.merge(df_cat[cat_cols_keep], on="conflict_qid", how="left", copy=False, validate="m:1")
        print(f"[debug] após join catalog: {before_cat} → {len(df_all_cat)} | is_military_col={has_is_military}", flush=True)

        # 5) Lineage única e join
        need_lin = set(_ci_pick(df_lin, ["Iso3Start","QID","FormationYear"]))
        if len(need_lin) < 3:
            raise ValueError("[state_lineage_level2_details] Faltam colunas: QID, Iso3Start, FormationYear")
        df_lin_u = prepare_lineage_unique(df_lin)
        before_lin = len(df_all_cat)
        df_final = df_all_cat.merge(df_lin_u, left_on="entity_qid", right_on="QID", how="left", copy=False, validate="m:1") \
                             .drop(columns=["QID"], errors="ignore")
        print(f"[debug] após join lineage: {before_lin} → {len(df_final)}", flush=True)

        # 6) Ordenação de colunas (garante is_military no output)
        ordered = [
            "conflict_qid","conflict_label","type_qid","type_label",
            "start","end","point_in_time","earliest_year","latest_year",
            "is_military",  # <- sempre no output (se não existir vem vazio)
            "role_label","entity_qid","entity_label","entity_type",
            "Iso3Start","FormationYear","is_human"  # is_human vem do conflicts_all.csv, se existir
        ]
        # criar coluna vazia se não existir (para consistência no CSV final)
        if "is_military" not in df_final.columns:
            df_final["is_military"] = ""
        if "is_human" not in df_final.columns:
            df_final["is_human"] = ""

        remaining = [c for c in df_final.columns if c not in ordered]
        df_final = df_final[[c for c in ordered if c in df_final.columns] + remaining]

        # 7) Dedup pós-join
        before_exact = len(df_final)
        df_final = df_final.drop_duplicates(keep="first")
        exact_removed = before_exact - len(df_final)

        dedup_removed = 0
        dedup_cols = [c.strip() for c in (args.dedup_key or "").split(",") if c.strip()]
        for col in dedup_cols:
            if col not in df_final.columns:
                raise ValueError(f"[dedup] coluna inexistente na chave: {col}")
        before_dedup = len(df_final)
        df_final = df_final.drop_duplicates(subset=dedup_cols, keep="first")
        dedup_removed = before_dedup - len(df_final)
        print(f"[debug] dedup final {dedup_cols}: removidos={dedup_removed} | restantes={len(df_final)}", flush=True)

        # 8) Escrita
        OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        df_final.to_csv(OUT_CSV, sep=";", index=False, encoding="utf-8")

        print(
            f"✔️ Escrevi {OUT_CSV} | linhas={len(df_final)} "
            f"| duplicados_exatos_removidos={exact_removed} "
            f"| duplicados_chave_removidos={dedup_removed} "
            f"| is_military=sempre_presente",
            flush=True
        )

    except Exception as e:
        print(f"Erro: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
