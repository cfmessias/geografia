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
    """Lê CSV com ;, tudo como string, sem NA automáticos."""
    if not path.exists():
        raise FileNotFoundError(f"Ficheiro não encontrado: {path}")
    return pd.read_csv(path, sep=";", dtype=str, encoding="utf-8", keep_default_na=False)

def _ci_pick(df: pd.DataFrame, names: list[str]) -> list[str]:
    """Escolhe colunas por nome (case-insensitive)."""
    low = {c.lower(): c for c in df.columns}
    out = []
    for n in names:
        c = low.get(n.lower())
        if c: out.append(c)
    return out

def _canon(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza DF: str, fillna(''), strip() para todas as colunas."""
    for c in df.columns:
        df[c] = df[c].astype(str).fillna("").str.strip()
    return df

def prepare_lineage_unique(df_lin: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza lineage para 1 linha por QID:
      - Iso3Start: valor não-vazio mais frequente (ou o primeiro não-vazio)
      - FormationYear: mínimo numérico (quando existir)
    Mantém apenas colunas QID, Iso3Start, FormationYear.
    """
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
        description="Enriquece conflicts_all.csv com datas/tipos e Iso3Start; exclui conflitos não militares."
    )
    ap.add_argument(
        "--include-is-military", action="store_true",
        help="Se presente, inclui a coluna is_military no ficheiro de saída (por omissão não inclui)."
    )
    ap.add_argument(
        "--dedup-key", type=str,
        default="conflict_qid,role_label,entity_qid,entity_type,type_qid,start,end,point_in_time",
        help="Lista de colunas para deduplicação final. Por omissão preserva participantes distintos."
    )
    args = ap.parse_args()

    try:
        # 1) Ler ficheiros base
        df_all = read_csv_semicolon(CONF_ALL_CSV)
        df_cat = read_csv_semicolon(CATALOG_CSV)
        df_lin = read_csv_semicolon(LINEAGE_CSV)

        # 2) Normalização canónica já no início
        df_all = _canon(df_all)
        df_cat = _canon(df_cat)
        df_lin = _canon(df_lin)

        print(f"[debug] lidos: conflicts_all={len(df_all)} | conflict_catalog={len(df_cat)} | lineage_raw={len(df_lin)}", flush=True)

        # 3) Validações mínimas de schema
        req_all = {"conflict_qid","conflict_label","role_label","entity_qid","entity_label","entity_type"}
        req_cat = {"conflict_qid","conflict_label","type_qid","type_label",
                   "start","end","point_in_time","earliest_year","latest_year"}  # 'is_military' pode não existir
        need_lin = set(_ci_pick(df_lin, ["Iso3Start","QID","FormationYear"]))
        if len(need_lin) < 3:
            raise ValueError("[state_lineage_level2_details] Faltam colunas necessárias: QID, Iso3Start, FormationYear")

        miss_all = req_all - set(df_all.columns)
        miss_cat = req_cat - set(df_cat.columns)
        if miss_all:
            raise ValueError(f"[conflicts_all] Faltam colunas: {sorted(miss_all)}")
        if miss_cat:
            raise ValueError(f"[conflict_catalog] Faltam colunas: {sorted(miss_cat)}")

        # 4) Dedup PRÉ-join (participantes iguais repetidos em conflicts_all)
        pre_key = ["conflict_qid","role_label","entity_qid","entity_type"]
        before_pre = len(df_all)
        df_all = df_all.drop_duplicates(subset=pre_key, keep="first")
        print(f"[debug] pre-join dedup {pre_key}: removidos={before_pre - len(df_all)} | restantes={len(df_all)}", flush=True)

        # 5) LEFT JOIN com o catálogo (datas e tipo) por conflict_qid
        has_is_military = "is_military" in df_cat.columns
        cat_cols_keep = ["conflict_qid","type_qid","type_label","start","end",
                         "point_in_time","earliest_year","latest_year"] + (["is_military"] if has_is_military else [])
        before_cat = len(df_all)
        df_all_cat = df_all.merge(
            df_cat[cat_cols_keep],
            on="conflict_qid",
            how="left",
            copy=False,
            validate="m:1"
        )
        print(f"[debug] após join catalog: {before_cat} → {len(df_all_cat)}", flush=True)

        # 5.1) Filtrar não militares, se disponível
        filtered_out = 0
        if has_is_military:
            before = len(df_all_cat)
            df_all_cat = df_all_cat[df_all_cat["is_military"].str.lower() != "non-military"].copy()
            filtered_out = before - len(df_all_cat)
            print(f"[debug] filtro is_military → removidos={filtered_out} | restantes={len(df_all_cat)}", flush=True)
        else:
            print("[aviso] 'is_military' não encontrado em conflict_catalog.csv — sem filtro de não militares.", flush=True)

        # 6) Normalizar lineage para 1 linha por QID (evita 1:n no merge)
        df_lin_u = prepare_lineage_unique(df_lin)
        print(f"[debug] lineage unique: rows={len(df_lin_u)} (antes={len(df_lin)})", flush=True)

        # 7) LEFT JOIN com a lineage (Iso3Start e FormationYear) por entity_qid = QID
        before_lin = len(df_all_cat)
        df_final = df_all_cat.merge(
            df_lin_u,  # único por QID
            left_on="entity_qid",
            right_on="QID",
            how="left",
            copy=False,
            validate="m:1"
        ).drop(columns=["QID"], errors="ignore")
        print(f"[debug] após join lineage: {before_lin} → {len(df_final)}", flush=True)

        # 8) Ordenar colunas para conveniência (inclui is_human se existir em df_all)
        have_is_human = "is_human" in df_final.columns  # já vem do conflicts_all, se existir
        ordered = [
            "conflict_qid","conflict_label","type_qid","type_label",
            "start","end","point_in_time","earliest_year","latest_year",
            # (is_military pode ser inserido aqui se incluído)
        ]
        if args.include_is_military and "is_military" in df_final.columns:
            ordered.append("is_military")
        ordered += [
            "role_label","entity_qid","entity_label","entity_type",
        ]
        if have_is_human:
            ordered.append("is_human")  # <- PROPAGADO PARA O ENRICHED
        ordered += ["Iso3Start","FormationYear"]

        wrote_is_military = args.include_is_military and ("is_military" in df_final.columns)
        if "is_military" in df_final.columns and not args.include_is_military:
            df_final = df_final.drop(columns=["is_military"], errors="ignore")

        remaining = [c for c in df_final.columns if c not in ordered]
        df_final = df_final[[c for c in ordered if c in df_final.columns] + remaining]

        # 9) Dedup PÓS-join — 1) linha exata, 2) chave composta (configurável)
        before_exact = len(df_final)
        df_final = df_final.drop_duplicates(keep="first")  # remove 100% iguais
        exact_removed = before_exact - len(df_final)

        dedup_removed = 0
        dedup_cols = [c.strip() for c in (args.dedup_key or "").split(",") if c.strip()]
        if dedup_cols:
            for col in dedup_cols:
                if col not in df_final.columns:
                    raise ValueError(f"[dedup] coluna inexistente na chave: {col}")
            before_dedup = len(df_final)
            df_final = df_final.drop_duplicates(subset=dedup_cols, keep="first")
            dedup_removed = before_dedup - len(df_final)
            print(f"[debug] dedup final {dedup_cols}: removidos={dedup_removed} | restantes={len(df_final)}", flush=True)
        else:
            print("[debug] dedup final DESLIGADO (preserva todas as linhas distintas).", flush=True)

        # 10) Escrita
        OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        df_final.to_csv(OUT_CSV, sep=";", index=False, encoding="utf-8")

        print(
            f"✔️ Escrevi {OUT_CSV} | linhas={len(df_final)} "
            f"| filtrados_nao_militares={filtered_out} "
            f"| duplicados_exatos_removidos={exact_removed} "
            + (f"| duplicados_chave_removidos={dedup_removed} " if dedup_cols else "")
            + ("" if not wrote_is_military else "| is_military incluído"),
            flush=True
        )

    except Exception as e:
        print(f"Erro: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
