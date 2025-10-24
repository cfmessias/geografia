# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
from collections import Counter, defaultdict
import pandas as pd

# ===== paths =====
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR     = PROJECT_ROOT / "data"

# usa o que preferires como input
IN_CSV       = DATA_DIR / "conflicts_long_for_ui.enriched.csv"         # ou .cleaned.csv
MANUAL_MAP   = DATA_DIR / "manual_iso3_map.csv"                         # opcional: entity_qid;iso3
OUT_CSV      = DATA_DIR / "conflicts_long_for_ui.enriched.backfilled.csv"
OUT_REPORT   = DATA_DIR / "tmp_iso3_backfill_report.csv"                # para curadoria

# ===== column names no teu CSV =====
COL_ISO3 = "mapped_iso3"
COL_ROLE = "role"
COL_CQID = "conflict_qid"
COL_CLBL = "conflict_label"
COL_EQID = "entity_qid"
COL_ELBL = "entity_label"

# ===== parâmetros =====
MAJORITY_THRESHOLD = 0.60    # regra C: domínio mínimo
MIN_CONFLICTS_FOR_MAJOR = 2  # só aplicar C se a entidade aparece em >=2 conflitos

def _read_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"[erro] não encontrei {path}")
    df = pd.read_csv(path, sep=";", dtype=str, keep_default_na=False)
    for c in (COL_ISO3, COL_ROLE, COL_CQID, COL_EQID):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    df[COL_ISO3] = df[COL_ISO3].str.upper()
    return df

def _read_manual_map(path: Path) -> dict[str, str]:
    if not path.exists(): return {}
    m: dict[str, str] = {}
    df = pd.read_csv(path, sep=";", dtype=str, keep_default_na=False)
    cols = {c.lower(): c for c in df.columns}
    qcol = cols.get("entity_qid") or cols.get("qid") or "entity_qid"
    icol = cols.get("iso3")
    if icol not in df.columns or qcol not in df.columns:
        return {}
    for _, r in df.iterrows():
        q = str(r[qcol]).strip()
        i = str(r[icol]).strip().upper()
        if q.startswith("Q") and len(i) == 3:
            m[q] = i
    return m

def most_freq(s: pd.Series) -> str:
    s = s.dropna().astype(str)
    s = s[s != ""]
    if s.empty: return ""
    return s.value_counts().idxmax()

def build_entity_iso_map(df: pd.DataFrame) -> dict[str, str]:
    """Regra A — ISO3 mais frequente do próprio entity_qid."""
    sub = df[(df[COL_EQID] != "") & (df[COL_ISO3].str.len() == 3)]
    if sub.empty: return {}
    return sub.groupby(COL_EQID)[COL_ISO3].agg(most_freq).to_dict()

def conflicts_countries_map(df: pd.DataFrame) -> dict[str, set[str]]:
    """Por conflito, o conjunto de ISO3 presentes em role=country."""
    sub = df[(df[COL_ROLE].str.lower() == "country") & (df[COL_ISO3].str.len() == 3)]
    out: dict[str, set[str]] = {}
    for cq, g in sub.groupby(COL_CQID):
        out[cq] = set(g[COL_ISO3].unique())
    return out

def entity_conflicts(df: pd.DataFrame) -> dict[str, set[str]]:
    """Para cada entidade, lista de conflitos onde aparece como participant."""
    sub = df[df[COL_ROLE].str.lower() == "participant"]
    out: dict[str, set[str]] = defaultdict(set)
    for _, r in sub[[COL_EQID, COL_CQID]].iterrows():
        out[r[COL_EQID]].add(r[COL_CQID])
    return out

def entity_country_votes(df: pd.DataFrame, e2confs: dict[str,set[str]], conf2countries: dict[str,set[str]]) -> dict[str, Counter]:
    """
    Para cada entidade, conta os ISO3 dos 'role=country' nos conflitos onde ela aparece.
    """
    votes: dict[str, Counter] = {}
    for eq, cset in e2confs.items():
        cnt = Counter()
        for cq in cset:
            for iso in conf2countries.get(cq, set()):
                if iso and len(iso) == 3:
                    cnt[iso] += 1
        votes[eq] = cnt
    return votes

def backfill(df: pd.DataFrame, manual: dict[str,str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    if "mapped_iso3_filled" not in df.columns:
        df["mapped_iso3_filled"] = df[COL_ISO3]
    if "mapped_iso3_fill_source" not in df.columns:
        df["mapped_iso3_fill_source"] = ""

    # índices
    ent_map   = build_entity_iso_map(df)          # A
    conf_ctry = conflicts_countries_map(df)       # suporte B/C
    e2confs   = entity_conflicts(df)
    votes     = entity_country_votes(df, e2confs, conf_ctry)

    filled_A = filled_B = filled_C = filled_manual = 0

    target = (df[COL_ROLE].str.lower() == "participant") & (df["mapped_iso3_filled"].str.len() != 3)

    for idx in df[target].index:
        eq = df.at[idx, COL_EQID]
        cq = df.at[idx, COL_CQID]

        # 0) manual
        iso = manual.get(eq, "")
        if iso:
            df.at[idx, "mapped_iso3_filled"] = iso
            df.at[idx, "mapped_iso3_fill_source"] = "manual_map"
            filled_manual += 1
            continue

        # A) entidade
        iso = ent_map.get(eq, "")
        if iso:
            df.at[idx, "mapped_iso3_filled"] = iso
            df.at[idx, "mapped_iso3_fill_source"] = "entity_context"
            filled_A += 1
            continue

        # B) conflito com 1 país em role=country
        cset = conf_ctry.get(cq, set())
        if len(cset) == 1:
            iso = next(iter(cset))
            df.at[idx, "mapped_iso3_filled"] = iso
            df.at[idx, "mapped_iso3_fill_source"] = "conflict_solo_country"
            filled_B += 1
            continue

        # C) maioria clara nos conflitos dessa entidade
        vc = votes.get(eq, Counter())
        total_votes = sum(vc.values())
        if total_votes >= MIN_CONFLICTS_FOR_MAJOR:
            iso_major, frac = ("", 0.0)
            if vc:
                iso_major, cnt = vc.most_common(1)[0]
                frac = cnt / total_votes
            if iso_major and frac >= MAJORITY_THRESHOLD:
                df.at[idx, "mapped_iso3_filled"] = iso_major
                df.at[idx, "mapped_iso3_fill_source"] = f"entity_conflicts_majority({frac:.0%})"
                filled_C += 1
                continue

    total = int(target.sum())
    print(f"[fill] alvo={total} · manual={filled_manual} · A(entity)={filled_A} · B(solo country)={filled_B} · C(majority)={filled_C} "
          f"· resolvidos={(filled_manual+filled_A+filled_B+filled_C)}")

    # ---------- relatório de pendentes por ENTIDADE ----------
    pending = df[(df[COL_ROLE].str.lower() == "participant") & (df["mapped_iso3_filled"].str.len() != 3)]
    rep_rows = []
    for eq, g in pending.groupby(COL_EQID):
        elbl = g[COL_ELBL].iloc[0] if COL_ELBL in g.columns else ""
        confs = set(g[COL_CQID].unique())
        # ISO3 dos países nos conflitos desta entidade (para ajudar à curadoria)
        vc = votes.get(eq, Counter())
        top = ";".join(f"{k}:{v}" for k, v in vc.most_common())
        rep_rows.append([eq, elbl, len(confs), top])

    report = pd.DataFrame(rep_rows, columns=["entity_qid","entity_label","n_conflitos","iso3_votes_em_conflitos"])
    report = report.sort_values(["n_conflitos"], ascending=False)

    return df, report

def main():
    df = _read_df(IN_CSV)
    manual = _read_manual_map(MANUAL_MAP)
    df2, rep = backfill(df, manual)
    df2.to_csv(OUT_CSV, sep=";", index=False, encoding="utf-8-sig")
    rep.to_csv(OUT_REPORT, sep=";", index=False, encoding="utf-8-sig")
    print(f"[ok] escrito → {OUT_CSV}\n[report] → {OUT_REPORT} (para curadoria manual)")

if __name__ == "__main__":
    main()
