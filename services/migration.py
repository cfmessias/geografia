# -*- coding: utf-8 -*-
from __future__ import annotations
import pandas as pd
from pathlib import Path
from .data_paths import migration_inout_path
from .io_csv import read_csv_safe_any, read_csv_filtered, file_sig

def load_migration_inout(path: str | Path | None = None) -> pd.DataFrame:
    p = Path(path) if path else migration_inout_path
    df = read_csv_safe_any(p)
    if df.empty:
        return pd.DataFrame(columns=["iso3","year","immigrants","emigrants"])
    # normaliza
    df.columns = [str(c).strip().strip("\ufeff") for c in df.columns]
    low = {c.lower(): c for c in df.columns}
    rename = {}
    for std, opts in {
        "iso3":["iso3","country","pais","code","codigo"],
        "year":["year","ano","time"],
        "immigrants":["immigrants","imigrantes","immig"],
        "emigrants":["emigrants","emigrantes","emig"],
    }.items():
        hit = next((low[k] for k in opts if k in low), None)
        if hit: rename[hit] = std
    if rename: df = df.rename(columns=rename)
    need = {"iso3","year","immigrants","emigrants"}
    if not need.issubset(df.columns):
        return pd.DataFrame(columns=list(need))
    df["iso3"] = df["iso3"].astype(str).str.upper().str.strip()
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["immigrants"] = pd.to_numeric(df["immigrants"], errors="coerce")
    df["emigrants"]  = pd.to_numeric(df["emigrants"],  errors="coerce")
    return (df.dropna(subset=["year"])[["iso3","year","immigrants","emigrants"]]
              .sort_values(["iso3","year"]).reset_index(drop=True))

def migration_inout_for_iso3(iso3: str) -> pd.DataFrame:
    sig = file_sig(migration_inout_path)
    sub = read_csv_filtered(str(migration_inout_path), str(iso3).upper(),
                            col_iso3="iso3",
                            usecols=["iso3","year","immigrants","emigrants"],
                            dtype={"iso3":"string"}, sig=sig)
    if sub.empty:
        return sub
    sub["year"] = pd.to_numeric(sub["year"], errors="coerce").astype("Int64")
    sub["immigrants"] = pd.to_numeric(sub["immigrants"], errors="coerce")
    sub["emigrants"]  = pd.to_numeric(sub["emigrants"],  errors="coerce")
    return sub.dropna(subset=["year"]).sort_values("year").reset_index(drop=True)
