# -*- coding: utf-8 -*-
from __future__ import annotations
import pandas as pd
from .data_paths import religion_path
from .io_csv import read_csv_safe

def load_religion() -> pd.DataFrame:
    # mantém colunas flexíveis; só normaliza iso3 e números
    df = read_csv_safe(religion_path, expected_cols=None)
    if df.empty:
        return df
    # tenta uniformizar algumas colunas frequentes
    low = {c.lower(): c for c in df.columns}
    if "iso3" in low:
        c = low["iso3"]; df = df.rename(columns={c:"iso3"})
        df["iso3"] = df["iso3"].astype(str).str.upper()
    for c in df.columns:
        if c.lower() in ("year","percent","share","value"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df
