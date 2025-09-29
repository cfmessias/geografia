# -*- coding: utf-8 -*-
from __future__ import annotations
import pandas as pd
from .data_paths import worldbank_timeseries_path
from .io_csv import read_csv_safe

def load_worldbank_timeseries() -> pd.DataFrame:
    df = read_csv_safe(worldbank_timeseries_path, expected_cols=["iso3","year","pop_total","pop_density","urban_pct"])
    if df.empty: 
        return df
    df["iso3"] = df["iso3"].astype(str).str.upper()
    for c in ("year","pop_total","pop_density","urban_pct"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def wb_series_for_country(iso3: str) -> pd.DataFrame:
    df = load_worldbank_timeseries()
    if df.empty: 
        return df
    iso3u = str(iso3).upper()
    return (df[df["iso3"] == iso3u].dropna(subset=["year"])
              .sort_values("year").reset_index(drop=True))
