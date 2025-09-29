# -*- coding: utf-8 -*-
from __future__ import annotations
import pandas as pd
from .data_paths import cities_path
from .io_csv import read_csv_safe, read_csv_filtered, file_sig

def load_cities_all() -> pd.DataFrame:
    cols = ["iso3","country","city","city_qid","admin","is_capital","population","year"]
    df = read_csv_safe(cities_path, expected_cols=cols)
    if df.empty: 
        return df
    df["iso3"] = df["iso3"].astype(str).str.upper()
    for c in ("population","year","is_capital"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def cities_for_iso3(iso3: str) -> pd.DataFrame:
    sig = file_sig(cities_path)
    cols = ["iso3","city","admin","is_capital","population","year"]
    df  = read_csv_filtered(str(cities_path), iso3, usecols=cols, sig=sig)
    if df.empty: 
        return df
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    return df.sort_values(["year","city"]).reset_index(drop=True)

def country_has_cities(iso3: str) -> bool:
    return not cities_for_iso3(iso3).empty
