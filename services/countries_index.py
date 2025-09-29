# -*- coding: utf-8 -*-
from __future__ import annotations
import pandas as pd
from .data_paths import countries_seed_path, countries_profiles_path
from .io_csv import read_csv_safe

def have_master_profiles() -> bool:
    return countries_profiles_path.exists()

def load_profiles_master() -> pd.DataFrame:
    df = read_csv_safe(countries_profiles_path, expected_cols=["name","iso3","qid","capital","area_km2","population"])
    if df.empty:
        return df
    df["iso3"] = df["iso3"].astype(str).str.upper()
    return df

def list_available_countries() -> pd.DataFrame:
    if have_master_profiles():
        df = load_profiles_master()
        if df.empty:
            return pd.DataFrame(columns=["name","iso3","qid"])
        out = pd.DataFrame({
            "name": df.get("name", pd.Series(dtype=str)),
            "iso3": df.get("iso3", pd.Series(dtype=str)).astype(str).str.upper(),
            "qid":  df.get("qid",  pd.Series(dtype=str)),
        })
        return (out.dropna(subset=["name"]).drop_duplicates(subset=["iso3"])
                    .sort_values("name").reset_index(drop=True))
    # fallback para seed
    seed = read_csv_safe(countries_seed_path)
    if seed.empty:
        return pd.DataFrame(columns=["name","iso3","qid"])
    out = pd.DataFrame({
        "name": seed.get("name_pt", seed.get("name_en", pd.Series(dtype=str))),
        "iso3": seed.get("iso3", pd.Series(dtype=str)).astype(str).str.upper(),
        "qid":  pd.Series([], dtype=str),
    })
    return (out.dropna(subset=["name"]).drop_duplicates(subset=["iso3"])
              .sort_values("name").reset_index(drop=True))
