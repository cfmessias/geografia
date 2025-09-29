# -*- coding: utf-8 -*-
from __future__ import annotations
import pandas as pd
from .data_paths import unesco_path
from .io_csv import read_csv_safe, read_csv_filtered, file_sig

def load_unesco_all() -> pd.DataFrame:
    cols = ["iso3","country","site","site_qid","year","lat","lon","category"]
    df = read_csv_safe(unesco_path, expected_cols=cols)
    if df.empty: 
        return df
    df["iso3"] = df["iso3"].astype(str).str.upper()
    for c in ("year","lat","lon"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def unesco_for_iso3(iso3: str) -> pd.DataFrame:
    sig = file_sig(unesco_path)
    cols = ["iso3","site","site_qid","year","lat","lon","category"]
    df  = read_csv_filtered(str(unesco_path), iso3, usecols=cols, sig=sig)
    if df.empty: 
        return df
    for c in ("year","lat","lon"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_values(["year","site"]).reset_index(drop=True)
