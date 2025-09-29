# -*- coding: utf-8 -*-
from __future__ import annotations
import pandas as pd
from typing import Tuple
from .data_paths import leaders_current_path, leaders_history_path
from .io_csv import read_csv_safe

def load_leaders_current() -> pd.DataFrame:
    cols = ["iso3","country","role","person","person_qid","start","end"]
    df = read_csv_safe(leaders_current_path, expected_cols=cols)
    if df.empty: 
        return df
    df["iso3"] = df["iso3"].astype(str).str.upper()
    return df

def load_leaders_history() -> pd.DataFrame:
    cols = ["iso3","country","role","person","person_qid","start","end"]
    df = read_csv_safe(leaders_history_path, expected_cols=cols)
    if df.empty: 
        return df
    df["iso3"] = df["iso3"].astype(str).str.upper()
    return df

def leaders_for_iso3(iso3: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    iso3u = str(iso3).upper()
    return (load_leaders_current().query("iso3 == @iso3u"),
            load_leaders_history().query("iso3 == @iso3u"))
