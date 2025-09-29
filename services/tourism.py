# -*- coding: utf-8 -*-
from __future__ import annotations
import pandas as pd
from functools import lru_cache
from .data_paths import (
    tourism_timeseries_path, tourism_latest_path,
    tourism_origin_eu_path, tourism_purpose_eu_path
)
from .io_csv import read_csv_safe, read_csv_filtered, file_sig

def load_tourism_ts() -> pd.DataFrame:
    cols = ["iso3","country","indicator","indicator_name","year","value"]
    df = read_csv_safe(tourism_timeseries_path, expected_cols=cols)
    if df.empty:
        return df
    df["iso3"] = df["iso3"].astype(str).str.upper()
    for c in ("year","value"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def load_tourism_latest() -> pd.DataFrame:
    cols = ["iso3","country","indicator","indicator_name","year","value"]
    df = read_csv_safe(tourism_latest_path, expected_cols=cols)
    if df.empty:
        return df
    df["iso3"] = df["iso3"].astype(str).str.upper()
    for c in ("year","value"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def tourism_series_for_iso3(iso3: str) -> pd.DataFrame:
    df = load_tourism_ts()
    if df.empty: 
        return df
    return df[df["iso3"] == str(iso3).upper()].sort_values("year").reset_index(drop=True)

# --- mapas simples ISO3<->ISO2 para UE/EFTA (suficiente para Eurostat) ---
_ISO3_TO_ISO2_EU = {
    "AUT":"AT","BEL":"BE","BGR":"BG","HRV":"HR","CYP":"CY","CZE":"CZ","DNK":"DK","EST":"EE",
    "FIN":"FI","FRA":"FR","DEU":"DE","GRC":"EL","HUN":"HU","IRL":"IE","ITA":"IT","LVA":"LV",
    "LTU":"LT","LUX":"LU","MLT":"MT","NLD":"NL","POL":"PL","PRT":"PT","ROU":"RO","SVK":"SK",
    "SVN":"SI","ESP":"ES","SWE":"SE", "ISL":"IS","LIE":"LI","NOR":"NO","CHE":"CH"
}

@lru_cache(maxsize=1)
def _load_origin_eu() -> pd.DataFrame:
    cols = ["geo","origin","year","arrivals","unit"]
    df = read_csv_safe(tourism_origin_eu_path, expected_cols=cols)
    if df.empty: 
        return df
    for c in ("year","arrivals"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["geo"] = df["geo"].astype(str).str.upper()
    df["origin"] = df["origin"].astype(str).str.upper()
    return df

def tourism_origin_for_iso3(iso3: str) -> pd.DataFrame:
    df = _load_origin_eu()
    if df.empty:
        return df
    iso2 = _ISO3_TO_ISO2_EU.get(str(iso3).upper())
    if not iso2:
        return pd.DataFrame(columns=df.columns)
    sub = df[df["geo"] == iso2].copy()
    return sub[["origin","year","arrivals","unit"]].sort_values(["year","arrivals"], ascending=[True, False])

@lru_cache(maxsize=1)
def _load_purpose_eu() -> pd.DataFrame:
    cols = ["geo","purpose","destination","year","trips","unit"]
    df = read_csv_safe(tourism_purpose_eu_path, expected_cols=cols)
    if df.empty:
        return df
    for c in ("year","trips"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["geo"] = df["geo"].astype(str).str.upper()
    df["purpose"] = df["purpose"].astype(str).str.upper()
    df["destination"] = df["destination"].astype(str).str.upper()
    return df

def tourism_purpose_for_iso3(iso3: str) -> pd.DataFrame:
    df = _load_purpose_eu()
    if df.empty:
        return df
    iso2 = _ISO3_TO_ISO2_EU.get(str(iso3).upper())
    if not iso2:
        return pd.DataFrame(columns=df.columns)
    return df[df["geo"] == iso2].sort_values(["year","trips"], ascending=[True, False]).reset_index(drop=True)
