# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
from functools import lru_cache
import io, pandas as pd

__all__ = ["read_csv_safe", "read_csv_safe_any", "file_sig", "read_csv_filtered"]

def read_csv_safe(path: Path, expected_cols: list[str] | None = None) -> pd.DataFrame:
    if not path or not Path(path).exists():
        return pd.DataFrame(columns=expected_cols or None)
    raw = Path(path).read_bytes()
    if not raw.strip():
        return pd.DataFrame(columns=expected_cols or None)
    for sep in (";", ",", "\t"):
        try:
            df = pd.read_csv(io.BytesIO(raw), sep=sep, engine="python")
            break
        except Exception:
            df = None
    if df is None:
        try:
            df = pd.read_csv(io.BytesIO(raw), sep=None, engine="python")
        except Exception:
            return pd.DataFrame(columns=expected_cols or None)
    if expected_cols:
        for c in expected_cols:
            if c not in df.columns:
                df[c] = pd.NA
        df = df[expected_cols]
    return df

def read_csv_safe_any(path: Path) -> pd.DataFrame:
    if not path or not Path(path).exists():
        return pd.DataFrame()
    raw = Path(path).read_bytes()
    if not raw.strip():
        return pd.DataFrame()
    for sep in (";", ",", "\t"):
        try:
            return pd.read_csv(io.BytesIO(raw), sep=sep, engine="python")
        except Exception:
            pass
    try:
        return pd.read_csv(io.BytesIO(raw), sep=None, engine="python")
    except Exception:
        return pd.DataFrame()

def file_sig(path: Path) -> tuple[int | None, int | None]:
    try:
        st = Path(path).stat()
        return (st.st_mtime_ns, st.st_size)
    except FileNotFoundError:
        return (None, None)

@lru_cache(maxsize=256)
def read_csv_filtered(
    path_str: str, iso3: str, col_iso3: str = "iso3",
    usecols: list[str] | None = None, dtype: dict | None = None,
    sep: str | None = None, sig: tuple[int | None, int | None] | None = None,
) -> pd.DataFrame:
    path = Path(path_str)
    if not path.exists():
        return pd.DataFrame(columns=usecols or [])
    iso3 = str(iso3).upper()
    chunks = []
    for ch in pd.read_csv(path, sep=sep or ";", usecols=usecols, dtype=dtype,
                          chunksize=200_000, low_memory=False, engine="python"):
        if col_iso3 not in ch.columns:
            continue
        m = ch[col_iso3].astype(str).str.upper().eq(iso3)
        if m.any():
            chunks.append(ch.loc[m])
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=usecols or [])
