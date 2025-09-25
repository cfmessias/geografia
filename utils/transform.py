# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

def monthly(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    out = df.copy()
    out["year"] = out["date"].dt.year
    out["month"] = out["date"].dt.month
    out["year_month"] = out["date"].dt.to_period("M").dt.to_timestamp()
    grp = out.groupby(["year","month","year_month"], as_index=False).agg(
        t_mean=("t_mean","mean"),
        precip=("precip","sum")
    )
    return grp.sort_values("year_month")

def normals(dfm: pd.DataFrame, base_start: int, base_end: int) -> pd.DataFrame:
    base = dfm[(dfm["year"]>=base_start) & (dfm["year"]<=base_end)]
    if base.empty: return pd.DataFrame()
    return base.groupby("month", as_index=False).agg(
        t_norm=("t_mean","mean"),
        p_norm=("precip","mean")
    )

def polyfit_trend(x_years: np.ndarray, y: np.ndarray):
    if len(x_years) < 3 or np.all(np.isnan(y)): return None, None
    msk = ~np.isnan(y)
    if msk.sum() < 3: return None, None
    m, b = np.polyfit(x_years[msk], y[msk], 1)
    return m*x_years + b, m*10.0  # por década

def pick_value_for(dfm: pd.DataFrame, month: int, year: int, col: str):
    row = dfm[(dfm["month"] == month) & (dfm["year"] == year)]
    if row.empty: return None
    return float(row.iloc[0][col])

def fmt_num(x, sufixo="", nd=1):
    if x is None or (isinstance(x,float) and np.isnan(x)): return "—"
    return f"{x:.{nd}f}{sufixo}"
