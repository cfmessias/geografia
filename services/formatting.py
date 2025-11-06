# services/formatting.py
from __future__ import annotations
import pandas as pd

def fmt_int(x) -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return ""
        return f"{int(float(x)):,}".replace(",", " ")
    except Exception:
        return str(x) if x is not None else ""

def fmt_year(x) -> str:
    try:
        if pd.isna(x):
            return ""
        return str(int(x))
    except Exception:
        s = str(x)
        return s[:4] if len(s) >= 4 and s[:4].isdigit() else s
