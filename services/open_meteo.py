# -*- coding: utf-8 -*-
from datetime import date, timedelta
import pandas as pd
from services.http import safe_get_json

OPEN_METEO_GEOCODE = "https://geocoding-api.open-meteo.com/v1/search"
OPEN_METEO_HIST = "https://archive-api.open-meteo.com/v1/era5"

YESTERDAY = date.today() - timedelta(days=1)

def geocode(query: str, count: int = 5) -> pd.DataFrame:
    if not query:
        return pd.DataFrame()
    data = safe_get_json(OPEN_METEO_GEOCODE, {
        "name": query, "count": count, "language": "pt", "format": "json"
    })
    results = data.get("results", []) or []
    rows = []
    for it in results:
        rows.append({
            "label": f"{it.get('name')}{', ' + it.get('admin1') if it.get('admin1') else ''} — {it.get('country')}",
            "latitude": it.get("latitude"),
            "longitude": it.get("longitude"),
            "timezone": it.get("timezone") or "auto",
        })
    return pd.DataFrame(rows)

def fetch_daily(lat: float, lon: float, tz: str, start: date, end: date) -> pd.DataFrame:
    start = max(start, date(1940, 1, 1))
    end = min(end, YESTERDAY)
    if start > end:
        return pd.DataFrame(columns=["date", "t_mean", "precip"])
    data = safe_get_json(OPEN_METEO_HIST, {
        "latitude": float(lat), "longitude": float(lon),
        "start_date": start.isoformat(), "end_date": end.isoformat(),
        "daily": ["temperature_2m_mean", "precipitation_sum"],
        "timezone": tz or "auto",
    })
    if "daily" not in data or not data.get("daily"):
        return pd.DataFrame(columns=["date", "t_mean", "precip"])
    d = data["daily"]
    return pd.DataFrame({
        "date": pd.to_datetime(d.get("time", [])),
        "t_mean": d.get("temperature_2m_mean", []),
        "precip": d.get("precipitation_sum", []),
    })
