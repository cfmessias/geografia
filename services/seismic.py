# -*- coding: utf-8 -*-
from datetime import datetime
import pandas as pd
from services.http import safe_get_json

USGS_EQ_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

def _fmt_date(d):
    return d if isinstance(d, str) else d.isoformat()

def fetch_usgs_quakes(lat: float, lon: float, start, end, radius_km: float = 500.0,
                      minmag: float = 2.5, limit: int = 20000) -> pd.DataFrame:
    """Eventos sísmicos da USGS centrados em (lat,lon) e raio em km."""
    params = {
        "format": "geojson",
        "starttime": _fmt_date(start),
        "endtime": _fmt_date(end),
        "latitude": float(lat),
        "longitude": float(lon),
        "maxradiuskm": float(radius_km),
        "minmagnitude": float(minmag),
        "orderby": "time",
        "limit": int(limit),
    }
    data = safe_get_json(USGS_EQ_URL, params)
    feats = (data.get("features") or [])
    if not feats:
        return pd.DataFrame(columns=[
            "time_utc","latitude","longitude","depth_km","mag","place","id"
        ])

    rows = []
    for f in feats:
        prop = f.get("properties") or {}
        geom = f.get("geometry") or {}
        coords = (geom.get("coordinates") or [None, None, None])
        tms = prop.get("time")  # epoch ms
        tstr = datetime.utcfromtimestamp(tms/1000.0).strftime("%Y-%m-%d %H:%M:%S") if tms else None
        rows.append({
            "time_utc": tstr,
            "latitude": coords[1],
            "longitude": coords[0],
            "depth_km": coords[2],
            "mag": prop.get("mag"),
            "place": prop.get("place"),
            "id": f.get("id")
        })
    return pd.DataFrame(rows).sort_values("time_utc", ascending=False).reset_index(drop=True)
