# -*- coding: utf-8 -*-
"""
Fontes de previsão:
- Open-Meteo (global, sem chave)
- IPMA (Portugal; mapeia cidade -> globalIdLocal)
- Meteostat via RapidAPI (precisa de RAPIDAPI_KEY em st.secrets ou env)
Retornam DataFrames no formato comum: date, source, place, country, tmax, tmin, precip
"""
from __future__ import annotations
from datetime import date, timedelta
import os
import pandas as pd
import requests
import streamlit as st
from services.open_meteo import YESTERDAY 
from services.http import safe_get_json


# --- resolver localId do IPMA a partir do nome da cidade (ex.: "Lisboa") ---
@st.cache_data(ttl=24 * 3600)
def ipma_resolve_local_id(city: str) -> int | None:
    try:
        url = "https://api.ipma.pt/open-data/distrits-islands.json"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        # esta lista não tem todos os locais; para cidades usa o cities.json:
        url2 = "https://api.ipma.pt/open-data/forecast/meteorology/cities.json"
        r2 = requests.get(url2, timeout=30)
        r2.raise_for_status()
        js = r2.json()
        df = pd.DataFrame(js)
        if df.empty:
            return None
        # tentar match case-insensitive no local name
        m = df[df["local"].str.lower() == city.strip().lower()]
        if m.empty:
            # fallback: contém
            m = df[df["local"].str.lower().str.contains(city.strip().lower())]
        return int(m.iloc[0]["globalIdLocal"]) if not m.empty else None
    except Exception:
        return None


# --- IPMA hourly: probabilidade de precipitação por hora (%) ---
@st.cache_data(ttl=30 * 60, show_spinner=False)
def ipma_hourly_prob(city_or_localid: str | int) -> pd.DataFrame:
    """
    Devolve DataFrame com colunas: time (datetime64), prob (%) [0..100].
    Aceita nome da cidade (ex. 'Lisboa') ou localId (int).
    Endpoint: /forecast/meteorology/cities/hourly/{localId}.json
    """
    try:
        if isinstance(city_or_localid, (int, float)) or str(city_or_localid).isdigit():
            local_id = int(city_or_localid)
        else:
            local_id = ipma_resolve_local_id(str(city_or_localid))
        if not local_id:
            return pd.DataFrame(columns=["time", "prob"])

        url = f"https://api.ipma.pt/open-data/forecast/meteorology/cities/hourly/{local_id}.json"
        r = requests.get(url, timeout=45)
        r.raise_for_status()
        j = r.json()
        rows = j.get("data") or j  # alguns dumps podem vir como lista direta
        if not rows:
            return pd.DataFrame(columns=["time", "prob"])

        df = pd.DataFrame(rows)
        # nomes típicos: 'dataPrev' (timestamp), 'precipitaProb' (string/num)
        time_col = "dataPrev" if "dataPrev" in df.columns else "data"
        prob_col = "precipitaProb" if "precipitaProb" in df.columns else "precipitaProbabilidade"

        if time_col not in df.columns or prob_col not in df.columns:
            return pd.DataFrame(columns=["time", "prob"])

        out = pd.DataFrame({
            "time": pd.to_datetime(df[time_col], errors="coerce"),
            "prob": pd.to_numeric(df[prob_col], errors="coerce"),
        }).dropna(subset=["time"])
        # garantir limites 0..100
        out["prob"] = out["prob"].clip(lower=0, upper=100)
        out = out.sort_values("time").reset_index(drop=True)
        return out
    except Exception:
        return pd.DataFrame(columns=["time", "prob"])

# -------------------------------------------------------------------
# Open-Meteo (forecast diário)
# -------------------------------------------------------------------
def openmeteo_daily(lat: float, lon: float, tz: str | None = "auto", days: int = 7) -> pd.DataFrame:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "timezone": tz or "auto",
        "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"],
        "forecast_days": days,
    }
    j = safe_get_json(url, params)
    d = j.get("daily") or {}
    if not d:
        return pd.DataFrame(columns=["date", "tmax", "tmin", "precip"])
    df = pd.DataFrame({
        "date": pd.to_datetime(d.get("time", [])),
        "tmax": d.get("temperature_2m_max", []),
        "tmin": d.get("temperature_2m_min", []),
        "precip": d.get("precipitation_sum", []),
    })
    return df

# -------------------------------------------------------------------
# IPMA (Portugal) — previsão diária por cidade
#   - lista de cidades: https://api.ipma.pt/open-data/distrits-islands.json
#   - previsão:        https://api.ipma.pt/open-data/forecast/meteorology/cities/daily/{globalIdLocal}.json
# -------------------------------------------------------------------
@st.cache_data(ttl=12*60*60)
def _ipma_city_index() -> pd.DataFrame:
    url = "https://api.ipma.pt/open-data/distrits-islands.json"
    j = safe_get_json(url, {})
    data = j.get("data") or []
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    # normaliza nomes para procurar por lowercase
    df["name_norm"] = df["local"].str.normalize("NFKD").str.encode("ascii", "ignore").str.decode("ascii").str.lower()
    return df[["globalIdLocal", "local", "name_norm"]]

def _ipma_find_city_id(city_name: str) -> int | None:
    idx = _ipma_city_index()
    if idx.empty:
        return None
    key = (city_name or "").strip()
    key = (key
           .normalize("NFKD") if hasattr(key, "normalize") else key)  # para compatibilidade
    key = (str(key).encode("ascii", "ignore").decode("ascii")).lower()
    # procura match exato; se falhar, tenta contains
    hit = idx[idx["name_norm"] == key]
    if hit.empty:
        hit = idx[idx["name_norm"].str.contains(key, na=False)]
    return None if hit.empty else int(hit.iloc[0]["globalIdLocal"])

def ipma_daily(city_name: str, days_limit: int = 5) -> pd.DataFrame:
    gid = _ipma_find_city_id(city_name)
    if gid is None:
        return pd.DataFrame(columns=["date", "tmax", "tmin", "precip"])
    url = f"https://api.ipma.pt/open-data/forecast/meteorology/cities/daily/{gid}.json"
    j = safe_get_json(url, {})
    d = j.get("data") or []
    if not d:
        return pd.DataFrame(columns=["date", "tmax", "tmin", "precip"])
    df = pd.DataFrame(d)
    # colunas: tMax, tMin, precipitaProb (probabilidade). IPMA não dá precipitação acumulada diária aqui.
    df = df.rename(columns={"tMin": "tmin", "tMax": "tmax"})
    df["date"] = pd.to_datetime(df["forecastDate"])
    # precipitação: usar probabilidade como proxy (0–100). Mantemos em coluna 'precip_prob'
    df["precip"] = pd.to_numeric(df.get("precipitaProb", 0), errors="coerce")
    df = df[["date", "tmax", "tmin", "precip"]].head(days_limit)
    # forçar numéricos
    for c in ["tmax", "tmin", "precip"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

# -------------------------------------------------------------------
# Meteostat (RapidAPI) — previsão diária por ponto
#   Guarda a key em .streamlit/secrets.toml: RAPIDAPI_KEY = "xxxx"
# -------------------------------------------------------------------
def _rapid_headers():
    key = st.secrets.get("RAPIDAPI_KEY") or os.getenv("RAPIDAPI_KEY")
    if not key:
        raise RuntimeError("RAPIDAPI_KEY em falta (defina em .streamlit/secrets.toml).")
    return {"X-RapidAPI-Key": key, "X-RapidAPI-Host": "meteostat.p.rapidapi.com"}

def meteostat_daily(lat: float, lon: float, days: int = 7, alt: int | None = None) -> pd.DataFrame:
    """
    Meteostat via RapidAPI: devolve OBSERVADO diário dos últimos `days` (até ontem).
    Usa /point/daily em vez de /point/forecast (que dá 403 no plano grátis).
    """
    end = YESTERDAY
    start = end - timedelta(days=int(days) - 1)
    url = "https://meteostat.p.rapidapi.com/point/daily"
    params = {"lat": lat, "lon": lon, "start": start.isoformat(), "end": end.isoformat(), "tz": "auto"}
    if alt is not None:
        params["alt"] = alt

    r = requests.get(url, headers=_rapid_headers(), params=params, timeout=45)
    r.raise_for_status()
    j = r.json()
    data = j.get("data") or []
    if not data:
        return pd.DataFrame(columns=["date", "tmax", "tmin", "precip"])

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    # Meteostat campos: tmin, tmax, prcp (mm). Alguns postos podem não ter tudo:
    if "tmax" not in df.columns: df["tmax"] = df.get("tavg")
    if "tmin" not in df.columns: df["tmin"] = df.get("tavg")
    df = df.rename(columns={"prcp": "precip"})
    keep = ["date", "tmax", "tmin", "precip"]
    for c in keep:
        if c not in df.columns:
            df[c] = None
    return df[keep]

def weatherapi_daily(lat: float, lon: float, days: int = 7) -> pd.DataFrame:
    """
    WeatherAPI: forecast diária até 14 dias.
    Requer WEATHERAPI_KEY em .streamlit/secrets.toml.
    Campos usados: maxtemp_c, mintemp_c, totalprecip_mm.
    """
    key = st.secrets.get("WEATHERAPI_KEY") or os.getenv("WEATHERAPI_KEY")
    if not key:
        raise RuntimeError("WEATHERAPI_KEY em falta (defina em .streamlit/secrets.toml).")

    url = "https://api.weatherapi.com/v1/forecast.json"
    params = {
        "key": key,
        "q": f"{lat},{lon}",
        "days": int(min(max(days, 1), 14)),
        "aqi": "no",
        "alerts": "no",
    }
    r = requests.get(url, params=params, timeout=45)
    r.raise_for_status()
    j = r.json()
    fc = (j.get("forecast") or {}).get("forecastday") or []
    if not fc:
        return pd.DataFrame(columns=["date", "tmax", "tmin", "precip"])
    rows = []
    for d in fc:
        day = d.get("day") or {}
        rows.append({
            "date": pd.to_datetime(d.get("date")),
            "tmax": day.get("maxtemp_c"),
            "tmin": day.get("mintemp_c"),
            "precip": day.get("totalprecip_mm"),
        })
    return pd.DataFrame(rows, columns=["date", "tmax", "tmin", "precip"])

# --- Open-Meteo: hourly (temperatura & precipitação) ---
def openmeteo_hourly(lat: float, lon: float, tz: str | None = "auto", hours: int = 24) -> pd.DataFrame:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "timezone": tz or "auto",
        "hourly": ["temperature_2m", "precipitation"],
        "forecast_days": 2,  # suficiente para cobrir 24–36h
    }
    j = safe_get_json(url, params)
    h = j.get("hourly") or {}
    if not h:
        return pd.DataFrame(columns=["time", "temp", "precip"])
    df = pd.DataFrame({
        "time": pd.to_datetime(h.get("time", [])),
        "temp": h.get("temperature_2m", []),
        "precip": h.get("precipitation", []),
    }).head(int(hours))
    return df


# --- WeatherAPI: hourly (próximas 24h) ---
def weatherapi_hourly(lat: float, lon: float, hours: int = 24) -> pd.DataFrame:
    key = st.secrets.get("WEATHERAPI_KEY") or os.getenv("WEATHERAPI_KEY")
    if not key:
        return pd.DataFrame(columns=["time", "temp", "precip"])
    url = "https://api.weatherapi.com/v1/forecast.json"
    params = {"key": key, "q": f"{lat},{lon}", "days": 2, "aqi": "no", "alerts": "no"}
    r = requests.get(url, params=params, timeout=45)
    r.raise_for_status()
    j = r.json()
    out = []
    for day in (j.get("forecast") or {}).get("forecastday", []):
        for hr in day.get("hour", []):
            out.append({
                "time": pd.to_datetime(hr.get("time")),
                "temp": hr.get("temp_c"),
                "precip": hr.get("precip_mm"),
            })
            if len(out) >= int(hours):
                break
        if len(out) >= int(hours):
            break
    return pd.DataFrame(out, columns=["time", "temp", "precip"])
