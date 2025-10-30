# scripts/fetch_koppen_by_country.py
# -*- coding: utf-8 -*-
"""
Köppen por país (dominante), a partir de diários 1991-01-01..2020-12-31 do Open-Meteo /v1/climate.

• Usa: daily=temperature_2m_mean,precipitation_sum  (NÃO existe 'monthly' neste endpoint)
• Requer: models=EC_Earth3P_HR (ou outro dos 7 CMIP6 high-res)
• Agrega: diários -> média/soma por mês-do-ano → 12 valores Jan..Dez → classifica Köppen

CLI:
  pip install pandas requests
  python scripts/fetch_koppen_by_country.py --debug --iso3 PRT,ESP,USA --limit 3

Se precisares de trocar o modelo: --model EC_Earth3P_HR|MRI_AGCM3_2_S|CMCC_CM2_VHR4|FGOALS_f3_H|HiRAM_SIT_HR|MPI_ESM1_2_XR|NICAM16_8S
"""
from __future__ import annotations
import argparse, time, sys
from pathlib import Path
from typing import Dict, Any, List, Optional
import requests
import pandas as pd

API = "https://climate-api.open-meteo.com/v1/climate"
UA  = "GeografiaApp/1.3 (cfmessias.pt)"
DATA = Path("data")
IN_GEOG = DATA / "geografia_paises.csv"
OUT_CSV = DATA / "koppen.csv"
LOG_DIR = Path("logs"); LOG_FILE = LOG_DIR / "koppen_debug.log"

def log_setup(debug: bool) -> None:
    if debug:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        LOG_FILE.write_text("# koppen debug log\n", encoding="utf-8")

def pdebug(s: str, debug: bool) -> None:
    if debug:
        print(s, flush=True)
        with LOG_FILE.open("a", encoding="utf-8") as f: f.write(s.rstrip()+"\n")

def fetch_daily(lat: float, lon: float, model: str, debug: bool, retries: int = 4, timeout: int = 60) -> Optional[Dict[str, Any]]:
    params = {
        "latitude":   f"{lat:.4f}",
        "longitude":  f"{lon:.4f}",
        "start_date": "1991-01-01",
        "end_date":   "2020-12-31",
        "models":     model,  # obrigatório neste endpoint
        "daily":      "temperature_2m_mean,precipitation_sum",
        "timezone":   "UTC",
        "cell_selection": "land",
    }
    headers = {"User-Agent": UA}
    last = None
    for i in range(1, retries+1):
        try:
            r = requests.get(API, params=params, headers=headers, timeout=timeout)
            if r.status_code != 200:
                pdebug(f"[http {r.status_code}] {r.text[:160]}", debug)
            r.raise_for_status()
            js = r.json()
            if debug:
                keys = list(js.keys())
                d = js.get("daily") or {}
                dkeys = list(d.keys())
                pdebug(f"[api ok] keys={keys} daily_keys={dkeys} n={len(d.get('time',[]))} lat={lat} lon={lon}", debug)
            return js.get("daily", {})
        except Exception as e:
            last = e
            pdebug(f"[api err try {i}] {e}", debug)
            time.sleep(1.2*i)
    pdebug(f"[api fail] {last}", debug)
    return None

def to_month12(series: List[float], how: str) -> Optional[List[float]]:
    """Agrupa por mês-do-ano: se how='mean' faz média; if how='sum' soma."""
    if not isinstance(series, list) or not series:
        return None
    try:
        arr = [float(x) for x in series]
    except Exception:
        return None
    # arr deve corresponder a 1991..2020 inclusive → 30 anos; 365/366 por ano
    # Mapeamos via pandas usando a sequência de datas devolvida pela API
    return arr  # placeholder: substituído na função abaixo onde temos o índice temporal

def agg_daily_to_12(daily: Dict[str, Any], debug: bool) -> Optional[tuple[list[float], list[float]]]:
    """Converte daily -> (temp12, prec12) usando o vetor 'time' da própria API."""
    times = daily.get("time"); t = daily.get("temperature_2m_mean"); p = daily.get("precipitation_sum")
    if not isinstance(times, list) or not isinstance(t, list) or not isinstance(p, list):
        return None
    if len(times) != len(t) or len(times) != len(p) or len(times) == 0:
        return None
    try:
        s = pd.DataFrame({"time": pd.to_datetime(times, errors="coerce"), "t": pd.to_numeric(t), "p": pd.to_numeric(p)})
    except Exception as e:
        return None
    s = s.dropna(subset=["time"])
    if s.empty: return None
    s["m"] = s["time"].dt.month
    # médias mensais (temperatura) e somas mensais (precipitação), agregadas em 30 anos
    t12 = s.groupby("m")["t"].mean().reindex(range(1,13)).tolist()
    p12 = s.groupby("m")["p"].sum().reindex(range(1,13)).tolist()
    # sanity: 12 valores e sem NaN
    if any(pd.isna(x) for x in t12+p12): return None
    return t12, p12

def warm_months(temp: List[float]) -> int: return sum(1 for v in temp if v >= 10.0)
def coldest(temp: List[float]) -> float: return min(temp)
def warmest(temp: List[float]) -> float: return max(temp)
def vsum(a: List[float]) -> float: return float(sum(a))
def vmean(a: List[float]) -> float: return float(sum(a)/len(a))

def is_summer(month: int, lat: float) -> bool: return (month in (6,7,8)) if lat>=0 else (month in (12,1,2))
def seasonal_share(p_month: List[float], lat: float) -> dict:
    pann = vsum(p_month); 
    if pann <= 0: return {"summer":0.0,"winter":0.0}
    summer = sum(v for i,v in enumerate(p_month,1) if is_summer(i,lat))
    winter = pann - summer
    return {"summer":summer/pann,"winter":winter/pann}

def dryness_threshold(temp: List[float], p_month: List[float], lat: float) -> float:
    add = 28.0 if seasonal_share(p_month,lat)["summer"]>=0.7 else (14.0 if seasonal_share(p_month,lat)["winter"]>=0.7 else 0.0)
    return 2.0*vmean(temp)+add

def koppen(lat: float, temp: List[float], p: List[float]) -> str:
    Tmin, Tmax, n_warm, Pann, Pdry, Tann = coldest(temp), warmest(temp), warm_months(temp), vsum(p), min(p), vmean(temp)
    # A
    if all(x>=18.0 for x in temp):
        if Pdry>=60.0: return "Af"
        elif Pdry>=100.0-(Pann/25.0): return "Am"
        else: return "Aw"
    # B
    Pth = dryness_threshold(temp,p,lat)
    if Pann < Pth:
        return ("BW" if Pann < 0.5*Pth else "BS") + ("h" if Tann>=18.0 else "k")
    # C/D/E
    if 0.0 < Tmin < 18.0 and n_warm >= 4: major = "C"
    elif Tmin <= 0.0 and n_warm >= 1:     major = "D"
    elif Tmax < 10.0:                     return "EF"
    else:                                 return "ET"
    # sazonalidade
    idx_s = [i for i in range(12) if is_summer(i+1,lat)]
    idx_w = [i for i in range(12) if i not in idx_s]
    Pds = min(p[i] for i in idx_s) if idx_s else 0.0
    Pww = max(p[i] for i in idx_w) if idx_w else 0.0
    Pdw = min(p[i] for i in idx_w) if idx_w else 0.0
    Pws = max(p[i] for i in idx_s) if idx_s else 0.0
    second = "s" if (Pds<40.0 and Pww>0 and Pds<(Pww/3.0)) else ("w" if (Pws>0 and Pdw<(Pws/10.0)) else "f")
    third  = "a" if (warmest(temp)>=22.0 and n_warm>=4) else ("b" if n_warm>=4 else "c")
    return major+second+third

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--iso3", type=str, default="", help="Filtra ISO3: PRT,ESP,USA")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=0.4)
    ap.add_argument("--model", type=str, default="EC_Earth3P_HR",
                    help="Um dos: EC_Earth3P_HR,MRI_AGCM3_2_S,CMCC_CM2_VHR4,FGOALS_f3_H,HiRAM_SIT_HR,MPI_ESM1_2_XR,NICAM16_8S")
    return ap.parse_args()

def main() -> None:
    args = parse_args(); log_setup(args.debug)
    DATA.mkdir(parents=True, exist_ok=True)
    if not IN_GEOG.exists():
        print(f"[erro] falta {IN_GEOG}"); sys.exit(1)
    df = pd.read_csv(IN_GEOG, sep=";", dtype=str, encoding="utf-8", keep_default_na=False)
    df["capital_lat"] = pd.to_numeric(df.get("capital_lat", 0), errors="coerce")
    df["capital_lon"] = pd.to_numeric(df.get("capital_lon", 0), errors="coerce")
    if args.iso3:
        only = {x.strip().upper() for x in args.iso3.split(",") if x.strip()}
        df = df[df["iso3"].str.upper().isin(only)].copy()
    if args.limit>0: df = df.head(args.limit).copy()
    pdebug(f"[info] países a processar: {len(df)}", args.debug)
    rows=[]; skipped=0
    for _, r in df.iterrows():
        iso3 = str(r.get("iso3","")).upper(); name=r.get("country","")
        lat=r.get("capital_lat"); lon=r.get("capital_lon")
        if pd.isna(lat) or pd.isna(lon):
            skipped+=1; pdebug(f"[skip coords] {iso3} {name}", args.debug); continue
        daily = fetch_daily(float(lat), float(lon), args.model, args.debug)
        if not daily:
            skipped+=1; pdebug(f"[skip api] {iso3} {name}", args.debug); continue
        tp = agg_daily_to_12(daily, args.debug)
        if not tp:
            skipped+=1; pdebug(f"[skip shape] {iso3} {name}", args.debug); continue
        t12, p12 = tp
        klass = koppen(float(lat), t12, p12)
        rows.append({"country_iso3": iso3, "koppen": klass})
        time.sleep(max(0.0, args.sleep))
    out = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["country_iso3","koppen"])
    out = out.sort_values("country_iso3").reset_index(drop=True)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, sep=";", index=False, encoding="utf-8")
    print(f"OK: {OUT_CSV} ({len(out)}) | skipped={skipped}")
    if args.debug: print(f"[debug] log em: {LOG_FILE}", flush=True)

if __name__ == "__main__":
    main()
