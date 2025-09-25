# -*- coding: utf-8 -*-
"""
Baixa dados de TURISMO (e complementa migração já existente) para uso OFFLINE.

Saídas (CSV em data/, sep=";"):
  - tourism_timeseries.csv         (WDI, séries anuais por país+indicador)
  - tourism_latest.csv            (WDI, último valor por país+indicador)
  - tourism_origin_eu.csv         (EUROSTAT, origem dos turistas por destino EU/EFTA)
  - tourism_purpose_eu.csv        (EUROSTAT, propósito das viagens dos residentes UE/EFTA)

Fontes:
  - World Bank WDI (indicadores de turismo) — ver códigos na constante WDI_INDICATORS.
  - Eurostat Statistics API (JSON-stat) — datasets:
      * tour_occ_arnraw  (chegadas por país de residência, inbound)
      * tour_dem_tttot   (n.º de viagens por duração, propósito, destino)

Observações:
  - Eurostat só cobre UE/EFTA; para o resto do mundo usa-se apenas WDI.
  - O módulo Eurostat pode ser desligado pondo EUROSTAT_ENABLE=False.
"""

from __future__ import annotations
from pathlib import Path
import csv, sys, time, math, itertools, json, re, random
import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# -------- WDI (World Bank) ----------------------------------------------------
WDI_BASE = "https://api.worldbank.org/v2/country/all/indicator/{code}"
WDI_UA   = "GeoTourism/1.0 (+https://example.org)"
WDI_INDICATORS = {
    # code                 : readable name
    "ST.INT.ARVL":        "Arrivals (inbound tourists, number)",
    "ST.INT.DPRT":        "Departures (outbound tourists, number)",
    "ST.INT.RCPT.CD":     "Tourism receipts (current US$)",
    "ST.INT.XPND.CD":     "Tourism expenditures (current US$)",
    "ST.INT.RCPT.XP.ZS":  "Tourism receipts (% of exports)",
    "ST.INT.XPND.MP.ZS":  "Tourism expenditures (% of imports)",
}

# -------- EUROSTAT (opcional) -------------------------------------------------
EUROSTAT_ENABLE = True
ESTAT_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
ESTAT_UA   = "GeoTourism/1.0 (+https://example.org)"

# ISO3->ISO2 (basta UE/EFTA; completa conforme necessário)
ISO3_TO_ISO2_EU = {
    # UE
    "AUT":"AT","BEL":"BE","BGR":"BG","HRV":"HR","CYP":"CY","CZE":"CZ","DNK":"DK","EST":"EE",
    "FIN":"FI","FRA":"FR","DEU":"DE","GRC":"EL","HUN":"HU","IRL":"IE","ITA":"IT","LVA":"LV",
    "LTU":"LT","LUX":"LU","MLT":"MT","NLD":"NL","POL":"PL","PRT":"PT","ROU":"RO","SVK":"SK",
    "SVN":"SI","ESP":"ES","SWE":"SE",
    # EFTA
    "ISL":"IS","LIE":"LI","NOR":"NO","CHE":"CH"
}
EU_EFTA_ISO2 = set(ISO3_TO_ISO2_EU.values())

# --------------- helpers ------------------------------------------------------

def _session(ua: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": ua})
    return s

def _safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

# ------------------- WDI ------------------------------------------------------

def fetch_wdi_indicator(code: str, sess: requests.Session) -> pd.DataFrame:
    url = WDI_BASE.format(code=code)
    params = {"format":"json", "per_page": 20000}
    rows = []
    for attempt in range(5):
        try:
            r = sess.get(url, params=params, timeout=40)
            r.raise_for_status()
            js = r.json()
            meta, data = js[0], js[1]
            pages = int(meta.get("pages", 1) or 1)
            allrows = list(data)
            # páginas adicionais (raro com per_page grande)
            for page in range(2, pages+1):
                r2 = sess.get(url, params={"format":"json","per_page":20000,"page":page}, timeout=40)
                r2.raise_for_status()
                js2 = r2.json()
                allrows.extend(js2[1])
            for z in allrows:
                iso3 = (z.get("countryiso3code") or "").upper()
                # filtra agregados regionais
                if not re.fullmatch(r"[A-Z]{3}", iso3): 
                    continue
                country = (z.get("country") or {}).get("value") or ""
                year = z.get("date")
                val  = z.get("value")
                if year is None: 
                    continue
                rows.append([iso3, country, code, WDI_INDICATORS.get(code, code), int(year), None if val is None else float(val)])
            break
        except Exception:
            time.sleep(0.6 + random.random())
            if attempt == 4:
                return pd.DataFrame(columns=["iso3","country","indicator","indicator_name","year","value"])

    df = pd.DataFrame(rows, columns=["iso3","country","indicator","indicator_name","year","value"])
    return df

def collect_wdi() -> tuple[pd.DataFrame, pd.DataFrame]:
    s = _session(WDI_UA)
    frames = []
    for code in WDI_INDICATORS:
        print(f"[WDI] {code} …")
        df = fetch_wdi_indicator(code, s)
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame(), pd.DataFrame()
    ts = pd.concat(frames, ignore_index=True)
    ts = ts.sort_values(["indicator","iso3","year"])
    latest = (
        ts.dropna(subset=["value"])
          .sort_values(["iso3","indicator","year"])
          .groupby(["iso3","indicator","indicator_name","country"], as_index=False, observed=False)
          .tail(1)
          .sort_values(["indicator","value"], ascending=[True, False])
    )
    return ts, latest

# ------------------- EUROSTAT (Statistics API, JSON-stat) ---------------------

def _jsonstat_to_df(js: dict) -> pd.DataFrame:
    """Converte JSON-stat 2.0 (Statistics API) -> DataFrame tidy."""
    if not js:
        return pd.DataFrame()
    # A API pode devolver {"error": {...}} ou {"warning": ...}
    if "error" in js:
        return pd.DataFrame()
    # A resposta é um objeto JSON-stat; pega no primeiro dataset
    dataset_key = next((k for k in js.keys() if k not in ("version","class","label","source","updated","note","error","warning")), None)
    data = js.get(dataset_key, {})
    dims_order = data.get("id") or list((data.get("dimension") or {}).keys())
    dimobj = data.get("dimension", {})
    # lista de códigos por dimensão, na ordem
    axes = []
    for d in dims_order:
        cat = (dimobj.get(d, {}) or {}).get("category", {})
        idx = cat.get("index")
        if idx is None:
            axes.append([])
            continue
        if isinstance(idx, list):
            codes = idx
        elif isinstance(idx, dict):
            codes = [code for code, pos in sorted(idx.items(), key=lambda kv: kv[1])]
        else:
            codes = []
        axes.append(codes)
    # valores: pode ser lista densa ou dict {pos: valor}
    v = data.get("value")
    if v is None:
        return pd.DataFrame()
    dense = isinstance(v, list)
    size = [len(a) for a in axes]
    total = math.prod(size) if size else 0

    out_rows = []
    # função para mapear tupla de índices -> posição linear
    def _linpos(ix):
        pos = 0
        mul = 1
        for s, i in zip(reversed(size), reversed(ix)):
            pos += i * mul
            mul *= s
        return pos

    # percorre o produto cartesiano das dimensões
    for ix_tuple in itertools.product(*[range(n) for n in size]):
        if not size:
            break
        pos = _linpos(ix_tuple)
        val = None
        if dense:
            if pos < len(v):
                val = v[pos]
        else:
            sval = str(pos)
            if sval in v:
                val = v[sval]
        if val is None:
            continue  # ignora missing
        row = {}
        for dim_name, ax, i in zip(dims_order, axes, ix_tuple):
            if i < len(ax):
                row[dim_name] = ax[i]
        row["value"] = _safe_float(val)
        out_rows.append(row)
    return pd.DataFrame(out_rows)

def estat_query(dataset: str, **filters) -> pd.DataFrame:
    """
    Chama Statistics API e devolve DataFrame tidy com colunas de dimensão + 'value'.
    Filtros: usar nomes de dimensão (ex.: geo='PT', sinceTimePeriod='2010', unit='NR', resid='DE', etc.)
    """
    if not EUROSTAT_ENABLE:
        return pd.DataFrame()
    s = _session(ESTAT_UA)
    params = {"format": "JSON", "lang": "EN"}
    params.update({k: str(v) for k, v in filters.items() if v is not None})
    url = f"{ESTAT_BASE}/{dataset}"
    for attempt in range(4):
        try:
            r = s.get(url, params=params, timeout=60)
            if r.status_code == 413:  # resposta assíncrona – tentar reduzir (ou re-tentar)
                time.sleep(1.5)
                continue
            r.raise_for_status()
            js = r.json()
            df = _jsonstat_to_df(js)
            return df
        except Exception:
            time.sleep(0.8 + random.random())
    return pd.DataFrame()

def collect_eurostat_origin(last_years: int = 10) -> pd.DataFrame:
    """
    Chegadas por país de residência para destinos UE/EFTA (tour_occ_arnraw).
    """
    frames = []
    since = None
    # usa "últimos N anos": Statistics API aceita 'sinceTimePeriod'
    # se quisermos sempre 2010+ trocamos por since="2010"
    if last_years and last_years > 0:
        since = f"{pd.Timestamp.now().year - last_years}"
    for iso2 in sorted(EU_EFTA_ISO2):
        print(f"[ESTAT] origem -> {iso2}")
        # Nota: unit costuma ser 'NR' (number); resid (country of residence) deixamos em aberto para trazer todos.
        df = estat_query(
            "tour_occ_arnraw",
            geo=iso2,
            sinceTimePeriod=since
        )
        if df.empty:
            continue
        # renomeia campos comuns (quando presentes)
        # dimensões usuais: unit, resid, geo, time, value
        if "time" not in df.columns and "TIME" in df.columns:
            df = df.rename(columns={"TIME":"time"})
        df["dest_geo"] = iso2
        frames.append(df)
        time.sleep(0.25)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    # limpeza mínima
    cols = [c for c in ["dest_geo","resid","time","value","unit"] if c in out.columns]
    out = out[cols].rename(columns={
        "dest_geo":"geo",
        "resid":"origin",
        "time":"year",
        "value":"arrivals",
        "unit":"unit"
    })
    # ano para int
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    return out

def collect_eurostat_purpose(last_years: int = 10) -> pd.DataFrame:
    """
    Viagens de residentes por propósito e destino (dom/abroad) — tour_dem_tttot.
    Útil para 'tipo de turismo' (negócios vs lazer) a nível nacional (procura).
    """
    frames = []
    since = f"{pd.Timestamp.now().year - last_years}" if last_years else None
    for iso2 in sorted(EU_EFTA_ISO2):
        print(f"[ESTAT] propósito -> {iso2}")
        # dimensões típicas: unit, geo, purp, dest, time
        df = estat_query(
            "tour_dem_tttot",
            geo=iso2,
            sinceTimePeriod=since
        )
        if df.empty:
            continue
        if "time" not in df.columns and "TIME" in df.columns:
            df = df.rename(columns={"TIME":"time"})
        frames.append(df)
        time.sleep(0.25)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    cols = [c for c in ["geo","purp","dest","time","value","unit"] if c in out.columns]
    out = out[cols].rename(columns={
        "purp":"purpose",
        "dest":"destination",
        "time":"year",
        "value":"trips",
        "unit":"unit"
    })
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    return out

# ------------------- main -----------------------------------------------------

def main():
    # 1) World Bank
    ts, latest = collect_wdi()
    if not ts.empty:
        ts = ts.sort_values(["indicator","iso3","year"])
        ts.to_csv(DATA_DIR / "tourism_timeseries.csv", index=False, sep=";", encoding="utf-8")
        latest.to_csv(DATA_DIR / "tourism_latest.csv", index=False, sep=";", encoding="utf-8")
        print(f"✔️ tourism_timeseries.csv: {len(ts):,} linhas")
        print(f"✔️ tourism_latest.csv: {len(latest):,} linhas")
    else:
        print("⚠️ WDI sem dados (ver ligação).")

    # 2) Eurostat (opcional)
    if EUROSTAT_ENABLE:
        origin = collect_eurostat_origin(last_years=10)
        if not origin.empty:
            origin.to_csv(DATA_DIR / "tourism_origin_eu.csv", index=False, sep=";", encoding="utf-8")
            print(f"✔️ tourism_origin_eu.csv: {len(origin):,} linhas")
        else:
            print("ℹ️ Eurostat origem: vazio (dataset/ligação pode não estar disponível para alguns países).")

        purpose = collect_eurostat_purpose(last_years=10)
        if not purpose.empty:
            purpose.to_csv(DATA_DIR / "tourism_purpose_eu.csv", index=False, sep=";", encoding="utf-8")
            print(f"✔️ tourism_purpose_eu.csv: {len(purpose):,} linhas")
        else:
            print("ℹ️ Eurostat propósito: vazio.")

if __name__ == "__main__":
    main()

