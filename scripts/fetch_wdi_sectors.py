# scripts/fetch_wdi_sectors.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, time, sys, json, math
from pathlib import Path
from typing import Iterable, List, Dict
import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUT_LONG   = DATA_DIR / "wdi_sectors_long.csv"
OUT_WIDE   = DATA_DIR / "wdi_sectors_wide.csv"
OUT_LATEST = DATA_DIR / "wdi_sectors_latest.csv"

# Indicadores WDI
IND = {
    # VAB (% do PIB)
    "NV.AGR.TOTL.ZS": "agr_vab",
    "NV.IND.TOTL.ZS": "ind_vab",
    "NV.SRV.TOTL.ZS": "srv_vab",
    # Emprego (% do total)
    "SL.AGR.EMPL.ZS": "agr_emp",
    "SL.IND.EMPL.ZS": "ind_emp",
    "SL.SRV.EMPL.ZS": "srv_emp",
}

DEFAULT_DATE = "1990:2024"
WB_URL = "https://api.worldbank.org/v2/country/{cc}/indicator/{code}?format=json&per_page=20000&date={date}"

HEADERS = {
    "User-Agent": "GeografiaApp/1.0 (contact: you@example.com)"
}

def read_iso3_from_csv(path: Path) -> List[str]:
    df = pd.read_csv(path, sep=";", dtype=str, encoding="utf-8", keep_default_na=False)
    for c in ["iso3", "ISO3", "country_iso3"]:
        if c in df.columns:
            iso = df[c].astype(str).str.upper().str.strip()
            iso = iso[iso.str.len() == 3]
            return sorted(iso.unique().tolist())
    raise SystemExit(f"[erro] CSV {path} não tem coluna iso3/ISO3/country_iso3")

def normalize_iso_list(iso_arg: str | None) -> List[str]:
    if not iso_arg:
        return []
    parts = [p.strip().upper() for p in iso_arg.split(",") if p.strip()]
    parts = [p for p in parts if len(p) == 3]
    return sorted(list(dict.fromkeys(parts)))

def chunked(seq: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def fetch_indicator_for_batch(countries: List[str], code: str, date: str, sleep_s: float = 0.5, retries: int = 3) -> pd.DataFrame:
    cc = ";".join(countries)
    url = WB_URL.format(cc=cc, code=code, date=date)
    for attempt in range(1, retries+1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")
            js = r.json()
            if not isinstance(js, list) or len(js) < 2:
                # às vezes vem só metadados
                time.sleep(sleep_s)
                return pd.DataFrame(columns=["iso3","year","code","value"])
            rows = []
            for item in js[1]:
                iso3  = str(item.get("countryiso3code","")).upper()
                year  = str(item.get("date","")).strip()
                value = item.get("value", None)
                if not iso3 or not year:
                    continue
                rows.append({"iso3": iso3, "year": year, "code": code, "value": value})
            time.sleep(sleep_s)  # respeitar limites
            return pd.DataFrame(rows, columns=["iso3","year","code","value"])
        except Exception as e:
            if attempt == retries:
                print(f"[warn] falhou {code} ({len(countries)} países): {e}", file=sys.stderr)
                return pd.DataFrame(columns=["iso3","year","code","value"])
            time.sleep(1.5 * attempt)
    return pd.DataFrame(columns=["iso3","year","code","value"])

def fetch_all(iso3_list: List[str], date: str) -> pd.DataFrame:
    frames = []
    # pedir por indicador, em lotes de países (p.ex. 30 por pedido é seguro)
    for code in IND.keys():
        print(f"[down] {code} …")
        for batch in chunked(iso3_list, 30):
            df_b = fetch_indicator_for_batch(batch, code, date)
            if not df_b.empty:
                frames.append(df_b)
    if frames:
        df = pd.concat(frames, ignore_index=True)
        # normalizar tipos
        df["iso3"] = df["iso3"].astype(str).str.upper().str.strip()
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
        df["code"] = df["code"].astype(str)
        df["value"]= pd.to_numeric(df["value"], errors="coerce")
        # manter apenas iso3 de interesse
        df = df[df["iso3"].isin(iso3_list)]
        return df
    return pd.DataFrame(columns=["iso3","year","code","value"])

def make_wide(df_long: pd.DataFrame) -> pd.DataFrame:
    if df_long.empty:
        return pd.DataFrame(columns=["iso3","year"] + list(IND.values()))
    dfw = (df_long
           .assign(var=lambda d: d["code"].map(IND))
           .pivot_table(index=["iso3","year"], columns="var", values="value", aggfunc="first")
           .reset_index())
    # garantir colunas na ordem
    cols = ["iso3","year"] + list(IND.values())
    for c in cols:
        if c not in dfw.columns:
            dfw[c] = pd.NA
    return dfw[cols].sort_values(["iso3","year"]).reset_index(drop=True)

def latest_complete_row(dfw: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Para cada iso3, procura o ano mais recente em que TODAS as colunas 'cols' não são NaN.
    """
    out_rows = []
    for iso, g in dfw.groupby("iso3", sort=False):
        g2 = g.dropna(subset=cols, how="any")
        if g2.empty:
            continue
        row = g2.sort_values("year").iloc[-1]
        out = {"iso3": iso, "year": int(row["year"])}
        for c in cols:
            out[c] = float(row[c]) if pd.notna(row[c]) else None
        out_rows.append(out)
    return pd.DataFrame(out_rows)

def save_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, sep=";", encoding="utf-8")

def main():
    
    ap = argparse.ArgumentParser(description="Fetch WDI sectoral shares (value added % GDP and employment %).")
    ap.add_argument("--countries",  type=str, help="CSV com lista de países (coluna iso3/ISO3/country_iso3)")
    ap.add_argument("--iso3",       type=str, help="Lista de ISO3 separados por vírgula (ex.: PRT,DEU,ESP)")
    ap.add_argument("--date",       type=str, default=DEFAULT_DATE, help="Intervalo WDI (ex.: 1990:2024)")
    args = ap.parse_args()

    # ===== países (usa data/countries_profiles.csv por omissão) =====
    iso3: List[str] = []
    if args.countries:
        iso3 = read_iso3_from_csv(Path(args.countries))
    elif args.iso3:
        iso3 = normalize_iso_list(args.iso3)
    else:
        default_csv = DATA_DIR / "countries_profiles.csv"
        if not default_csv.exists():
            sys.exit(f"[erro] Não foi indicado --countries/--iso3 e {default_csv} não existe.")
        print(f"[info] A usar CSV por omissão: {default_csv}")
        iso3 = read_iso3_from_csv(default_csv)


    print(f"[run] países: {len(iso3)} | intervalo: {args.date}")

    df_long = fetch_all(iso3, args.date)
    if df_long.empty:
        print("[warn] Sem dados recebidos.")
        save_csv(df_long, OUT_LONG)
        save_csv(pd.DataFrame(), OUT_WIDE)
        save_csv(pd.DataFrame(), OUT_LATEST)
        return

    # Guardar LONG
    save_csv(df_long, OUT_LONG)
    print(f"[ok] {OUT_LONG} | rows: {len(df_long)}")

    # WIDE
    df_wide = make_wide(df_long)
    save_csv(df_wide, OUT_WIDE)
    print(f"[ok] {OUT_WIDE} | rows: {len(df_wide)}")

    # LATEST (ano mais recente com 3 valores completos em cada grupo)
    latest_vab  = latest_complete_row(df_wide, ["agr_vab","ind_vab","srv_vab"])
    latest_emp  = latest_complete_row(df_wide, ["agr_emp","ind_emp","srv_emp"])
    latest = pd.merge(latest_vab.add_prefix("vab_"), latest_emp.add_prefix("emp_"),
                      left_on="vab_iso3", right_on="emp_iso3", how="outer")
    # normalizar nomes
    if not latest.empty:
        latest["iso3"] = latest["vab_iso3"].fillna(latest["emp_iso3"])
        latest = latest.drop(columns=["vab_iso3","emp_iso3"])
        latest = latest.rename(columns={"vab_year":"year_vab", "emp_year":"year_emp"})
        # ordenar colunas
        base_cols = ["iso3","year_vab","vab_agr_vab","vab_ind_vab","vab_srv_vab","year_emp","emp_agr_emp","emp_ind_emp","emp_srv_emp"]
        keep_cols = [c for c in base_cols if c in latest.columns] + [c for c in latest.columns if c not in base_cols]
        latest = latest[keep_cols]

    save_csv(latest, OUT_LATEST)
    print(f"[ok] {OUT_LATEST} | rows: {len(latest)}")

if __name__ == "__main__":
    main()
