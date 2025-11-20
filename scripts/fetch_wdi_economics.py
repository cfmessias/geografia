# scripts/fetch_wdi_economics.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import List

import pandas as pd
from urllib.error import URLError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUT_ECON = DATA_DIR / "wdi_economics.csv"

# Base dos ficheiros CSV WDI na plataforma Data360
WB_WDI_FILES_BASE = "https://data360files.worldbank.org/data360-data/data/WB_WDI"

# Indicadores económicos usados no painel
ECON_INDICATORS: List[str] = [
    # PIB e crescimento
    "NY.GDP.MKTP.CD",      # PIB (US$, corrente)
    "NY.GDP.MKTP.KD",      # PIB (constante, US$)
    "NY.GDP.MKTP.KD.ZG",   # Crescimento do PIB (% anual)

    # PIB per capita
    "NY.GDP.PCAP.CD",      # PIB per capita (US$, corrente)
    "NY.GDP.PCAP.KD.ZG",   # Crescimento PIB pc (% anual)

    # Pobreza e desigualdade
    "SI.POV.DDAY",         # Pobreza a 2.15 USD/dia
    "SI.POV.LMIC",         # Pobreza 3.65 (LMIC)
    "SI.POV.UMIC",         # Pobreza 6.85 (UMIC)
    "SI.POV.GINI",         # Índice de Gini
]


# -------------------------------------------------------------------
# Utilitários
# -------------------------------------------------------------------

def _parse_date_range(date: str) -> tuple[int, int]:
    """Converte '1990:2024' em (1990, 2024)."""
    try:
        a, b = (date or "").split(":")
        return int(a), int(b)
    except Exception:
        return 1990, 2024


def _indicator_to_file_id(code: str) -> str:
    """NY.GDP.MKTP.CD -> WB_WDI_NY_GDP_MKTP_CD"""
    return f"WB_WDI_{code.replace('.', '_')}"


def read_iso3_from_csv(path: Path) -> List[str]:
    """
    Lê um CSV com coluna 'iso3' e devolve a lista de códigos (únicos).
    Usado com data/countries_profiles.csv.
    """
    df = pd.read_csv(path, sep=";")
    if "iso3" not in df.columns:
        raise SystemExit(f"[erro] {path} não tem coluna 'iso3'")
    iso3 = (
        df["iso3"]
        .astype(str)
        .str.upper()
        .str.strip()
        .dropna()
        .unique()
        .tolist()
    )
    return sorted([c for c in iso3 if len(c) == 3])


def normalize_iso_list(iso_arg: str | None) -> List[str]:
    if not iso_arg:
        return []
    parts = [p.strip().upper() for p in iso_arg.split(",") if p.strip()]
    parts = [p for p in parts if len(p) == 3]
    # remover duplicados mantendo ordem
    return sorted(list(dict.fromkeys(parts)))


def save_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, sep=";", index=False, encoding="utf-8")


# -------------------------------------------------------------------
# Ligação ao WDI via Data360 (CSVs)
# -------------------------------------------------------------------

def fetch_indicator_via_csv(iso3_list: List[str], code: str, date: str) -> pd.DataFrame:
    """
    Vai buscar um indicador WDI usando o CSV da Data360 (WB_WDI),
    filtra pelos iso3 desejados e intervalo de anos, e devolve long:

        iso3 | year | code | value
    """
    year_min, year_max = _parse_date_range(date)
    file_id = _indicator_to_file_id(code)
    url = f"{WB_WDI_FILES_BASE}/{file_id}.csv"

    print(f"[down] {code} (CSV Data360) …")

    try:
        raw = pd.read_csv(url)
    except URLError as e:
        print(f"[warn] erro de ligação a {url}: {e}", file=sys.stderr)
        return pd.DataFrame(columns=["iso3", "year", "code", "value"])
    except Exception as e:
        print(f"[warn] erro a ler {url}: {e}", file=sys.stderr)
        return pd.DataFrame(columns=["iso3", "year", "code", "value"])

    if raw.empty:
        return pd.DataFrame(columns=["iso3", "year", "code", "value"])

    needed = {"REF_AREA", "TIME_PERIOD", "OBS_VALUE"}
    if not needed.issubset(raw.columns):
        print(
            f"[warn] CSV {file_id}.csv sem colunas {needed - set(raw.columns)}",
            file=sys.stderr,
        )
        return pd.DataFrame(columns=["iso3", "year", "code", "value"])

    df = raw.copy()
    df["REF_AREA"] = df["REF_AREA"].astype(str).str.upper().str.strip()
    df = df[df["REF_AREA"].isin(iso3_list)]

    df["TIME_PERIOD_int"] = pd.to_numeric(df["TIME_PERIOD"], errors="coerce")
    df["OBS_VALUE_num"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
    df = df.dropna(subset=["TIME_PERIOD_int", "OBS_VALUE_num"])

    df = df[
        (df["TIME_PERIOD_int"] >= year_min)
        & (df["TIME_PERIOD_int"] <= year_max)
    ]
    if df.empty:
        return pd.DataFrame(columns=["iso3", "year", "code", "value"])

    out = pd.DataFrame(
        {
            "iso3": df["REF_AREA"],
            "year": df["TIME_PERIOD_int"].astype("Int64"),
            "code": code,
            "value": df["OBS_VALUE_num"],
        }
    )
    return out


def fetch_all(iso3_list: List[str], date: str) -> pd.DataFrame:
    """
    Vai buscar todos os indicadores definidos em ECON_INDICATORS
    para a lista de países indicada.
    """
    frames: list[pd.DataFrame] = []

    for code in ECON_INDICATORS:
        df = fetch_indicator_via_csv(iso3_list, code, date)
        if df.empty:
            print(f"[warn] {code}: sem dados para o intervalo {date}")
        else:
            frames.append(df)
        # pequena pausa para ser simpático com o servidor
        time.sleep(0.3)

    if not frames:
        return pd.DataFrame(columns=["iso3", "year", "code", "value"])

    out = pd.concat(frames, ignore_index=True)
    return out


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Fetch indicadores WDI económicos (Data360 CSV) para uso offline."
    )
    ap.add_argument(
        "--countries",
        help="CSV com coluna iso3 (por omissão: data/countries_profiles.csv)",
    )
    ap.add_argument(
        "--iso3",
        help="Lista de países (ex: PRT,ESP,FRA). Se não indicado usa --countries.",
    )
    ap.add_argument(
        "--date",
        default="1990:2024",
        help="Intervalo WDI (ex.: 1990:2024)",
    )
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
            sys.exit(
                f"[erro] Não foi indicado --countries/--iso3 e {default_csv} não existe."
            )
        print(f"[info] A usar CSV por omissão: {default_csv}")
        iso3 = read_iso3_from_csv(default_csv)

    print(f"[run] países: {len(iso3)} | intervalo: {args.date}")

    df_long = fetch_all(iso3, args.date)
    if df_long.empty:
        print("[warn] Sem dados recebidos.")
        save_csv(df_long, OUT_ECON)
        return

    save_csv(df_long, OUT_ECON)
    print(f"[ok] {OUT_ECON} | rows: {len(df_long)}")


if __name__ == "__main__":
    main()
