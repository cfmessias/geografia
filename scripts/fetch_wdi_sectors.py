# scripts/fetch_wdi_sectors.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, time, sys
from pathlib import Path
from typing import List, Dict
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUT_LONG      = DATA_DIR / "wdi_sectors_long.csv"
OUT_WIDE      = DATA_DIR / "wdi_sectors_wide.csv"
OUT_LATEST    = DATA_DIR / "wdi_sectors_latest.csv"
OUT_PIB_TABLE = DATA_DIR / "wdi_pib_sectors_table.csv"   # <-- para a tabela da app

# Indicadores WDI (códigos oficiais)
IND: Dict[str, str] = {
    # VAB (% do PIB)
    "NV.AGR.TOTL.ZS": "agr_vab",  # Primário (Agricultura, % PIB)
    "NV.IND.TOTL.ZS": "ind_vab",  # Secundário (Indústria, % PIB)
    "NV.SRV.TOTL.ZS": "srv_vab",  # Terciário (Serviços, % PIB)
    # Emprego (% do total)
    "SL.AGR.EMPL.ZS": "agr_emp",
    "SL.IND.EMPL.ZS": "ind_emp",
    "SL.SRV.EMPL.ZS": "srv_emp",
}

DEFAULT_DATE = "1990:2024"


# -------------------------------------------------------------------
# Utilitários de ISO3 / leitura de CSV de países
# -------------------------------------------------------------------

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
    # remover duplicados mantendo ordem
    return sorted(list(dict.fromkeys(parts)))


# -------------------------------------------------------------------
# Ligação ao WDI via Data360 (CSVs)
# -------------------------------------------------------------------

def _parse_date_range(date: str) -> tuple[int, int]:
    """Converte '1990:2024' em (1990, 2024)."""
    try:
        a, b = (date or "").split(":")
        return int(a), int(b)
    except Exception:
        return 1990, 2024


def fetch_indicator_via_csv(iso3_list: List[str], code: str, date: str) -> pd.DataFrame:
    """
    Vai buscar um indicador WDI usando o CSV da Data360 (WB_WDI),
    filtra pelos iso3 desejados e intervalo de anos, e devolve long:

        iso3 | year | code | value
    """
    year_min, year_max = _parse_date_range(date)

    file_id = f"WB_WDI_{code.replace('.', '_')}"
    url = f"https://data360files.worldbank.org/data360-data/data/WB_WDI/{file_id}.csv"

    df = None
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            df = pd.read_csv(url)
            break
        except Exception as e:
            last_exc = e
            print(
                f"[warn] tentativa {attempt+1}/3 a ler CSV {file_id} falhou: {e}",
                file=sys.stderr,
            )
            time.sleep(1.5 * (attempt + 1))

    if df is None:
        print(f"[warn] falhou CSV para {code}: {last_exc}", file=sys.stderr)
        return pd.DataFrame(columns=["iso3", "year", "code", "value"])

    needed = {"REF_AREA", "TIME_PERIOD", "OBS_VALUE"}
    if not needed.issubset(df.columns):
        print(f"[warn] CSV {file_id}.csv sem colunas {needed - set(df.columns)}", file=sys.stderr)
        return pd.DataFrame(columns=["iso3", "year", "code", "value"])

    df = df.copy()
    df["REF_AREA"] = df["REF_AREA"].astype(str).str.upper().str.strip()
    df = df[df["REF_AREA"].isin(iso3_list)]

    df["TIME_PERIOD_int"] = pd.to_numeric(df["TIME_PERIOD"], errors="coerce")
    df["OBS_VALUE_num"]   = pd.to_numeric(df["OBS_VALUE"],   errors="coerce")
    df = df.dropna(subset=["TIME_PERIOD_int", "OBS_VALUE_num"])

    df = df[
        (df["TIME_PERIOD_int"] >= year_min) &
        (df["TIME_PERIOD_int"] <= year_max)
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
    Vai buscar todos os indicadores definidos em IND para a lista de países,
    usando os CSVs Data360.
    """
    frames = []
    for code in IND.keys():
        print(f"[down] {code} (CSV Data360) …")
        df_b = fetch_indicator_via_csv(iso3_list, code, date)
        if not df_b.empty:
            frames.append(df_b)

    if not frames:
        return pd.DataFrame(columns=["iso3", "year", "code", "value"])

    df = pd.concat(frames, ignore_index=True)

    # normalizar tipos / iso3 válidos
    df["iso3"] = df["iso3"].astype(str).str.upper().str.strip()
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["code"] = df["code"].astype(str)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = df[df["iso3"].isin(iso3_list)]
    return df


# -------------------------------------------------------------------
# Transformações: long -> wide -> latest
# -------------------------------------------------------------------

def make_wide(df_long: pd.DataFrame) -> pd.DataFrame:
    if df_long.empty:
        return pd.DataFrame(columns=["iso3", "year"] + list(IND.values()))
    dfw = (
        df_long
        .assign(var=lambda d: d["code"].map(IND))
        .pivot_table(index=["iso3", "year"], columns="var", values="value", aggfunc="first")
        .reset_index()
    )
    cols = ["iso3", "year"] + list(IND.values())
    for c in cols:
        if c not in dfw.columns:
            dfw[c] = pd.NA
    return dfw[cols].sort_values(["iso3", "year"]).reset_index(drop=True)


def latest_complete_row(dfw: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Para cada iso3, procura o ano mais recente em que TODAS as colunas 'cols' não são NaN.
    (Isto é apenas para o resumo LATEST; os gráficos usam o WIDE completo.)
    """
    out_rows = []
    if dfw.empty:
        return pd.DataFrame()

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


# -------------------------------------------------------------------
# main()
# -------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Fetch WDI sectoral shares (value added % GDP and employment %) via Data360 CSV."
    )
    ap.add_argument(
        "--countries",
        type=str,
        help="CSV com lista de países (coluna iso3/ISO3/country_iso3)",
    )
    ap.add_argument(
        "--iso3",
        type=str,
        help="Lista de ISO3 separados por vírgula (ex.: PRT,DEU,ESP)",
    )
    ap.add_argument(
        "--date",
        type=str,
        default=DEFAULT_DATE,
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
        save_csv(pd.DataFrame(), OUT_PIB_TABLE)
        return

    # LONG
    save_csv(df_long, OUT_LONG)
    print(f"[ok] {OUT_LONG} | rows: {len(df_long)}")

    # WIDE
    df_wide = make_wide(df_long)
    save_csv(df_wide, OUT_WIDE)
    print(f"[ok] {OUT_WIDE} | rows: {len(df_wide)}")

    # LATEST (ano mais recente com 3 valores completos em cada grupo)
    latest_vab = latest_complete_row(df_wide, ["agr_vab", "ind_vab", "srv_vab"])
    latest_emp = latest_complete_row(df_wide, ["agr_emp", "ind_emp", "srv_emp"])

    if latest_vab.empty and latest_emp.empty:
        latest = pd.DataFrame()
    elif latest_emp.empty:
        latest = latest_vab.copy()
        latest = latest.rename(columns={"iso3": "iso3", "year": "year_vab"})
        for c in ["agr_emp", "ind_emp", "srv_emp"]:
            latest[c] = pd.NA
        latest = latest.rename(
            columns={
                "agr_vab": "vab_agr_vab",
                "ind_vab": "vab_ind_vab",
                "srv_vab": "vab_srv_vab",
            }
        )
        latest["year_emp"] = pd.NA
        latest = latest[
            ["iso3", "year_vab", "vab_agr_vab", "vab_ind_vab", "vab_srv_vab",
             "year_emp", "agr_emp", "ind_emp", "srv_emp"]
        ]
    elif latest_vab.empty:
        latest = latest_emp.copy()
        latest = latest.rename(columns={"iso3": "iso3", "year": "year_emp"})
        for c in ["agr_vab", "ind_vab", "srv_vab"]:
            latest[c] = pd.NA
        latest = latest.rename(
            columns={
                "agr_emp": "emp_agr_emp",
                "ind_emp": "emp_ind_emp",
                "srv_emp": "emp_srv_emp",
            }
        )
        latest["year_vab"] = pd.NA
        latest = latest[
            ["iso3", "year_vab", "agr_vab", "ind_vab", "srv_vab",
             "year_emp", "emp_agr_emp", "emp_ind_emp", "emp_srv_emp"]
        ]
    else:
        latest = pd.merge(
            latest_vab.add_prefix("vab_"),
            latest_emp.add_prefix("emp_"),
            left_on="vab_iso3",
            right_on="emp_iso3",
            how="outer",
        )
        if not latest.empty:
            latest["iso3"] = latest["vab_iso3"].fillna(latest["emp_iso3"])
            latest = latest.drop(columns=["vab_iso3", "emp_iso3"])
            latest = latest.rename(columns={"vab_year": "year_vab", "emp_year": "year_emp"})

    save_csv(latest, OUT_LATEST)
    print(f"[ok] {OUT_LATEST} | rows: {len(latest)}")

    # ------------------------------------------------------------------
    # CSV específico para a tabela “Primário / Secundário / Terciário, % PIB”
    # ------------------------------------------------------------------
    if not df_wide.empty:
        df_tbl = df_wide[["iso3", "year", "agr_vab", "ind_vab", "srv_vab"]].copy()
        df_tbl = df_tbl.rename(
            columns={
                "year": "Ano",
                "agr_vab": "Primario_Agricultura_pct_PIB",
                "ind_vab": "Secundario_Industria_pct_PIB",
                "srv_vab": "Terciario_Servicos_pct_PIB",
            }
        )
        df_tbl = df_tbl.sort_values(["iso3", "Ano"]).reset_index(drop=True)
        save_csv(df_tbl, OUT_PIB_TABLE)
        print(f"[ok] {OUT_PIB_TABLE} | rows: {len(df_tbl)}")
    else:
        save_csv(pd.DataFrame(), OUT_PIB_TABLE)
        print(f"[ok] {OUT_PIB_TABLE} | rows: 0")


if __name__ == "__main__":
    main()
