# scripts/meteo/fetch_cmip6_global.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import numpy as np
import pandas as pd
import xarray as xr
from intake_esm import esm_datastore
from pathlib import Path

CAT = "https://storage.googleapis.com/cmip6/pangeo-cmip6.json"
#OUT_DIR = Path(__file__).resolve().parents[2] / "data"   # <-- agora em data/
#OUT_DIR.mkdir(parents=True, exist_ok=True)

HERE = Path(__file__).resolve()
# o script está em …\Geografia\scripts\ → sobe 1 nível até à pasta Geografia
OUT_DIR = HERE.parents[1] / "data"
OUT_DIR.mkdir(parents=True, exist_ok=True)

print(f"[cmip6] OUT_DIR = {OUT_DIR}")  # opcional p/ confirmares no terminal
VAR = "tas"           # temperatura do ar a 2 m
TABLE = "Amon"        # mensal
EXPS = ["historical", "ssp126", "ssp245", "ssp370", "ssp585"]
MODELS_PREFERRED = [
    "EC-Earth3", "MPI-ESM1-2-HR", "CMCC-ESM2",
    "UKESM1-0-LL", "IPSL-CM6A-LR", "GFDL-ESM4", "NorESM2-LM"
]
BASELINE = (1991, 2020)  # para anomalias

def _open_first_zarr(q) -> xr.Dataset | None:
    if len(q.df) == 0:
        return None
    # 1) tentativa via intake_esm
    try:
        dct = q.to_dataset_dict(
            zarr_kwargs={"consolidated": True},
            storage_options={"token": "anon"},
        )
        ds = list(dct.values())[0][[VAR]]
        return xr.decode_cf(ds, use_cftime=True)
    except Exception:
        pass
    # 2) fallback: abrir o primeiro zstore
    df = q.df.copy()
    df = df[df["zstore"].notna()]
    if df.empty:
        return None
    z = df.iloc[0]["zstore"]
    try:
        ds = xr.open_zarr(z, consolidated=True, storage_options={"token": "anon"})
    except Exception:
        ds = xr.open_zarr(z, consolidated=False, storage_options={"token": "anon"})
    ds = ds[[VAR]]
    return xr.decode_cf(ds, use_cftime=True)

def _global_mean_tas(ds: xr.Dataset) -> xr.DataArray:
    """Média global área-ponderada (°C) a partir de 'tas' (K)."""
    da = ds[VAR] - 273.15  # K -> °C
    w = np.cos(np.deg2rad(da["lat"]))
    return da.weighted(w).mean(("lat", "lon"), skipna=True)  # mensal

def _annual_index_from_years(years: np.ndarray) -> pd.DatetimeIndex:
    # anos -> datas no meio do ano (evita calendários CF exóticos)
    return pd.to_datetime(pd.Series(years.astype(int)).astype(str)) + pd.offsets.MonthBegin(7)

def main() -> None:
    print("[CMIP6] a carregar catálogo…")
    cat = esm_datastore(CAT)
    df = cat.df
    avail = df[(df["variable_id"] == VAR) & (df["table_id"] == TABLE)]
    models = [m for m in MODELS_PREFERRED if m in set(avail["source_id"])]
    if not models:
        raise SystemExit("Sem modelos elegíveis no catálogo CMIP6.")

    rows = []
    for model in models:
        sub = avail[avail["source_id"] == model]
        if sub.empty:
            continue
        prefer = sub[sub["member_id"] == "r1i1p1f1"]
        if not prefer.empty:
            member = prefer.iloc[0]["member_id"]
            grid = prefer.iloc[0]["grid_label"]
        else:
            member = sub.iloc[0]["member_id"]
            grid = sub.iloc[0]["grid_label"]

        for exp in EXPS:
            print(f"[CMIP6] {model} / {exp} …")
            q = cat.search(
                source_id=model,
                variable_id=VAR,
                table_id=TABLE,
                experiment_id=exp,
                member_id=member,
                grid_label=grid,
            )
            ds = _open_first_zarr(q)
            if ds is None or "time" not in ds.dims:
                print(f"  › falhou: {model} {exp}")
                continue
            try:
                tas_gm = _global_mean_tas(ds)          # mensal °C
                # ⚠️ xarray.groupby NÃO aceita observed=…
                ann = tas_gm.groupby("time.year").mean("time", skipna=True)

                years = np.asarray(ann["year"].values).astype(int)
                idx = _annual_index_from_years(years)
                vals = np.asarray(ann.values).reshape(-1)

                s = pd.Series(vals, index=idx)
                out = pd.DataFrame({
                    "time": s.index,
                    "tasC": s.values,
                    "model": model,
                    "scenario": exp,
                })
                rows.append(out)
            except Exception as e:
                print(f"  › erro a processar {model}/{exp}: {e}")

    if not rows:
        raise SystemExit("Sem dados processados.")

    all_models = pd.concat(rows, ignore_index=True)
    all_models["year"] = all_models["time"].dt.year.astype(int)

    # anomalias por modelo vs baseline
    def _anom(g: pd.DataFrame) -> pd.DataFrame:
        base = g[(g["year"] >= BASELINE[0]) & (g["year"] <= BASELINE[1])]
        ref = base["tasC"].mean() if not base.empty else g["tasC"].mean()
        g["anom"] = g["tasC"] - ref
        return g

    all_models = (
        all_models
        .groupby(["model", "scenario"], group_keys=False)
        .apply(_anom)
        .sort_values(["scenario", "model", "time"])
    )

    # estatísticas do ensemble por cenário/ano (em anomalia)
    stats = (
        all_models.groupby(["scenario", "time"])["anom"]
        .agg(mean="mean", min="min", max="max")
        .reset_index()
        .sort_values(["scenario", "time"])
    )

    # gravação (em data/)
    f_all  = OUT_DIR / "cmip6_global_all_models.csv"
    f_stat = OUT_DIR / "cmip6_global_ensemble_stats.csv"
    all_models.to_csv(f_all, index=False, encoding="utf-8")
    stats.to_csv(f_stat, index=False, encoding="utf-8")
    print(f"✔️ Escrevi {f_all} ({len(all_models):,} linhas)")
    print(f"✔️ Escrevi {f_stat} ({len(stats):,} linhas)")

if __name__ == "__main__":
    main()
