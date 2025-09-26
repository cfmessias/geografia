# services/cmip6.py
from __future__ import annotations
from typing import Dict, Tuple, List, Optional

import numpy as np
import pandas as pd
import xarray as xr
from intake_esm import esm_datastore
import streamlit as st

# Catálogo público Pangeo/CMIP6 (Google Cloud)
_PANGEO_CMIP6_CATALOG = "https://storage.googleapis.com/cmip6/pangeo-cmip6.json"

# Variável alvo: temperatura do ar a 2 m, mensal (tabela Amon) em Kelvin
_VAR = "tas"


@st.cache_data(ttl=24 * 3600, show_spinner=False)
def list_model_members() -> pd.DataFrame:
    """
    Modelos/membros/grelha com 'tas' mensal disponível (historical + SSPs).
    Devolve colunas: source_id, experiment_id, member_id, grid_label
    """
    cat = esm_datastore(_PANGEO_CMIP6_CATALOG)
    df = cat.df
    keep_exps = ["historical", "ssp126", "ssp245", "ssp370", "ssp585"]
    sel = df[
        (df["variable_id"] == _VAR)
        & (df["table_id"] == "Amon")
        & (df["experiment_id"].isin(keep_exps))
    ][["source_id", "experiment_id", "member_id", "grid_label"]].drop_duplicates()
    return sel.sort_values(["source_id", "experiment_id", "member_id"])


def _coord_name(ds: xr.Dataset, names: List[str]) -> Optional[str]:
    """Primeiro nome existente em `names` que exista no dataset."""
    for n in names:
        if n in ds.coords:
            return n
    return None


def _open_dataset(model: str, member: str, grid: str, experiment: str) -> xr.Dataset | None:
    """
    Abre via intake_esm; se falhar, tenta abrir diretamente o primeiro zstore com xarray.open_zarr.
    Mantém apenas a variável _VAR e normaliza o tempo (cftime ok).
    """
    try:
        cat = esm_datastore(_PANGEO_CMIP6_CATALOG)
        q = cat.search(
            source_id=model,
            variable_id=_VAR,     # 'tas'
            table_id="Amon",
            experiment_id=experiment,
            member_id=member,
            grid_label=grid,
        )
        if len(q.df) == 0:
            return None

        # 1) Tentativa padrão (to_dataset_dict)
        try:
            dset_dict = q.to_dataset_dict(
                zarr_kwargs={"consolidated": True},
                storage_options={"token": "anon"},
            )
            ds = list(dset_dict.values())[0][[_VAR]]
            ds = xr.decode_cf(ds, use_cftime=True)
            return ds
        except Exception:
            # 2) Fallback: abre o primeiro zstore manualmente
            df = q.df.copy()
            df = df[df["zstore"].notna()]
            if df.empty:
                return None
            z = df.iloc[0]["zstore"]
            try:
                ds = xr.open_zarr(z, consolidated=True, storage_options={"token": "anon"})
            except Exception:
                ds = xr.open_zarr(z, consolidated=False, storage_options={"token": "anon"})
            ds = ds[[_VAR]]
            ds = xr.decode_cf(ds, use_cftime=True)
            return ds

    except Exception as e:
        st.caption(f"⚠️ CMIP6: falha a abrir {model}/{experiment} ({member},{grid}): {e}")
        return None


def _subset_point(ds: xr.Dataset, lat: float, lon: float) -> xr.DataArray:
    """
    Série no ponto mais próximo; trata longitudes 0..360 vs -180..180 e nomes alternativos.
    """
    lat_name = _coord_name(ds, ["lat", "latitude"])
    lon_name = _coord_name(ds, ["lon", "longitude"])
    if lat_name is None or lon_name is None:
        # dataset inesperado
        return ds[_VAR]

    lon_val = float(lon)
    # datasets com 0..360
    if float(ds[lon_name].max()) > 180:
        lon_val = lon_val if lon_val >= 0 else lon_val + 360

    return ds[_VAR].sel({lat_name: float(lat), lon_name: lon_val}, method="nearest")


def _subset_box(ds: xr.Dataset, lat_min: float, lat_max: float, lon_min: float, lon_max: float) -> xr.DataArray:
    """
    Média espacial ponderada por cos(lat) numa caixa lat/lon. Lida com 0..360 e nomes alternativos.
    """
    lat_name = _coord_name(ds, ["lat", "latitude"])
    lon_name = _coord_name(ds, ["lon", "longitude"])
    if lat_name is None or lon_name is None:
        return ds[_VAR]

    def to360(x: float) -> float:
        return x if x >= 0 else x + 360

    if float(ds[lon_name].max()) > 180:
        lon_min2, lon_max2 = to360(lon_min), to360(lon_max)
        if lon_min2 <= lon_max2:
            sub = ds[_VAR].sel({lat_name: slice(lat_min, lat_max), lon_name: slice(lon_min2, lon_max2)})
        else:  # atravessa 0/360
            s1 = ds[_VAR].sel({lat_name: slice(lat_min, lat_max), lon_name: slice(lon_min2, 360)})
            s2 = ds[_VAR].sel({lat_name: slice(lat_min, lat_max), lon_name: slice(0, lon_max2)})
            sub = xr.concat([s1, s2], dim=lon_name)
    else:
        sub = ds[_VAR].sel({lat_name: slice(lat_min, lat_max), lon_name: slice(lon_min, lon_max)})

    # média ponderada por latitude
    w = np.cos(np.deg2rad(sub[lat_name]))
    return sub.weighted(w).mean(dim=(lat_name, lon_name))


@st.cache_data(ttl=24 * 3600, show_spinner=True)
def fetch_series(
    model: str,
    member: str,
    grid: str,
    experiment: str,   # "historical" | "ssp126" | "ssp245" | "ssp370" | "ssp585"
    location: Dict,    # {"type":"point","lat","lon"} ou {"type":"box","lat_min","lat_max","lon_min","lon_max"}
    annual: bool = True,
) -> pd.Series:
    """
    Extrai uma série temporal (°C) para um ponto/caixa. Se annual=True devolve série anual (média),
    caso contrário devolve mensal.
    """
    ds = _open_dataset(model, member, grid, experiment)
    if ds is None:
        return pd.Series(dtype=float)

    try:
        # 1) subset espacial -> 1D no tempo
        if location.get("type") == "point":
            da = _subset_point(ds, float(location["lat"]), float(location["lon"]))
        else:
            da = _subset_box(
                ds,
                float(location["lat_min"]), float(location["lat_max"]),
                float(location["lon_min"]), float(location["lon_max"]),
            )

        # garantir ordem temporal
        if "time" in da.dims:
            da = da.sortby("time")

        # 2) Kelvin -> °C
        da = da - 273.15

        # 3) agregação temporal
        if annual:
            # via xarray, sem pandas.Grouper (evita conflitos de “by+groupers”)
            # groupby por ano do eixo temporal (funciona com cftime)
            da_ann = da.groupby("time.year").mean(dim="time", skipna=True)

            # remover dimensões residuais
            extra_dims = [d for d in da_ann.dims if d != "year"]
            if extra_dims:
                da_ann = da_ann.squeeze(drop=True)
                extra_dims = [d for d in da_ann.dims if d != "year"]
                if extra_dims:
                    da_ann = da_ann.mean(dim=extra_dims, skipna=True)

            years = pd.Index(np.asarray(da_ann["year"].values).reshape(-1), name="year")
            vals = np.asarray(da_ann.values).reshape(-1)

            # índice gregoriano aproximado (meados de ano)
            dt_index = pd.to_datetime(years.astype(str)) + pd.offsets.MonthBegin(6)
            n = min(len(vals), len(dt_index))
            s = pd.Series(vals[:n], index=dt_index[:n])
        else:
            # mensal (meio do mês)
            yy = np.asarray(da["time.year"].values).reshape(-1)
            mm = np.asarray(da["time.month"].values).reshape(-1)
            vals = np.asarray(da.values).reshape(-1)
            dt_index = pd.to_datetime(pd.DataFrame({"y": yy, "m": mm, "d": 15}))
            n = min(len(vals), len(dt_index))
            s = pd.Series(vals[:n], index=dt_index[:n])

        s.name = f"{model}|{experiment}"
        return s

    except Exception as e:
        st.caption(f"⚠️ CMIP6: erro ao sub-definir/agregar {model}/{experiment}: {e}")
        return pd.Series(dtype=float)


def anomalies(s: pd.Series, baseline: Tuple[int, int] = (1991, 2020)) -> pd.Series:
    """Anomalias vs média no período baseline (inclusivo)."""
    if s.empty:
        return s
    base = s[(s.index.year >= baseline[0]) & (s.index.year <= baseline[1])]
    ref = base.mean() if not base.empty else s.mean()
    out = s - ref
    out.name = f"{s.name} Δ({baseline[0]}–{baseline[1]})"
    return out


@st.cache_data(ttl=24 * 3600, show_spinner=False)
def default_members_for_models(models: List[str]) -> pd.DataFrame:
    """Escolhe um membro/grelha por modelo (idealmente r1i1p1f1)."""
    df = list_model_members()
    rows = []
    for m in models:
        d = df[df["source_id"] == m]
        if d.empty:
            continue
        if "r1i1p1f1" in set(d["member_id"]):
            row = d[d["member_id"] == "r1i1p1f1"].iloc[0]
        else:
            row = d.iloc[0]
        rows.append(row)
    return pd.DataFrame(rows).reset_index(drop=True)

# --- CACHE DISCO PARA SÉRIES CMIP6 ---
from pathlib import Path

_CACHE_DIR = Path(__file__).resolve().parents[1] / "data" / "cmip6_cache"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _norm_loc_key(location: dict) -> str:
    """Chave compacta e estável para a localização (arredonda p/ aumentar reutilização)."""
    t = (location or {}).get("type", "point")
    if t == "point":
        lat = round(float(location["lat"]), 2)
        lon = round(float(location["lon"]), 2)
        return f"pt_{lat:+.2f}_{lon:+.2f}"
    else:
        lat_min = round(float(location["lat_min"]), 2)
        lat_max = round(float(location["lat_max"]), 2)
        lon_min = round(float(location["lon_min"]), 2)
        lon_max = round(float(location["lon_max"]), 2)
        return f"bx_{lat_min:+.2f}_{lat_max:+.2f}_{lon_min:+.2f}_{lon_max:+.2f}"

def _series_to_parquet(path: Path, s: pd.Series) -> None:
    df = pd.DataFrame({"time": s.index, "value": s.values})
    # guardamos também o nome para restaurar
    if s.name:
        df.attrs = {"name": s.name}
    df.to_parquet(path, index=False)

def _parquet_to_series(path: Path) -> pd.Series:
    df = pd.read_parquet(path)
    s = pd.Series(df["value"].to_numpy(), index=pd.to_datetime(df["time"]))
    nm = getattr(df, "attrs", {}).get("name")
    if nm:
        s.name = nm
    return s

def fetch_series_cached(
    model: str,
    member: str,
    grid: str,
    experiment: str,
    location: dict,
    annual: bool = True,
    refresh: bool = False,     # força refazer e regravar
) -> pd.Series:
    """
    Igual a fetch_series(), mas usa cache persistente em data/cmip6_cache/.
    """
    key = f"{model}__{member}__{grid}__{experiment}__{_norm_loc_key(location)}__{'ann' if annual else 'mon'}.parquet"
    path = _CACHE_DIR / key
    if path.exists() and not refresh:
        try:
            return _parquet_to_series(path)
        except Exception:
            pass  # se der erro, refaz

    s = fetch_series(model, member, grid, experiment, location=location, annual=annual)
    if not s.empty:
        try:
            _series_to_parquet(path, s)
        except Exception:
            pass
    return s
