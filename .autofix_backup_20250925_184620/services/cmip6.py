# services/cmip6.py
from __future__ import annotations
import numpy as np
import pandas as pd
import xarray as xr
import intake_esm
from intake_esm import esm_datastore
import streamlit as st

_PANGEO_CMIP6_CATALOG = "https://storage.googleapis.com/cmip6/pangeo-cmip6.json"
_VAR = "tas"  # temperatura 2 m (Kelvin), tabela mensal Amon

@st.cache_data(ttl=24*3600, show_spinner=False)
def list_model_members() -> pd.DataFrame:
    """Modelos/membros/grelha com 'tas' mensal disponível (historical + SSPs)."""
    cat = cat = esm_datastore(_PANGEO_CMIP6_CATALOG)
    df = cat.df
    keep_exps = ["historical", "ssp126", "ssp245", "ssp370", "ssp585"]
    sel = df[
        (df["variable_id"] == _VAR)
        & (df["table_id"] == "Amon")
        & (df["experiment_id"].isin(keep_exps))
    ][["source_id", "experiment_id", "member_id", "grid_label"]].drop_duplicates()
    return sel.sort_values(["source_id", "experiment_id", "member_id"])

def _open_dataset(model: str, member: str, grid: str, experiment: str) -> xr.Dataset | None:
    """
    Tenta abrir via intake_esm.to_dataset_dict(). Se falhar (ESMDataSourceError),
    faz fallback para abrir diretamente o primeiro zstore com xarray.open_zarr.
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

        # 1) tentativa padrão via intake_esm
        try:
            dset_dict = q.to_dataset_dict(
                zarr_kwargs={"consolidated": True},
                storage_options={"token": "anon"},
            )
            ds = list(dset_dict.values())[0][[_VAR]]
            ds = xr.decode_cf(ds, use_cftime=True)
            return ds
        except Exception:
            # 2) fallback: abrir manualmente o primeiro zstore
            df = q.df.copy()
            # preferir entradas com zstore definido
            df = df[df["zstore"].notna()]
            if df.empty:
                return None
            z = df.iloc[0]["zstore"]

            # tentar consolidated=True, depois False
            try:
                ds = xr.open_zarr(z, consolidated=True, storage_options={"token": "anon"})
            except Exception:
                ds = xr.open_zarr(z, consolidated=False, storage_options={"token": "anon"})

            # manter só a variável de interesse e normalizar tempo
            ds = ds[[ _VAR ]]
            ds = xr.decode_cf(ds, use_cftime=True)
            return ds
    except Exception as e:
        # opcional: comentário discreto p/ diagnosticar
        st.caption(f"⚠️ CMIP6: falha a abrir {model}/{experiment} ({member},{grid}): {e}")
        return None

@st.cache_data(ttl=24*3600, show_spinner=True)
@st.cache_data(ttl=24*3600, show_spinner=True)
def fetch_series(
    model: str,
    member: str,
    grid: str,
    experiment: str,   # "historical" | "ssp126" | "ssp245" | "ssp370" | "ssp585"
    location: dict,    # {"type":"point","lat","lon"} ou {"type":"box","lat_min","lat_max","lon_min","lon_max"}
    annual: bool = True,
) -> pd.Series:
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

        if annual:
            # 3) média anual (compatível com calendários não gregorianos)
            da_ann = da.groupby("time.year").mean("time", skipna=True)

            # 4) remover quaisquer dimensões residuais (ex.: comprimento 1)
            #    - se ainda houver dims além de 'year', achatamos
            extra_dims = [d for d in da_ann.dims if d != "year"]
            if extra_dims:
                da_ann = da_ann.squeeze(drop=True)
                # se mesmo assim restar mais de 1 dim, reduzimos por média
                extra_dims = [d for d in da_ann.dims if d != "year"]
                if extra_dims:
                    da_ann = da_ann.mean(dim=extra_dims, skipna=True)

            years = pd.Index(da_ann["year"].values, name="year")
            vals = np.asarray(da_ann.values).reshape(-1)  # <- ACHATAR para 1D

            # criar um índice de datas “gregoriano” (meados do ano)
            dt_index = pd.to_datetime(years.astype(str)) + pd.offsets.MonthBegin(6)
            if len(vals) != len(dt_index):
                # segurança extra se o motor CF devolveu algo estranho
                vals = np.resize(vals, len(dt_index))
            s = pd.Series(vals, index=dt_index)
        else:
            # mensal (meio do mês) – também com achatamento seguro
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

    
def _subset_point(ds: xr.Dataset, lat: float, lon: float) -> xr.DataArray:
    """Série no ponto mais próximo; trata longitudes 0..360 vs -180..180."""
    lon = float(lon)
    if "lon" in ds.coords and float(ds.lon.max()) > 180:  # dataset em 0..360
        lon = lon if lon >= 0 else lon + 360
    return ds[_VAR].sel(lat=float(lat), lon=lon, method="nearest")

def _subset_box(ds: xr.Dataset, lat_min: float, lat_max: float, lon_min: float, lon_max: float) -> xr.DataArray:
    """Média espacial ponderada por cos(lat) numa caixa lat/lon. Lida com 0..360."""
    if "lon" in ds.coords and float(ds.lon.max()) > 180:  # dataset em 0..360
        to360 = lambda x: x if x >= 0 else x + 360
        lon_min2, lon_max2 = to360(lon_min), to360(lon_max)
        if lon_min2 <= lon_max2:
            sub = ds[_VAR].sel(lat=slice(lat_min, lat_max), lon=slice(lon_min2, lon_max2))
        else:  # atravessa 0/360
            s1 = ds[_VAR].sel(lat=slice(lat_min, lat_max), lon=slice(lon_min2, 360))
            s2 = ds[_VAR].sel(lat=slice(lat_min, lat_max), lon=slice(0, lon_max2))
            sub = xr.concat([s1, s2], dim="lon")
    else:
        sub = ds[_VAR].sel(lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max))
    # média ponderada por latitude
    w = np.cos(np.deg2rad(sub["lat"]))
    return sub.weighted(w).mean(dim=("lat", "lon"))


def anomalies(s: pd.Series, baseline: tuple[int, int] = (1991, 2020)) -> pd.Series:
    """Anomalias vs média no período baseline (inclusivo)."""
    if s.empty:
        return s
    base = s[(s.index.year >= baseline[0]) & (s.index.year <= baseline[1])]
    ref = base.mean() if not base.empty else s.mean()
    out = s - ref
    out.name = f"{s.name} Δ({baseline[0]}–{baseline[1]})"
    return out

@st.cache_data(ttl=24*3600, show_spinner=False)
def default_members_for_models(models: list[str]) -> pd.DataFrame:
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
