# -*- coding: utf-8 -*-
"""
Gera CSVs OFFLINE de projeções CMIP6 por local (ponto), agregados por cenário.

Saídas (por local):
  data/cmip6_offline/<loc_key>/
    - timeseries.csv            -> série anual por modelo+cenário (tas °C e ΔT °C)
    - stat_mean_band.csv        -> por cenário+ano: mean/min/max de ΔT (média dos modelos)
    - decades.csv               -> por cenário+Década: média de ΔT (pivotado)

Notas:
- Usa services.cmip6.fetch_series_cached() — primeira execução pode ir à rede para
  criar o cache Parquet; depois, tudo é lido do disco.
- <loc_key> é baseado em lat/lon arredondados para 2 casas (ex.: pt_38.72_-9.14).
- Podes fornecer um CSV de locais em data/cmip6_locations.csv (colunas: name,lat,lon),
  ou então passar um único local via argumentos.
"""
from __future__ import annotations
from pathlib import Path
import argparse
import sys
import io
import pandas as pd

# importa utilidades já existentes no teu projeto
sys.path.append(str(Path(__file__).resolve().parents[1]))
from services.cmip6 import (
    list_model_members, default_members_for_models,
    fetch_series_cached, anomalies,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUT_BASE     = PROJECT_ROOT / "data" / "cmip6_offline"
OUT_BASE.mkdir(parents=True, exist_ok=True)

SCENARIOS = ["historical", "ssp126", "ssp245", "ssp370", "ssp585"]
DEFAULT_MODELS = ["EC-Earth3", "MPI-ESM1-2-HR", "CMCC-ESM2", "UKESM1-0-LL"]
BASELINES = {
    "1991-2020": (1991, 2020),
    "1981-2010": (1981, 2010),
    "1961-1990": (1961, 1990),
}

def loc_key(lat: float, lon: float) -> str:
    lat2 = round(float(lat), 2)
    lon2 = round(float(lon), 2)
    return f"pt_{lat2:+.2f}_{lon2:+.2f}"

def _load_locations_list() -> list[tuple[str, float, float]]:
    """
    Tenta data/cmip6_locations.csv (name,lat,lon). Se não existir, devolve um default.
    """
    f = PROJECT_ROOT / "data" / "cmip6_locations.csv"
    if f.exists():
        df = pd.read_csv(f)
        rows = []
        for _, r in df.iterrows():
            try:
                rows.append((str(r["name"]), float(r["lat"]), float(r["lon"])))
            except Exception:
                pass
        if rows:
            return rows
    # fallback simples
    return [
        ("Lisboa", 38.72, -9.14),
        ("Porto", 41.15, -8.61),
        ("Madrid", 40.42, -3.70),
        ("Paris", 48.86, 2.35),
        ("New York", 40.71, -74.01),
    ]

def bake_one_location(name: str, lat: float, lon: float, baseline_key: str) -> None:
    print(f"[BAKE] {name} ({lat:.2f},{lon:.2f}) · baseline {baseline_key}")
    baseline = BASELINES[baseline_key]
    avail = list_model_members()

    # escolher modelos (usar os sugeridos se existirem)
    have = set(avail["source_id"])
    models = [m for m in DEFAULT_MODELS if m in have]
    if not models:
        # fallback: apanha os 3 primeiros modelos que tenham membros
        models = list(sorted(have))[:3]
    members = default_members_for_models(models)
    if members.empty:
        print("  … sem membros para os modelos escolhidos; a saltar.")
        return

    location = {"type": "point", "lat": float(lat), "lon": float(lon)}
    rows = []

    # recolha (cria cache parquet da 1ª vez)
    for _, mrow in members.iterrows():
        model = mrow["source_id"]; member = mrow["member_id"]; grid = mrow["grid_label"]
        for exp in SCENARIOS:
            s = fetch_series_cached(model, member, grid, exp, location=location, annual=True, refresh=False)
            if s.empty:
                continue
            sa = anomalies(s, baseline=baseline)
            df = pd.DataFrame({"time": s.index, "tas (°C)": s.values})
            df["ΔT (°C)"] = sa.reindex(s.index).values
            df["model"] = model
            df["scenario"] = exp
            rows.append(df)

    if not rows:
        print("  … sem séries devolvidas; nada a gravar.")
        return

    full = pd.concat(rows, ignore_index=True)
    full["year"] = full["time"].dt.year
    full = full[full["year"] >= 1950].copy()

    # suavização leve (5 anos) antes da estatística por cenário
    def _smooth_rolling(g: pd.DataFrame, y: str = "ΔT (°C)", win: int = 5) -> pd.DataFrame:
        g = g.sort_values("time").copy()
        g[y] = g[y].rolling(win, center=True, min_periods=1).mean()
        return g

    smooth = (
        full.groupby(["model", "scenario"], group_keys=False, observed=False)
            .apply(_smooth_rolling)
    )

    # estatística por cenário
    stat = (
        smooth.groupby(["scenario", "time"], observed=False)["ΔT (°C)"]
              .agg(["mean", "min", "max"])
              .reset_index()
    )

    # décadas (pivot)
    smooth["decada"] = (smooth["year"] // 10) * 10
    dec = (
        smooth[smooth["year"] >= 1950]
        .groupby(["scenario", "decada"], observed=False)["ΔT (°C)"]
        .mean()
        .reset_index()
        .pivot(index="decada", columns="scenario", values="ΔT (°C)")
        .sort_index()
        .reset_index()
    )

    # gravar
    base = OUT_BASE / loc_key(lat, lon)
    base.mkdir(parents=True, exist_ok=True)
    # incluir metadados mínimos na primeira linha dos ficheiros (comentário)
    meta = f"# location={name} lat={lat} lon={lon} baseline={baseline_key}\n"

    def _to_csv(path: Path, df: pd.DataFrame):
        txt = io.StringIO()
        txt.write(meta)
        df.to_csv(txt, index=False)
        path.write_text(txt.getvalue(), encoding="utf-8")

    _to_csv(base / "timeseries.csv", full)
    _to_csv(base / "stat_mean_band.csv", stat)
    _to_csv(base / "decades.csv", dec)

    print(f"  ✔ gravado em {base}/")

def main():
    ap = argparse.ArgumentParser(description="Bake CSVs offline de CMIP6 por local.")
    ap.add_argument("--lat", type=float, help="latitude do ponto")
    ap.add_argument("--lon", type=float, help="longitude do ponto")
    ap.add_argument("--name", type=str, help="nome opcional do ponto")
    ap.add_argument("--baseline", type=str, default="1991-2020",
                    choices=list(BASELINES.keys()))
    args = ap.parse_args()

    if args.lat is not None and args.lon is not None:
        bake_one_location(args.name or "Local", args.lat, args.lon, args.baseline)
    else:
        for (nm, la, lo) in _load_locations_list():
            bake_one_location(nm, la, lo, args.baseline)

if __name__ == "__main__":
    main()
