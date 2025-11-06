#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extrai lagos por país a partir do Natural Earth.

Preferência:
  1) Polígonos (ne_10m_lakes.shp) → com área_km2
  2) Fallback centerlines (ne_10m_rivers_lake_centerlines.shp) → sem área

Saída: data/lakes_ne.csv
Colunas:
  iso3;lake_name;area_km2;lat;lon;scalerank;featurecla;source
"""

from __future__ import annotations
import argparse
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd


DATA_DIR = Path("data")
DEFAULT_OUT = DATA_DIR / "lakes_ne.csv"

# Caminhos default (ajusta se usares outra estrutura)
DEFAULT_LAKES_SHP       = DATA_DIR / "ne_lakes"  / "ne_10m_lakes.shp"
DEFAULT_CENTERLINES_SHP = DATA_DIR / "ne_rivers" / "ne_10m_rivers_lake_centerlines.shp"
DEFAULT_ADMIN0_SHP      = DATA_DIR / "ne_admin0" / "ne_10m_admin_0_countries.shp"


def _read_admin0(path: Path) -> gpd.GeoDataFrame:
    if not path.exists():
        raise FileNotFoundError(f"[admin0] não encontrado: {path}")

    gdf = gpd.read_file(path)
    # Deteção robusta da coluna ISO3
    iso_col = None
    for c in ["ADM0_A3", "adm0_a3", "ISO_A3", "iso_a3", "ISO3", "iso3"]:
        if c in gdf.columns:
            iso_col = c
            break
    if iso_col is None:
        raise RuntimeError("[admin0] não encontrei coluna ISO3 (ADM0_A3/ISO_A3/iso3 …).")

    gdf = gdf[[iso_col, "geometry"]].rename(columns={iso_col: "iso3"})
    gdf["iso3"] = gdf["iso3"].astype(str).str.upper().str.strip()
    gdf = gdf[~gdf["iso3"].isin(["-99", ""])].copy()

    # Garantir CRS
    if gdf.crs is None:
        gdf.set_crs(4326, inplace=True)
    else:
        gdf = gdf.to_crs(4326)

    return gdf


def _best_name(gdf: gpd.GeoDataFrame) -> pd.Series:
    """Escolhe melhor coluna de nome disponível."""
    for c in ["name_pt", "name", "name_en"]:
        if c in gdf.columns:
            return gdf[c].astype(str)
    # última tentativa: "namepar" / "name_alt" (algumas versões antigas)
    for c in ["namepar", "name_alt"]:
        if c in gdf.columns:
            return gdf[c].astype(str)
    return pd.Series([""] * len(gdf))


def _load_lakes_polys(path: Path) -> gpd.GeoDataFrame | None:
    if not path.exists():
        return None
    gdf = gpd.read_file(path)
    if gdf.empty:
        return None

    # Normalizar campos
    gdf["lake_name"] = _best_name(gdf)
    if "featurecla" not in gdf.columns:
        gdf["featurecla"] = "Lake"
    if "scalerank" not in gdf.columns:
        gdf["scalerank"] = None

    # CRS a WGS84 para centroides
    if gdf.crs is None:
        gdf.set_crs(4326, inplace=True)
    else:
        gdf = gdf.to_crs(4326)

    # Centróide em WGS84
    cent = gdf.geometry.centroid
    gdf["lat"] = cent.y
    gdf["lon"] = cent.x

    # Área em km² com CRS de área igual
    try:
        gdf_area = gdf.to_crs(6933)  # NSIDC EASE-Grid 2.0 (Equal-Area mundial)
        gdf["area_km2"] = (gdf_area.geometry.area / 1_000_000).round(3)
    except Exception:
        gdf["area_km2"] = None

    gdf["source"] = "NaturalEarth_10m_lakes"
    return gdf[["lake_name", "area_km2", "lat", "lon", "scalerank", "featurecla", "geometry", "source"]]


def _load_lake_centerlines(path: Path) -> gpd.GeoDataFrame | None:
    if not path.exists():
        return None
    gdf = gpd.read_file(path)
    if gdf.empty:
        return None

    gdf["lake_name"] = _best_name(gdf)
    if "featurecla" not in gdf.columns:
        gdf["featurecla"] = "Lake_centerline"
    if "scalerank" not in gdf.columns:
        gdf["scalerank"] = None

    # CRS a WGS84
    if gdf.crs is None:
        gdf.set_crs(4326, inplace=True)
    else:
        gdf = gdf.to_crs(4326)

    cent = gdf.geometry.centroid
    gdf["lat"] = cent.y
    gdf["lon"] = cent.x

    gdf["area_km2"] = None
    gdf["source"] = "NaturalEarth_10m_rivers_lake_centerlines"
    return gdf[["lake_name", "area_km2", "lat", "lon", "scalerank", "featurecla", "geometry", "source"]]


def _assign_to_countries(
    lakes: gpd.GeoDataFrame,
    admin0: gpd.GeoDataFrame,
    mode: str = "intersect",
) -> pd.DataFrame:
    """
    mode = 'intersect'  (default): lagos que INTERSECTAM o país (pode dar múltiplas linhas por lago)
         = 'centroid'            : usa o país do centróide do lago (apenas 1 país)
    """
    if mode not in {"intersect", "centroid"}:
        mode = "intersect"

    if mode == "centroid":
        pts = lakes.copy()
        pts = pts.set_geometry(pts.geometry.centroid, crs=lakes.crs)
        sj = gpd.sjoin(pts, admin0[["iso3", "geometry"]], how="inner", predicate="within")
    else:
        sj = gpd.sjoin(lakes, admin0[["iso3", "geometry"]], how="inner", predicate="intersects")

    # Remover cols do spatial join
    cols = ["iso3", "lake_name", "area_km2", "lat", "lon", "scalerank", "featurecla", "source"]
    out = sj[cols].copy()

    # Normalizações finais
    out["iso3"] = out["iso3"].astype(str).str.upper().str.strip()
    out["lake_name"] = out["lake_name"].astype(str).str.strip()
    out = out.drop_duplicates(subset=["iso3", "lake_name"]).reset_index(drop=True)

    return out


def main():
    ap = argparse.ArgumentParser(description="Extrai lagos por país a partir de Natural Earth.")
    ap.add_argument("--admin0", default=str(DEFAULT_ADMIN0_SHP), help="Shapefile admin0 (países) do Natural Earth.")
    ap.add_argument("--lakes",   default=str(DEFAULT_LAKES_SHP), help="Shapefile ne_10m_lakes.shp (polígonos).")
    ap.add_argument("--centerlines", default=str(DEFAULT_CENTERLINES_SHP),
                    help="Fallback: ne_10m_rivers_lake_centerlines.shp (linhas).")
    ap.add_argument("--assign", choices=["intersect", "centroid"], default="intersect",
                    help="Atribuição do lago ao país: interseção (default) ou país do centróide.")
    ap.add_argument("--min-area-km2", type=float, default=0.0,
                    help="Filtro de área mínima (apenas para polígonos; default 0).")
    ap.add_argument("--out", default=str(DEFAULT_OUT), help="CSV de saída.")
    ap.add_argument("--overwrite", action="store_true", help="Recria o CSV do zero.")
    args = ap.parse_args()

    admin0_path      = Path(args.admin0)
    lakes_polys_path = Path(args.lakes)
    centerlines_path = Path(args.centerlines)
    out_csv          = Path(args.out)

    try:
        admin0 = _read_admin0(admin0_path)
    except Exception as e:
        print(f"[ERRO] admin0: {e}")
        sys.exit(1)

    # 1) Polígonos
    lakes = _load_lakes_polys(lakes_polys_path)

    # 2) Fallback centerlines
    if lakes is None:
        print(f"[aviso] {lakes_polys_path.name} não encontrado/sem dados — a usar centerlines.")
        lakes = _load_lake_centerlines(centerlines_path)
        if lakes is None:
            print("[ERRO] Não encontrei lagos nem centerlines do Natural Earth.")
            sys.exit(1)

    # Filtro de área (se existir)
    if args.min_area_km2 > 0 and "area_km2" in lakes.columns:
        before = len(lakes)
        lakes = lakes[(lakes["area_km2"].fillna(0) >= float(args.min_area_km2)) | (lakes["area_km2"].isna())]
        print(f"[info] filtro área ≥ {args.min_area_km2} km² → {before} → {len(lakes)}")

    # Atribuição por país
    out = _assign_to_countries(lakes, admin0, mode=args.assign)

    # Ordenar/colunas finais
    out = out[["iso3", "lake_name", "area_km2", "lat", "lon", "scalerank", "featurecla", "source"]]
    out = out.sort_values(["iso3", "lake_name"]).reset_index(drop=True)

    # Gravação
    if out_csv.exists() and not args.overwrite:
        # Append sem duplicar
        prev = pd.read_csv(out_csv, sep=";", dtype=str, encoding="utf-8", keep_default_na=False)
        # normalizar tipos
        for c in out.columns:
            if c not in prev.columns:
                prev[c] = ""
        prev = prev[out.columns]

        merged = pd.concat([prev, out], ignore_index=True)
        merged = merged.drop_duplicates(subset=["iso3", "lake_name"]).reset_index(drop=True)
        merged.to_csv(out_csv, sep=";", index=False, encoding="utf-8")
        print(f"[ok] appended/merged → {out_csv} | linhas: {len(merged)}")
    else:
        out.to_csv(out_csv, sep=";", index=False, encoding="utf-8")
        print(f"[ok] escrito → {out_csv} | linhas: {len(out)}")


if __name__ == "__main__":
    main()
