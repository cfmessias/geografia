# scripts/build_rivers_from_natural_earth.py
# -*- coding: utf-8 -*-
"""
Gera data/rivers.csv a partir do Natural Earth (10m Rivers + Lake Centerlines).
- Lê países de data/countries_profiles.csv (usa iso3)
- Intersecta com rios (linhas) e calcula comprimento em km (CRS métrico)
- Filtra por comprimento mínimo (--min_km) e por 'scalerank' (--max_scalerank)
- Incremental (evita duplicados iso3+river_name) e suporta --overwrite

Saída (sep=";"):
iso3;river_name;length_km;scalerank;featurecla;source

Sugestão de execução:
    python -u scripts/build_rivers_from_natural_earth.py
ou com overrides:
    python -u scripts/build_rivers_from_natural_earth.py --only PRT,ESP --min_km 40 --overwrite
"""

from __future__ import annotations
from pathlib import Path
import argparse, csv, sys
import pandas as pd
import geopandas as gpd

# ──────────────────────────────────────────────────────────────────────────────
# Caminhos base
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR     = PROJECT_ROOT / "data"
PROFILES     = DATA_DIR / "countries_profiles.csv"
OUT_CSV      = DATA_DIR / "rivers.csv"

# Pastas esperadas com Natural Earth
NE_RIVERS_DIR = DATA_DIR / "ne_rivers"
NE_ADMIN_DIR  = DATA_DIR / "ne_admin0"

# Candidatos a ficheiros (shp/gpkg)
RIVER_FILES = [
    NE_RIVERS_DIR / "ne_10m_rivers_lake_centerlines.shp",
    NE_RIVERS_DIR / "ne_10m_rivers_lake_centerlines.gpkg",
]
ADMIN_FILES = [
    NE_ADMIN_DIR / "ne_10m_admin_0_countries.shp",
    NE_ADMIN_DIR / "ne_10m_admin_0_countries.gpkg",
]

# CRS métrico (Equal Earth) — adequado para comprimentos globais
METRIC_CRS = "EPSG:8857"

# Colunas de saída
OUT_COLS = ["iso3", "river_name", "length_km", "scalerank", "featurecla", "source"]

# ──────────────────────────────────────────────────────────────────────────────
def _pick_file(candidates) -> Path | None:
    for p in candidates:
        if p.exists():
            return p
    return None

def _read_profiles(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";", dtype=str, keep_default_na=False, encoding="utf-8")
    cols = {c.lower(): c for c in df.columns}
    iso3_col = cols.get("iso3") or cols.get("country_iso3") or "iso3"
    out = pd.DataFrame({"iso3": df[iso3_col].astype(str).str.upper().str.strip()})
    out = out.drop_duplicates(subset=["iso3"])
    out = out[out["iso3"] != ""]
    if out.empty:
        raise RuntimeError("countries_profiles.csv não tem ISO3 válidos.")
    return out

def _load_admin_polygons(admin_path: Path | None = None) -> gpd.GeoDataFrame:
    admin_path = admin_path or _pick_file(ADMIN_FILES)
    if not admin_path:
        print("[rivers] ERRO: não encontrei Natural Earth admin0. Coloca os ficheiros em data/ne_admin0/", file=sys.stderr)
        sys.exit(2)
    g = gpd.read_file(admin_path)
    # detetar coluna ISO3
    candidates = [c for c in ["ADM0_A3", "ADM0_A3_US", "ADM0_A3_IS", "ISO_A3", "WB_A3"] if c in g.columns]
    if not candidates:
        raise RuntimeError("Admin0: não encontrei coluna ISO3 (tenta ISO_A3/ADM0_A3).")
    iso_col = candidates[0]
    g["iso3"] = g[iso_col].astype(str).str.upper().str.strip()
    g = g[~g.geometry.is_empty & g.geometry.notnull()].copy()
    g = g.set_crs("EPSG:4326", allow_override=True)
    return g[["iso3", "geometry"]].drop_duplicates()

def _load_rivers_geoms(river_path: Path | None = None) -> gpd.GeoDataFrame:
    river_path = river_path or _pick_file(RIVER_FILES)
    if not river_path:
        print("[rivers] ERRO: não encontrei Natural Earth rivers. Coloca os ficheiros em data/ne_rivers/", file=sys.stderr)
        sys.exit(2)
    r = gpd.read_file(river_path)

    # Nome do rio (tenta name_en → name)
    if "name_en" in r.columns and r["name_en"].notna().any():
        r["river_name"] = r["name_en"].fillna(r.get("name", ""))
    else:
        r["river_name"] = r.get("name", "").fillna("")

    # scalerank e featurecla (se existirem)
    r["scalerank"]  = pd.to_numeric(r.get("scalerank"), errors="coerce")
    r["featurecla"] = r.get("featurecla", "").astype(str)

    r = r[~r.geometry.is_empty & r.geometry.notnull()].copy()
    r = r.set_crs("EPSG:4326", allow_override=True)
    return r[["river_name", "scalerank", "featurecla", "geometry"]]

def _ensure_header(out_csv: Path, overwrite: bool = False):
    if overwrite and out_csv.exists():
        out_csv.unlink(missing_ok=True)
    if out_csv.exists() and out_csv.stat().st_size > 0:
        return
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        csv.writer(f, delimiter=";").writerow(OUT_COLS)

def _load_existing_keys(out_csv: Path) -> set[tuple[str, str]]:
    if not out_csv.exists():
        return set()
    try:
        df = pd.read_csv(out_csv, sep=";", dtype=str, keep_default_na=False, encoding="utf-8")
        if df.empty: 
            return set()
        return set((row["iso3"].upper(), row["river_name"]) for _, row in df.iterrows())
    except Exception:
        return set()

# ──────────────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Build data/rivers.csv from Natural Earth")
    ap.add_argument("--profiles", default=str(PROFILES), help="CSV com países (iso3)")
    ap.add_argument("--out",      default=str(OUT_CSV),   help="Ficheiro de saída CSV")
    ap.add_argument("--admin",    default="",             help="Pasta com admin0 (shp/gpkg) opcional")
    ap.add_argument("--rivers",   default="",             help="Pasta com rivers (shp/gpkg) opcional")

    # Defaults conforme pedido:
    ap.add_argument("--min_km", type=float, default=50.0, help="Comprimento mínimo em km (default: 50)")
    ap.add_argument("--max_scalerank", type=int, default=8, help="Máximo scalerank (menor = mais importante). Default 8.")
    ap.add_argument("--overwrite", action="store_true", default=True, help="Recria o CSV de saída (default: True)")
    ap.add_argument("--only", help="Lista ISO3 separados por vírgula (ex.: PRT,ESP)")

    args = ap.parse_args()

    # Caminhos personalizados (se fornecidos)
    admin_path = None
    rivers_path = None
    if args.admin:
        p1 = Path(args.admin) / "ne_10m_admin_0_countries.shp"
        p2 = Path(args.admin) / "ne_10m_admin_0_countries.gpkg"
        admin_path = p1 if p1.exists() else (p2 if p2.exists() else None)
    if args.rivers:
        p1 = Path(args.rivers) / "ne_10m_rivers_lake_centerlines.shp"
        p2 = Path(args.rivers) / "ne_10m_rivers_lake_centerlines.gpkg"
        rivers_path = p1 if p1.exists() else (p2 if p2.exists() else None)

    countries = _read_profiles(Path(args.profiles))
    if args.only:
        only = {x.strip().upper() for x in args.only.split(",") if x.strip()}
        countries = countries[countries["iso3"].isin(only)]

    admin = _load_admin_polygons(admin_path)
    rivers = _load_rivers_geoms(rivers_path)

    # Filtro opcional por scalerank (se existir)
    if pd.notna(rivers["scalerank"]).any():
        rivers = rivers[(rivers["scalerank"].isna()) | (rivers["scalerank"] <= int(args.max_scalerank))].copy()

    out_csv = Path(args.out)
    _ensure_header(out_csv, overwrite=bool(args.overwrite))
    existing = _load_existing_keys(out_csv)

    # Convertir para CRS métrico
    admin_m  = admin.to_crs(METRIC_CRS)
    rivers_m = rivers.to_crs(METRIC_CRS)

    total_new = 0
    writer = None

    for idx, iso3 in enumerate(countries["iso3"], start=1):
        poly = admin_m[admin_m["iso3"] == iso3]
        if poly.empty:
            print(f"[rivers] {iso3}: (sem polígono admin0)")
            continue

        # Recorte preciso por país
        clip = gpd.overlay(rivers_m, poly, how="intersection")
        if clip.empty:
            print(f"[rivers] {iso3}: 0")
            continue

        # Comprimento do segmento dentro do país (km)
        clip = clip.copy()
        clip["length_km"] = clip.geometry.length / 1000.0

        # Agregar por nome
        grp = (clip.groupby(["river_name"], dropna=False)
                  .agg(length_km=("length_km", "sum"),
                       scalerank=("scalerank", "min"),
                       featurecla=("featurecla", "first"))
                  .reset_index())

        # Filtrar nome e comprimento mínimo
        show = grp[
            (grp["river_name"].astype(str).str.strip() != "") &
            (grp["length_km"] >= float(args.min_km))
        ].copy()

        if show.empty:
            print(f"[rivers] {iso3}: 0 (após filtros)")
            continue

        # Preparar escrita incremental
        rows_out = []
        for _, r in show.iterrows():
            key = (iso3, str(r["river_name"]))
            if key in existing:
                continue
            rows_out.append({
                "iso3": iso3,
                "river_name": str(r["river_name"]),
                "length_km": f"{float(r['length_km']):.0f}",
                "scalerank": "" if pd.isna(r["scalerank"]) else int(r["scalerank"]),
                "featurecla": str(r.get("featurecla", "")),
                "source": "NaturalEarth_10m_rivers",
            })

        if rows_out:
            with out_csv.open("a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=OUT_COLS, delimiter=";")
                for it in rows_out:
                    w.writerow(it)
            total_new += len(rows_out)
            existing.update((row["iso3"], row["river_name"]) for row in rows_out)
            print(f"[rivers] {iso3}: +{len(rows_out)}")
        else:
            print(f"[rivers] {iso3}: 0 (tudo já existente)")

    print(f"[rivers] concluído → {out_csv} | novos: {total_new}")

if __name__ == "__main__":
    main()
