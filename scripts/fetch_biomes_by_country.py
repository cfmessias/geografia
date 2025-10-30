# scripts/fetch_biomes_by_country.py
# -*- coding: utf-8 -*-
"""
Gera 'data/biomes.csv' (heurístico) a partir de 'data/koppen.csv'.
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd

DATA = Path("data")
IN_KOPPEN = DATA / "koppen.csv"
OUT_CSV   = DATA / "biomes.csv"

KOPPEN_TO_BIOME = {
    "A": "Tropical & Subtropical Moist Broadleaf Forests / Savannas",
    "B": "Deserts & Xeric Shrublands / Drylands",
    "C": "Temperate Broadleaf & Mixed Forests / Mediterranean",
    "D": "Temperate Conifer / Boreal Forests (Taiga)",
    "E": "Tundra / Ice",
}

def main() -> None:
    if not IN_KOPPEN.exists():
        raise SystemExit(f"Ficheiro não encontrado: {IN_KOPPEN} — corre primeiro fetch_koppen_by_country.py")
    df = pd.read_csv(IN_KOPPEN, sep=';', dtype=str, encoding='utf-8', keep_default_na=False)
    rows = []
    for _, r in df.iterrows():
        iso3 = str(r.get('country_iso3','')).upper()
        klass = str(r.get('koppen','')).strip()
        major = klass[:1].upper() if klass else ''
        biome = KOPPEN_TO_BIOME.get(major, 'Temperate / Mixed')
        rows.append({'country_iso3': iso3, 'biome': biome, 'share_pct': 100.0})
    out = pd.DataFrame(rows).sort_values('country_iso3').reset_index(drop=True)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, sep=';', index=False, encoding='utf-8')
    print(f"OK: {OUT_CSV} ({len(out)})")

if __name__ == '__main__':
    main()
