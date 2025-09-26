# scripts/build_country_seed.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import csv, re, sys, os
import subprocess

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
OUT_SEED     = DATA_DIR / "countries_seed.csv"

# Quais CSVs manter durante a limpeza
KEEP_CSV_NAMES = {"demografia_mundial.csv","index.csv"}  # case-insensitive

def slugify(s: str) -> str:
    s = re.sub(r"[^\w\-]+", "_", s, flags=re.U)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "pais"

def purge_csvs_except_demografia() -> None:
    """
    Apaga todos os .csv dentro de data/ (recursivo), EXCETO 'demografia_mundial.csv'.
    Útil para forçar refresh limpo antes de reconstruir datasets.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    deleted = 0
    for p in DATA_DIR.rglob("*.csv"):
        try:
            if p.name.lower() in {n.lower() for n in KEEP_CSV_NAMES}:
                continue
            p.unlink()
            deleted += 1
        except Exception as e:
            print(f"⚠️ Não consegui apagar {p}: {e}", file=sys.stderr)
    print(f"🧹 Limpeza concluída: removidos {deleted} CSV(s) (preservado(s): {', '.join(KEEP_CSV_NAMES)})")

def build_seed() -> None:
    try:
        import pycountry
    except Exception:
        print("❌ Falta 'pycountry'. Instala com: pip install pycountry")
        sys.exit(1)

    # nomes PT (opcional)
    name_pt = {}
    try:
        from babel import Locale
        loc = Locale.parse("pt")
        for code, n in loc.territories.items():
            if len(code) == 2 and code.isalpha():
                name_pt[code.upper()] = n
    except Exception:
        pass

    rows = []
    for c in pycountry.countries:
        iso2 = getattr(c, "alpha_2", "").upper()
        iso3 = getattr(c, "alpha_3", "").upper()
        if not iso2:
            continue
        name_en = (
            getattr(c, "common_name", None)
            or getattr(c, "official_name", None)
            or getattr(c, "name", "")
        )
        nm_pt = name_pt.get(iso2, "")
        slug = slugify(nm_pt or name_en or iso2)
        rows.append(
            {"iso2": iso2, "iso3": iso3, "name_en": name_en, "name_pt": nm_pt, "slug": slug}
        )

    OUT_SEED.parent.mkdir(parents=True, exist_ok=True)
    with OUT_SEED.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["iso2", "iso3", "name_en", "name_pt", "slug"])
        w.writeheader()
        w.writerows(rows)
    print(f"✔️ Escrevi {OUT_SEED} ({len(rows)} países)")

def run_aux_scripts() -> None:
    """
    Descobre e executa os restantes scripts na pasta 'scripts', por ordem recomendada.
    """
    SCRIPTS_DIR = Path(__file__).resolve().parent
    SELF_NAME   = Path(__file__).name  # "build_country_seed.py"

    # Ordem recomendada (se existirem). O resto vai a seguir por ordem alfabética.
    preferred_order = [
        "extract_country_data.py",
        "fetch_worldbank_timeseries.py",
        "fetch_leaders.py",
        "fetch_unesco.py",
        "fetch_olympics.py",
        "fetch_gastronomy_all.py",
        "fetch_cities.py",
        "fetch_migration.py",
        "fetch_religion.py",
        "fetch_tourism_all.py",
        "fetch_migration_inout.py",
        "fetch_cmip6_blobal.py",
    ]

    # 1) pega nos preferidos que existam
    to_run = [SCRIPTS_DIR / s for s in preferred_order if (SCRIPTS_DIR / s).exists()]

    # 2) acrescenta quaisquer outros .py na pasta (exclui este próprio ficheiro e já listados)
    others = [
        p for p in sorted(SCRIPTS_DIR.glob("*.py"))
        if p.name not in preferred_order and p.name != SELF_NAME
    ]
    to_run.extend(others)

    print("\n=== A executar scripts auxiliares ===")
    for script in to_run:
        print(f"▶ {script.name}")
        result = subprocess.run([sys.executable, str(script)], cwd=str(SCRIPTS_DIR))
        if result.returncode != 0:
            print(f"✖ {script.name} falhou (exit {result.returncode}). A interromper.")
            break
    else:
        print("✔ Todos os scripts concluídos.")

if __name__ == "__main__":
    # 1) Limpeza de CSVs (preserva apenas demografia_mundial.csv)
    purge_csvs_except_demografia()
    # 2) Construir seed de países
    build_seed()
    # 3) Executar os restantes scripts por ordem
    run_aux_scripts()
