# scripts/build_country_seed.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import csv, re, sys, os
import subprocess
import argparse
from typing import Iterable, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
OUT_SEED     = DATA_DIR / "countries_seed.csv"

# Quais CSVs manter durante a limpeza (case-insensitive)
KEEP_CSV_NAMES = {"demografia_mundial.csv","index.csv","olympics_summer_manual.csv","un_m49_iso.csv"}

def slugify(s: str) -> str:
    s = re.sub(r"[^\w\-]+", "_", s, flags=re.U)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "pais"

def _gather_csvs_to_delete() -> List[Path]:
    """Lista de CSVs dentro de data/ a remover (exclui KEEP_CSV_NAMES)."""
    keep_lower = {n.lower() for n in KEEP_CSV_NAMES}
    to_delete: List[Path] = []
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for p in DATA_DIR.rglob("*.csv"):
        if p.name.lower() in keep_lower:
            continue
        to_delete.append(p)
    return to_delete

def _confirm_dangerous_action(to_delete: List[Path], assume_yes: bool, dry_run: bool) -> None:
    """Pede confirmação explícita antes de apagar muitos ficheiros."""
    if dry_run:
        print("🔎 [dry-run] Simulação ativa: não será apagado nenhum ficheiro.")
        return
    if assume_yes:
        print("⚠️  '--yes' fornecido: a limpeza prosseguirá sem confirmação interativa.")
        return

    total = len(to_delete)
    if total == 0:
        print("🧹 Nada para apagar.")
        return

    examples = "\n".join(f"   · {p}" for p in to_delete[:10])
    rest = "" if total <= 10 else f"\n   … e mais {total-10} ficheiro(s)."
    keep_list = ", ".join(sorted(KEEP_CSV_NAMES))

    print(
        f"⚠️  ATENÇÃO!\n"
        f"Isto vai apagar {total} ficheiro(s) CSV dentro de '{DATA_DIR}'.\n"
        f"Os seguintes ficheiros serão PRESERVADOS: {keep_list}\n\n"
        f"Exemplos de ficheiros a remover:\n{examples}{rest}\n"
    )
    print("Para continuar, escreve exatamente: APAGAR")
    try:
        answer = input("> ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\n❌ Ação cancelada.")
        sys.exit(1)

    if answer != "APAGAR":
        print("❌ Confirmação falhada. Nada foi apagado.")
        sys.exit(1)

def purge_csvs_except_demografia(assume_yes: bool = False, dry_run: bool = False) -> None:
    """
    Apaga todos os .csv dentro de data/ (recursivo), EXCETO os em KEEP_CSV_NAMES.
    Agora com confirmação interativa.
    """
    to_delete = _gather_csvs_to_delete()
    _confirm_dangerous_action(to_delete, assume_yes=assume_yes, dry_run=dry_run)

    deleted = 0
    if dry_run:
        print(f"🧪 [dry-run] Seriam removidos {len(to_delete)} CSV(s).")
    else:
        for p in to_delete:
            try:
                p.unlink()
                deleted += 1
            except Exception as e:
                print(f"⚠️ Não consegui apagar {p}: {e}", file=sys.stderr)
        print(f"🧹 Limpeza concluída: removidos {deleted} CSV(s) "
              f"(preservado(s): {', '.join(KEEP_CSV_NAMES)})")

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
        w = csv.DictWriter(
            f,
            fieldnames=["iso2", "iso3", "name_en", "name_pt", "slug"],
            delimiter=";",             # ← separador ponto e vírgula
            quoting=csv.QUOTE_MINIMAL, # só cita quando precisa
            lineterminator="\n",       # linhas limpas
        )
        w.writeheader()
        w.writerows(rows)
    print(f"✔️ Escrevi {OUT_SEED} ({len(rows)} países) em CSV com SEP=';'")

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
        ## "fetch_olympics.py",
        "fetch_gastronomy_all.py",
        "fetch_cities.py",
        "fetch_migration.py",
        "fetch_religion.py",
        "fetch_tourism_all.py",
        "fetch_migration_inout.py",
        "fetch_cmip6_global.py",
        ## "fetch_all_country_forms.py",
        ## "fetch_conflicts_participants.py",
        ## "fetch_conflicts_countries.py",
        ## "merge_conflicts_with_labels.py",
        ## "enrich_conflicts_long_for_ui.py",
        ## "clean_enriched_conflicts.py",
        ## "online_backfill_iso3_mwapi.py",
		"fetch_state_forms_by_class.py",
		"build_lineage_level2_from_seed.py",
		"build_conflict_types.py",
		"make_conflict_types_2col.py",
		"fetch_conflicts_from_types.py",
		"build_conflict_catalog.py",
		"fetch_conflict_roles_from_catalog.py",
		"flag_humans_by_qid.py",
		"append_conflicts.py",
		"enrich_conflicts_with_dates_and_iso.py",
        ##"map_qids_to_iso3.py",
        "fetch_country_languages_pt.py",
        "fetch_colonies_from_forms.py",
        "enrich_en_pt_labels.py",
        "fetch_monarchs.py",
        "fetch_timezones.py",
        "fetch_geography_all.py",
        "fetch_koppen_by_country.py",
        "fetch_biomes_by_country.py",
        "fetch_coastlines.py",
        "fetch_ports_and_routes.py",
    ]

    to_run = [p for s in preferred_order if (p := (SCRIPTS_DIR / s)).exists()]
    others = [
        p for p in sorted(SCRIPTS_DIR.glob("*.py"))
        if p.name not in {Path(x).name for x in preferred_order} and p.name != SELF_NAME
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

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Constrói countries_seed.csv e (opcionalmente) limpa CSVs em data/ com confirmação."
    )
    ap.add_argument(
        "--yes", action="store_true",
        help="Não pedir confirmação interativa para a limpeza (atenção: perigoso)."
    )
    ap.add_argument(
        "--dry-run", action="store_true",
        help="Simular a limpeza (não apaga ficheiros)."
    )
    ap.add_argument(
        "--skip-clean", action="store_true",
        help="Não executar a limpeza de CSVs."
    )
    ap.add_argument(
        "--skip-run", action="store_true",
        help="Não executar scripts auxiliares após gerar o seed."
    )
    return ap.parse_args()

if __name__ == "__main__":
    args = parse_args()

    # 1) Limpeza de CSVs (preserva apenas os de KEEP_CSV_NAMES)
    if not args.skip_clean:
        purge_csvs_except_demografia(assume_yes=args.yes, dry_run=args.dry_run)
    else:
        print("⏭️  Limpeza ignorada por --skip-clean.")

    # 2) Construir seed de países
    build_seed()

    # 3) Executar os restantes scripts por ordem
    if not args.skip_run:
        run_aux_scripts()
    else:
        print("⏭️  Execução de scripts auxiliares ignorada por --skip-run.")
