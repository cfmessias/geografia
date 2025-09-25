# services/__init__.py
# -*- coding: utf-8 -*-
"""
Hub de reexportação do pacote services.
Carrega apenas os símbolos que EXISTEM em services.offline_store,
evitando ImportError quando alguma função ainda não foi implementada.
"""

from __future__ import annotations
from . import offline_store as _os  # importa o módulo, não nomes específicos


__all__: list[str] = []

def _export(name: str) -> None:
    if hasattr(_os, name):
        globals()[name] = getattr(_os, name)
        __all__.append(name)

# caminhos / diretórios (se existirem)
for _n in [
    "DATA_DIR",
    "countries_seed_path",
    "countries_profiles_path",
    "worldbank_timeseries_path",
    "olympics_summer_manual_path",    
    "cities_path",
    "leaders_path",
    "unesco_path",
    "gastronomy_dishes_path",
    "gastronomy_beverages_path",
    # --- NOVOS ---
    "tourism_timeseries_path",
    "tourism_latest_path",
    "tourism_origin_eu_path",
    "tourism_purpose_eu_path",
    "migration_inout_path",
]:
    _export(_n)



# loaders (se existirem)
for _n in [
    "list_available_countries",
    "load_country_profile",
    "load_worldbank_timeseries",
    "wb_series_for_country",
    "load_cities",
    "country_has_cities",
    "load_leaders",
    "load_unesco",
    "load_olympics_summer_csv",
    "load_flag_info",
    # --- NOVOS ---
    "load_tourism_ts",
    "load_tourism_latest",
    "tourism_series_for_iso3",
    "load_tourism_origin_eu",
    "tourism_origin_for_iso3",
    "load_tourism_purpose_eu",
    "tourism_purpose_for_iso3",
    "load_migration_inout",
    "migration_inout_for_iso3",
]:
    _export(_n)



# alias úteis se o offline_store tiver nomes diferentes
if "list_available_countries" not in globals() and hasattr(_os, "list_countries_index"):
    globals()["list_available_countries"] = getattr(_os, "list_countries_index")
    __all__.append("list_available_countries")

# nunca exportar símbolos obsoletos
for _obsolete in ("have_export_data",):
    if _obsolete in globals():
        del globals()[_obsolete]
        try:
            __all__.remove(_obsolete)
        except ValueError:
            pass
