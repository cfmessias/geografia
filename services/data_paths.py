# -*- coding: utf-8 -*-
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR     = PROJECT_ROOT / "data"

countries_seed_path       = DATA_DIR / "countries_seed.csv"
countries_profiles_path   = DATA_DIR / "countries_profiles.csv"

worldbank_timeseries_path = DATA_DIR / "wb_timeseries.csv"

cities_path               = DATA_DIR / "cities_all.csv"
unesco_path               = DATA_DIR / "unesco_all.csv"

leaders_current_path      = DATA_DIR / "leaders_current.csv"
leaders_history_path      = DATA_DIR / "leaders_history.csv"

religion_path             = DATA_DIR / "religion.csv"

olympics_summer_path      = DATA_DIR / "olympics_summer_manual.csv"

tourism_timeseries_path   = DATA_DIR / "tourism_timeseries.csv"
tourism_latest_path       = DATA_DIR / "tourism_latest.csv"
tourism_origin_eu_path    = DATA_DIR / "tourism_origin_eu.csv"
tourism_purpose_eu_path   = DATA_DIR / "tourism_purpose_eu.csv"

migration_inout_path      = DATA_DIR / "migration_inout.csv"
