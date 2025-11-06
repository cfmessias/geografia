# paises.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import streamlit as st
from services.i18n import t as tr
from services.i18n_boot import _ensure_lang_state
from services.countries_names import country_display_name

from views.paises_submenu import paises_submenu
from components.country_picker import country_selector
from views.paises_overview import render_overview_panel
from views.migration_section import render_migration_section
from views.migration_tables import render_country_migration_tables
from views.render_monarchy_expander import render_monarchy_expander
from views.origins import render_origins_expander
from views.colonizacao import render_colonization_expander
from views.render_wars_battles_expander import render_wars_battles_expander
from views.economia import render_wdi_panel
from views.demografia_paises import render_demography_expander
from views.paises_geografia import render_geography_panel
from views.lideranca import render_leadership_block

def render_paises_tab():
    _ensure_lang_state()

    mode = paises_submenu()

    from services.offline_store import list_available_countries
   
    countries = list_available_countries()
    if countries.empty:
        st.error(tr("labels.sem_paises_disponiveis")); return

    country_name, iso3 = country_selector(countries)
    if not (country_name and iso3):
        st.info(tr("labels.escolhe_um_pais_e_clica_abrir")); return

    # Cabeçalho com nome do país conforme idioma
    st.subheader(country_display_name(iso3, country_name))

    if mode == "ov":
        render_overview_panel(iso3=iso3, country_name=country_name)

    elif mode == "demog":
        render_demography_expander(iso3=iso3, country_name=country_name, tr=tr)
        render_migration_section(iso3)
        
        render_country_migration_tables(iso3, year=2024, top=20)

    elif mode == "hist":
        render_origins_expander(iso3, default_open=False)
        render_monarchy_expander(iso3, default_open=False)
        render_colonization_expander(iso3, default_open=False)
        render_leadership_block(iso3)
        render_wars_battles_expander(iso3, default_open=False)

    elif mode == "econ":
        render_wdi_panel(iso3=iso3, country_name=country_name)

    elif mode == "geo":
        render_geography_panel(iso3=iso3, country_name=country_name)
