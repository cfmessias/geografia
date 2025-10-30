# services/geo_names.py
from __future__ import annotations
import streamlit as st

def country_display_name(iso3: str, fallback: str | None = None) -> str:
    """
    Devolve o nome do país na língua ativa (PT/EN) usando datasets locais.
    Ordem de preferência:
      - língua ativa (PT→country_pt, EN→country_en)
      - fallback recebido
      - a outra língua
      - ISO3
    """
    iso3 = (iso3 or "").upper().strip()
    lang = st.session_state.get("lang", "pt")
    name_pt, name_en = None, None

    # 1) coastlines.csv
    try:
        from services.geo_store import load_coastlines
        dfc = load_coastlines()
        if not dfc.empty:
            row = dfc[dfc["iso3"] == iso3]
            if not row.empty:
                if "country_pt" in dfc.columns: name_pt = row["country_pt"].iloc[0]
                if "country_en" in dfc.columns: name_en = row["country_en"].iloc[0]
    except Exception:
        pass

    # 2) ports_and_routes.csv (fallback)
    if not (name_pt or name_en):
        try:
            from services.geo_store import load_ports_and_routes
            dfp = load_ports_and_routes()
            if not dfp.empty:
                row = dfp[dfp["iso3"] == iso3]
                if not row.empty:
                    if "country_pt" in dfp.columns: name_pt = row["country_pt"].iloc[0]
                    if "country_en" in dfp.columns: name_en = row["country_en"].iloc[0]
        except Exception:
            pass

    # 3) timezones.csv (mais um fallback possível)
    if not (name_pt or name_en):
        try:
            from services.geo_store import load_timezones_new
            dft = load_timezones_new()
            if not dft.empty and "country_iso3" in dft.columns:
                row = dft[dft["country_iso3"] == iso3]
                if not row.empty:
                    if "country" in dft.columns:   # se só houver 1 coluna “country”
                        name_pt = name_pt or row["country"].iloc[0]
                        name_en = name_en or row["country"].iloc[0]
        except Exception:
            pass

    if lang == "en":
        return name_en or fallback or name_pt or iso3
    else:
        return name_pt or fallback or name_en or iso3
