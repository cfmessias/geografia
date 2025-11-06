# -*- coding: utf-8 -*-
from __future__ import annotations
import pandas as pd
import streamlit as st
from services.i18n import t as tr
from services.geo_store import rivers_for_iso3

def _colcfg_rivers() -> dict:
    return {
        "river_name":   st.column_config.TextColumn(tr("geo.sections.rivers.table.river")),
        "length_km":    st.column_config.NumberColumn(tr("geo.sections.rivers.table.length_km"), format="%.0f"),
        "source_label": st.column_config.TextColumn(tr("geo.sections.rivers.table.source")),
        "mouth_label":  st.column_config.TextColumn(tr("geo.sections.rivers.table.mouth")),
        "basin_label":  st.column_config.TextColumn(tr("geo.sections.rivers.table.basin")),
        "scalerank":    st.column_config.NumberColumn("NE.rank"),
        "featurecla":   st.column_config.TextColumn("NE.class"),
    }

def render_rivers_block(iso3: str):
    st.markdown(f"### {tr('geo.sections.rivers.title')}")
    df = rivers_for_iso3(iso3, top_n=12, min_km=50.0)
    if df is None or df.empty:
        st.caption(tr("geo.sections.rivers.empty"))
        return

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config=_colcfg_rivers()
    )
    st.caption(tr("geo.sections.rivers.note"))
