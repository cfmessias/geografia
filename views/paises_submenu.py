# views/paises_submenu.py
from __future__ import annotations
import streamlit as st
from services.i18n import t as tr

def paises_submenu() -> str:
    """Submenu de Países. Retorna a key da aba selecionada."""
    try:
        label_ov    = tr("subnav.visao_global")
        label_demog = tr("subnav.demografia_pais")
        label_hist  = tr("subnav.historia")
        label_econ  = tr("subnav.economia")
        label_geo   = tr("subnav.geografia")
    except Exception:
        label_ov, label_demog, label_hist, label_econ, label_geo = (
            "Visão global", "Demografia", "História", "Economia", "Geografia"
        )

    tabs = [
        ("ov",    label_ov),
        ("demog", label_demog),
        ("hist",  label_hist),
        ("geo",   label_geo),
        ("econ",  label_econ),
    ]

    keys   = [k for k, _ in tabs]
    labels = dict(tabs)

    cur = st.session_state.get("paises_mode", "ov")
    if cur not in keys:
        cur = "ov"

    mode = st.radio(
        label="",
        options=keys,
        index=keys.index(cur),
        format_func=lambda k: labels[k],
        horizontal=True,
        key="paises_submenu_radio",
    )
    st.session_state["paises_mode"] = mode
    return mode
