# -*- coding: utf-8 -*-
from __future__ import annotations
import streamlit as st

# --- PAGE CONFIG ---
st.set_page_config(page_title="Geografía e Metereologia Mundiais", layout="wide")

# --- IMPORTS DOS TEUS MÓDULOS (inalterados) ---
from paises import render_paises_tab
from views.ind_demograficos import render_indicadores_tab
from meteo import render_meteo


# ---------- helpers ----------
def _get_qp(name: str, default: str = "") -> str:
    """Lê query param com tolerância a versões de Streamlit."""
    try:
        v = st.query_params.get(name, default)
        # em versões mais antigas pode vir lista
        if isinstance(v, list) and v:
            return v[0]
        return v
    except Exception:
        return default

def _is_mobile() -> bool:
    """Decide se está em layout mobile (query param > toggle > sessão)."""
    q = _get_qp("mobile", "").lower()
    if q in ("1", "true", "t", "yes", "y"):
        return True
    if q in ("0", "false", "f", "no", "n"):
        return False
    return bool(st.session_state.get("mobile_mode", False))


def _css_desktop_tabs():
    st.markdown(
        """
        <style>
        /* Fonte maior nas tabs (desktop) */
        .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
            font-size: 18px !important;
            font-weight: 600 !important;
        }
        .stTabs [data-baseweb="tab-list"] button { height: 60px !important; }
        </style>
        """,
        unsafe_allow_html=True,
    )

def _css_mobile():
    st.markdown(
        """
        <style>
        /* mobile-first: menos padding e targets maiores */
        .block-container { padding-top: .6rem; padding-bottom: .8rem; }
        .stButton>button, .stSelectbox, .stTextInput input, .stDateInput input, .stSlider { font-size: 16px; }
        .stRadio [role='radiogroup'] { gap: .5rem; }
        .stRadio label p { font-size: 16px !important; }
        </style>
        """,
        unsafe_allow_html=True,
    )


# ---------- HEADER ----------
st.title("🌎 Geográfia e Metereologia Mundiais")
st.markdown("---")
st.markdown("### Explore dados demográficos e informações sobre países")

# Toggle para forçar mobile (e também vale ?mobile=1 na URL)
left, _, _ = st.columns([2, 3, 7])
with left:
    st.toggle("Mobile", key="mobile_mode", value=_is_mobile(), help="Também podes usar ?mobile=1 na URL")

mobile = _is_mobile()

# ---------- LAYOUT ----------
if not mobile:
    # ----- DESKTOP: tabs como já tinhas -----
    _css_desktop_tabs()

    tab_paises, tab_ind, tab_meteo = st.tabs([
        "🌍 Países", "📊 Demografia", "☁️ Meteorologia"
    ])

    with tab_paises:
        render_paises_tab()

    with tab_ind:
        render_indicadores_tab()

    with tab_meteo:
        render_meteo(embed=True, key_prefix="meteo", show_title=True)

else:
    # ----- MOBILE: 1 secção de cada vez + navegação simples -----
    _css_mobile()

    # Usa segmented control quando disponível; caso contrário, radio horizontal
    choice_key = "mobile_nav_choice"
    options = ["🌍 Países", "📊 Demografia", "☁️ Meteorologia"]

    segmented = getattr(st, "segmented_control", None)
    if callable(segmented):
        choice = st.segmented_control("Navegar", options=options, default=options[0], key=choice_key)
    else:
        choice = st.radio("Navegar", options=options, horizontal=True, key=choice_key)

    st.markdown("---")

    if choice.startswith("🌍"):
        render_paises_tab()

    elif choice.startswith("📊"):
        render_indicadores_tab()

    else:  # "☁️ Meteorologia"
        # a tua função de meteorologia já existente
        render_meteo(embed=True, key_prefix="meteo_m", show_title=True)
