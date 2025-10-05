# -*- coding: utf-8 -*-
from __future__ import annotations

import streamlit as st
from services.i18n_boot import render_lang_select
from services.i18n import t
from services.i18n import t as tr
from services.i18n import t as tr   # <-- se estás a usar tr("..."); caso contrário, podes omitir
from utils.timing import timed, show_perf_panel
# importar a inicialização do idioma (compat com nomes antigos)
try:
    from services.i18n_boot import _ensure_lang_state, render_lang_select_right
except ImportError:
    from services.i18n_boot import init_i18n_state as _ensure_lang_state
    from services.i18n_boot import render_lang_select_right
# ── PAGE CONFIG ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="Geografía e Metereologia Mundiais", layout="wide")
_ensure_lang_state()

# seletor no canto superior direito
render_lang_select(ratios=(10, 2), show_label=False)  # ajusta ratios se quiseres


#st.title(t("app.title"))
# ── IMPORTS DOS MÓDULOS DA APP ───────────────────────────────────────────────
from paises import render_paises_tab
from demografia import render_indicadores_tab
from meteo import render_meteo
from utils.streamlit_compat import patch_streamlit
from utils.timing import timed, clear_perf, show_perf_panel

# aplicar eventuais compatibilidades/hotfixes
patch_streamlit()

# limpar o log de performance a cada rerun
clear_perf()

# ── HELPERS ──────────────────────────────────────────────────────────────────
def _get_qp(name: str, default: str = "") -> str:
    """Lê query param com tolerância a versões de Streamlit."""
    try:
        v = st.query_params.get(name, default)
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


def _css_desktop_tabs() -> None:
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


def _css_mobile() -> None:
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


# ── HEADER ───────────────────────────────────────────────────────────────────
st.title(tr("labels.geografia_e_metereologia_mundiais"))
st.markdown(tr("labels.text"))
st.markdown(tr("labels.explore_dados_demogr_ficos_e_informa_es_sobre_pa_ses"))

# Toggle para forçar mobile (também vale ?mobile=1 na URL)
left, _, _ = st.columns([2, 3, 7])
with left:
    st.toggle(tr("labels.mobile"), key="mobile_mode", value=_is_mobile(), help="Também podes usar ?mobile=1 na URL")

mobile = _is_mobile()

# ── FRAGMENTS (para evitar recomputar tudo a cada interação) ─────────────────
fragment = getattr(st, "fragment", None) or getattr(st, "experimental_fragment", None)
if fragment is None:
    # fallback no-op caso a versão não tenha fragment
    def fragment(fn):  # type: ignore
        return fn  # pragma: no cover

@fragment
def _tab_paises():
    with timed("🌍 Países"):
        render_paises_tab()
        
@fragment
def _tab_demografia():
    with timed("📊 Demografia"):
        render_indicadores_tab()
        
@fragment
def _tab_meteo():
    with timed("☁️ Meteorologia"):
        render_meteo(embed=True, key_prefix="meteo", show_title=True)
        
@fragment
def _tab_meteo_mobile():
    with timed("☁️ Meteorologia (mobile)"):
        render_meteo(embed=True, key_prefix="meteo_m", show_title=True)
        

# ── LAYOUT ───────────────────────────────────────────────────────────────────
if not mobile:
    # ----- DESKTOP: tabs -----
    _css_desktop_tabs()

    tab_paises, tab_ind, tab_meteo = st.tabs([
        tr("app.tabs.pa_ses"), tr("app.tabs.demografia"), tr("app.tabs.meteorologia")
    ])

    with tab_paises:
        _tab_paises()

    with tab_ind:
        _tab_demografia()

    with tab_meteo:
        _tab_meteo()

else:
    # ----- MOBILE: uma secção de cada vez -----
    _css_mobile()

    options = ["🌍 Países", "📊 Demografia", "☁️ Meteorologia"]
    segmented = getattr(st, "segmented_control", None)
    if callable(segmented):
        choice = st.segmented_control("Navegar", options=options, default=options[0], key="mobile_nav_choice")
    else:
        choice = st.radio(tr("labels.navegar"), options=options, horizontal=True, key="mobile_nav_choice")

    st.markdown(tr("labels.text"))

    if choice.startswith("🌍"):
        _tab_paises()
    elif choice.startswith("📊"):
        _tab_demografia()
    else:
        _tab_meteo_mobile()

# ── PAINEL DE PERFORMANCE (sidebar) ──────────────────────────────────────────
show_perf_panel("sidebar")
