# services/i18n_boot.py
import streamlit as st
from services.i18n import set_language

DEFAULT_LANG = "pt"
LANGS = ["pt", "en"]
LABELS = {"pt": "PT 🇵🇹", "en": "EN 🇬🇧"}

def _ensure_lang_state():
    """Garante que o idioma está no session_state e aplica-o ao i18n."""
    if "lang" not in st.session_state:
        st.session_state.lang = DEFAULT_LANG
    set_language(st.session_state.lang)

def render_lang_select_right(key: str = "lang_select", ratios=(12, 2), show_label: bool = False):
    """Selectbox PT/EN no canto superior direito (chamar no topo da página)."""
    _ensure_lang_state()
    col_left, col_right = st.columns(ratios)
    with col_right:
        cur = st.session_state.lang
        new = st.selectbox(
            "Idioma / Language",
            options=LANGS,
            index=LANGS.index(cur),
            format_func=lambda x: LABELS.get(x, x),
            key=key,
            label_visibility="visible" if show_label else "collapsed",
        )
    if new != cur:
        st.session_state.lang = new
        set_language(new)
        st.rerun()

# —— Compatibilidade com nomes antigos ——
init_i18n_state = _ensure_lang_state                  # alguns ficheiros importavam este
def render_lang_select(*args, **kwargs):              # alias para quem usava este nome
    return render_lang_select_right(*args, **kwargs)

__all__ = ["_ensure_lang_state", "init_i18n_state", "render_lang_select_right", "render_lang_select"]
