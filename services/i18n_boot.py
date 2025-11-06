# services/i18n_boot.py
import streamlit as st
from services.i18n import set_language

DEFAULT_LANG = "pt"
LANGS = ["pt", "en"]
LABELS = {"pt": "PT 🇵🇹", "en": "EN 🇬🇧"}

# services/i18n_boot.py
import streamlit as st
from services.i18n import set_language

DEFAULT_LANG = "pt"
LANGS = ["pt", "en"]
LABELS = {"pt": "PT 🇵🇹", "en": "EN 🇬🇧"}

def _ensure_lang_state():
    if "lang" not in st.session_state:
        st.session_state.lang = DEFAULT_LANG
    set_language(st.session_state.lang)

def render_lang_select_right(key: str = "lang_select_top", ratios=(12, 2), show_label: bool = False):
    _ensure_lang_state()
    col_left, col_right = st.columns(ratios)
    with col_right:
        # guarda o valor no próprio key
        st.selectbox(
            "Idioma / Language",
            options=LANGS,
            index=LANGS.index(st.session_state.lang),
            format_func=lambda x: LABELS.get(x, x),
            key=key,
            label_visibility="visible" if show_label else "collapsed",
        )

    # sincroniza o estado da app com o valor do widget
    new_lang = st.session_state.get(key, st.session_state.lang)
    if new_lang != st.session_state.lang:
        st.session_state.lang = new_lang
        set_language(new_lang)  # NÃO chamar st.rerun()


# —— Compatibilidade com nomes antigos ——
init_i18n_state = _ensure_lang_state                  # alguns ficheiros importavam este
def render_lang_select(*args, **kwargs):              # alias para quem usava este nome
    return render_lang_select_right(*args, **kwargs)

__all__ = ["_ensure_lang_state", "init_i18n_state", "render_lang_select_right", "render_lang_select"]
