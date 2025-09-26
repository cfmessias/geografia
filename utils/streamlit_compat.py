# utils/streamlit_compat.py
from __future__ import annotations
import inspect
import streamlit as st

def _bridge_width_kwargs(fn):
    """Aceita tanto width='stretch'/'content' como use_container_width=True/False,
    convertendo para o que a versão de Streamlit em execução espera."""
    sig = inspect.signature(fn)

    def wrapper(*args, **kwargs):
        # Se a função suporta 'width', converte use_container_width -> width
        if "width" in sig.parameters:
            if "use_container_width" in kwargs:
                ucw = kwargs.pop("use_container_width")
                kwargs.setdefault("width", "stretch" if ucw else "content")
        # Se a função NÃO suporta 'width', mas suporta use_container_width,
        # converte width -> use_container_width (para instalações mais antigas).
        elif "use_container_width" in sig.parameters and "width" in kwargs:
            w = kwargs.pop("width")
            if isinstance(w, str):
                kwargs.setdefault("use_container_width", (w == "stretch"))
        # Remove resíduos para evitar warnings
        kwargs.pop("use_container_width", None) if "use_container_width" not in sig.parameters else None
        kwargs.pop("width", None)               if "width" not in sig.parameters else None
        return fn(*args, **kwargs)

    return wrapper


def patch_streamlit():
    # Componentes onde queremos o “bridge”
    names = [
        "dataframe", "table",
        "plotly_chart", "altair_chart", "pyplot",
        "map", "line_chart", "bar_chart", "area_chart",
        "scatter_chart",
    ]
    for name in names:
        if hasattr(st, name):
            setattr(st, name, _bridge_width_kwargs(getattr(st, name)))

    # download_button: versões novas não aceitam width/use_container_width — removemos silenciosamente
    if hasattr(st, "download_button"):
        _orig = st.download_button
        def _dl(*a, **kw):
            kw.pop("width", None)
            kw.pop("use_container_width", None)
            return _orig(*a, **kw)
        st.download_button = _dl


# aplicar patch ao importar
patch_streamlit()

# --- Aliases para manter compatibilidade de nomes ---------------------------
# Usa o nome que já existe no teu projeto como "oficial".
# ---------------------------------------------------------------------------
# Compat aliases — mantém patch_streamlit como nome oficial
# ---------------------------------------------------------------------------
try:
    patch_streamlit  # deve existir acima
except NameError:
    # fallback defensivo: no-op (não deve acontecer)
    def patch_streamlit():
        pass

# Todos estes nomes passam a apontar para a MESMA função:
patch_streamlit_width = patch_streamlit
ensure_mobile_width    = patch_streamlit
install_width_patch    = patch_streamlit
compat_width           = patch_streamlit
make_streamlit_mobile  = patch_streamlit

