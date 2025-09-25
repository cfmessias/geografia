# utils/streamlit_compat.py
from __future__ import annotations
import inspect
import streamlit as st

def _pop_width_kwargs(kwargs: dict) -> dict:
    """Normaliza 'width' vs 'use_container_width'."""
    if "width" in kwargs and "use_container_width" not in kwargs:
        w = kwargs.pop("width")
        # 'stretch' → True, 'content' → False; inteiros mantêm como estão
        if isinstance(w, str):
            kwargs["use_container_width"] = (w.strip().lower() == "stretch")
        elif isinstance(w, (int, float)):
            # deixar como está; o widget aceita int em algumas APIs
            kwargs["use_container_width"] = False  # fallback neutro
    return kwargs

def _lift_plotly_config(kwargs: dict) -> dict:
    """Move kwargs de Plotly para config, evitando avisos na cloud."""
    cfg = dict(kwargs.pop("config", {}) or {})
    for k in (
        "displayModeBar", "scrollZoom", "modeBarButtonsToRemove",
        "toImageButtonOptions", "editable", "responsive"
    ):
        if k in kwargs:
            cfg[k] = kwargs.pop(k)
    if cfg:
        kwargs["config"] = cfg
    return kwargs

def patch_streamlit() -> None:
    """Aplica shims compatíveis a várias funções do Streamlit."""
    # --- plotly_chart: mover config + lidar com width/use_container_width
    _orig_plotly = st.plotly_chart
    def _plotly_compat(fig, *args, **kwargs):
        # 1) mover opções de plotly para config
        kwargs = _lift_plotly_config(kwargs)
        # 2) reconciliar width vs use_container_width
        had_width = "width" in kwargs
        kwargs = _pop_width_kwargs(kwargs)
        try:
            # tentar com os kwargs atuais
            return _orig_plotly(fig, *args, **kwargs)
        except TypeError as e:
            # ambiente antigo → não aceita 'width'
            if had_width and "width" in str(e).lower():
                # voltar a transformar para use_container_width
                ucw = kwargs.pop("use_container_width", True)
                # meter explicitamente True se pedimos stretch
                kwargs["use_container_width"] = True if ucw else False
                return _orig_plotly(fig, *args, **kwargs)
            raise
    st.plotly_chart = _plotly_compat  # type: ignore

    # --- altair_chart: só normalizar width/use_container_width
    _orig_altair = st.altair_chart
    def _altair_compat(obj, *args, **kwargs):
        had_width = "width" in kwargs
        kwargs = _pop_width_kwargs(kwargs)
        try:
            return _orig_altair(obj, *args, **kwargs)
        except TypeError as e:
            if had_width and "width" in str(e).lower():
                # fallback para use_container_width nos ambientes antigos
                kwargs.pop("width", None)
                kwargs.setdefault("use_container_width", True)
                return _orig_altair(obj, *args, **kwargs)
            raise
    st.altair_chart = _altair_compat  # type: ignore

    # --- dataframe / table: ignorar 'width' se não existir na versão local
    for name in ("dataframe", "table"):
        _orig = getattr(st, name)
        def _make_wrap(func):
            def _wrap(*args, **kwargs):
                had_width = "width" in kwargs
                kwargs = _pop_width_kwargs(kwargs)
                try:
                    return func(*args, **kwargs)
                except TypeError as e:
                    if had_width and "width" in str(e).lower():
                        kwargs.pop("width", None)
                        kwargs.setdefault("use_container_width", True)
                        return func(*args, **kwargs)
                    raise
            return _wrap
        setattr(st, name, _make_wrap(_orig))  # type: ignore

    # --- botões que não suportam 'width': só removemos
    for name in ("download_button", "form_submit_button", "button"):
        if hasattr(st, name):
            _orig_btn = getattr(st, name)
            def _wrap_btn(*args, **kwargs):
                kwargs.pop("width", None)            # remover
                kwargs.pop("use_container_width", None)
                return _orig_btn(*args, **kwargs)
            setattr(st, name, _wrap_btn)  # type: ignore
