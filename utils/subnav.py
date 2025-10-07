# utils/subnav.py
from __future__ import annotations
import streamlit as st
from typing import List, Tuple

def subnav(ns_key: str, items: List[Tuple[str, str]], *, default: str | None = None) -> str:
    """
    Submenu horizontal logo abaixo do título.
    items = [(value, label), ...]
    Retorna o value selecionado. Persiste em ?ns_key=value.
    """
    qp = dict(st.query_params)
    cur = qp.get(ns_key) or st.session_state.get(f"_subnav_{ns_key}") or default or (items[0][0] if items else "")

    labels = [lbl for _, lbl in items]
    values = [val for val, _ in items]
    try:
        idx = values.index(cur)
    except ValueError:
        idx, cur = 0, values[0]

    chosen_label = st.radio(
        label="",
        options=labels,
        index=idx,
        horizontal=True,
        key=f"subnav_{ns_key}_lang_{st.session_state.get('lang','pt')}",
    )

    sel = values[labels.index(chosen_label)]
    if sel != cur:
        st.session_state[f"_subnav_{ns_key}"] = sel
        st.query_params.update({ns_key: sel})
        st.rerun()
    return sel
