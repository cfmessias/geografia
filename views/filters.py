# views/filters.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import date
from typing import Optional
import streamlit as st

_MONTHS = ["Todos","Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]

def render_filters(*,
                   mode: str = "full",
                   key_prefix: str = "flt",
                   default_place: str = "Lisboa",
                   default_start: Optional[date] = None,
                   default_end: Optional[date] = None,
                   place_full_label: str | None = None) -> dict:
    """
    Layout:
      linha 1: Início | Fim | Mês | Normais
      linha 2: Local | [pill com nome completo, só leitura]
      linha 3: [ Destacar 'há 50 anos' ]        [ (à direita) Média últimos 2 anos ]

    mode="place_only": mostra só o campo Local (resto devolvido como None/False).
    """
    today = date.today()
    if default_end is None:
        default_end = today
    if default_start is None:
        default_start = date(today.year - 10, 1, 1)

    if mode == "place_only":
        c = st.columns([1])
        with c[0]:
            q = st.text_input("Local", value=default_place,
                              placeholder="Cidade ou localidade",
                              key=f"{key_prefix}_q")
        return dict(
            query=q, start=None, end=None,
            month_num=None, month_label=None,
            base_start=None, base_end=None,
            show_50=False, show_last2=False,
        )

    # — Linha 1 —
    r1c1, r1c2, r1c3, r1c4 = st.columns([1,1,1,1])
    with r1c1:
        start = st.date_input("Início", default_start,
                              min_value=date(1940,1,1), max_value=default_end,
                              key=f"{key_prefix}_start")
    with r1c2:
        end = st.date_input("Fim", default_end,
                            min_value=date(1940,1,1), max_value=default_end,
                            key=f"{key_prefix}_end")
        if start > end:
            st.error("Início posterior ao fim.")
            st.stop()
    with r1c3:
        month_label = st.selectbox("Mês", _MONTHS, index=0, key=f"{key_prefix}_month")
        month_num = None if month_label == "Todos" else _MONTHS.index(month_label)
    with r1c4:
        base_opt = st.selectbox("Normais", ["1991–2020", "1961–1990", "Custom"],
                                index=0, key=f"{key_prefix}_norm")
        if base_opt == "1991–2020":
            base_start, base_end = 1991, 2020
        elif base_opt == "1961–1990":
            base_start, base_end = 1961, 1990
        else:
            base_start, base_end = st.slider(
                "Período base", 1940, end.year, (1981, 2010),
                label_visibility="collapsed",
                key=f"{key_prefix}_norm_custom"
            )

    # — Linha 2 —
    # Local (à esquerda) e pill com label completo (à direita)
    r2c1, r2c2 = st.columns([1,1])
    with r2c1:
        q = st.text_input("Local", value=default_place,
                          placeholder="Cidade ou localidade",
                          key=f"{key_prefix}_q")
    with r2c2:
        if place_full_label:
            st.markdown(f"<div class='pill' title='{place_full_label}'>{place_full_label}</div>",
                        unsafe_allow_html=True)
        else:
            st.markdown("<div style='height:36px'></div>", unsafe_allow_html=True)

    # — Linha 3 —
    r3c1, r3c2 = st.columns([1,1])
    with r3c1:
        show_50 = st.checkbox("Destacar 'há 50 anos'", value=True, key=f"{key_prefix}_h50")
    with r3c2:
        st.markdown("<div class='right-align'>", unsafe_allow_html=True)
        show_last2 = st.checkbox("Média últimos 2 anos", value=True, key=f"{key_prefix}_last2")
        st.markdown("</div>", unsafe_allow_html=True)

    return dict(
        query=q, start=start, end=end,
        month_num=month_num, month_label=month_label,
        base_start=base_start, base_end=base_end,
        show_50=show_50, show_last2=show_last2,
    )
