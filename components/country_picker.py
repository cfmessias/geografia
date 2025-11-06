# components/country_picker.py
from __future__ import annotations
import streamlit as st
import pandas as pd
from services.i18n import t as tr

def country_selector(
    countries_df: pd.DataFrame,
    *,
    key_prefix: str = "paises"
) -> tuple[str | None, str | None]:
    """
    Pesquisa + select numa só linha. Sem botão: ao mudar o select, on_change atualiza o país.
    Retorna (label, iso3) atuais ou (None, None).
    """
    lang = st.session_state.get("lang", "pt").lower()

    df = countries_df.copy()
    df["iso3u"] = df["iso3"].astype(str).str.upper().str.strip()

    if lang == "pt" and "name_pt" in df.columns:
        df["label"] = df["name_pt"].astype(str)
    elif lang != "pt" and "name_en" in df.columns:
        df["label"] = df["name_en"].astype(str)
    elif "name" in df.columns:
        df["label"] = df["name"].astype(str)
    else:
        df["label"] = df["iso3u"]

    label_by_iso = dict(zip(df["iso3u"], df["label"]))
    iso_by_label = {v: k for k, v in label_by_iso.items()}

    placeholder = tr("labels.selecione_um_pais")

    # ---- layout numa linha
    c1, c2 = st.columns([3, 7], gap="small")

    # Pesquisa (sem form)
    q_key = f"{key_prefix}_search"
    with c1:
        q_value = st.text_input(
            tr("paises.pesquisar_nome_contem"),
            value=st.session_state.get(q_key, ""),
            placeholder=tr("paises.placeholder_pesquisa"),
            key=q_key,
            label_visibility="collapsed",
        )

    # Opções filtradas
    opts = df[df["label"].str.contains(q_value, case=False, na=False)] if q_value else df
    labels = [placeholder] + opts["label"].tolist()

    # Valor atual
    cur_iso   = st.session_state.get("paises_iso3")
    cur_label = label_by_iso.get(str(cur_iso).upper()) if cur_iso else None
    idx = labels.index(cur_label) if cur_label in labels else 0

    sel_key = f"{key_prefix}_country_select"

    # Callback para aplicar seleção (sem st.rerun())
    def _on_country_change(mapping: dict, placeholder_text: str, key_sel: str):
        chosen_label = st.session_state.get(key_sel)
        if not chosen_label or chosen_label == placeholder_text:
            return
        chosen_iso = mapping.get(chosen_label)
        if not chosen_iso:
            return
        # Guardamos apenas em chaves *diferentes* da key do widget
        st.session_state["pais_selected"] = chosen_label
        st.session_state["paises_iso3"]   = chosen_iso
        # Nada de st.rerun(): o Streamlit já refaz a app após o callback

    with c2:
        st.selectbox(
            tr("labels.pais"),
            options=labels,
            index=idx,
            key=sel_key,
            label_visibility="collapsed",
            on_change=_on_country_change,
            kwargs={"mapping": iso_by_label, "placeholder_text": placeholder, "key_sel": sel_key},
        )

    # Devolve seleção corrente sem modificar o estado do widget
    chosen_label = st.session_state.get(sel_key)
    if not chosen_label or chosen_label == placeholder:
        if cur_iso:
            return (label_by_iso.get(str(cur_iso).upper()), cur_iso)
        return (None, None)

    return chosen_label, iso_by_label.get(chosen_label)
