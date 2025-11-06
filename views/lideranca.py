from __future__ import annotations
import pandas as pd
import streamlit as st
from services.i18n import t as tr
from services import offline_store as store  # leaders_for_iso3

def render_leadership_block(iso3: str) -> None:
    cur_df, hist_df = store.leaders_for_iso3(iso3)
    base = hist_df if (hist_df is not None and not hist_df.empty) else cur_df

    with st.expander(tr("labels.lideranca_atual_e_historica"), expanded=False):
        if base is None or base.empty:
            st.caption(tr("paises.label"))
            return

        h = base.copy()

        role_map = {
            "head_of_state":      tr("labels.presidente"),
            "head_of_government": tr("labels.chefe_de_governo"),
        }
        h["role_disp"] = h.get("role").map(role_map).fillna(h.get("role"))

        h["__start_dt"] = pd.to_datetime(h.get("start"), errors="coerce")
        h["__end_dt"]   = pd.to_datetime(h.get("end"),   errors="coerce")
        h["start_fmt"]  = h["__start_dt"].dt.strftime("%Y-%m-%d").fillna("")
        h["end_fmt"]    = h["__end_dt"].dt.strftime("%Y-%m-%d").fillna("")

        # <- correção aqui (usar .str.strip())
        h["party_norm"]     = h.get("party").fillna("").astype(str).str.strip()
        h["end_cause_norm"] = h.get("end_cause").fillna("").astype(str).str.strip()

        COL_PERSON   = tr("cols.person")
        COL_PARTY    = tr("cols.party")
        COL_START    = tr("cols.start")
        COL_END      = tr("cols.end")
        COL_ENDCAUSE = tr("cols.end_cause")

        def _prep(df: pd.DataFrame) -> pd.DataFrame:
            if df is None or df.empty:
                return pd.DataFrame(columns=[COL_PERSON, COL_PARTY, COL_START, COL_END, COL_ENDCAUSE])
            out = pd.DataFrame({
                COL_PERSON:   df.get("person"),
                COL_PARTY:    h.loc[df.index, "party_norm"],
                COL_START:    h.loc[df.index, "start_fmt"],
                COL_END:      h.loc[df.index, "end_fmt"],
                COL_ENDCAUSE: h.loc[df.index, "end_cause_norm"],
            })
            return (out.assign(__ord=h.loc[out.index, "__start_dt"])
                        .sort_values(["__ord"], ascending=[False])
                        .drop(columns="__ord"))

        pres = _prep(h[h.get("role") == "head_of_state"])
        gov  = _prep(h[h.get("role") == "head_of_government"])

        c1, c2 = st.columns(2)
        with c1:
            st.markdown(tr("labels.presidentes"))
            st.dataframe(pres, use_container_width=True, hide_index=True)
        with c2:
            st.markdown(tr("labels.chefes_de_governo"))
            st.dataframe(gov, use_container_width=True, hide_index=True)
