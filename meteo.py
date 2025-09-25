# meteo/app.py
# -*- coding: utf-8 -*-
from datetime import date
import numpy as np
import pandas as pd
import streamlit as st
import traceback, datetime as dt, os

from services.open_meteo import geocode, fetch_daily, YESTERDAY
from utils.transform import monthly, normals, pick_value_for
from views.filters import render_filters
from views.temperature import render_temperature_tab
from views.precipitation import render_precipitation_tab
from views.comparison import render_comparison_tab
from views.seismicity import render_seismicity_tab
from views.forecast import render_forecast_tab
from views.climate_scenarios import render_climate_tab
from views.climate_indicators import render_climate_indicators_tab


# --------- helpers (iguais aos teus) ----------
def _pick_place(query: str, key_prefix: str):
    try:
        places = geocode(query)
    except Exception as e:
        st.error(f"Falha a geocodificar '{query}': {e}")
        return None, None, None, None
    if places is None or places.empty:
        st.warning("Nenhum local encontrado.")
        return None, None, None, None

    idx = st.selectbox(
        "Escolher local",
        options=places.index,
        format_func=lambda i: places.loc[i, "label"],
        label_visibility="collapsed",
        key=f"{key_prefix}_place_sel",
    )
    row = places.loc[idx]
    lat = float(row["latitude"]); lon = float(row["longitude"])
    tz = row.get("timezone", "auto")
    label = row.get("label", f"{lat:.4f},{lon:.4f}")
    return lat, lon, tz, label


def _prep_monthly(lat, lon, tz, start, end, month_num, base_start, base_end):
    try:
        df = fetch_daily(lat, lon, tz, start, end)
    except Exception as e:
        st.error(f"Falhou a descarga de diários: {e}")
        return None
    if df is None or df.empty:
        return None

    dfm = monthly(df)
    if dfm is None or dfm.empty:
        return None

    norm = normals(dfm, base_start, base_end)
    if norm is not None and not norm.empty:
        dfm = dfm.merge(norm, on="month", how="left")
        dfm["t_anom"] = (dfm["t_mean"] - dfm["t_norm"]) if "t_norm" in dfm.columns else pd.NA
        dfm["p_anom"] = (dfm["precip"] - dfm["p_norm"]) if "p_norm" in dfm.columns else pd.NA
    else:
        dfm["t_norm"] = pd.NA; dfm["p_norm"] = pd.NA
        dfm["t_anom"] = pd.NA; dfm["p_anom"] = pd.NA

    view_df = dfm if not month_num else dfm[dfm["month"] == month_num]
    ref_year    = max(start.year, end.year - 50)
    last2_years = [end.year, end.year - 1]
    m = month_num or end.month

    def _safe(v):
        try: return float(v) if v is not None else None
        except Exception: return None

    t_50 = _safe(pick_value_for(dfm, m, ref_year, "t_mean"))
    p_50 = _safe(pick_value_for(dfm, m, ref_year, "precip"))

    t_last2 = view_df[(view_df["month"] == m) & (view_df["year"].isin(last2_years))]["t_mean"].mean()
    p_last2 = view_df[(view_df["month"] == m) & (view_df["year"].isin(last2_years))]["precip"].mean()

    return dict(
        dfm=dfm, view_df=view_df,
        ref_year=ref_year, last2_years=last2_years,
        t_50=t_50, p_50=p_50,
        t_last2=(None if pd.isna(t_last2) else float(t_last2)),
        p_last2=(None if pd.isna(p_last2) else float(p_last2)),
    )
# ---------------------------------------------------------------


def _safe_render(fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except Exception:
        tb = traceback.format_exc()
        st.error("Houve um erro a renderizar esta secção.")
        with st.expander("Detalhes técnicos"):
            st.code(tb, language="text")
        os.makedirs(".cache", exist_ok=True)
        with open(".cache/last_error.log", "a", encoding="utf-8") as f:
            f.write(f"\n--- {dt.datetime.utcnow()}Z ---\n{tb}\n")


def render_meteo(embed: bool = True, key_prefix: str = "meteo", show_title: bool = True) -> None:
    """
    Desenha a UI completa de Meteorologia.
    - embed=True: para correr dentro de outra app/tab (não faz set_page_config).
    - key_prefix: prefixo para todos os widgets, evitando colisões.
    """
    # Estilo mínimo
    st.markdown("""
    <style>
      .stSelectbox, .stDateInput, .stTextInput { font-size: 0.9rem; }
      div[data-baseweb="select"] > div { min-height: 34px; }
      .stDateInput input { height: 34px; padding: 2px 8px; }
    </style>
    """, unsafe_allow_html=True)

    if show_title:
        st.title("☁️ Metereologia (ERA5)")

    # Tabs principais
    tab_fc, tab_hist, tab_eq, tab_ind, tab_cs = st.tabs(
        ["🌦️ Previsão", "📚 Histórico", "🌍 Sismicidade", "🧭 Indicadores", "📅 Cenários 2100"]
    )

    # PREVISÃO
    with tab_fc:
        flt = render_filters(mode="place_only", key_prefix=f"{key_prefix}_fc", default_place="Lisboa")
        q_fc = flt["query"]
        lat, lon, tz, label = _pick_place(q_fc, key_prefix=f"{key_prefix}_fc")
        if lat is not None:
            _safe_render(render_forecast_tab)  # a tua view trata do resto

    # HISTÓRICO
    with tab_hist:
        flt_h = render_filters(
            mode="full",
            key_prefix=f"{key_prefix}_hist",
            default_place="Lisboa",
            default_start=date(YESTERDAY.year - 10, 1, 1),
            default_end=YESTERDAY,
            place_full_label=st.session_state.get(f"{key_prefix}_hist_place_label"),
        )
        q = flt_h["query"]; start = flt_h["start"]; end = flt_h["end"]
        month_num = flt_h["month_num"]; month_label = flt_h["month_label"]
        base_start = flt_h["base_start"]; base_end = flt_h["base_end"]
        show_50 = flt_h["show_50"]; show_last2 = flt_h["show_last2"]

        lat, lon, tz, label = _pick_place(q, key_prefix=f"{key_prefix}_hist")
        if label:
            st.session_state[f"{key_prefix}_hist_place_label"] = label

        if lat is not None:
            prep = _prep_monthly(lat, lon, tz, start, end, month_num, base_start, base_end)
            if not prep:
                st.info("Sem dados para o período selecionado.")
            else:
                dfm = prep["dfm"]; view_df = prep["view_df"]
                ref_year = prep["ref_year"]; last2_years = prep["last2_years"]
                t_50 = prep["t_50"]; p_50 = prep["p_50"]
                t_last2 = prep["t_last2"]; p_last2 = prep["p_last2"]

                st.caption(f"Local: **{label}** • Lat/Lon: {lat:.4f}, {lon:.4f} • Fuso: {tz}")

                sub_t, sub_p, sub_cmp = st.tabs(["🌡️ Temperatura", "🌧️ Precipitação", "📊 Comparação"])

                with sub_t:
                    _safe_render(
                        render_temperature_tab,
                        view_df, month_num, month_label,
                        ref_year, last2_years, t_50, t_last2,
                        show_50, show_last2
                    )

                with sub_p:
                    _safe_render(
                        render_precipitation_tab,
                        view_df, month_num, month_label,
                        ref_year, last2_years, p_50, p_last2,
                        show_50, show_last2
                    )

                with sub_cmp:
                    _safe_render(render_comparison_tab, dfm)

    # SISMICIDADE
    with tab_eq:
        flt_eq = render_filters(mode="place_only", key_prefix=f"{key_prefix}_eq", default_place="Lisboa")
        q_eq = flt_eq["query"]
        lat, lon, tz, label = _pick_place(q_eq, key_prefix=f"{key_prefix}_eq")
        with st.expander("Período (opcional)", expanded=False):
            s_eq = st.date_input("Início", date.today().replace(year=date.today().year - 10), key=f"{key_prefix}_eq_s")
            e_eq = st.date_input("Fim",    date.today(),                                      key=f"{key_prefix}_eq_e")
        if lat is not None:
            _safe_render(render_seismicity_tab, lat, lon, s_eq, e_eq)

    # INDICADORES
    with tab_ind:
        _safe_render(render_climate_indicators_tab)

    # CENÁRIOS
    with tab_cs:
        _safe_render(render_climate_tab)


# Suporte a execução standalone do módulo meteo/app.py
def _standalone():
    st.set_page_config(page_title="Metereologia", layout="wide")
    render_meteo(embed=False, key_prefix="meteo", show_title=True)

if __name__ == "__main__":
    _standalone()
