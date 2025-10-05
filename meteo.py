# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date
import traceback
import pandas as pd
import streamlit as st

# Utils / i18n
from utils.timing import timed
from services.i18n import t as tr
try:
    from services.i18n_boot import _ensure_lang_state
except ImportError:
    from services.i18n_boot import init_i18n_state as _ensure_lang_state


# ─────────────────────────────────────────────────────────────────────────────
# DATA HELPERS (sem UI → seguros para cache)
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=24 * 3600, show_spinner=False)
def _geocode_cached(query: str) -> pd.DataFrame:
    """Procura locais (geocoding). Cache 24h."""
    from services.open_meteo import geocode
    return geocode(query)

@st.cache_data(ttl=3600, show_spinner=True)
def _fetch_daily_cached(lat: float, lon: float, tz: str, start: date, end: date) -> pd.DataFrame:
    """Descarga diários. Cache 1h."""
    from services.open_meteo import fetch_daily
    return fetch_daily(float(lat), float(lon), str(tz), start, end)

def _prep_monthly_no_ui(
    lat: float, lon: float, tz: str,
    start: date, end: date,
    month_num: int | None, base_start: date, base_end: date,
) -> dict | None:
    """
    Prepara agregados mensais + normais + valores de referência.
    (não cria widgets → seguro para cache/perf)
    """
    from utils.transform import monthly, normals, pick_value_for

    df = _fetch_daily_cached(lat, lon, tz, start, end)
    if df is None or df.empty:
        return None

    dfm = monthly(df)
    if dfm is None or dfm.empty:
        return None

    norm = normals(dfm, base_start, base_end)
    if norm is not None and not norm.empty:
        dfm = dfm.merge(norm, on="month", how="left")
        dfm["t_anom"] = (dfm["t_mean"] - dfm["t_norm"]) if "t_norm" in dfm else pd.NA
        dfm["p_anom"] = (dfm["precip"] - dfm["p_norm"]) if "p_norm" in dfm else pd.NA
    else:
        dfm["t_norm"] = pd.NA; dfm["p_norm"] = pd.NA
        dfm["t_anom"] = pd.NA; dfm["p_anom"] = pd.NA

    view_df = dfm if (month_num is None) else dfm[dfm["month"] == month_num]
    ref_year    = max(start.year, end.year - 50)
    last2_years = [end.year, end.year - 1]
    m = (month_num or end.month)

    def _safe(v):
        try: return float(v) if v is not None else None
        except Exception: return None

    t_50   = _safe(pick_value_for(dfm, m, ref_year, "t_mean"))
    p_50   = _safe(pick_value_for(dfm, m, ref_year, "precip"))
    t_last2 = view_df[(view_df["month"] == m) & (view_df["year"].isin(last2_years))]["t_mean"].mean()
    p_last2 = view_df[(view_df["month"] == m) & (view_df["year"].isin(last2_years))]["precip"].mean()

    return dict(
        dfm=dfm, view_df=view_df,
        ref_year=ref_year, last2_years=last2_years,
        t_50=t_50, p_50=p_50,
        t_last2=(None if pd.isna(t_last2) else float(t_last2)),
        p_last2=(None if pd.isna(p_last2) else float(p_last2)),
    )


# ─────────────────────────────────────────────────────────────────────────────
# UI HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _pick_place_ui(query: str, key_prefix: str):
    """Selectbox para escolher um local (geocoding com cache)."""
    try:
        places = _geocode_cached(query)
    except Exception as e:
        st.error(tr("meteo.falha_geocodificar_q", q=query, error=str(e)))
        return None, None, None, None

    if places is None or places.empty:
        st.warning(tr("meteo.nenhum_local_encontrado"))
        return None, None, None, None

    idx = st.selectbox(
        tr("labels.escolher_local"),
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


# ─────────────────────────────────────────────────────────────────────────────
# RENDER PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def render_meteo(embed: bool = True, key_prefix: str = "meteo", show_title: bool = True) -> None:
    _ensure_lang_state()
    from services.open_meteo import YESTERDAY

    # Título
    if show_title:
        st.header("🌥️ " + tr("meteorologia_era5"))

    # Tabs logo abaixo do título (opcional: sticky quando faz scroll)
    st.markdown("""
    <style>
    div[data-baseweb="tab-list"]{
      position: sticky; top: 56px; z-index: 10;
      background: var(--background-color, #0e1117);
      padding-top: .25rem;
    }
    </style>""", unsafe_allow_html=True)

    tabs = st.tabs([
        "🌧️ " + tr("tabs.forecast"),
        "📜 " + tr("tabs.history"),
        "🌐 " + tr("tabs.seismicity"),
        "🧭 " + tr("tabs.indicators"),
        "📊 " + tr("tabs.scenarios2100"),
    ])

    # Helper: filtros partilhados (renderiza dentro da tab ativa)
    def _render_shared_filters():
        from views.filters import render_filters
        with st.form(f"{key_prefix}_filters_form"):
            with timed("Meteo · filtros"):
                flt = render_filters(
                    mode="full",
                    key_prefix=f"{key_prefix}_flt",
                    default_place="Lisboa",
                    default_start=date(YESTERDAY.year - 10, 1, 1),
                    default_end=YESTERDAY,
                    place_full_label=st.session_state.get(f"{key_prefix}_place_label"),
                )
            submitted = st.form_submit_button(tr("meteo.atualizar"))
        if submitted or f"{key_prefix}_lastflt" not in st.session_state:
            st.session_state[f"{key_prefix}_lastflt"] = flt
        return st.session_state[f"{key_prefix}_lastflt"]

    # ── TAB 0: PREVISÃO ──────────────────────────────────────────────────────
    with tabs[0]:
        flt = _render_shared_filters()

        q           = str(flt.get("query", "")).strip()
        start       = flt.get("start");  end = flt.get("end")
        month_num   = flt.get("month_num")                      # None ou 1..12
        month_label = flt.get("month_label") or ""
        base_start  = flt.get("base_start") or start
        base_end    = flt.get("base_end") or end
        show_50     = bool(flt.get("show_50", False))
        show_last2  = bool(flt.get("show_last2", False))

        with timed("Meteo · geocoding"):
            lat, lon, tz, label = _pick_place_ui(q, key_prefix=f"{key_prefix}_geo")
        if label:
            st.session_state[f"{key_prefix}_place_label"] = label

        if lat is None or lon is None:
            st.info(tr("meteo.escolhe_um_local_valido"))
        else:
            st.caption(tr("labels.local_label_lat_lon_lat_4f_lon_4f_fuso_tz",
                          label=label, lat=lat, lon=lon, tz=tz))
            try:
                from views.forecast import render_forecast_tab as _rft
                import inspect
                sig = inspect.signature(_rft); params = sig.parameters
                candidates = dict(
                    lat=lat, lon=lon, latitude=lat, longitude=lon,
                    tz=tz, timezone=tz, place_label=label, label=label,
                    key_prefix=f"{key_prefix}_fc"
                )
                kwargs = {k: v for k, v in candidates.items() if k in params}
                if kwargs: _rft(**kwargs)
                else:
                    ordered_args = [lat, lon, tz, label, f"{key_prefix}_fc"]
                    _rft(*ordered_args[:len(params)]) 
            except Exception:
                st.error(tr("meteo.falha_view_previsao"))
                st.exception(traceback.format_exc())

    # ── TAB 1: HISTÓRICO ─────────────────────────────────────────────────────
    with tabs[1]:
        flt = st.session_state.get(f"{key_prefix}_lastflt", {})
        if not flt:
            st.info(tr("meteo.escolhe_um_local_valido"))
        else:
            q           = str(flt.get("query", "")).strip()
            start       = flt.get("start");  end = flt.get("end")
            month_num   = flt.get("month_num")
            month_label = flt.get("month_label") or ""
            base_start  = flt.get("base_start") or start
            base_end    = flt.get("base_end") or end
            show_50     = bool(flt.get("show_50", False))
            show_last2  = bool(flt.get("show_last2", False))

            with timed("Meteo · geocoding"):
                lat, lon, tz, label = _pick_place_ui(q, key_prefix=f"{key_prefix}_geo_hist")
            if (lat is None) or (lon is None):
                st.info(tr("meteo.escolhe_um_local_valido"))
            else:
                st.caption(tr("labels.local_label_lat_lon_lat_4f_lon_4f_fuso_tz",
                              label=label, lat=lat, lon=lon, tz=tz))

                with timed("Meteo · preparar mensal"):
                    prep = _prep_monthly_no_ui(lat, lon, tz, start, end, month_num, base_start, base_end)

                if not prep:
                    st.info(tr("meteo.sem_dados_periodo"))
                else:
                    dfm = prep["dfm"]; view_df = prep["view_df"]
                    ref_year = prep["ref_year"]; last2_years = prep["last2_years"]
                    t_50 = prep["t_50"]; p_50 = prep["p_50"]
                    t_last2 = prep["t_last2"]; p_last2 = prep["p_last2"]

                    sub_t, sub_p, sub_cmp = st.tabs([
                        tr("app.tabs.temperatura"), tr("app.tabs.precipita_o"), tr("app.tabs.compara_o")
                    ])

                    with sub_t:
                        try:
                            from views.temperature import render_temperature_tab
                            with timed("Meteo · tabs · temperatura"):
                                render_temperature_tab(view_df, month_num, month_label,
                                                       ref_year, last2_years, t_50, t_last2,
                                                       show_50, show_last2)
                        except Exception:
                            st.error(tr("meteo.falha_view_temperatura"))
                            st.exception(traceback.format_exc())

                    with sub_p:
                        try:
                            from views.precipitation import render_precipitation_tab
                            with timed("Meteo · tabs · precipitação"):
                                render_precipitation_tab(view_df, month_num, month_label,
                                                         ref_year, last2_years, p_50, p_last2,
                                                         show_50, show_last2)
                        except Exception:
                            st.error(tr("meteo.falha_view_precipitacao"))
                            st.exception(traceback.format_exc())

                    with sub_cmp:
                        try:
                            from views.comparison import render_comparison_tab
                            with timed("Meteo · tabs · comparação"):
                                render_comparison_tab(dfm)
                        except Exception:
                            st.error(tr("meteo.falha_view_comparacao"))
                            st.exception(traceback.format_exc())

    # ── TAB 2: SISMICIDADE ───────────────────────────────────────────────────
    with tabs[2]:
        flt = st.session_state.get(f"{key_prefix}_lastflt", {})
        q = str(flt.get("query", "")).strip() if flt else ""
        with timed("Meteo · geocoding"):
            lat, lon, tz, label = _pick_place_ui(q, key_prefix=f"{key_prefix}_geo_eq")
        if lat and lon:
            try:
                from views.seismicity import render_seismicity_tab
                with timed("Meteo · sismicidade"):
                    render_seismicity_tab(
                        lat, lon,
                        start=date.today().replace(year=date.today().year - 10),
                        end=date.today(),
                        key_prefix=f"{key_prefix}_eq"
                    )
            except Exception:
                st.error(tr("meteo.falha_view_sismicidade"))
                st.exception(traceback.format_exc())

    # ── TAB 3: INDICADORES ───────────────────────────────────────────────────
    with tabs[3]:
        st.subheader(tr("labels.indicadores_clim_ticos"))
        load_ind = st.toggle(tr("labels.carregar_indicadores"), value=False,
                             key=f"{key_prefix}_load_ind")
        if not load_ind:
            st.caption(tr("labels.carregamento_adiado_para_acelerar_o_arranque_desta_p_gina"))
        else:
            try:
                from views.climate_indicators import render_climate_indicators_tab
                with timed("Meteo · indicadores"):
                    render_climate_indicators_tab()
            except Exception:
                st.error(tr("meteo.falha_carregar_indicadores"))
                st.exception(traceback.format_exc())

    # ── TAB 4: CENÁRIOS ──────────────────────────────────────────────────────
    with tabs[4]:
        try:
            from views.climate_scenarios import render_climate_tab
            with timed("Meteo · cenários"):
                render_climate_tab()
        except Exception:
            st.error(tr("meteo.falha_view_cenarios"))
            st.exception(traceback.format_exc())


# Execução standalone opcional
def _standalone():
    st.set_page_config(page_title=tr("meteorologia_era5"), layout="wide")
    render_meteo(embed=False, key_prefix="meteo", show_title=True)

if __name__ == "__main__":
    _standalone()
