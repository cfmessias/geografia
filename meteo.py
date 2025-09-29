# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date
import traceback
import streamlit as st
import pandas as pd

# utils
from utils.timing import timed
from utils.profiler import cprofile_block  # opcional; só é usado se o checkbox estiver ativo

# -----------------------------------------------------------------------------
# DATA-ONLY HELPERS (podem ser cacheados; não usam st.* nem widgets)
# -----------------------------------------------------------------------------

@st.cache_data(ttl=24 * 3600, show_spinner=False)
def _geocode_cached(query: str) -> pd.DataFrame:
    """Procura locais (geocoding). Sem UI. Cache 24h."""
    from services.open_meteo import geocode
    return geocode(query)

@st.cache_data(ttl=3600, show_spinner=True)
def _fetch_daily_cached(lat: float, lon: float, tz: str, start: date, end: date) -> pd.DataFrame:
    """Descarga diários. Sem UI. Cache 1h."""
    from services.open_meteo import fetch_daily
    return fetch_daily(float(lat), float(lon), str(tz), start, end)

def _prep_monthly_no_ui(
    lat: float, lon: float, tz: str,
    start: date, end: date,
    month_num: int, base_start: date, base_end: date,
) -> dict | None:
    """
    Prepara agregados mensais + normais + valores de referência.
    Não usa widgets nem Streamlit → segura para ser chamada dentro de cache/profilers.
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
        if "t_norm" in dfm.columns:
            dfm["t_anom"] = dfm["t_mean"] - dfm["t_norm"]
        else:
            dfm["t_anom"] = pd.NA
        if "p_norm" in dfm.columns:
            dfm["p_anom"] = dfm["precip"] - dfm["p_norm"]
        else:
            dfm["p_anom"] = pd.NA
    else:
        dfm["t_norm"] = pd.NA; dfm["p_norm"] = pd.NA
        dfm["t_anom"] = pd.NA; dfm["p_anom"] = pd.NA

    view_df = dfm if not month_num else dfm[dfm["month"] == month_num]
    ref_year    = max(start.year, end.year - 50)
    last2_years = [end.year, end.year - 1]
    m = month_num or end.month

    def _safe(v):
        try:
            return float(v) if v is not None else None
        except Exception:
            return None

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

# -----------------------------------------------------------------------------
# UI HELPERS (podem usar widgets; NÃO usar cache aqui)
# -----------------------------------------------------------------------------

def _pick_place_ui(query: str, key_prefix: str):
    """Mostra selectbox para escolher um local a partir do geocoding cacheado."""
    try:
        places = _geocode_cached(query)
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

# -----------------------------------------------------------------------------
# RENDER PRINCIPAL
# -----------------------------------------------------------------------------

def render_meteo(embed: bool = True, key_prefix: str = "meteo", show_title: bool = True) -> None:
    """
    Desenha a UI completa de Meteorologia.
    - embed=True: para correr dentro de outra app/tab.
    - key_prefix: prefixo para widgets.
    """
    from services.open_meteo import YESTERDAY

    # Estilo mínimo
    st.markdown(
        """
        <style>
          .stSelectbox, .stDateInput, .stTextInput { font-size: 0.9rem; }
          div[data-baseweb="select"] > div { min-height: 34px; }
          .stDateInput input { height: 34px; padding: 2px 8px; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if show_title:
        st.title("☁️ Meteorologia (ERA5)")

    # Filtros (com botão para evitar reruns a cada alteração)
    try:
        from views.filters import render_filters
    except Exception:
        st.error("Falha a carregar o módulo de filtros.")
        st.exception(traceback.format_exc())
        return

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
        submitted = st.form_submit_button("Atualizar")

    # manter últimos filtros se não clicares
    if not submitted and f"{key_prefix}_lastflt" in st.session_state:
        flt = st.session_state[f"{key_prefix}_lastflt"]
    else:
        st.session_state[f"{key_prefix}_lastflt"] = flt

    # Extração robusta dos valores dos filtros
    def _as_int(v, default: int) -> int:
        try:
            return int(v)
        except Exception:
            return default

    q           = str(flt.get("query", "")).strip()
    start       = flt.get("start");  end = flt.get("end")
    month_num   = _as_int(flt.get("month_num"), default=(end.month if hasattr(end, "month") else date.today().month))
    month_label = flt.get("month_label") or ""
    base_start  = flt.get("base_start") or start
    base_end    = flt.get("base_end") or end
    show_50     = bool(flt.get("show_50", False))
    show_last2  = bool(flt.get("show_last2", False))

    # Geocoding / escolha do local (UI; sem cache para não dar CachedWidgetWarning)
    with timed("Meteo · geocoding"):
        lat, lon, tz, label = _pick_place_ui(q, key_prefix=f"{key_prefix}_geo")
    if label:
        st.session_state[f"{key_prefix}_place_label"] = label

    if lat is None or lon is None:
        st.info("Escolhe um local válido.")
        return

    st.caption(f"Local: **{label}** • Lat/Lon: {lat:.4f}, {lon:.4f} • Fuso: {tz}")

    # Tabs principais (cálculos pesados só quando necessário)
    tab_fc, tab_hist, tab_eq, tab_ind, tab_cs = st.tabs(
        ["🌦️ Previsão", "📚 Histórico", "🌍 Sismicidade", "🧭 Indicadores", "📅 Cenários 2100"]
    )

    
    # --- PREVISÃO ---
    with tab_fc:
        try:
            from views.forecast import render_forecast_tab as _rft
            with timed("Meteo · previsão"):
                import inspect

                sig = inspect.signature(_rft)
                params = sig.parameters

                # tenta chamar por keywords que a função realmente aceita
                candidates = {
                    "lat": lat, "lon": lon,
                    "latitude": lat, "longitude": lon,
                    "tz": tz, "timezone": tz,
                    "place_label": label, "label": label,
                    "key_prefix": f"{key_prefix}_fc",
                }
                kwargs = {k: v for k, v in candidates.items() if k in params}

                if kwargs:
                    _rft(**kwargs)
                else:
                    # fallback por posição: [lat, lon, tz, label, key_prefix]
                    ordered_args = [lat, lon, tz, label, f"{key_prefix}_fc"]
                    _rft(*ordered_args[:len(params)])
        except Exception:
            st.error("Falha a carregar a view de previsão.")
            st.exception(traceback.format_exc())

    # HISTÓRICO (onde costuma estar o custo pesado)
    with tab_hist:
        st.checkbox("🔬 Profiling detalhado do processamento mensal", key=f"{key_prefix}_profile", value=False)
        do_prof = st.session_state.get(f"{key_prefix}_profile", False)

        with timed("Meteo · preparar mensal"):
            if do_prof:
                with cprofile_block(
                    "prep mensal (top 30)", sort="cumtime",
                    include=("meteo.py", "services/open_meteo", "utils/transform"),
                    top=30,
                ):
                    prep = _prep_monthly_no_ui(lat, lon, tz, start, end, month_num, base_start, base_end)
            else:
                prep = _prep_monthly_no_ui(lat, lon, tz, start, end, month_num, base_start, base_end)

        if not prep:
            st.info("Sem dados para o período selecionado.")
        else:
            dfm = prep["dfm"]; view_df = prep["view_df"]
            ref_year = prep["ref_year"]; last2_years = prep["last2_years"]
            t_50 = prep["t_50"]; p_50 = prep["p_50"]
            t_last2 = prep["t_last2"]; p_last2 = prep["p_last2"]

            sub_t, sub_p, sub_cmp = st.tabs(["🌡️ Temperatura", "🌧️ Precipitação", "📊 Comparação"])

            with sub_t:
                try:
                    from views.temperature import render_temperature_tab
                    with timed("Meteo · tabs · temperatura"):
                        render_temperature_tab(view_df, month_num, month_label,
                                               ref_year, last2_years, t_50, t_last2, show_50, show_last2)
                except Exception:
                    st.error("Falha a carregar a view de temperatura.")
                    st.exception(traceback.format_exc())

            with sub_p:
                try:
                    from views.precipitation import render_precipitation_tab
                    with timed("Meteo · tabs · precipitação"):
                        render_precipitation_tab(view_df, month_num, month_label,
                                                 ref_year, last2_years, p_50, p_last2, show_50, show_last2)
                except Exception:
                    st.error("Falha a carregar a view de precipitação.")
                    st.exception(traceback.format_exc())

            with sub_cmp:
                try:
                    from views.comparison import render_comparison_tab
                    with timed("Meteo · tabs · comparação"):
                        render_comparison_tab(dfm)
                except Exception:
                    st.error("Falha a carregar a view de comparação.")
                    st.exception(traceback.format_exc())

    # SISMICIDADE
    with tab_eq:
        try:
            from views.seismicity import render_seismicity_tab
            with timed("Meteo · sismicidade"):
                render_seismicity_tab(lat, lon,
                                      start=date.today().replace(year=date.today().year - 10),
                                      end=date.today(), key_prefix=f"{key_prefix}_eq")
        except Exception:
            st.error("Falha a carregar a view de sismicidade.")
            st.exception(traceback.format_exc())

    # INDICADORES
    # ───────────────────── INDICADORES ─────────────────────
    with tab_ind:
        st.subheader("🧭 Indicadores climáticos")

        # não calcular por defeito: o utilizador decide quando carregar
        load_ind = st.toggle("Carregar indicadores", value=False,
                            key=f"{key_prefix}_load_ind")

        if not load_ind:
            st.caption("Carregamento adiado para acelerar o arranque desta página.")
        else:
            # opcional: profiling detalhado (top 30 funções por tempo cumulativo)
            do_prof = st.toggle("🔬 Profiling detalhado", value=False,
                                key=f"{key_prefix}_ind_prof")

            try:
                from views.climate_indicators import render_climate_indicators_tab
            except Exception:
                st.error("Falha a carregar indicadores.")
                st.exception(traceback.format_exc())
            else:
                with timed("Meteo · indicadores"):
                    if do_prof:
                        from utils.profiler import cprofile_block
                        with cprofile_block(
                            "indicadores (top 30)",
                            sort="cumtime",
                            include=("views/climate_indicators", "services", "pandas"),
                            top=30,
                        ):
                            render_climate_indicators_tab()
                    else:
                        render_climate_indicators_tab()

    # CENÁRIOS
    with tab_cs:
        try:
            from views.climate_scenarios import render_climate_tab
            with timed("Meteo · cenários"):
                render_climate_tab()
        except Exception:
            st.error("Falha a carregar cenários climáticos.")
            st.exception(traceback.format_exc())


# Suporte a execução standalone deste módulo (opcional)
def _standalone():
    st.set_page_config(page_title="Meteorologia", layout="wide")
    render_meteo(embed=False, key_prefix="meteo", show_title=True)

if __name__ == "__main__":
    _standalone()
