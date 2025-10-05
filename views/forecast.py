# -*- coding: utf-8 -*-
"""
Aba de Previsão — multi-fonte (Open-Meteo, IPMA, WeatherAPI)

Mantém:
  • 2 gráficos (Tmax / Tmin)
  • Secção horária (24h, de 2 em 2h):
      1) Temperatura (°C) — TABELA
      2) Tabela diária (fonte → intervalo min–max; precip à direita)
      3) Expander com:
         – Precipitação diária (gráfico + chips Hoje/Amanhã)
         – Prob. precipitação IPMA (%), se existir
         – Precipitação (mm) — TABELA horária
"""

from __future__ import annotations

import io
import os
from datetime import datetime
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from services.open_meteo import geocode
from services.forecast_sources import (
    openmeteo_daily, ipma_daily, weatherapi_daily,
    openmeteo_hourly, weatherapi_hourly,
    ipma_hourly_prob,
)
from utils import charts
from services.i18n import t as tr
try:
    from services.i18n_boot import _ensure_lang_state
except ImportError:
    from services.i18n_boot import init_i18n_state as _ensure_lang_state

MAX_LOCATIONS = 5


# ------------------------------ helpers ------------------------------ #

def _to_text(v):
    """Converte qualquer valor (escalares, NaN, listas, Series) em string segura."""
    if v is None:
        return ""
    # tratar NaN apenas quando v é escalar
    try:
        isna = pd.isna(v)
        if isinstance(isna, (bool, np.bool_)) and isna:
            return ""
    except Exception:
        pass
    if isinstance(v, (list, tuple, set)):
        return ", ".join(_to_text(x) for x in v)
    if isinstance(v, pd.Series):
        return ", ".join(_to_text(x) for x in v.tolist())
    return str(v)


def _pick_places(query: str, max_results: int = 6) -> pd.DataFrame:
    """Geocoding (Open-Meteo) + colunas place/country preparadas."""
    df = geocode(query)
    if df is None or df.empty:
        return pd.DataFrame(columns=["label", "latitude", "longitude", "timezone", "place", "country"])
    df["country"] = df["label"].str.split("—").str[-1].str.strip()
    df["place"] = df["label"].str.split("—").str[0].str.strip()
    return df.head(max_results)


def _has_weatherapi() -> bool:
    """Há chave da WeatherAPI em secrets/env?"""
    return bool(st.secrets.get("WEATHERAPI_KEY") or os.getenv("WEATHERAPI_KEY"))


def _fetch_for_source(src: str, place_row: pd.Series, days: int) -> pd.DataFrame:
    """Previsão diária para uma fonte + um local → formato comum."""
    lat = float(place_row["latitude"])
    lon = float(place_row["longitude"])
    tz = place_row.get("timezone", "auto")
    country = place_row.get("country", "")
    place = place_row.get("place", place_row.get("label", ""))

    if src == "Open-Meteo":
        df = openmeteo_daily(lat, lon, tz=tz, days=days)
    elif src == "IPMA":
        if (country or "").lower().startswith("portugal"):
            city = str(place).split(",")[0].strip()
            df = ipma_daily(city)
        else:
            df = pd.DataFrame(columns=["date", "tmax", "tmin", "precip"])
    elif src == "WeatherAPI":
        df = weatherapi_daily(lat, lon, days=days)
    else:
        df = pd.DataFrame(columns=["date", "tmax", "tmin", "precip"])

    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "source", "place", "country", "tmax", "tmin", "precip"])

    df["source"] = src
    df["place"] = place
    df["country"] = country
    return df[["date", "source", "place", "country", "tmax", "tmin", "precip"]]


# ------------------------------ main tab ------------------------------ #

def render_forecast_tab():
    _ensure_lang_state()
    st.subheader(tr("forecast.previsao_meteorologica_multi_fonte"))

    # ========= pesquisa / locais =========
    col_a, col_b = st.columns([2, 1])
    with col_a:
        q = st.text_input(tr("forecast.adicionar_local"), "Lisboa")
        if st.button(tr("forecast.procurar")):
            st.session_state["forecast_search"] = _pick_places(q)
    with col_b:
        days = st.number_input(tr("forecast.dias_de_previsao"), 3, 14, 7, 1)

    # mês vem da barra de filtros (views/filters.py)
    mes_raw = st.session_state.get("filters_month", None)
    mes = mes_raw if isinstance(mes_raw, int) and 1 <= mes_raw <= 12 else None

    res = st.session_state.get("forecast_search")
    if isinstance(res, pd.DataFrame) and not res.empty:
        st.caption(tr("forecast.resultados_da_pesquisa"))
        st.dataframe(
            res[["place", "country", "latitude", "longitude", "timezone"]],
            hide_index=True, use_container_width=True
        )
        sel_idx = st.multiselect(
            tr("forecast.selecionar_locais_max_5"),
            options=list(res.index),
            format_func=lambda i: f"{res.loc[i,'place']} — {res.loc[i,'country']}",
            max_selections=MAX_LOCATIONS,
        )
        selected_places = res.loc[sel_idx] if sel_idx else pd.DataFrame()
    else:
        selected_places = _pick_places("Lisboa").head(1)

    # ========= fontes por defeito =========
    countries = selected_places["country"].fillna("").str.lower().tolist() if not selected_places.empty else ["portugal"]
    has_pt = any("portugal" in c for c in countries)
    default_sources = ["Open-Meteo"] + (["IPMA"] if has_pt else []) + (["WeatherAPI"] if _has_weatherapi() else [])
    sources = st.multiselect(tr("forecast.fontes_de_previsao"),
                             ["Open-Meteo", "IPMA", "WeatherAPI"],
                             default=default_sources)
    if not sources:
        st.warning(tr("forecast.escolha_pelo_menos_uma_fonte"))
        return
    if not _has_weatherapi():
        st.caption(tr("forecast.i_weatherapi_nao_ativa_adicione_weatherapi_key"))
    if not has_pt:
        st.caption(tr("forecast.i_ipma_so_devolve_dados_para_locais_em_portugal"))

    # ========= diário =========
    frames = []
    with st.spinner(tr("forecast.a_obter_previsoes_diarias")):
        for _, row in selected_places.iterrows():
            for src in sources:
                try:
                    d = _fetch_for_source(src, row, days)
                except Exception as e:
                    st.warning(tr("forecast.falha_fonte_para_local_e",
                                  src=src, local=row.get("place"), e=str(e)))
                    d = pd.DataFrame(columns=["date", "source", "place", "country", "tmax", "tmin", "precip"])
                if not d.empty:
                    frames.append(d)

    if not frames:
        st.info(tr("forecast.sem_dados_para_mostrar"))
        return

    df_all = pd.concat(frames, ignore_index=True)
    df_all["date"] = pd.to_datetime(df_all["date"], format="%Y-%m-%d", errors="coerce").dt.normalize()
    for c in ["tmax", "tmin", "precip"]:
        df_all[c] = pd.to_numeric(df_all[c], errors="coerce")
    if "tavg" in df_all.columns:
        df_all["tmax"] = df_all["tmax"].fillna(df_all["tavg"])
        df_all["tmin"] = df_all["tmin"].fillna(df_all["tavg"])
    df_all["tmax"] = df_all.groupby(["source", "place"], group_keys=False, observed=False)["tmax"].transform(lambda s: s.ffill().bfill())
    df_all["tmin"] = df_all.groupby(["source", "place"], group_keys=False, observed=False)["tmin"].transform(lambda s: s.ffill().bfill())
    df_all = df_all.sort_values(["date", "place", "source"]).reset_index(drop=True)

    # filtrar por mês, se escolhido
    if mes is not None:
        df_all = df_all[df_all["date"].dt.month == mes]
        if df_all.empty:
            st.info(tr("forecast.sem_dados_para_mostrar"))
            return

    # ========= gráficos (diário) =========
    st.subheader(tr("forecast.graficos"))
    dfp = df_all.sort_values("date")

    # Chips pequenos “Hoje/Amanhã” (por fonte) — por cima de cada gráfico
    def _norm_day(x):
        s = pd.to_datetime(x, errors="coerce")
        return s.dt.normalize()

    dfd = dfp.copy()
    dfd["__day"] = _norm_day(dfd["date"])

    today_local = pd.Timestamp.now().normalize()
    days_av = pd.Series(sorted(dfd["__day"].dropna().unique()))
    day_today = (days_av[days_av >= today_local].iloc[0] if not days_av[days_av >= today_local].empty else None)
    day_tomorrow = (days_av[days_av > (day_today if day_today is not None else today_local)].iloc[0]
                    if not days_av[days_av > (day_today if day_today is not None else today_local)].empty else None)

    try:
        if (selected_places is not None) and (not selected_places.empty):
            place0 = selected_places.iloc[0].get("label") or selected_places.iloc[0].get("place") or ""
        else:
            place0 = (dfd["place"].dropna().iloc[0] if "place" in dfd.columns else "")
    except Exception:
        place0 = (dfd["place"].dropna().iloc[0] if "place" in dfd.columns else "")

    def _vals_on(day, metric):
        out = {}
        for src in sources:
            if day is None:
                out[src] = None
                continue
            q = (dfd["source"] == src) & (dfd["__day"] == day)
            row = dfd.loc[q & (dfd["place"] == place0)] if ("place" in dfd.columns and place0) else dfd.loc[q]
            if row.empty:
                row = dfd.loc[q]
            v = row[metric].iloc[0] if not row.empty else None
            try:
                out[src] = None if pd.isna(v) else float(v)
            except Exception:
                out[src] = None
        return out

    tmax_today = _vals_on(day_today, "tmax")
    tmax_tom   = _vals_on(day_tomorrow, "tmax")
    tmin_today = _vals_on(day_today, "tmin")
    tmin_tom   = _vals_on(day_tomorrow, "tmin")

    def _chips_html(title, data):
        items = []
        for src in sources:
            v = data.get(src)
            val = "—" if (v is None) else f"{v:.1f}°"
            items.append(
                f'<span style="font-size:.88rem;padding:2px 8px;border:1px solid rgba(255,255,255,.15);'
                f'border-radius:999px;background:rgba(255,255,255,.05);white-space:nowrap">'
                f'<span style="opacity:.75">{src}</span> <b>{val}</b></span>'
            )
        return (
            f'<div style="display:flex;gap:.5rem;flex-wrap:wrap;align-items:center;margin:.25rem 0 .25rem 0">'
            f'<span style="font-size:.88rem;opacity:.7;margin-right:.5rem">{title} — {place0}</span>'
            + " ".join(items) + "</div>"
        )

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(_chips_html(tr("forecast.hoje_max"),   tmax_today), unsafe_allow_html=True)
        st.markdown(_chips_html(tr("forecast.amanha_max"), tmax_tom),   unsafe_allow_html=True)
        fig_max = charts.line_with_tail_labels(
            dfp, x="date", y="tmax", color="source",
            title=tr("forecast.temperatura_maxima_c"), x_title=tr("forecast.data"), y_title=tr("comparison.c"),
            height=280, label_font_size=12,
        )
        st.plotly_chart(fig_max, use_container_width=True)

    with c2:
        st.markdown(_chips_html(tr("forecast.hoje_min"),   tmin_today), unsafe_allow_html=True)
        st.markdown(_chips_html(tr("forecast.amanha_min"), tmin_tom),   unsafe_allow_html=True)
        fig_min = charts.line_with_tail_labels(
            dfp, x="date", y="tmin", color="source",
            title=tr("forecast.temperatura_minima_c"), x_title=tr("forecast.data"), y_title=tr("comparison.c"),
            height=280, label_font_size=12,
        )
        st.plotly_chart(fig_min, use_container_width=True)

    # ========= PREVISÕES HORÁRIAS (24h, 2 em 2) =========
    st.subheader(tr("forecast.previsoes_horarias_proximas_24_h_2_em_2_horas"))
    csv_hourly_temp = csv_hourly_prec = csv_ipma_prob = None

    if selected_places is None or selected_places.empty:
        st.info(tr("forecast.sem_local_para_previsoes_horarias"))
    else:
        p0 = selected_places.iloc[0]
        lat0, lon0 = float(p0["latitude"]), float(p0["longitude"])
        tz0 = p0.get("timezone", "auto")

        # --- obter 24h (2 em 2) para cada fonte ---
        rows_h = []
        for src in sources:
            try:
                if src == "Open-Meteo":
                    h = openmeteo_hourly(lat0, lon0, tz=tz0, hours=24)
                elif src == "WeatherAPI":
                    h = weatherapi_hourly(lat0, lon0, hours=24)
                else:
                    h = pd.DataFrame(columns=["time", "temp", "precip"])
            except Exception as e:
                st.caption(tr("forecast.falha_no_horario_de_src_err", src=src, err=str(e)))
                h = pd.DataFrame(columns=["time", "temp", "precip"])

            if not h.empty:
                h = h.dropna(subset=["time"]).copy()
                h["time"] = pd.to_datetime(h["time"], format="%Y-%m-%d %H:%M:%S")
                h = h.sort_values("time")
                h2 = h.iloc[::2].head(12)  # 2 em 2 horas
                row = {"source": src}
                for t, tC, pr in zip(h2["time"], h2["temp"], h2["precip"]):
                    row[f"T@{t.strftime('%H:%M')}"] = None if pd.isna(tC) else round(float(tC), 1)
                    row[f"P@{t.strftime('%H:%M')}"] = None if pd.isna(pr) else round(float(pr), 1)
                rows_h.append(row)

        if not rows_h:
            st.info(tr("forecast.sem_dados_horarios_para_fontes_selecionadas"))
        else:
            wide_all = pd.DataFrame(rows_h).fillna("")
            t_cols = sorted([c for c in wide_all.columns if c.startswith("T@")], key=lambda x: x[2:])
            p_cols = sorted([c for c in wide_all.columns if c.startswith("P@")], key=lambda x: x[2:])
            hourly_temp = wide_all[["source"] + t_cols].copy()
            hourly_prec = wide_all[["source"] + p_cols].copy()

            # ---------- 1) Temperatura (°C) — TABELA ----------
            st.markdown(tr("forecast.temperatura_c"))
            headers_T = [(tr("cols.fonte") if c == "source" else c) for c in hourly_temp.columns]
            cell_vals_T = [hourly_temp[c].apply(lambda v: "" if pd.isna(v) else str(v)).tolist()
                           for c in hourly_temp.columns]
            fig_T = go.Figure(data=[go.Table(
                header=dict(values=headers_T, align="center", line_color="white", line_width=0.3),
                cells=dict(values=cell_vals_T, align="center", line_color="white", line_width=0.3),
            )])
            fig_T.update_layout(margin=dict(l=0, r=0, t=8, b=0), height=140)
            st.plotly_chart(fig_T, use_container_width=True)

            # ---------- 2) Tabela diária (min–max; precip à direita) ----------
            st.markdown(tr("forecast.tabela_diaria_fonte_intervalo_min_max_precip_a_direita"))
            wide = (
                df_all.pivot_table(index=["place", "country", "date"], columns="source",
                                   values=["tmax", "tmin", "precip"], aggfunc="first")
                .sort_index(level=["place", "date"])
            )
            intervals = []
            presentes = sorted({c[1] for c in wide.columns})
            for src in presentes:
                tminS = wide[("tmin", src)] if ("tmin", src) in wide.columns else pd.Series(index=wide.index, dtype=float)
                tmaxS = wide[("tmax", src)] if ("tmax", src) in wide.columns else pd.Series(index=wide.index, dtype=float)
                inter = pd.Series(index=wide.index, dtype="object")
                for i in wide.index:
                    a, b = tminS.get(i, np.nan), tmaxS.get(i, np.nan)
                    inter.loc[i] = "" if (pd.isna(a) and pd.isna(b)) else (
                        f"{b:.1f}" if pd.isna(a) else (f"{a:.1f}" if pd.isna(b) else f"{a:.1f}–{b:.1f}")
                    )
                wide[(f"intervalo_{src}", "")] = inter
                intervals.append((f"intervalo_{src}", ""))
            pcols = [("precip", src) for src in presentes if ("precip", src) in wide.columns]
            wide = wide[intervals + pcols].copy()

            prefix_intervalo = tr("forecast.intervalo")
            wide.columns = [
                (f"{prefix_intervalo}_{c[1]}".replace(" ", "_") if c[0].startswith("intervalo_")
                 else f"{c[0]}_{c[1]}".replace(" ", "_"))
                for c in wide.columns
            ]
            wide = wide.reset_index()
            wide["date"] = wide["date"].dt.strftime("%Y-%m-%d")
            wide = wide.sort_values(["date", "place"]).reset_index(drop=True)

            rename_hdr = {"place": tr("cols.place"), "country": tr("cols.country"), "date": tr("cols.date")}
            headers = [rename_hdr.get(c, c) for c in wide.columns]

            # normaliza valores (por posição, evita DataFrame quando há nomes dup.)
            for j in range(wide.shape[1]):
                col = wide.iloc[:, j]
                wide.iloc[:, j] = col.apply(_to_text)

            cell_vals = [wide.iloc[:, j].tolist() for j in range(wide.shape[1])]
            fig_tbl = go.Figure(data=[go.Table(
                header=dict(values=headers, align="center", line_color="white", line_width=0.3),
                cells=dict(values=cell_vals, align="center", line_color="#F4F1F1", line_width=0.3),
            )])
            fig_tbl.update_layout(margin=dict(l=0, r=0, t=8, b=0), height=260)
            st.plotly_chart(fig_tbl, use_container_width=True)

            # ---------- 3) EXPANDER (aberto) com precipitação ----------
            with st.expander("🌧️ " + tr("forecast.precipitacao_diaria_e_horaria"), expanded=True):

                # (a) Precipitação diária — GRÁFICO + chips HOJE/AMANHÃ
                pp_today = _vals_on(day_today, "precip")
                pp_tom   = _vals_on(day_tomorrow, "precip")

                st.markdown(_chips_html(tr("forecast.hoje_mm"),   pp_today), unsafe_allow_html=True)
                st.markdown(_chips_html(tr("forecast.amanha_mm"), pp_tom),   unsafe_allow_html=True)

                st.markdown(tr("forecast.precipitacao_diaria_grafico"))
                fig_precip_daily = charts.line_with_tail_labels(
                    dfp, x="date", y="precip", color="source",
                    title=None, x_title=tr("forecast.data"), y_title=tr("comparison.mm")
                )
                ymax = pd.to_numeric(dfp["precip"], errors="coerce").max()
                ymax = 0.0 if pd.isna(ymax) else float(ymax)
                fig_precip_daily.update_traces(mode="lines+markers", marker=dict(size=6))
                fig_precip_daily.update_layout(height=260, yaxis=dict(range=[0, max(1.0, ymax * 1.15)]))
                st.plotly_chart(fig_precip_daily, use_container_width=True)

                # (b) Probabilidade IPMA (%) — se existir
                fig_ipma_prob = None
                if "IPMA" in sources:
                    try:
                        # TODO: substituir pelo id do local selecionado quando disponível
                        local_override = 1110600  # Lisboa (exemplo)
                        df_prob = ipma_hourly_prob(local_override)
                    except Exception:
                        df_prob = pd.DataFrame()
                    if not df_prob.empty:
                        df_prob2 = df_prob.sort_values("time").iloc[::2].head(12)
                        rowp = {"source": "IPMA"}
                        for t, pr in zip(df_prob2["time"], df_prob2["prob"]):
                            rowp[f"P@{t.strftime('%H:%M')}"] = None if pd.isna(pr) else float(pr)
                        ipma_prob = pd.DataFrame([rowp]).fillna("")
                        p_cols_ipma = sorted([c for c in ipma_prob.columns if c.startswith("P@")], key=lambda x: x[2:])
                        ipma_prob = ipma_prob.reindex(columns=["source"] + p_cols_ipma)
                        headers_ipma = list(ipma_prob.columns)
                        cell_vals_ipma = [ipma_prob[c].apply(lambda v: "" if pd.isna(v) else str(v)).tolist()
                                          for c in ipma_prob.columns]
                        fig_ipma_prob = go.Figure(data=[go.Table(
                            header=dict(values=headers_ipma, align="center", line_color="white", line_width=0.3),
                            cells=dict(values=cell_vals_ipma, align="center", line_color="white", line_width=0.3),
                        )])
                        fig_ipma_prob.update_layout(margin=dict(l=0, r=0, t=8, b=0), height=220)
                        # download opcional:
                        s = io.StringIO(); ipma_prob.to_csv(s, index=False)
                        csv_ipma_prob = s.getvalue()

                if fig_ipma_prob is not None:
                    st.markdown(tr("forecast.probabilidade_de_precipitacao_ipma"))
                    st.plotly_chart(fig_ipma_prob, use_container_width=True)
                else:
                    st.caption(tr("forecast.i_ipma_sem_dados_horarios_de_probabilidade_de_precipitacao_para_este_local"))

                # (c) Precipitação horária — TABELA
                st.markdown(tr("forecast.precipitacao_mm"))
                headers_P = [(tr("cols.fonte") if c == "source" else c) for c in hourly_prec.columns]
                cell_vals_P = [hourly_prec[c].apply(lambda v: "" if pd.isna(v) else str(v)).tolist()
                               for c in hourly_prec.columns]
                fig_precip_hourly = go.Figure(data=[go.Table(
                    header=dict(values=headers_P, align="center", line_color="white", line_width=0.3),
                    cells=dict(values=cell_vals_P, align="center", line_color="white", line_width=0.3),
                )])
                fig_precip_hourly.update_layout(margin=dict(l=0, r=0, t=8, b=0), height=160)
                st.plotly_chart(fig_precip_hourly, use_container_width=True)

            # CSVs de download (se precisares noutro lado)
            b1, b2 = io.StringIO(), io.StringIO()
            hourly_temp.to_csv(b1, index=False); csv_hourly_temp = b1.getvalue()
            hourly_prec.to_csv(b2, index=False); csv_hourly_prec = b2.getvalue()
