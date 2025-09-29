# -*- coding: utf-8 -*-
"""
Aba de Previsão — multi-fonte (Open-Meteo, IPMA e WeatherAPI)
- Seleciona vários locais (até 5)
- Liga automaticamente as fontes disponíveis (IPMA só para PT; WeatherAPI se houver chave)
- Consolida previsões diárias em: date, source, place, country, tmax, tmin, precip
- Mostra gráficos e tabela com download
"""

from __future__ import annotations

import io
import os
import numpy as np
import pandas as pd
import streamlit as st
import pytz
from datetime import datetime
import plotly.graph_objects as go

from services.open_meteo import geocode

from services.forecast_sources import (
    openmeteo_daily, ipma_daily, weatherapi_daily,
    openmeteo_hourly, weatherapi_hourly,
    ipma_hourly_prob,            # <-- novo
)
from utils import charts

MAX_LOCATIONS = 5

# ------------------------------ helpers ------------------------------ #
def _pick_places(query: str, max_results: int = 6) -> pd.DataFrame:
    """Usa o geocoder (Open-Meteo) e prepara colunas place/country."""
    df = geocode(query)
    if df is None or df.empty:
        return pd.DataFrame(columns=["label", "latitude", "longitude", "timezone", "place", "country"])
    df["country"] = df["label"].str.split("—").str[-1].str.strip()
    df["place"] = df["label"].str.split("—").str[0].str.strip()
    return df.head(max_results)

def _has_weatherapi() -> bool:
    """Verifica se há chave da WeatherAPI em secrets/env."""
    return bool(st.secrets.get("WEATHERAPI_KEY") or os.getenv("WEATHERAPI_KEY"))

def _fetch_for_source(src: str, place_row: pd.Series, days: int) -> pd.DataFrame:
    """Busca previsão diária para uma fonte + um local, devolvendo o formato comum."""
    lat = float(place_row["latitude"])
    lon = float(place_row["longitude"])
    tz = place_row.get("timezone", "auto")
    country = place_row.get("country", "")
    place = place_row.get("place", place_row.get("label", ""))

    if src == "Open-Meteo":
        df = openmeteo_daily(lat, lon, tz=tz, days=days)
    elif src == "IPMA":
        # IPMA só para PT; fora de PT devolvemos vazio silenciosamente
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
    st.subheader("🌦️ Previsão meteorológica — multi-fonte")

    # ========= pesquisa / locais =========
    MAX_LOCATIONS = 5

    def _pick_places(q: str, n: int = 6) -> pd.DataFrame:
        df = geocode(q)
        if df is None or df.empty:
            return pd.DataFrame(columns=["label","latitude","longitude","timezone","place","country"])
        df["country"] = df["label"].str.split("—").str[-1].str.strip()
        df["place"]   = df["label"].str.split("—").str[0].str.strip()
        return df.head(n)

    def _has_wapi() -> bool:
        return bool(st.secrets.get("WEATHERAPI_KEY") or os.getenv("WEATHERAPI_KEY"))

    left, right = st.columns([2,1])
    with left:
        q = st.text_input("Adicionar local", "Lisboa")
        if st.button("🔎 Procurar"):
            st.session_state["forecast_search"] = _pick_places(q)
    with right:
        days = st.number_input("Dias de previsão", 3, 14, 7, 1)

    res = st.session_state.get("forecast_search")
    if isinstance(res, pd.DataFrame) and not res.empty:
        st.caption("Resultados da pesquisa:")
        st.dataframe(res[["place","country","latitude","longitude","timezone"]],
                     hide_index=True, width="stretch")
        sel_idx = st.multiselect(
            "Selecionar locais (máx. 5):",
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
    default_sources = ["Open-Meteo"] + (["IPMA"] if has_pt else []) + (["WeatherAPI"] if _has_wapi() else [])
    sources = st.multiselect("Fontes de previsão", ["Open-Meteo","IPMA","WeatherAPI"], default=default_sources)
    if not sources:
        st.warning("Escolha pelo menos uma fonte."); return
    if not _has_wapi(): st.caption("ℹ️ WeatherAPI não ativa (adicione WEATHERAPI_KEY).")
    if not has_pt:      st.caption("ℹ️ IPMA só devolve dados para locais em Portugal.")

    # ========= diário =========
    def _fetch_daily(src: str, row: pd.Series, n_days: int) -> pd.DataFrame:
        lat, lon = float(row["latitude"]), float(row["longitude"])
        tz  = row.get("timezone","auto")
        cc  = row.get("country",""); plc = row.get("place", row.get("label",""))
        if src == "Open-Meteo":
            df = openmeteo_daily(lat, lon, tz=tz, days=n_days)
        elif src == "IPMA":
            if (cc or "").lower().startswith("portugal"):
                city = str(plc).split(",")[0].strip()
                df = ipma_daily(city)
            else:
                df = pd.DataFrame(columns=["date","tmax","tmin","precip"])
        elif src == "WeatherAPI":
            df = weatherapi_daily(lat, lon, days=n_days)
        else:
            df = pd.DataFrame()
        if df is None or df.empty:
            return pd.DataFrame(columns=["date","source","place","country","tmax","tmin","precip"])
        df["source"] = src; df["place"] = plc; df["country"] = cc
        return df[["date","source","place","country","tmax","tmin","precip"]]

    frames = []
    with st.spinner("A obter previsões diárias…"):
        for _, row in selected_places.iterrows():
            for src in sources:
                try:
                    d = _fetch_daily(src, row, days)
                except Exception as e:
                    st.warning(f"Falha em {src} para {row.get('place')}: {e}")
                    d = pd.DataFrame(columns=["date","source","place","country","tmax","tmin","precip"])
                if not d.empty: frames.append(d)

    if not frames:
        st.info("Sem dados para mostrar."); return

    df_all = pd.concat(frames, ignore_index=True)
    #df_all["date"] = pd.to_datetime(df_all["date"], errors="coerce").dt.normalize()
    df_all["date"] = pd.to_datetime(df_all["date"], format="%Y-%m-%d", errors="coerce").dt.normalize()
    for c in ["tmax","tmin","precip"]:
        df_all[c] = pd.to_numeric(df_all[c], errors="coerce")
    if "tavg" in df_all.columns:
        df_all["tmax"] = df_all["tmax"].fillna(df_all["tavg"])
        df_all["tmin"] = df_all["tmin"].fillna(df_all["tavg"])
    df_all["tmax"] = df_all.groupby(["source","place"], group_keys=False, observed=False)["tmax"].transform(lambda s: s.ffill().bfill())
    df_all["tmin"] = df_all.groupby(["source","place"], group_keys=False, observed=False)["tmin"].transform(lambda s: s.ffill().bfill())
    df_all = df_all.sort_values(["date","place","source"]).reset_index(drop=True)

    # ========= gráficos (diário) =========
    st.subheader("Gráficos")
    dfp = df_all.sort_values("date")

    # ── RESUMO 'AMANHÃ' (Tmax/Tmin por fonte) ─────────────────────────────
    # usamos o primeiro local selecionado (mesma lógica da secção horária)
    # ---------- Amanhã (por fonte, para o 1.º local) ----------
    def _norm_day(x):
        s = pd.to_datetime(x, errors="coerce")
        return s.dt.normalize()

    dfd = dfp.copy()
    dfd["__day"] = _norm_day(dfd["date"])

    # 1) escolher o próximo dia disponível no dataset (≈ amanhã)
    today_local = pd.Timestamp.now().normalize()
    days = pd.Series(sorted(dfd["__day"].dropna().unique()))
    cand = days[days >= (today_local + pd.Timedelta(days=1))]
    day_sel = (cand.iloc[0] if not cand.empty else days.iloc[0]) if not days.empty else None

    # 2) escolher o 1.º local (como na tua secção horária); se não casar, faz fallback
    try:
        if (selected_places is not None) and (not selected_places.empty):
            place0 = selected_places.iloc[0].get("label") or selected_places.iloc[0].get("place") or ""
        else:
            place0 = (dfd["place"].dropna().iloc[0] if "place" in dfd.columns else "")
    except Exception:
        place0 = (dfd["place"].dropna().iloc[0] if "place" in dfd.columns else "")

    def _vals_for(metric):
        out = {}
        for src in sources:
            if day_sel is None:
                out[src] = None
                continue
            q = (dfd["source"] == src) & (dfd["__day"] == day_sel)
            row = dfd.loc[q & (dfd["place"] == place0)] if ("place" in dfd.columns and place0) else dfd.loc[q]
            if row.empty:  # fallback sem o filtro de place
                row = dfd.loc[q]
            v = row[metric].iloc[0] if not row.empty else None
            try:
                out[src] = None if pd.isna(v) else float(v)
            except Exception:
                out[src] = None
        return out

    tmax_by = _vals_for("tmax")
    tmin_by = _vals_for("tmin")

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
            f'<div style="display:flex;gap:.5rem;flex-wrap:wrap;align-items:center;margin:.25rem 0 .5rem 0">'
            f'<span style="font-size:.88rem;opacity:.7;margin-right:.5rem">{title}</span>'
            + " ".join(items) + "</div>"
        )


    # ---------- Gráficos + cabeçalho pequeno com valores ----------
    c1, c2 = st.columns(2)

    with c1:
        # cabeçalho pequeno (máximas)
        st.markdown(_chips_html("Amanhã (máx.)", tmax_by), unsafe_allow_html=True)
        fig_max = charts.line_with_tail_labels(
            dfp, x="date", y="tmax", color="source",
            title="Temperatura máxima (°C)", x_title="Data", y_title="°C",
            height=280, label_font_size=12,
        )
        st.plotly_chart(fig_max, width="stretch")

    with c2:
        # cabeçalho pequeno (mínimas)
        st.markdown(_chips_html("Amanhã (mín.)", tmin_by), unsafe_allow_html=True)
        fig_min = charts.line_with_tail_labels(
            dfp, x="date", y="tmin", color="source",
            title="Temperatura mínima (°C)", x_title="Data", y_title="°C",
            height=280, label_font_size=12,
        )
        st.plotly_chart(fig_min, width="stretch")

    with st.expander("💧 Precipitação diária (abrir)", expanded=False):
        st.plotly_chart(
            charts.bar(
                dfp, x="date", y="precip", color="source",
                title="Precipitação prevista", x_title="Data", y_title="mm"
            ),
            width="stretch"
        )

    # ========= horários (2 em 2 h / 24 h) =========
    st.subheader("Previsões horárias (próximas 24 h) — 2 em 2 horas")
    csv_hourly_temp = csv_hourly_prec = csv_ipma_prob = None

    if selected_places is None or selected_places.empty:
        st.info("Sem local selecionado para previsões horárias.")
    else:
        p0 = selected_places.iloc[0]
        lat0, lon0 = float(p0["latitude"]), float(p0["longitude"])
        tz0 = p0.get("timezone","auto")

        # fetch 24h para OM/WAPI (IPMA mm não comparável aqui)
        rows_h = []
        for src in sources:
            try:
                if src == "Open-Meteo":
                    h = openmeteo_hourly(lat0, lon0, tz=tz0, hours=24)
                elif src == "WeatherAPI":
                    h = weatherapi_hourly(lat0, lon0, hours=24)
                else:
                    h = pd.DataFrame(columns=["time","temp","precip"])
            except Exception as e:
                st.caption(f"Falha no horário de {src}: {e}")
                h = pd.DataFrame(columns=["time","temp","precip"])

            if not h.empty:
                h = h.dropna(subset=["time"]).copy()
                #h["time"] = pd.to_datetime(h["time"]); h = h.sort_values("time")
                h["time"] = pd.to_datetime(h["time"], format="%Y-%m-%d %H:%M:%S")
                h = h.sort_values("time")
                h2 = h.iloc[::2].head(12)  # 2 em 2 horas
                row = {"source": src}
                for t, tC, pr in zip(h2["time"], h2["temp"], h2["precip"]):
                    row[f"T@{t.strftime('%H:%M')}"] = None if pd.isna(tC) else round(float(tC),1)
                    row[f"P@{t.strftime('%H:%M')}"] = None if pd.isna(pr) else round(float(pr),1)
                rows_h.append(row)

        if rows_h:
            wide_all = pd.DataFrame(rows_h).fillna("")
            t_cols = sorted([c for c in wide_all.columns if c.startswith("T@")], key=lambda x: x[2:])
            p_cols = sorted([c for c in wide_all.columns if c.startswith("P@")], key=lambda x: x[2:])
            hourly_temp = wide_all[["source"] + t_cols].copy()
            hourly_prec = wide_all[["source"] + p_cols].copy()

            # hora local p/ destaque
            try:
                tzname = tz0 if tz0 and tz0 != "auto" else "UTC"
                now_local = datetime.now(pytz.timezone(tzname))
            except Exception:
                now_local = datetime.utcnow()
            now_mins = now_local.hour*60 + (0 if now_local.minute < 30 else 60)
            def _mins(c): hh,mm = c[2:].split(":"); return int(hh)*60+int(mm)

            nearest_T   = min(t_cols, key=lambda c: abs(_mins(c)-now_mins)) if t_cols else None
            nearest_Pmm = min(p_cols, key=lambda c: abs(_mins(c)-now_mins)) if p_cols else None

            # Temperatura (Tabela Plotly centralizada)
            st.markdown("**Temperatura (°C)**")
            headers_T = list(hourly_temp.columns)
            cell_vals_T = [hourly_temp[c].apply(lambda v: "" if pd.isna(v) else str(v)).tolist() for c in hourly_temp.columns]
            fig_T = go.Figure(data=[go.Table(
                header=dict(values=headers_T, align="center",line_color="white", line_width=0.3),
                cells=dict(values=cell_vals_T, align="center",line_color="white", line_width=0.3),
            )])
            fig_T.update_layout(margin=dict(l=0, r=0, t=8, b=0), height=140)
            st.plotly_chart(fig_T, width="stretch")


            
            # ---------- IPMA: prob. precipitação (%) (ACIMA do mm) ----------
            if "IPMA" in sources:
                city = str(p0.get("place","")).split(",")[0].strip() or "Lisboa"
                local_override = 1110600  # Lisboa (globalIdLocal)
                df_prob = ipma_hourly_prob(local_override)
                if df_prob.empty:
                    st.caption("ℹ️ IPMA: sem dados horários de probabilidade de precipitação para este local.")
                else:
                    df_prob2 = df_prob.sort_values("time").iloc[::2].head(12)
                    rowp = {"source":"IPMA"}
                    for t, pr in zip(df_prob2["time"], df_prob2["prob"]):
                        rowp[f"P@{t.strftime('%H:%M')}"] = None if pd.isna(pr) else float(pr)
                    ipma_prob = pd.DataFrame([rowp]).fillna("")
                    p_cols_ipma = sorted([c for c in ipma_prob.columns if c.startswith("P@")], key=lambda x: x[2:])
                    ipma_prob = ipma_prob.reindex(columns=["source"]+p_cols_ipma)
                    nearest_Pipma = min(p_cols_ipma, key=lambda c: abs(_mins(c)-now_mins)) if p_cols_ipma else None
                    # Plotly Table para IPMA Prob
                    headers_ipma = list(ipma_prob.columns)
                    cell_vals_ipma = [ipma_prob[c].apply(lambda v: "" if pd.isna(v) else str(v)).tolist() for c in ipma_prob.columns]
                    fig_ipma = go.Figure(data=[go.Table(
                        header=dict(values=headers_ipma, align="center",line_color="white", line_width=0.3),
                        cells=dict(values=cell_vals_ipma, align="center",line_color="white", line_width=0.3),
                    )])
                    fig_ipma.update_layout(margin=dict(l=0, r=0, t=8, b=0), height=220)
                    st.markdown("**Probabilidade de precipitação — IPMA (%)**")
                    st.plotly_chart(fig_ipma, width="stretch")
                    # CSV p/ downloads
                    b_ip = io.StringIO(); ipma_prob.to_csv(b_ip, index=False); csv_ipma_prob = b_ip.getvalue()

            # Precipitação (mm) (Tabela Plotly centralizada)
            st.markdown("**Precipitação (mm)**")
            headers_P = list(hourly_prec.columns)
            cell_vals_P = [hourly_prec[c].apply(lambda v: "" if pd.isna(v) else str(v)).tolist() for c in hourly_prec.columns]
            fig_P = go.Figure(data=[go.Table(
                header=dict(values=headers_P, align="center",line_color="white", line_width=0.3),
                cells=dict(values=cell_vals_P, align="center",line_color="white", line_width=0.3),
            )])
           
            fig_P.update_layout(margin=dict(l=0, r=0, t=8, b=0), height=140)
            st.plotly_chart(fig_P, width="stretch")

            # CSVs horários p/ downloads
            b1,b2 = io.StringIO(), io.StringIO()
            hourly_temp.to_csv(b1, index=False); csv_hourly_temp = b1.getvalue()
            hourly_prec.to_csv(b2, index=False); csv_hourly_prec = b2.getvalue()
        else:
            st.info("Sem dados horários disponíveis para as fontes selecionadas.")

    # ========= tabela diária larga =========
    st.subheader("Tabela diária (fonte → intervalo min–max; precip à direita)")
    wide = (
        df_all.pivot_table(index=["place","country","date"], columns="source",
                           values=["tmax","tmin","precip"], aggfunc="first")
             .sort_index(level=["place","date"])
    )
    intervals = []
    present = sorted({c[1] for c in wide.columns})
    for src in present:
        tmin = wide[("tmin",src)] if ("tmin",src) in wide.columns else pd.Series(index=wide.index, dtype=float)
        tmax = wide[("tmax",src)] if ("tmax",src) in wide.columns else pd.Series(index=wide.index, dtype=float)
        inter = pd.Series(index=wide.index, dtype="object")
        for i in wide.index:
            a,b = tmin.get(i,np.nan), tmax.get(i,np.nan)
            inter.loc[i] = "" if (pd.isna(a) and pd.isna(b)) else (f"{b:.1f}" if pd.isna(a) else (f"{a:.1f}" if pd.isna(b) else f"{a:.1f}–{b:.1f}"))
        wide[(f"intervalo_{src}","")] = inter; intervals.append((f"intervalo_{src}",""))
    pcols = [("precip",src) for src in present if ("precip",src) in wide.columns]
    wide = wide[intervals + pcols].copy()
    wide.columns = [c[0].replace(" ","_") if c[0].startswith("intervalo_") else f"{c[0]}_{c[1]}".replace(" ","_") for c in wide.columns]
    wide = wide.reset_index(); wide["date"] = wide["date"].dt.strftime("%Y-%m-%d")
    wide = wide.sort_values(["date","place"]).reset_index(drop=True)

    # cabeçalhos bonitos
    rename_hdr = {
        "place": "Local",
        "country": "País",
        "date": "Data",
    }
    headers = [rename_hdr.get(c, c) for c in wide.columns]

    # Formatar valores (tudo string, NaN para "")
    for c in wide.columns:
        wide[c] = wide[c].apply(lambda v: "" if pd.isna(v) else str(v))

    cell_vals = [wide[c].tolist() for c in wide.columns]
    fig_tbl = go.Figure(
    data=[go.Table(
        header=dict(
            values=headers,
            align="center",
            line_color="white",   # cor da grelha do cabeçalho
            line_width=0.3          # espessura da grelha do cabeçalho
        ),
        cells=dict(
            values=cell_vals,
            align="center",
            line_color="#F4F1F1",   # cor da grelha das células
            line_width=0.3          # espessura da grelha das células
        ),
    )]
    )
    fig_tbl.update_layout(margin=dict(l=0, r=0, t=8, b=0), height=420)
    st.plotly_chart(fig_tbl, width="stretch")

    # ========= downloads =========
    st.markdown("---"); st.subheader("⬇️ Downloads")
    if csv_hourly_temp:
        st.download_button("💾 Horário — Temperatura (CSV)", data=csv_hourly_temp,
                           file_name="forecast_hourly_temperature.csv", mime="text/csv",
                           key="dl_csv_hourly_temp")
    if csv_hourly_prec:
        st.download_button("💾 Horário — Precipitação (CSV)", data=csv_hourly_prec,
                           file_name="forecast_hourly_precipitation.csv", mime="text/csv",
                           key="dl_csv_hourly_prec")
    if csv_ipma_prob:
        st.download_button("💾 IPMA — Prob. precipitação horária (%)", data=csv_ipma_prob,
                           file_name="ipma_hourly_precip_probability.csv", mime="text/csv",
                           key="dl_csv_ipma_prob")
    _buf = io.StringIO(); wide.to_csv(_buf, index=False)
    st.download_button("💾 Diário — Tabela larga (CSV)", data=_buf.getvalue(),
                       file_name="forecast_daily_wide.csv", mime="text/csv",
                       key="dl_csv_daily_wide")
