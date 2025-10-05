# -*- coding: utf-8 -*-
import io
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go          # para a tabela centrada
from streamlit import components           # para renderizar a tabela com scroll (iframe)
from utils.transform import polyfit_trend, fmt_num
from utils import charts
from services.i18n import t as tr
try:
    from services.i18n_boot import _ensure_lang_state
except ImportError:
    from services.i18n_boot import init_i18n_state as _ensure_lang_state


def render_precipitation_tab(
    view_df: pd.DataFrame,
    month_num: int | None,
    month_label: str,          # <- é ignorado; recalculamos para o idioma atual
    ref_year: int,
    last2_years: list[int],
    p_50: float | None,
    p_last2: float | None,
    show_50: bool,
    show_last2: bool,
):
    _ensure_lang_state()

    # — Rótulo do mês no idioma atual (None -> "Todos os meses" / "All months")
    def _month_lbl(m: int | None) -> str:
        return tr("months.all") if not m else tr(f"months.long.{int(m)}")
    month_label_i18n = _month_lbl(month_num)

    # Subtítulo
    st.subheader("🌧️ " + tr("precipitation.precipitacao_acumulada_mensal") + " — " + month_label_i18n)

    # --- Gráfico
    if month_num:
        x = view_df["year"].to_numpy()
        y = view_df["precip"].to_numpy()
        fitted, per_decade = polyfit_trend(x, y)

        fig_p = charts.bar(
            view_df, x="year", y="precip",
            title=tr("precipitation.precipitacao_mes", month=month_label_i18n),
            x_title=tr("climate_indicators.ano"), y_title=tr("comparison.mm")
        )
        if fitted is not None:
            charts.add_trend_line(
                fig_p, x, fitted,
                name=tr("precipitation.tendencia_fmt_decada", per_decade=f"{per_decade:+.1f}")
            )
        if show_50 and (p_50 is not None):
            fig_p.add_scatter(
                x=[ref_year], y=[p_50], mode="markers+text",
                name=f"{ref_year}", text=[f"{ref_year}"], textposition="top center"
            )
        if show_last2 and (p_last2 is not None) and not np.isnan(p_last2):
            fig_p.add_scatter(
                x=[min(last2_years), max(last2_years)],
                y=[p_last2, p_last2], mode="lines", name=tr("filters.media_ultimos_2_anos")
            )
    else:
        annual_p = view_df.groupby("year", as_index=False, observed=False)["precip"].sum()
        fig_p = charts.bar(
            annual_p, x="year", y="precip",
            title=tr("precipitation.pluviosidade_anual_soma_dos_12_meses"),
            x_title=tr("climate_indicators.ano"), y_title=tr("comparison.mm")
        )

    st.plotly_chart(fig_p, width="stretch")

    # --- Métricas
    c3, c4 = st.columns(2)
    with c3:
        st.metric(
            tr("precipitation.metric_precip_em",
               label=(month_label_i18n), year=ref_year),
            fmt_num(p_50, " mm", 1)
        )
    with c4:
        st.metric(
            tr("precipitation.precip_media_ultimos_2_anos"),
            fmt_num(p_last2, " mm", 1),
            delta=(None if (p_50 is None or p_last2 is None or np.isnan(p_last2))
                   else f"{p_last2 - p_50:+.1f} mm")
        )

    # --- Tabela + CSV (go.Table centrada com scroll via iframe)
    with st.expander("📄 " + tr("precipitation.dados_mensais_por_ano")):
        show_cols = ["year", "month", "year_month", "t_mean", "t_norm", "t_anom",
                     "precip", "p_norm", "p_anom"]
        grid = view_df[show_cols].sort_values(["year", "month"]).copy()
        grid["year"] = grid["year"].astype(int).astype(str)                 # sem milhares
        grid["year-month"] = pd.to_datetime(grid["year_month"]).dt.strftime("%Y-%m")
        cols_out = ["year", "month", "year-month", "t_mean", "t_norm", "t_anom",
                    "precip", "p_norm", "p_anom"]

        disp = grid[cols_out].copy()
        def _fmt_1(v): return "" if pd.isna(v) else f"{float(v):.1f}"
        def _fmt_s(v): return "" if pd.isna(v) else f"{float(v):+.1f}"
        for c in ["t_mean", "t_norm", "precip", "p_norm"]:
            if c in disp.columns: disp[c] = disp[c].apply(_fmt_1)
        for c in ["t_anom", "p_anom"]:
            if c in disp.columns: disp[c] = disp[c].apply(_fmt_s)

        # cabeçalhos traduzidos
        hdr_map = {
            "year": tr("cols.year"),
            "month": tr("cols.month"),
            "year-month": tr("cols.year_month"),
            "t_mean": tr("cols.t_mean_degC"),
            "t_norm": tr("cols.t_norm_degC"),
            "t_anom": tr("cols.t_anom_degC"),
            "precip": tr("cols.precip_mm"),
            "p_norm": tr("cols.p_norm_mm"),
            "p_anom": tr("cols.p_anom_mm"),
        }
        headers = [hdr_map.get(c, c) for c in disp.columns]
        cell_vals = [disp[c].tolist() for c in disp.columns]
        n_rows = len(disp)

        fig_tbl = go.Figure(data=[go.Table(
            header=dict(values=headers, align="center",
                        fill_color="#0b1220", font=dict(color="#ffffff", size=12)),
            cells=dict(values=cell_vals, align="center",
                       fill_color="#111827", font=dict(color="#e5e7eb"), height=28),
        )])
        fig_tbl.update_layout(margin=dict(l=0, r=0, t=8, b=0),
                              height=int(36 + 28 * n_rows + 12))

        html = fig_tbl.to_html(include_plotlyjs="cdn", full_html=False)
        components.v1.html(html, height=320, scrolling=True)

        buf = io.StringIO()
        grid[cols_out].to_csv(buf, index=False)
        st.download_button(
            tr("precipitation.download_csv"),
            data=buf.getvalue(),
            file_name="tendencias_mensais_precip.csv",
            mime="text/csv",
            key="dl_csv_precip"
        )
