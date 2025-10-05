# -*- coding: utf-8 -*-
import io
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go           # para a tabela centrada
from streamlit import components            # para renderizar a tabela com scroll (iframe)
from utils.transform import polyfit_trend, fmt_num
from utils import charts
from services.i18n import t as tr
try:
    from services.i18n_boot import _ensure_lang_state
except ImportError:
    from services.i18n_boot import init_i18n_state as _ensure_lang_state


def render_temperature_tab(
    view_df: pd.DataFrame,
    month_num: int | None,
    month_label: str,
    ref_year: int,
    last2_years: list[int],
    t_50: float | None,
    t_last2: float | None,
    show_50: bool,
    show_last2: bool,
):
    _ensure_lang_state()
    st.subheader("🌡️ " + tr("temperature.temperatura_media"))

    # — Recalcular o rótulo do mês com o idioma atual —
    def _month_lbl(month_num: int | None) -> str:
        return tr("months.all") if not month_num else tr(f"months.long.{int(month_num)}")

    month_label_i18n = _month_lbl(month_num)

    # ---- Gráfico
    if month_num:
        x = view_df["year"].to_numpy()
        y = view_df["t_mean"].to_numpy()
        fitted, per_decade = polyfit_trend(x, y)

        fig_t = charts.line(
            view_df, x="year", y="t_mean",
            title=tr("temperature.temperatura_media_mes", month=month_label_i18n),
            x_title=tr("climate_indicators.ano"), y_title=tr("comparison.c"), markers=True
        )
        if fitted is not None:
            trend_label = tr("temperature.tendencia_fmt_decada", per_decade=f"{per_decade:+.2f}")
            charts.add_trend_line(fig_t, x, fitted, name=trend_label)
        if show_50 and (t_50 is not None):
            fig_t.add_scatter(
                x=[ref_year], y=[t_50], mode="markers+text",
                name=f"{ref_year}", text=[f"{ref_year}"], textposition="top center"
            )
        if show_last2 and (t_last2 is not None) and not np.isnan(t_last2):
            fig_t.add_scatter(
                x=[min(last2_years), max(last2_years)],
                y=[t_last2, t_last2], mode="lines", name=tr("filters.media_ultimos_2_anos")
            )
    else:
        annual = view_df.groupby("year", as_index=False, observed=False)["t_mean"].mean()
        fig_t = charts.line(
            annual, x="year", y="t_mean",
            title=tr("temperature.temperatura_media_anual_media_dos_12_meses"),
            x_title=tr("climate_indicators.ano"), y_title=tr("comparison.c"), markers=True
        )

    st.plotly_chart(fig_t, width="stretch")

    # ---- Métricas
    c1, c2 = st.columns(2)
    with c1:
        label1 = tr("temperature.metric_temp_em",
            label=(month_label_i18n),
            year=ref_year)

        st.metric(label1, fmt_num(t_50, " °C"))
    with c2:
        st.metric(
            tr("temperature.temp_media_ultimos_2_anos"),
            fmt_num(t_last2, " °C"),
            delta=(None if (t_50 is None or t_last2 is None or np.isnan(t_last2))
                   else f"{t_last2 - t_50:+.1f} °C")
        )

    # ---- Tabela + CSV (go.Table centrada COM SCROLL via iframe)
    with st.expander("📄 " + tr("temperature.dados_mensais_por_ano")):
        show_cols = ["year", "month", "year_month", "t_mean", "t_norm", "t_anom", "precip", "p_norm", "p_anom"]
        grid = view_df[show_cols].sort_values(["year", "month"]).copy()
        grid["year"] = grid["year"].astype(int).astype(str)  # sem separador de milhares
        grid["year-month"] = pd.to_datetime(grid["year_month"]).dt.strftime("%Y-%m")

        cols_out = ["year", "month", "year-month", "t_mean", "t_norm", "t_anom", "precip", "p_norm", "p_anom"]

        # DISPLAY: formatação legível (CSV abaixo mantém valores crus)
        disp = grid[cols_out].copy()

        def _fmt_1(v):  return "" if pd.isna(v) else f"{float(v):.1f}"
        def _fmt_s(v):  return "" if pd.isna(v) else f"{float(v):+.1f}"

        for c in ["t_mean", "t_norm", "precip", "p_norm"]:
            if c in disp.columns:
                disp[c] = disp[c].apply(_fmt_1)
        for c in ["t_anom", "p_anom"]:
            if c in disp.columns:
                disp[c] = disp[c].apply(_fmt_s)

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
            header=dict(
                values=headers,
                align="center",
                fill_color="#0b1220",                 # header para tema escuro
                font=dict(color="#ffffff", size=12)
            ),
            cells=dict(
                values=cell_vals,
                align="center",
                fill_color="#111827",                 # células (contraste)
                font=dict(color="#e5e7eb"),
                height=28
            ),
        )])
        # altura “natural” da tabela; o iframe faz o scroll
        fig_tbl.update_layout(margin=dict(l=0, r=0, t=8, b=0), height=int(36 + 28 * n_rows + 12))

        # 👉 Scroll real: render HTML dentro de um iframe com scroll
        html = fig_tbl.to_html(include_plotlyjs="cdn", full_html=False)
        components.v1.html(html, height=320, scrolling=True)  # ajusta 320 se quiseres mais/menos viewport

        # CSV com dados crus
        buf = io.StringIO()
        grid[cols_out].to_csv(buf, index=False)
        st.download_button(
            tr("temperature.download_csv"),
            data=buf.getvalue(),
            file_name="tendencias_mensais_temp.csv",
            mime="text/csv",
            key="dl_csv_temp"
        )
