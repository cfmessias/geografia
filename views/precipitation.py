# -*- coding: utf-8 -*-
import io
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go          # para a tabela centrada
from streamlit import components           # para renderizar a tabela com scroll (iframe)
from utils.transform import polyfit_trend, fmt_num
from utils import charts


def render_precipitation_tab(
    view_df: pd.DataFrame,
    month_num: int | None,
    month_label: str,
    ref_year: int,
    last2_years: list[int],
    p_50: float | None,
    p_last2: float | None,
    show_50: bool,
    show_last2: bool,
):
    st.subheader(f"🌧️ Pluviosidade (acumulado mensal) — {'Todos os meses' if not month_num else month_label}")

    # --- Gráfico (mantido como estava)
    if month_num:
        x = view_df["year"].to_numpy()
        y = view_df["precip"].to_numpy()
        fitted, per_decade = polyfit_trend(x, y)

        fig_p = charts.bar(
            view_df, x="year", y="precip",
            title=f"Pluviosidade — {month_label}",
            x_title="Ano", y_title="mm"
        )
        if fitted is not None:
            charts.add_trend_line(fig_p, x, fitted, name=f"Tendência (~{per_decade:+.1f} mm/década)")
        if show_50 and (p_50 is not None):
            fig_p.add_scatter(
                x=[ref_year], y=[p_50], mode="markers+text",
                name=f"{ref_year}", text=[f"{ref_year}"], textposition="top center"
            )
        if show_last2 and (p_last2 is not None) and not np.isnan(p_last2):
            fig_p.add_scatter(
                x=[min(last2_years), max(last2_years)],
                y=[p_last2, p_last2], mode="lines", name="Média últimos 2 anos"
            )
    else:
        annual_p = view_df.groupby("year", as_index=False)["precip"].sum()
        fig_p = charts.bar(
            annual_p, x="year", y="precip",
            title="Pluviosidade anual (soma dos 12 meses)",
            x_title="Ano", y_title="mm"
        )

    st.plotly_chart(fig_p, use_container_width=True)

    # --- Métricas (inalterado)
    c3, c4 = st.columns(2)
    with c3:
        st.metric(
            f"Chuva em {month_label if month_num else 'mês atual'} — {ref_year}",
            fmt_num(p_50, " mm", 1)
        )
    with c4:
        st.metric(
            "Chuva — média últimos 2 anos", fmt_num(p_last2, " mm", 1),
            delta=(None if (p_50 is None or p_last2 is None or np.isnan(p_last2))
                   else f"{p_last2 - p_50:+.1f} mm")
        )

    # --- Tabela + CSV (única alteração: go.Table centrada COM SCROLL via iframe)
    with st.expander("📄 Dados (mensal por ano)"):
        show_cols = ["year", "month", "year_month", "t_mean", "t_norm", "t_anom", "precip", "p_norm", "p_anom"]
        grid = view_df[show_cols].sort_values(["year", "month"]).copy()
        grid["year"] = grid["year"].astype(int).astype(str)  # evitar separadores de milhar
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

        headers = list(disp.columns)
        cell_vals = [disp[c].tolist() for c in headers]
        n_rows = len(disp)

        fig_tbl = go.Figure(data=[go.Table(
            header=dict(
                values=headers,
                align="center",
                fill_color="#0b1220",                 # header adaptado ao tema escuro
                font=dict(color="#ffffff", size=12)
            ),
            cells=dict(
                values=cell_vals,
                align="center",
                fill_color="#111827",                 # células (bom contraste)
                font=dict(color="#e5e7eb"),
                height=28
            ),
        )])
        # altura "natural" da tabela; o iframe faz o scroll
        fig_tbl.update_layout(
            margin=dict(l=0, r=0, t=8, b=0),
            height=int(36 + 28 * n_rows + 12)
        )

        # 👉 Scroll verdadeiro: render como HTML dentro de um iframe com scroll
        html = fig_tbl.to_html(include_plotlyjs="cdn", full_html=False)
        components.v1.html(html, height=320, scrolling=True)  # altera 320 se quiseres mais/menos viewport

        # CSV (dados crus)
        buf = io.StringIO()
        grid[cols_out].to_csv(buf, index=False)
        st.download_button(
            "💾 Download CSV",
            data=buf.getvalue(),
            file_name="tendencias_mensais_precip.csv",
            mime="text/csv",
            key="dl_csv_precip"
        )
