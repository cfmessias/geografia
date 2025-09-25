# -*- coding: utf-8 -*-
import io
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go           # para a tabela centrada
from streamlit import components            # para renderizar a tabela com scroll (iframe)
from utils.transform import polyfit_trend, fmt_num
from utils import charts


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
    st.subheader(f"🌡️ Temperatura média ")

    # ---- Gráfico (inalterado)
    if month_num:
        x = view_df["year"].to_numpy()
        y = view_df["t_mean"].to_numpy()
        fitted, per_decade = polyfit_trend(x, y)

        fig_t = charts.line(
            view_df, x="year", y="t_mean",
            title=f"Temperatura média — {month_label}",
            x_title="Ano", y_title="°C", markers=True
        )
        if fitted is not None:
            charts.add_trend_line(fig_t, x, fitted, name=f"Tendência (~{per_decade:+.2f} °C/década)")
        if show_50 and (t_50 is not None):
            fig_t.add_scatter(
                x=[ref_year], y=[t_50], mode="markers+text",
                name=f"{ref_year}", text=[f"{ref_year}"], textposition="top center"
            )
        if show_last2 and (t_last2 is not None) and not np.isnan(t_last2):
            fig_t.add_scatter(
                x=[min(last2_years), max(last2_years)],
                y=[t_last2, t_last2], mode="lines", name="Média últimos 2 anos"
            )
    else:
        annual = view_df.groupby("year", as_index=False)["t_mean"].mean()
        fig_t = charts.line(
            annual, x="year", y="t_mean",
            title="Temperatura média anual (média dos 12 meses)",
            x_title="Ano", y_title="°C", markers=True
        )

    st.plotly_chart(fig_t, use_container_width=True)

    # ---- Métricas (inalterado)
    c1, c2 = st.columns(2)
    with c1:
        st.metric(f"Temp. em {month_label if month_num else 'mês atual'} — {ref_year}", fmt_num(t_50, " °C"))
    with c2:
        st.metric(
            "Temp. — média últimos 2 anos", fmt_num(t_last2, " °C"),
            delta=(None if (t_50 is None or t_last2 is None or np.isnan(t_last2))
                   else f"{t_last2 - t_50:+.1f} °C")
        )

    # ---- Tabela + CSV (única alteração: go.Table centrada COM SCROLL via iframe)
    with st.expander("📄 Dados (mensal por ano)"):
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

        headers = list(disp.columns)
        cell_vals = [disp[c].tolist() for c in headers]
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
        fig_tbl.update_layout(
            margin=dict(l=0, r=0, t=8, b=0),
            height=int(36 + 28 * n_rows + 12)
        )

        # 👉 Scroll real: render HTML dentro de um iframe com scroll
        html = fig_tbl.to_html(include_plotlyjs="cdn", full_html=False)
        components.v1.html(html, height=320, scrolling=True)  # altera 320 se quiseres mais/menos viewport

        # CSV com dados crus (inalterado)
        buf = io.StringIO()
        grid[cols_out].to_csv(buf, index=False)
        st.download_button(
            "💾 Download CSV",
            data=buf.getvalue(),
            file_name="tendencias_mensais_temp.csv",
            mime="text/csv",
            key="dl_csv_temp"          # chave única
        )
