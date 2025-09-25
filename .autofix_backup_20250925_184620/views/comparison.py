# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from utils.transform import fmt_num
from utils import charts

# ─────────────────────────────────────────────────────────────
# Tabela centrada (mesma metodologia pedida: Plotly go.Table)
# ─────────────────────────────────────────────────────────────
def _plotly_centered_table(df: pd.DataFrame, formatters: dict | None = None, key: str | None = None):
    """
    Renderiza um DataFrame como Plotly Table com cabeçalhos e células centradas.
    - `formatters`: dict opcional {col_name: callable} para formatar cada coluna.
    """
    if df is None or df.empty:
        st.info("Sem dados para mostrar.")
        return

    d = df.copy()
    if formatters:
        for col, fmt in formatters.items():
            if col in d.columns:
                try:
                    d[col] = d[col].apply(lambda v: "" if pd.isna(v) else fmt(v))
                except Exception:
                    pass

    headers = list(d.columns)
    cell_vals = [d[c].tolist() for c in headers]
    n_rows = len(d)

    fig = go.Figure(
        data=[go.Table(
            header=dict(values=headers, align="center"),
            cells=dict(values=cell_vals, align="center"),
        )]
    )
    # Altura compacta: 36 (header) + 32 por linha + 12 folga
    fig.update_layout(margin=dict(l=0, r=0, t=8, b=0), height=int(36 + 32*n_rows + 12))
    st.plotly_chart(fig, use_container_width=True, key=key)


# ─────────────────────────────────────────────────────────────
# FUNÇÃO EXPORTADA (nome exatamente como o app importa)
# ─────────────────────────────────────────────────────────────
def render_comparison_tab(dfm: pd.DataFrame):
    st.subheader("📊 Comparação entre 2 anos (mês a mês)")

    # Anos disponíveis
    years_avail = sorted(dfm["year"].unique())
    if len(years_avail) < 2:
        st.info("São necessários pelo menos 2 anos de dados para comparar.")
        return

    default_b = years_avail[-1]
    default_a = next((y for y in years_avail if y == default_b - 50), years_avail[0])

    cA, cB = st.columns(2)
    with cA:
        idx_a = years_avail.index(default_a) if default_a in years_avail else 0
        year_a = st.selectbox("Ano A", years_avail, index=idx_a)
    with cB:
        idx_b = years_avail.index(default_b) if default_b in years_avail else len(years_avail) - 1
        year_b = st.selectbox("Ano B", years_avail, index=idx_b)
    if year_a == year_b:
        st.warning("Escolha dois anos diferentes.")

    # Subconjunto e grelha completa mês a mês
    cmp = dfm[dfm["year"].isin([year_a, year_b])][["year","month","t_mean","precip"]].copy()
    full = pd.MultiIndex.from_product([[year_a, year_b], list(range(1,13))], names=["year","month"])
    cmp = cmp.set_index(["year","month"]).reindex(full).reset_index()

    temp_w = cmp.pivot(index="month", columns="year", values="t_mean")
    rain_w = cmp.pivot(index="month", columns="year", values="precip")

    # ── Gráficos: temperatura
    c1, c2 = st.columns(2)
    with c1:
        df_temp_plot = temp_w.reset_index().melt(id_vars="month", var_name="Ano", value_name="Temp (°C)")
        fig_ct = charts.line(df_temp_plot, x="month", y="Temp (°C)", color="Ano",
                             title="Temperatura média mensal — comparação",
                             x_title="Mês", y_title="°C", markers=True)
        fig_ct.update_xaxes(tickmode="linear", dtick=1)
        st.plotly_chart(fig_ct, use_container_width=True)
    with c2:
        temp_delta = (temp_w.get(year_b) - temp_w.get(year_a)).rename("Δ Temp (°C)")
        fig_ctd = charts.bar(temp_delta.reset_index(), x="month", y="Δ Temp (°C)",
                             title=f"Diferença de temperatura ( {year_b} − {year_a} )",
                             x_title="Mês", y_title="Δ Temp (°C)")
        try:
            absmax = float(np.nanmax(np.abs(temp_delta.values)))
            if absmax > 0:
                fig_ctd.update_yaxes(range=[-absmax * 2, absmax * 2])  # escala simétrica x2
        except Exception:
            pass
        fig_ctd.update_xaxes(tickmode="linear", dtick=1)
        st.plotly_chart(fig_ctd, use_container_width=True)

    # ── Gráficos: precipitação
    c3, c4 = st.columns(2)
    with c3:
        df_rain_plot = rain_w.reset_index().melt(id_vars="month", var_name="Ano", value_name="Chuva (mm)")
        fig_cp = charts.line(df_rain_plot, x="month", y="Chuva (mm)", color="Ano",
                             title="Pluviosidade mensal — comparação",
                             x_title="Mês", y_title="mm", markers=True)
        fig_cp.update_xaxes(tickmode="linear", dtick=1)
        st.plotly_chart(fig_cp, use_container_width=True)
    with c4:
        rain_delta = (rain_w.get(year_b) - rain_w.get(year_a)).rename("Δ Chuva (mm)")
        fig_cpd = charts.bar(rain_delta.reset_index(), x="month", y="Δ Chuva (mm)",
                             title=f"Diferença de precipitação ( {year_b} − {year_a} )",
                             x_title="Mês", y_title="Δ Chuva (mm)")
        fig_cpd.update_xaxes(tickmode="linear", dtick=1)
        st.plotly_chart(fig_cpd, use_container_width=True)

    # ── Resumo anual
    ann = dfm[dfm["year"].isin([year_a, year_b])].groupby("year", as_index=False).agg(
        t_year=("t_mean","mean"), p_year=("precip","sum")
    )
    tA = float(ann.loc[ann["year"]==year_a, "t_year"].iloc[0]) if (ann["year"]==year_a).any() else np.nan
    tB = float(ann.loc[ann["year"]==year_b, "t_year"].iloc[0]) if (ann["year"]==year_b).any() else np.nan
    pA = float(ann.loc[ann["year"]==year_a, "p_year"].iloc[0]) if (ann["year"]==year_a).any() else np.nan
    pB = float(ann.loc[ann["year"]==year_b, "p_year"].iloc[0]) if (ann["year"]==year_b).any() else np.nan

    st.subheader("Resumo anual")
    m1, m2, m3, m4 = st.columns(4)
    with m1: st.metric(f"Temp. média anual — {year_a}", fmt_num(tA, " °C"))
    with m2: st.metric(f"Temp. média anual — {year_b}", fmt_num(tB, " °C"),
                       delta=(None if (np.isnan(tA) or np.isnan(tB)) else f"{tB - tA:+.1f} °C vs {year_a}"))
    with m3: st.metric(f"Chuva total — {year_a}", fmt_num(pA, " mm", 1))
    with m4: st.metric(f"Chuva total — {year_b}", fmt_num(pB, " mm", 1),
                       delta=(None if (np.isnan(pA) or np.isnan(pB)) else f"{pB - pA:+.1f} mm vs {year_a}"))

    # ── Tabela final centrada (Plotly) + CSV
    temp_a = temp_w.get(year_a).reindex(range(1,13))
    temp_b = temp_w.get(year_b).reindex(range(1,13))
    rain_a = rain_w.get(year_a).reindex(range(1,13))
    rain_b = rain_w.get(year_b).reindex(range(1,13))
    delta_t = (temp_b - temp_a)
    delta_p = (rain_b - rain_a)

    comp_table = pd.DataFrame({
        "month": list(range(1,13)),
        "month_name": ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"],
        f"t_mean_{year_a}": temp_a.values,
        f"t_mean_{year_b}": temp_b.values,
        "Δ_temp_(°C)": delta_t.values,
        f"precip_{year_a}": rain_a.values,
        f"precip_{year_b}": rain_b.values,
        "Δ_precip_(mm)": delta_p.values,
    })

    st.subheader("Tabela de comparação (mês a mês)")
    fmt = {
        f"t_mean_{year_a}": lambda v: f"{v:.1f}" if pd.notna(v) else "",
        f"t_mean_{year_b}": lambda v: f"{v:.1f}" if pd.notna(v) else "",
        "Δ_temp_(°C)":      lambda v: f"{v:+.1f}" if pd.notna(v) else "",
        f"precip_{year_a}": lambda v: f"{v:.1f}" if pd.notna(v) else "",
        f"precip_{year_b}": lambda v: f"{v:.1f}" if pd.notna(v) else "",
        "Δ_precip_(mm)":    lambda v: f"{v:+.1f}" if pd.notna(v) else "",
    }
    _plotly_centered_table(comp_table, formatters=fmt, key="cmp_table_centered")

    st.download_button(
        "💾 Download CSV (comparação)",
        data=comp_table.to_csv(index=False),
        file_name=f"comparacao_{year_a}_vs_{year_b}.csv",
        mime="text/csv",
        key="dl_csv_cmp"
    )
