# -*- coding: utf-8 -*-
from __future__ import annotations
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from utils.transform import fmt_num
from utils import charts
from services.i18n import t as tr
try:
    from services.i18n_boot import _ensure_lang_state
except ImportError:
    from services.i18n_boot import init_i18n_state as _ensure_lang_state


# ─────────────────────────────────────────────────────────────
# Tabela centrada (Plotly go.Table) com i18n nos cabeçalhos
# ─────────────────────────────────────────────────────────────
def _plotly_centered_table(
    df: pd.DataFrame,
    headers: list[str] | None = None,
    formatters: dict | None = None,
    key: str | None = None,
):
    if df is None or df.empty:
        st.info(tr("comparison.sem_dados_para_mostrar"))
        return

    d = df.copy()
    if formatters:
        for col, fmt in formatters.items():
            if col in d.columns:
                try:
                    d[col] = d[col].apply(lambda v: "" if pd.isna(v) else fmt(v))
                except Exception:
                    pass

    hdrs = headers or list(d.columns)
    cell_vals = [d[c].tolist() for c in d.columns]
    n_rows = len(d)

    fig = go.Figure(data=[go.Table(
        header=dict(values=hdrs, align="center"),
        cells=dict(values=cell_vals, align="center"),
    )])
    # Altura compacta
    fig.update_layout(margin=dict(l=0, r=0, t=8, b=0), height=int(36 + 32*n_rows + 12))
    st.plotly_chart(fig, width="stretch", key=key)


# ─────────────────────────────────────────────────────────────
# Aba: comparação entre dois anos (mês a mês)
# ─────────────────────────────────────────────────────────────
def render_comparison_tab(dfm: pd.DataFrame):
    _ensure_lang_state()
    st.subheader(tr("comparison.comparacao_entre_2_anos_mes_a_mes"))

    # Anos disponíveis
    years_avail = sorted(pd.to_numeric(dfm["year"], errors="coerce").dropna().astype(int).unique().tolist())
    if len(years_avail) < 2:
        st.info(tr("comparison.precisa_dois_anos"))
        return

    default_b = years_avail[-1]
    default_a = next((y for y in years_avail if y == default_b - 50), years_avail[0])

    cA, cB = st.columns(2)
    with cA:
        idx_a = years_avail.index(default_a) if default_a in years_avail else 0
        year_a = st.selectbox(tr("comparison.ano_a"), years_avail, index=idx_a)
    with cB:
        idx_b = years_avail.index(default_b) if default_b in years_avail else len(years_avail) - 1
        year_b = st.selectbox(tr("comparison.ano_b"), years_avail, index=idx_b)
    if year_a == year_b:
        st.warning(tr("comparison.escolha_anos_diferentes"))

    # Subconjunto e grelha completa mês a mês
    cmp = dfm[dfm["year"].isin([year_a, year_b])][["year", "month", "t_mean", "precip"]].copy()
    full = pd.MultiIndex.from_product([[year_a, year_b], list(range(1, 13))], names=["year", "month"])
    cmp = cmp.set_index(["year", "month"]).reindex(full).reset_index()

    temp_w = cmp.pivot(index="month", columns="year", values="t_mean")
    rain_w = cmp.pivot(index="month", columns="year", values="precip")

    # Rótulos de meses i18n
    tickvals = list(range(1, 13))
    ticktext = [tr(f"months.short.{i}") for i in tickvals]

    # ── Gráficos: temperatura
    c1, c2 = st.columns(2)
    with c1:
        df_temp_plot = temp_w.reset_index().melt(id_vars="month", var_name="Ano", value_name=tr("comparison.temp_c_label"))
        fig_ct = charts.line(
            df_temp_plot, x="month", y=tr("comparison.temp_c_label"), color="Ano",
            title=tr("comparison.temperatura_media_mensal_comparacao"),
            x_title=tr("comparison.mes"), y_title=tr("comparison.c"), markers=True
        )
        fig_ct.update_xaxes(tickmode="array", tickvals=tickvals, ticktext=ticktext)
        st.plotly_chart(fig_ct, width="stretch")
    with c2:
        temp_delta = (temp_w.get(year_b) - temp_w.get(year_a)).rename(tr("comparison.delta_temp_c"))
        fig_ctd = charts.bar(
            temp_delta.reset_index(), x="month", y=tr("comparison.delta_temp_c"),
            title=tr("comparison.diferenca_temperatura_title", a=year_a, b=year_b),
            x_title=tr("comparison.mes"), y_title=tr("comparison.δ_temp_c")
        )
        try:
            absmax = float(np.nanmax(np.abs(temp_delta.values)))
            if absmax > 0:
                fig_ctd.update_yaxes(range=[-absmax * 2, absmax * 2])  # escala simétrica
        except Exception:
            pass
        fig_ctd.update_xaxes(tickmode="array", tickvals=tickvals, ticktext=ticktext)
        st.plotly_chart(fig_ctd, width="stretch")

    # ── Gráficos: precipitação
    c3, c4 = st.columns(2)
    with c3:
        df_rain_plot = rain_w.reset_index().melt(id_vars="month", var_name="Ano", value_name=tr("comparison.rain_mm_label"))
        fig_cp = charts.line(
            df_rain_plot, x="month", y=tr("comparison.rain_mm_label"), color="Ano",
            title=tr("comparison.pluviosidade_mensal_comparacao"),
            x_title=tr("comparison.mes"), y_title=tr("comparison.mm"), markers=True
        )
        fig_cp.update_xaxes(tickmode="array", tickvals=tickvals, ticktext=ticktext)
        st.plotly_chart(fig_cp, width="stretch")
    with c4:
        rain_delta = (rain_w.get(year_b) - rain_w.get(year_a)).rename(tr("comparison.delta_rain_mm"))
        fig_cpd = charts.bar(
            rain_delta.reset_index(), x="month", y=tr("comparison.delta_rain_mm"),
            title=tr("comparison.diferenca_precipitacao_title", a=year_a, b=year_b),
            x_title=tr("comparison.mes"), y_title=tr("comparison.δ_chuva_mm")
        )
        fig_cpd.update_xaxes(tickmode="array", tickvals=tickvals, ticktext=ticktext)
        st.plotly_chart(fig_cpd, width="stretch")

    # ── Resumo anual
    ann = (dfm[dfm["year"].isin([year_a, year_b])]
           .groupby("year", as_index=False, observed=False)
           .agg(t_year=("t_mean", "mean"), p_year=("precip", "sum")))
    tA = float(ann.loc[ann["year"] == year_a, "t_year"].iloc[0]) if (ann["year"] == year_a).any() else np.nan
    tB = float(ann.loc[ann["year"] == year_b, "t_year"].iloc[0]) if (ann["year"] == year_b).any() else np.nan
    pA = float(ann.loc[ann["year"] == year_a, "p_year"].iloc[0]) if (ann["year"] == year_a).any() else np.nan
    pB = float(ann.loc[ann["year"] == year_b, "p_year"].iloc[0]) if (ann["year"] == year_b).any() else np.nan

    st.subheader(tr("comparison.resumo_anual"))
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric(tr("comparison.temp_media_anual") + f" — {year_a}", fmt_num(tA, " °C"))
    with m2:
        st.metric(tr("comparison.temp_media_anual") + f" — {year_b}",
                  fmt_num(tB, " °C"),
                  delta=(None if (np.isnan(tA) or np.isnan(tB)) else f"{tB - tA:+.1f} °C · {year_b}-{year_a}"))
    with m3:
        st.metric(tr("comparison.chuva_total") + f" — {year_a}", fmt_num(pA, " mm", 1))
    with m4:
        st.metric(tr("comparison.chuva_total") + f" — {year_b}",
                  fmt_num(pB, " mm", 1),
                  delta=(None if (np.isnan(pA) or np.isnan(pB)) else f"{pB - pA:+.1f} mm · {year_b}-{year_a}"))

    # ── Tabela final centrada (Plotly) + CSV
    temp_a = temp_w.get(year_a).reindex(range(1, 13))
    temp_b = temp_w.get(year_b).reindex(range(1, 13))
    rain_a = rain_w.get(year_a).reindex(range(1, 13))
    rain_b = rain_w.get(year_b).reindex(range(1, 13))
    delta_t = (temp_b - temp_a)
    delta_p = (rain_b - rain_a)

    comp_table = pd.DataFrame({
        tr("cols.month_num"): list(range(1, 13)),
        tr("cols.month"):     [tr(f"months.short.{m}") for m in range(1, 13)],
        f"{tr('comparison.temp_media_anual')} — {year_a}": temp_a.values,
        f"{tr('comparison.temp_media_anual')} — {year_b}": temp_b.values,
        tr("comparison.δ_temp_c"):            delta_t.values,
        f"{tr('comparison.chuva_total')} — {year_a}": rain_a.values,
        f"{tr('comparison.chuva_total')} — {year_b}": rain_b.values,
        tr("comparison.δ_chuva_mm"):          delta_p.values,
    })

    st.subheader(tr("comparison.tabela_de_comparacao_mes_a_mes"))
    fmt = {
        f"{tr('comparison.temp_media_anual')} — {year_a}": lambda v: f"{v:.1f}" if pd.notna(v) else "",
        f"{tr('comparison.temp_media_anual')} — {year_b}": lambda v: f"{v:.1f}" if pd.notna(v) else "",
        tr("comparison.δ_temp_c"):      lambda v: f"{v:+.1f}" if pd.notna(v) else "",
        f"{tr('comparison.chuva_total')} — {year_a}": lambda v: f"{v:.1f}" if pd.notna(v) else "",
        f"{tr('comparison.chuva_total')} — {year_b}": lambda v: f"{v:.1f}" if pd.notna(v) else "",
        tr("comparison.δ_chuva_mm"):    lambda v: f"{v:+.1f}" if pd.notna(v) else "",
    }
    _plotly_centered_table(
        comp_table,
        headers=list(comp_table.columns),
        formatters=fmt,
        key="cmp_table_centered"
    )

    # st.download_button(
    #     tr("comparison.download_csv_comparacao"),
    #     data=comp_table.to_csv(index=False),
    #     file_name=f"comparacao_{year_a}_vs_{year_b}.csv",
    #     mime="text/csv",
    #     key="dl_csv_cmp"
    # )
