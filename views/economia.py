# views/economia.py
# -*- coding: utf-8 -*-
"""
Painel de Indicadores Económicos (WDI / World Bank)
- Donuts (VAB %PIB e Emprego %) com paleta consistente
- Área empilhada normalizada com a MESMA paleta
- Leitura de data/wdi_economics.csv (sep=";") com colunas: iso3;code;year;value
"""

from __future__ import annotations

import pandas as pd
import streamlit as st
import altair as alt
from functools import lru_cache
from pathlib import Path

# Integrações do teu projeto (funcionam se existirem; há fallbacks abaixo)
try:
    from services.i18n_boot import _ensure_lang_state  # type: ignore
    from services.i18n import t as tr  # type: ignore
    from services.countries_names import country_display_name  # type: ignore
except Exception:  # fallbacks para correr isolado
    def _ensure_lang_state() -> None:
        return
    def tr(key: str, default: str | None = None) -> str:
        return default or key
    def country_display_name(iso3: str) -> str:
        return iso3

DATA_CSV = Path("data/wdi_economics.csv")

# -----------------------------
# Mapeamento de indicadores
# -----------------------------
# VAB (% do PIB)
VAB_AGR = "NV.AGR.TOTL.ZS"
VAB_IND = "NV.IND.TOTL.ZS"
VAB_SRV = "NV.SRV.TOTL.ZS"

# Emprego (%)
EMP_AGR = "SL.AGR.EMPL.ZS"
EMP_IND = "SL.IND.EMPL.ZS"
EMP_SRV = "SL.SRV.EMPL.ZS"

VAB_CODES = [VAB_SRV, VAB_IND, VAB_AGR]
EMP_CODES = [EMP_SRV, EMP_IND, EMP_AGR]

# -----------------------------
# Rotulagem (PT/EN) e paleta
# -----------------------------

def _sector_label_map() -> dict[str, str]:
    """
    Devolve labels legíveis por código WDI (respeitando i18n se disponível).
    """
    return {
        VAB_SRV: tr("economics.sectors.vab.services", "Terciário (Serviços, % PIB)"),
        VAB_IND: tr("economics.sectors.vab.industry", "Secundário (Indústria, % PIB)"),
        VAB_AGR: tr("economics.sectors.vab.agri",     "Primário (Agri, % PIB)"),
        EMP_SRV: tr("economics.sectors.emp.services", "Serviços (% emprego)"),
        EMP_IND: tr("economics.sectors.emp.industry", "Indústria (% emprego)"),
        EMP_AGR: tr("economics.sectors.emp.agri",     "Agricultura (% emprego)"),
    }

def _sector_palette(lbl: dict[str, str]) -> dict[str, str]:
    """
    Paleta única e coerente por label:
      Serviços -> azul-claro, Indústria -> azul, Agricultura -> rosa
    """
    COLORS = {
        "srv": "#9ecae1",  # light blue
        "ind": "#2c7fb8",  # blue
        "agr": "#fcbba1",  # pink
    }
    return {
        # VAB
        lbl[VAB_SRV]: COLORS["srv"],
        lbl[VAB_IND]: COLORS["ind"],
        lbl[VAB_AGR]: COLORS["agr"],
        # Emprego
        lbl[EMP_SRV]: COLORS["srv"],
        lbl[EMP_IND]: COLORS["ind"],
        lbl[EMP_AGR]: COLORS["agr"],
    }

# -----------------------------
# Leitura e helpers de dados
# -----------------------------

@lru_cache(maxsize=1)
def _load_all_economics() -> pd.DataFrame:
    if not DATA_CSV.exists():
        return pd.DataFrame(columns=["iso3", "code", "year", "value"])
    df = pd.read_csv(DATA_CSV, sep=";", dtype={"iso3": str, "code": str}, encoding="utf-8")
    # normalizações
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["iso3"] = df["iso3"].str.upper().str.strip()
    df["code"] = df["code"].str.strip()
    return df.dropna(subset=["iso3", "code", "year"])

def _subset_iso3_codes(iso3: str, codes: list[str]) -> pd.DataFrame:
    df = _load_all_economics()
    if df.empty:
        return df
    return df[(df["iso3"] == iso3.upper()) & (df["code"].isin(codes))].copy()

def _latest_values(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    # pega no último ano com pelo menos um valor válido por código
    df = df.copy()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    latest_year = int(df.dropna(subset=["value"])["year"].max()) if not df.dropna(subset=["value"]).empty else None
    if latest_year is None:
        return df.iloc[0:0]
    return df[df["year"] == latest_year]

# -----------------------------
# Gráficos
# -----------------------------

def _donut_fig(labels: list[str], values: list[float], title: str, color_map: dict[str, str] | None = None):
    import plotly.express as px
    df = pd.DataFrame({"label": labels, "value": values})
    fig = px.pie(
        df, names="label", values="value", hole=0.55,
        color="label",
        color_discrete_map=(color_map or {})
    )
    fig.update_traces(textinfo="percent", hovertemplate="%{label}: %{value:.2f}%<extra></extra>")
    fig.update_layout(
        showlegend=True,
        legend=dict(orientation="h", y=-0.12),
        margin=dict(l=10, r=10, t=30, b=10),
        height=320, title=title
    )
    return fig

def _stacked_area(long_df: pd.DataFrame, domain: list[str], rng: list[str]) -> alt.Chart:
    ch = (
        alt.Chart(long_df)
        .mark_area(opacity=0.85)
        .encode(
            x=alt.X("year:Q", axis=alt.Axis(format="d", title=tr("economics.metrics.year", "Ano"))),
            y=alt.Y("value:Q", stack="normalize", axis=alt.Axis(format=".0%"), title=None),
            color=alt.Color(
                "code:N", title="",
                legend=alt.Legend(orient="bottom"),
                scale=alt.Scale(domain=domain, range=rng)
            ),
            tooltip=[
                alt.Tooltip("code:N", title=tr("paises.indicador", "Indicador")),
                alt.Tooltip("year:Q", title=tr("economics.metrics.year", "Ano"), format="d"),
                alt.Tooltip("value:Q", title=tr("paises.valor", "Valor"), format=".2f"),
            ],
        )
        .properties(height=300, width="container")
    )
    return ch

# -----------------------------
# Painel principal
# -----------------------------

def render_wdi_panel(iso3: str, country_name: str | None = None) -> None:
    """
    Renderiza o painel de setores com paleta coerente.
    """
    _ensure_lang_state()
    lbl = _sector_label_map()
    palette = _sector_palette(lbl)
    country = country_name or country_display_name(iso3)

    st.markdown(f"## {tr('economics.title', 'Setores da Economia')} — {country}")

    # ---- Donuts (lado a lado) ----
    c1, c2 = st.columns(2)

    # VAB (% do PIB)
    vab_df = _subset_iso3_codes(iso3, VAB_CODES)
    vab_last = _latest_values(vab_df)
    if not vab_last.empty:
        # ordenar como no donut: Agr, Ind, Serv para ficar gradual (rosa, azul, azul-claro)
        order = [VAB_AGR, VAB_IND, VAB_SRV]
        vals = [float(vab_last.loc[vab_last["code"] == k, "value"].dropna().values[0]) if not vab_last.loc[vab_last["code"] == k, "value"].dropna().empty else 0.0
                for k in order]
        labels = [lbl[k] for k in order]
        with c1:
            st.plotly_chart(
                _donut_fig(labels, vals, tr("economics.sectors.vab.title", ""), color_map=palette),
                use_container_width=True, config={"displayModeBar": False}
            )
    else:
        with c1:
            st.info(tr("economics.sectors.vab.empty", "Sem dados de VAB (% do PIB)."))

    # Emprego (% do total)
    emp_df = _subset_iso3_codes(iso3, EMP_CODES)
    emp_last = _latest_values(emp_df)
    if not emp_last.empty:
        order = [EMP_AGR, EMP_IND, EMP_SRV]
        vals = [float(emp_last.loc[emp_last["code"] == k, "value"].dropna().values[0]) if not emp_last.loc[emp_last["code"] == k, "value"].dropna().empty else 0.0
                for k in order]
        labels = [lbl[k] for k in order]
        with c2:
            st.plotly_chart(
                _donut_fig(labels, vals, tr("economics.sectors.emp.title", ""), color_map=palette),
                use_container_width=True, config={"displayModeBar": False}
            )
    else:
        with c2:
            st.info(tr("economics.sectors.emp.empty", "Sem dados de emprego (%)."))

    # ---- Evolução temporal (área empilhada) ----
    st.markdown(f"#### {tr('economics.sectors.evolution', 'Evolução setorial ao longo do tempo (%)')}")

    # Alternador entre VAB e Emprego (mantém chave estável para não perder seleção ao trocar país)
    view = st.radio(
        label="",
        options=[tr("economics.presets.sectors_vab", "Setores — VAB (% do PIB)"),
                 tr("economics.presets.sectors_emp", "Setores — Emprego (%)")],
        index=0,
        horizontal=True,
        key=f"sectors_view_{iso3.lower()}",
    )

    if view == tr("economics.presets.sectors_vab", "Setores — VAB (% do PIB)"):
        df = _subset_iso3_codes(iso3, VAB_CODES)
        if df.empty:
            st.info(tr("economics.sectors.vab.empty", "Sem dados de VAB (% do PIB)."))
            return
        # renomear para labels
        long = df.copy()
        long["code"] = long["code"].map(lambda c: lbl.get(c, c))
        domain = [lbl[VAB_SRV], lbl[VAB_IND], lbl[VAB_AGR]]
        rng = [palette[d] for d in domain]
        st.altair_chart(_stacked_area(long, domain, rng), use_container_width=True)

    else:
        df = _subset_iso3_codes(iso3, EMP_CODES)
        if df.empty:
            st.info(tr("economics.sectors.emp.empty", "Sem dados de emprego (%)."))
            return
        long = df.copy()
        long["code"] = long["code"].map(lambda c: lbl.get(c, c))
        domain = [lbl[EMP_SRV], lbl[EMP_IND], lbl[EMP_AGR]]
        rng = [palette[d] for d in domain]
        st.altair_chart(_stacked_area(long, domain, rng), use_container_width=True)


# -----------------------------
# Execução direta (debug local)
# -----------------------------
if __name__ == "__main__":
    st.set_page_config(page_title="Economia", layout="wide")
    iso = st.sidebar.text_input("ISO3", value="PRT").upper()
    nm = country_display_name(iso)
    render_wdi_panel(iso, nm)
