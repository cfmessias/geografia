# demografia.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Optional, Callable

import pandas as pd
import streamlit as st

# i18n
try:
    from services.i18n import t as tr
except Exception:
    def tr(key: str, **kwargs) -> str:
        s = key
        try:
            return s.format(**kwargs) if kwargs else s
        except Exception:
            return s

try:
    from services.i18n_boot import _ensure_lang_state
except Exception:
    def _ensure_lang_state():
        return

# submenu
from utils.subnav import subnav

# importa a TUA visão global antiga (renomeaste para demografia_global.py na raiz)
_RENDER_GLOBAL: Optional[Callable[[], None]] = None
try:
    from demografia_global import render_indicadores_tab as _RENDER_GLOBAL
except Exception:
    _RENDER_GLOBAL = None


def _load_un_desa_long() -> Optional[pd.DataFrame]:
    """
    Tenta carregar CSV longo (origin_name,dest_name,year,value,source)
    gerado pelos teus scripts (DESA).
    """
    root = Path(__file__).resolve().parent
    candidates = [
        root / "data" / "migration" / "un_desa_od_long_2024.csv",
        root / "data" / "migration" / "un_desa_od_long_2020.csv",
    ]
    for p in candidates:
        if p.exists():
            try:
                df = pd.read_csv(p)
                req = {"origin_name", "dest_name", "year", "value"}
                if req.issubset(set(df.columns.astype(str))):
                    return df
            except Exception:
                pass
    return None


def render_demografia_compare():
    st.info(tr("labels.label_val", label="ℹ️", val="Em construção…"))


def render_migration_global():
    df = _load_un_desa_long()
    if df is None or df.empty:
        st.caption("— sem dados globais de migrações — gera primeiro data/migration/un_desa_od_long_202*.csv")
        return

    years = sorted(pd.to_numeric(df["year"], errors="coerce").dropna().unique().tolist())
    year = st.selectbox(tr("filters.ano") if "filters.ano" in tr.__code__.co_consts else "Ano",
                        years, index=len(years) - 1 if years else 0)

    sub = df[df["year"] == year].copy()
    sub["value"] = pd.to_numeric(sub["value"], errors="coerce").fillna(0)

    topn = st.slider("Top N", 5, 50, 20, 1)
    top = (sub.sort_values("value", ascending=False)
              .head(topn)[["origin_name", "dest_name", "value"]]
              .reset_index(drop=True))
    st.subheader("🌍 Top fluxos globais")
    st.dataframe(top, hide_index=True, use_container_width=True)

    c1, c2 = st.columns(2)
    with c1:
        by_origin = (sub.groupby("origin_name", as_index=False)["value"].sum()
                        .sort_values("value", ascending=False).head(10))
        st.caption("🔼 Maiores origens")
        st.dataframe(by_origin, hide_index=True, use_container_width=True)
    with c2:
        by_dest = (sub.groupby("dest_name", as_index=False)["value"].sum()
                      .sort_values("value", ascending=False).head(10))
        st.caption("🔽 Maiores destinos")
        st.dataframe(by_dest, hide_index=True, use_container_width=True)


def render_demografia_tab():
    """
    Página 'Demografia' com submenu:
      - Continentes
      - Comparar países
      - Fluxos migratórios globais
    """
    _ensure_lang_state()

    # título (usa i18n; fallback simples)
    try:
        title = tr("demografia.title")
    except Exception:
        title = "📊 Demografia"
    st.title(title)

    # tabs (usa chaves do JSON; sem hacks ao __code__)
    try:
        label_cont = tr("subnav.continentes")
        label_comp = tr("subnav.comparar_paises")
        label_flux = tr("subnav.fluxos_migratorios")
    except Exception:
        label_cont, label_comp, label_flux = "Continentes", "Comparar países", "Fluxos migratórios"

    mode = subnav(
        "demografia",
        [
            ("glob",    label_cont),
            ("compare", label_comp),
            ("fluxos",  label_flux),
        ],
        default="glob",
    )

    _FRAG = getattr(st, "fragment", None)

    def _render_global_inner():
        if _RENDER_GLOBAL is not None:
            _RENDER_GLOBAL()
        else:
            st.warning("Não encontrei 'demografia_global.render_indicadores_tab()'.")

    if mode == "glob":
        _render_global_inner()
    elif mode == "compare":
        st.info("🧪 Em breve: comparação entre países.")
    elif mode == "fluxos":
        # coloca aqui o renderer dos fluxos se existir
        st.info("🌍 Em breve: fluxos migratórios globais.")
