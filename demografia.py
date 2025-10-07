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
      - Continentes (usa a tua visão global antiga)
      - Comparar países (placeholder)
      - Fluxos migratórios globais (se houver CSV DESA)
    """
    _ensure_lang_state()
    st.title(tr("demografia.title") if "demografia.title" in tr.__code__.co_consts else "📊 Demografia")

    mode = subnav(
        "demografia",
        [
            ("glob",    tr("subnav.continentes") if "subnav.continentes" in tr.__code__.co_consts else "Continentes"),
            ("compare", tr("subnav.comparar_paises") if "subnav.comparar_paises" in tr.__code__.co_consts else "Comparar países"),
            ("fluxos",  tr("subnav.fluxos_migratorios") if "subnav.fluxos_migratorios" in tr.__code__.co_consts else "Fluxos migratórios"),
        ],
        default="glob",
    )

    _FRAG = getattr(st, "fragment", None)

    def _render_global_inner():
        if _RENDER_GLOBAL is not None:
            _RENDER_GLOBAL()
        else:
            st.warning("Não encontrei 'demografia_global.render_indicadores_tab()'.")

    def _render_compare_inner():
        render_demografia_compare()

    def _render_fluxos_inner():
        render_migration_global()

    if mode == "glob":
        (_FRAG(_render_global_inner) if _FRAG else _render_global_inner)()
    elif mode == "compare":
        (_FRAG(_render_compare_inner) if _FRAG else _render_compare_inner)()
    elif mode == "fluxos":
        (_FRAG(_render_fluxos_inner) if _FRAG else _render_fluxos_inner)()
    else:
        st.error("Modo desconhecido.")

__all__ = ["render_demografia_tab"]
