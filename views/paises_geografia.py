# views/paises_geografia.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import math
from typing import List
import pandas as pd
import streamlit as st

# Dados (novo módulo com os loads de Geografia)
from services import geo_store as store
# i18n (t == tr)
from services.i18n import t as tr

from services.i18n_boot import _ensure_lang_state
# p = Path("services/i18n/geo.en.json")  # ajusta o caminho
# s = p.read_text(encoding="utf-8")
# try:
#     json.loads(s)
# except json.JSONDecodeError as e:
#     i = e.pos
#     print("Erro em pos:", i, "linha:", e.lineno, "col:", e.colno)
#     print(repr(s[max(0,i-40):i+40]))


# ───────────────────────── helpers UI ─────────────────────────

def _badge_list(items: List[str]) -> str:
    """HTML com badges (“pills”) para listas curtas (ex.: ISO3 vizinhos)."""
    if not items:
        return '<span style="color:#666">—</span>'
    pills = []
    for x in items:
        x = str(x).strip()
        if not x:
            continue
        pills.append(
            f'<span style="display:inline-block;background:#F3F4F6;color:#111;'
            f'border:1px solid #E5E7EB;padding:2px 8px;border-radius:999px;'
            f'font-size:0.85rem;margin:2px 6px 2px 0;">{x}</span>'
        )
    return "".join(pills) or '<span style="color:#666">—</span>'


def _koppen_label(k: str) -> str:
    """Descrição breve da classe Köppen (mapa mínimo em PT para UX)."""
    if not k:
        return ""
    k = k.strip()
    mapping = {
        "Af": "Tropical florestal (equatorial, húmido)",
        "Am": "Tropical monçónico",
        "Aw": "Tropical savana (inverno seco)",
        "BWh": "Desértico quente", "BWk": "Desértico frio",
        "BSh": "Estepe quente (semiárido)", "BSk": "Estepe frio (semiárido)",
        "Csa": "Mediterrânico quente (verões quentes e secos)",
        "Csb": "Mediterrânico ameno (verões amenos e secos)",
        "Cfa": "Temperado húmido (verão quente)", "Cfb": "Temperado húmido oceânico",
        "Dfa": "Continental húmido (verão quente)", "Dfb": "Continental húmido (verão ameno)",
        "Dfc": "Subártico", "ET": "Tundra", "EF": "Gelo perpétuo",
    }
    return mapping.get(k, "")


def _format_float(x, nd=1):
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return "—"
        return f"{float(x):.{nd}f}"
    except Exception:
        return "—"


def _split_list(s: str) -> list[str]:
    """Divide por vírgula/ponto-e-vírgula/pipe e limpa espaços."""
    import re
    return [x.strip() for x in re.split(r"[|;,]", s or "") if x.strip()]


def _pick_lang(pt_val: str | None, en_val: str | None, lang: str) -> str:
    """Devolve o valor na língua pedida com fallback para a outra."""
    pt = (pt_val or "").strip()
    en = (en_val or "").strip()
    if (lang or "pt").lower().startswith("pt"):
        return pt or en
    return en or pt


# ─────────────────────── painel principal ───────────────────────

def render_geography_panel(iso3: str, country_name: str) -> None:
    """
    Render da aba 'Geografia' para um país.

    Data sources (via services/geo_store.py):
      - borders_for_iso3(iso3)               → fronteiras (terra), c/ 'neighbor_iso3','neighbor_name','land_km_num'
      - timezones_for_iso3(iso3)             → fusos (labels 'UTC±…' na col. 'tz_label')
      - geografia_for_iso3(iso3)             → linha-resumo c/ 'seasons_estimate','capital_lat','capital_lon'
      - koppen_for_iso3(iso3)                → 1 linha c/ 'koppen' (ou 'class')
      - biomes_for_iso3(iso3)                → colunas 'biome','share_pct'
      - coastlines_for_iso3(iso3) (opcional) → 'has_coast','coast_km','adjacent_seas_pt','adjacent_seas_en'
      - ports_and_routes_for_iso3(iso3)      → 'ports_pt','ports_en','waters_pt','waters_en'
    """
    iso3 = (iso3 or "").upper().strip()
    lang = st.session_state.get("lang", "pt")
    _ensure_lang_state() 
    # ===== Carregamento seguro =====
    df_borders = store.borders_for_iso3(iso3)            # DataFrame
    df_tz      = store.timezones_for_iso3(iso3)          # DataFrame ('tz_label')
    geo_row    = store.geografia_for_iso3(iso3)          # Series ou None
    df_kopp    = store.koppen_for_iso3(iso3)             # DataFrame (0/1 linhas)
    df_biomes  = store.biomes_for_iso3(iso3)             # DataFrame
    df_coast   = store.coastlines_for_iso3(iso3)         # DataFrame
    # aceitar ambos os nomes por compat (alias existe no geo_store)
    try:
        df_ports = store.ports_and_routes_for_iso3(iso3)
    except Exception:
        df_ports = store.ports_routes_for_iso3(iso3)

    # ===== Cabeçalho ==========================================================
    st.subheader(tr("geo.header"))

    # ===== Cartões resumidos (KPIs) ===========================================
    c1, c2, c3, c4 = st.columns([1.2, 1.2, 1.2, 1])

    # (1) Nº de vizinhos terrestres
    n_vizinhos = 0
    if not df_borders.empty and "neighbor_iso3" in df_borders.columns:
        n_vizinhos = int(df_borders["neighbor_iso3"].nunique())
    c1.metric(tr("geo.metrics.borders"), f"{n_vizinhos}")

    # (2) Nº de fusos (labels UTC…)
    n_tz = 0
    if not df_tz.empty and "tz_label" in df_tz.columns:
        n_tz = int(df_tz["tz_label"].nunique())
    c2.metric(tr("geo.metrics.timezones"), f"{n_tz}")

    # (3) Nº de estações (estimativa)
    seasons = "—"
    if isinstance(geo_row, pd.Series):
        val = geo_row.get("seasons_estimate", "")
        seasons = int(val) if str(val).strip().isdigit() else "—"
    c3.metric(tr("geo.metrics.seasons"), f"{seasons}")

    # (4) Köppen dominante
    k_class = ""
    if not df_kopp.empty:
        # aceitar 'koppen' ou 'class'
        row0 = df_kopp.iloc[0]
        k_class = str(row0.get("koppen") or row0.get("class") or "").strip()
    c4.metric(tr("geo.metrics.koppen"), k_class or "—")

    # ===== Secções detalhadas =================================================

    # --- Fronteiras -----------------------------------------------------------
    st.markdown(f"### {tr('geo.sections.borders.title')}")
    if df_borders.empty:
        st.info(tr("geo.sections.borders.empty"))
    else:
        cols = []
        if "neighbor_iso3" in df_borders.columns: cols.append("neighbor_iso3")
        if "neighbor_name" in df_borders.columns: cols.append("neighbor_name")
        if "land_km_num" in df_borders.columns:   cols.append("land_km_num")
        view = df_borders[cols].rename(columns={
            "neighbor_iso3": tr("geo.sections.borders.table.iso3"),
            "neighbor_name": tr("geo.sections.borders.table.name"),
            "land_km_num":   tr("geo.sections.borders.table.km"),
        }).copy()
        km_col = tr("geo.sections.borders.table.km")
        if km_col in view.columns:
            view[km_col] = view[km_col].apply(lambda x: _format_float(x, 0))
        st.dataframe(view, use_container_width=True, hide_index=True)

        # Chips com ISO3
        if "neighbor_iso3" in df_borders.columns:
            viz = sorted([x for x in df_borders["neighbor_iso3"].dropna().unique().tolist() if x])
            st.markdown(_badge_list(viz), unsafe_allow_html=True)

    # --- Fuso horário (lista) -------------------------------------------------
    st.markdown(f"### {tr('geo.sections.timezones.title')}")
    if df_tz.empty or "tz_label" not in df_tz.columns:
        st.info(tr("geo.sections.timezones.empty"))
    else:
        tz_list = sorted([s for s in df_tz["tz_label"].dropna().unique().tolist() if s])
        st.markdown(_badge_list(tz_list), unsafe_allow_html=True)

    # --- Litoral / Mares adjacentes ------------------------------------------
    st.markdown(f"### {tr('geo.sections.coast.title')}")
    if df_coast.empty:
        st.info(tr("geo.sections.coast.empty"))
    else:
        r = df_coast.iloc[0].to_dict()
        has_coast = str(r.get("has_coast", "")).strip()
        coast_km  = _format_float(r.get("coast_km", None), 0)
        st.write(f"**{tr('geo.sections.coast.has_coast')}** {has_coast or '—'}")
        st.write(f"**{tr('geo.sections.coast.coast_len')}** {coast_km} km")

        # mares adjacentes — mostrar apenas a língua selecionada (com fallback)
        seas_txt = _pick_lang(r.get("adjacent_seas_pt"), r.get("adjacent_seas_en"), lang)
        seas = _split_list(seas_txt)
        if seas:
            st.caption(tr("geo.sections.coast.seas.caption"))
            st.markdown(_badge_list(seas), unsafe_allow_html=True)
        else:
            st.caption(tr("geo.sections.coast.seas.none"))

    # --- Portos & Rotas (estreitos/canais) -----------------------------------
    st.markdown(f"### {tr('geo.sections.ports.title')}")
    if df_ports.empty:
        st.info(tr("geo.sections.ports.empty"))
    else:
        r = df_ports.iloc[0].to_dict()

        # Portos (uma língua + fallback)
        ports_txt = _pick_lang(r.get("ports_pt"), r.get("ports_en"), lang)
        ports = _split_list(ports_txt)

        st.caption(tr("geo.sections.ports.caption_main"))
        st.markdown(_badge_list(ports), unsafe_allow_html=True)

        # Rotas/Passagens de água (uma língua + fallback)
        waters_txt = _pick_lang(r.get("waters_pt"), r.get("waters_en"), lang)
        waters = _split_list(waters_txt)

        st.caption(tr("geo.sections.ports.caption_waters"))
        st.markdown(_badge_list(waters), unsafe_allow_html=True)

    # --- Clima (Köppen) ------------------------------------------------------
    st.markdown(f"### {tr('geo.sections.climate.title')}")
    if not k_class:
        st.warning(tr("geo.sections.climate.empty"))
    else:
        k_label = _koppen_label(k_class)
        st.write(f"**{tr('geo.sections.climate.label')}** `{k_class}`" + (f" — {k_label}" if k_label else ""))
        if isinstance(geo_row, pd.Series):
            lat = geo_row.get("capital_lat", None)
            lon = geo_row.get("capital_lon", None)
            st.caption(tr("geo.sections.climate.capital_coords").format(
                lat=_format_float(lat, 3), lon=_format_float(lon, 3)
            ))

    # --- Biomas ---------------------------------------------------------------
    st.markdown(f"### {tr('geo.sections.biomes.title')}")
    if df_biomes.empty:
        st.info(tr("geo.sections.biomes.empty"))
    else:
        vb = df_biomes.copy()
        keep = [c for c in ("biome", "share_pct") if c in vb.columns]
        vb = vb[keep]
        if "share_pct" in vb.columns:
            vb["share_pct"] = pd.to_numeric(vb["share_pct"], errors="coerce")
            vb = vb.sort_values("share_pct", ascending=False)
            vb["share_pct"] = vb["share_pct"].map(lambda v: f"{v:.1f}%" if pd.notna(v) else "—")
        vb = vb.rename(columns={
            "biome": tr("geo.sections.biomes.table.biome"),
            "share_pct": tr("geo.sections.biomes.table.area_pct"),
        })
        st.dataframe(vb, use_container_width=True, hide_index=True)

    # --- Nota de fontes -------------------------------------------------------
    st.caption(tr("geo.sources.caption"))


# Conveniência: correr como script para teste rápido
if __name__ == "__main__":
    import sys
    # Definir língua por omissão em execução direta (sem i18n_boot)
    st.session_state.lang = st.session_state.get("lang", "pt")
    st.set_page_config(page_title="Geografia — País", page_icon="🗺️", layout="wide")
    iso = "PRT"
    if len(sys.argv) > 1:
        iso = sys.argv[1].upper()
    render_geography_panel(iso, "País")
