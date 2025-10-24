# views/colonizacao.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
from datetime import datetime
import pandas as pd
import streamlit as st
import re

# i18n (mesmo padrão do resto da app)
from services.i18n import t as tr  # tr("colonizacao.title"), etc.

_DATA_DIR_CANDIDATES = [
    Path(__file__).resolve().parents[1] / "data",
    Path.cwd() / "data",
]

_QID_RE = re.compile(r"(Q\d+)$", re.I)

def _data_path(filename: str) -> Path:
    for base in _DATA_DIR_CANDIDATES:
        p = base / filename
        if p.exists():
            return p
    return _DATA_DIR_CANDIDATES[0] / filename

def _read_csv_semicolon(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str, sep=";", encoding="utf-8-sig", engine="python")
    except Exception:
        return pd.read_csv(path, dtype=str, sep=";", engine="python")

def _to_qid(x: str) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    m = _QID_RE.search(s)
    return m.group(1) if m else ""

def _to_year_str(v) -> str:
    """Aceita '0800', '1879-01-01', etc.; devolve 'YYYY' ou ''. Nunca formata com milhares."""
    if v is None:
        return ""
    s = str(v).strip()
    if not s:
        return ""
    s = s[:4]
    try:
        yi = int(s)
        this_year = datetime.now().year
        if 600 <= yi <= this_year:
            return str(yi)
        return ""
    except Exception:
        return ""

def _mtime_sig(path: Path) -> str:
    if path.exists():
        try:
            return f"{path.name}:{path.stat().st_mtime_ns}:{path.stat().st_size}"
        except Exception:
            return f"{path.name}:exists"
    return f"{path.name}:missing"

# --------- loaders em cache ---------

def _colonies_sig() -> str:
    return _mtime_sig(_data_path("colonies_all.enriched.csv"))

@st.cache_data(show_spinner=False)
def _load_countries_seed_cached(sig: str) -> pd.DataFrame:
    p = _data_path("countries_seed.csv")
    if not p.exists():
        p = _data_path("countries_seed.enriched.csv")
    df = _read_csv_semicolon(p)
    if df.empty:
        return pd.DataFrame(columns=["iso3","name_pt","name_en","country_qid"])
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    for c in ("iso3","name_pt","name_en","country_qid"):
        if c not in df.columns: df[c] = ""
    df["iso3"] = df["iso3"].astype(str).str.upper().str.strip()
    df["country_qid"] = df["country_qid"].map(_to_qid)
    return df[["iso3","name_pt","name_en","country_qid"]]

def _load_countries_seed() -> pd.DataFrame:
    return _load_countries_seed_cached(_colonies_sig())

@st.cache_data(show_spinner=False)
def _load_colonies_enriched(sig: str) -> pd.DataFrame:
    """Lê apenas data/colonies_all.enriched.csv com as colunas do teu exemplo."""
    p = _data_path("colonies_all.enriched.csv")
    df = _read_csv_semicolon(p)
    if df.empty:
        return df
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    for c in ("iso3_colonizer","colony_iso3","colony_qid","start_year","end_year","source","colony_label_pt","colony_label_en"):
        if c not in df.columns: df[c] = ""
    df["iso3_colonizer"] = df["iso3_colonizer"].astype(str).str.upper().str.strip()
    df["colony_iso3"]     = df["colony_iso3"].astype(str).str.upper().str.strip()
    df["colony_qid"]      = df["colony_qid"].map(_to_qid)
    return df[["iso3_colonizer","colony_iso3","colony_qid","start_year","end_year","source","colony_label_pt","colony_label_en"]]

def _load_colonies() -> pd.DataFrame:
    return _load_colonies_enriched(_colonies_sig())

# --------- transformação para a língua activa ---------

def _build_view_for_iso3(iso3: str, lang: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    iso3 = (iso3 or "").upper().strip()
    lang = (lang or "pt").lower()

    base = _load_colonies()
    if base.empty:
        return pd.DataFrame(), pd.DataFrame()

    seed = _load_countries_seed()

    # =========================
    # Como colonizador (remove autocolónia)
    # =========================
    d1 = base.loc[(base["iso3_colonizer"] == iso3) & (base["colony_iso3"] != iso3)].copy()
    if not d1.empty:
        d1["De"]  = d1["start_year"].apply(_to_year_str)
        d1["Até"] = d1["end_year"].apply(_to_year_str)
        name_col = "colony_label_pt" if lang == "pt" else "colony_label_en"
        d1[tr("colonizacao.colony")] = d1[name_col].fillna("").astype(str)
        d1["QID"]   = d1["colony_qid"]
        d1["Fonte"] = d1["source"]
        d1 = d1[[tr("colonizacao.colony"), "QID", "De", "Até", "Fonte"]].sort_values(
            by=["De", tr("colonizacao.colony")], ascending=[True, True], kind="mergesort"
        )
    else:
        d1 = pd.DataFrame(columns=[tr("colonizacao.colony"), "QID", "De", "Até", "Fonte"])

    # =========================
    # Como colonizado (remove autocolónia e, por omissão, inferências 'B')
    # =========================
    tmp = base.merge(
        seed.rename(columns={
            "iso3": "iso3_colonizer",
            "country_qid": "colonizer_qid",
            "name_pt": "colonizer_pt",
            "name_en": "colonizer_en",
        }),
        on="iso3_colonizer", how="left"
    )

    d2 = tmp.loc[(tmp["colony_iso3"] == iso3) & (tmp["iso3_colonizer"] != iso3)].copy()
    d2 = d2.query('source != "B"')  # se quiseres ver as inferências, remove esta linha

    if not d2.empty:
        d2["De"]  = d2["start_year"].apply(_to_year_str)
        d2["Até"] = d2["end_year"].apply(_to_year_str)
        name_col = "colonizer_pt" if lang == "pt" else "colonizer_en"
        d2[tr("colonizacao.colonizer")] = d2[name_col].fillna("").astype(str)
        d2["QID"]   = d2["colonizer_qid"].map(_to_qid)
        d2["Fonte"] = d2["source"]
        d2 = d2[[tr("colonizacao.colonizer"), "QID", "De", "Até", "Fonte"]].sort_values(
            by=["De", tr("colonizacao.colonizer")], ascending=[True, True], kind="mergesort"
        )
    else:
        d2 = pd.DataFrame(columns=[tr("colonizacao.colonizer"), "QID", "De", "Até", "Fonte"])

    return d1, d2


def render_colonization_expander(iso3: str, default_open: bool = False) -> None:
    with st.expander(tr("colonizacao.title"), expanded=bool(default_open)):
        if not iso3 or not isinstance(iso3, str):
            st.info(tr("colonizacao.select_country"))
            return

        lang = str(st.session_state.get("lang", "pt")).lower()
        df_colonizer, df_colonized = _build_view_for_iso3(iso3, lang)

        st.write(tr("colonizacao.summary", iso3=iso3, n_col=len(df_colonizer), n_dep=len(df_colonized)))

        col1, col2 = st.columns(2)

        with col1:
            st.caption(tr("colonizacao.left"))
            if df_colonizer.empty:
                st.write(tr("colonizacao.no_records"))
            else:
                h = min(560, 50 + 28 * max(8, len(df_colonizer)))
                st.dataframe(df_colonizer, use_container_width=True, height=h)

        with col2:
            st.caption(tr("colonizacao.right"))
            if df_colonized.empty:
                st.write(tr("colonizacao.no_records"))
            else:
                h = min(560, 50 + 28 * max(8, len(df_colonized)))
                st.dataframe(df_colonized, use_container_width=True, height=h)
