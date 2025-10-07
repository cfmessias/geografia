# views/languages.py
from __future__ import annotations
from pathlib import Path
import pandas as pd
import streamlit as st
from services.i18n import t as tr

# Caminhos padrão (ajusta se necessário)
ROOT = Path(__file__).resolve().parents[1]
OFF_PATHS  = [
    ROOT / "data" / "country_languages_official.enriched.csv",
    ROOT / "data" / "country_languages_official.csv",  # fallback
]
USED_PATHS = [
    ROOT / "data" / "country_languages_used.enriched.csv",
    ROOT / "data" / "country_languages_used.csv",      # fallback
]
def _ensure_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df

def _read_any(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    # autodetecta separador
    try:
        return pd.read_csv(path, sep=None, engine="python", dtype=str)
    except Exception:
        try:
            return pd.read_csv(path, sep=";", dtype=str)
        except Exception:
            return pd.read_csv(path, dtype=str)

def _load_first(paths: list[Path]) -> pd.DataFrame:
    for p in paths:
        df = _read_any(p)
        if df is not None:
            return df
    return pd.DataFrame()

def _pick_label_cols(df: pd.DataFrame) -> tuple[str, str]:
    """
    Decide as colunas de label de acordo com a língua da UI.
    Retorna (lang_label_col, region_label_col).
    """
    lang = (getattr(st.session_state, "lang", "pt") or "pt").lower()
    lang_col_candidates = [f"lang_label_{lang}", "lang_label_pt", "lang_label_en", "lang_label"]
    reg_col_candidates  = [f"region_label_{lang}", "region_label_pt", "region_label_en", "region_label"]

    def _first_ok(cands):
        for c in cands:
            if c in df.columns:
                return c
        return cands[0]

    return _first_ok(lang_col_candidates), _first_ok(reg_col_candidates)

def _fmt_list(items: list[str]) -> str:
    # lista curta bonitinha: "A", "A e B", "A, B e C"
    items = [s for s in items if s and str(s).strip()]
    if not items:
        return "—"
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} · {items[1]}"
    return ", ".join(items[:-1]) + " · " + items[-1]

def render_country_languages_line(iso3: str) -> None:
    """
    Linha compacta para o cartão de topo:
    mostra as línguas oficiais **nacionais** (não regionais).
    Se não houver nacionais, cai para todas as oficiais.
    """
    iso3u = (iso3 or "").upper()
    off = _load_first(OFF_PATHS)
    if off.empty or "iso3" not in off.columns:
        return

    # limpar e filtrar
    off["iso3"] = off["iso3"].astype(str).str.upper().str.strip()
    base = off[off["iso3"] == iso3u].copy()
    if base.empty:
        return

    lang_col, reg_col = _pick_label_cols(base)

    # preferir oficiais nacionais; se não houver, usar todas
    nat = base[base.get("scope", "").astype(str).str.lower().eq("national")]
    show = nat if not nat.empty else base

    # deduplicar por QID da língua (se existir)
    if "lang_qid" in show.columns:
        show = show.drop_duplicates(subset=["lang_qid"])
    else:
        show = show.drop_duplicates(subset=[lang_col])

    # construir lista de nomes (podes incluir ISO se quiseres)
    names = (show[lang_col].fillna("")
                   .astype(str)
                   .str.strip()
                   .tolist())
    label = tr("paises.linguas_oficiais")  # adicionar aos teus JSON
    st.markdown(tr("labels.label_val", label=label, val=_fmt_list(names)))

def render_country_languages_expander(iso3: str, *, default_open: bool = False) -> None:
    """
    Expander com co-oficiais regionais (tabela) + outras línguas usadas (texto com contador).
    """
    iso3u = (iso3 or "").upper()
    off = _load_first(OFF_PATHS)
    used = _load_first(USED_PATHS)

    title = tr("paises.outras_linguas")
    with st.expander(title, expanded=default_open):
        # ---------- Co-oficiais regionais ----------
        if not off.empty and "iso3" in off.columns:
            off["iso3"] = off["iso3"].astype(str).str.upper().str.strip()
            reg = off[(off["iso3"] == iso3u) & (off.get("scope","").astype(str).str.lower().eq("regional"))].copy()
        else:
            reg = pd.DataFrame()

        if not reg.empty:
            lang_col, reg_col = _pick_label_cols(reg)
            reg = _ensure_cols(reg, [lang_col, reg_col])
            show = reg[[lang_col, reg_col, "start_year", "end_year"]].copy().rename(columns={
                lang_col: tr("paises.col.lingua"),
                reg_col:  tr("paises.col.regiao"),
                "start_year": tr("paises.col.desde"),
                "end_year":   tr("paises.col.ate"),
            })
            st.markdown(f"**{tr('paises.sub.co_oficiais_regionais')}**")
            st.dataframe(show, use_container_width=True, hide_index=True, height=220)
        else:
            st.caption(tr("paises.sem_co_oficiais_regionais"))

        st.divider()

        # ---------- Outras línguas usadas (TEXTO + CONTADOR) ----------
        # base "used" para o país
        if not used.empty and "iso3" in used.columns:
            used["iso3"] = used["iso3"].astype(str).str.upper().str.strip()
            u = used[used["iso3"] == iso3u].copy()
        else:
            u = pd.DataFrame()

        # excluir oficiais (por QID; senão por nome)
        other_names: list[str] = []
        if not u.empty:
            lang_col, _ = _pick_label_cols(u)
            u = _ensure_cols(u, [lang_col, "lang_qid"])

            off_here = off[off["iso3"] == iso3u] if (not off.empty and "iso3" in off.columns) else pd.DataFrame()
            off_qids = set(off_here.get("lang_qid", pd.Series([], dtype=str)).dropna().astype(str))
            off_names = set()
            if not off_here.empty:
                off_lang_col, _ = _pick_label_cols(off_here)
                off_names = set(
                    off_here.get(off_lang_col, pd.Series([], dtype=str)).dropna().astype(str).str.strip().tolist()
                )

            if off_qids and "lang_qid" in u.columns:
                u = u[~u["lang_qid"].astype(str).isin(off_qids)].copy()
            elif off_names:
                u = u[~u[lang_col].astype(str).str.strip().isin(off_names)].copy()

            other_names = sorted(set(u[lang_col].dropna().astype(str).str.strip().tolist()))

        # título com contador (0 quando vazio)
        count = len(other_names)
        st.markdown(f"**{tr('paises.sub.outras_linguas_usadas')} ({count})**")
        if other_names:
            st.markdown(", ".join(other_names))
        else:
            st.caption(tr("paises.sem_outras_linguas"))
