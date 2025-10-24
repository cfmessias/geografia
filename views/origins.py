import pandas as pd
from pathlib import Path
from datetime import datetime
import streamlit as st

def render_origins_expander(iso3: str, *, default_open: bool = False) -> None:
    iso3u = (iso3 or "").strip().upper()
    lang  = st.session_state.get("lang", "pt").lower()

    # ---- i18n helper ----
    def _tr(key: str, default_pt: str, default_en: str | None = None) -> str:
        """
        Tenta tr(key). Se falhar ou devolver o próprio key/placeholder, faz fallback:
        - EN se a sessão estiver em inglês e houver default_en
        - caso contrário, PT (default_pt)
        """
        try:
            if "tr" in globals():
                val = tr(key)
                s = str(val).strip()
                if s and s not in (key, f"[{key}]"):
                    return s
        except Exception:
            pass

        lang = (st.session_state.get("lang", "pt") if "st" in globals() else "pt").lower()
        if default_en and lang.startswith("en"):
            return default_en
        return default_pt

    title     = _tr("history.origens_pais", "Origens do país", "Country origins")
    sem_dados = _tr("history.sem_dados",    "— sem dados —",   "— no data —")

    # ---- leitura robusta do CSV ----
    def _csv_path() -> Path:
        base = Path(__file__).resolve().parents[1] / "data" / "history" / "origins.enriched.csv"
        return base if base.exists() else (Path.cwd() / "data" / "history" / "origins.enriched.csv")

    def _read_csv_smart(path: Path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame()
        for sep in (";", ",", None):
            try:
                df = pd.read_csv(path, dtype=str, sep=sep, engine="python", encoding="utf-8-sig")
                if df.shape[1] >= 2:
                    break
            except Exception:
                continue
        else:
            df = pd.read_csv(path, dtype=str, engine="python", encoding="utf-8-sig")
        df.columns = [str(c).replace("\ufeff", "").strip().lower().replace(" ", "_") for c in df.columns]
        return df

    df = _read_csv_smart(_csv_path())
    if df.empty or "iso3" not in df.columns:
        with st.expander(title, expanded=default_open):
            st.caption(sem_dados)
        return

    df["iso3"] = df["iso3"].astype(str).str.upper().str.strip()
    row = df[df["iso3"] == iso3u].head(1)
    if row.empty:
        with st.expander(title, expanded=default_open):
            st.caption(sem_dados)
        return
    r = row.iloc[0].to_dict()

    # ---- helpers ----
    def _get(col: str, default: str = "") -> str:
        v = r.get(col, "")
        if v is None:
            return default
        s = str(v).strip()
        return s if s.lower() not in ("nan", "none", "null") else default

    # nome do país (PT→EN→ISO3)
    country_label = _get("country_label_pt") or _get("country_label_en") or iso3u

    # texto de origens: prioridade wp_origins -> wp_summary -> origins_text
    txt = (
        _get(f"wp_origins_{lang}") or
        _get(f"wp_summary_{lang}") or
        _get(f"origins_text_{lang}")
    )

    # factos
    nm_after = _get("named_after_pt" if lang == "pt" else "named_after_en")
    preds    = _get("predecessors_pt" if lang == "pt" else "predecessors_en")
    y_ear    = _get("earliest_year")
    y_inc    = _get("inception_year")

    # lógica Fundação vs Estado moderno
    chip_found_key = None
    chip_found_val = None
    if y_inc:
        # se há predecessores e inception é relativamente recente, tratamos como "Estado moderno"
        try:
            inc_year = int(str(y_inc)[:4])
        except Exception:
            inc_year = None
        has_preds = bool(preds)
        if inc_year is not None and has_preds and inc_year >= 1800:
            chip_found_key = _tr("history.estado_moderno", "Estado moderno")
            chip_found_val = str(inc_year)
        else:
            chip_found_key = _tr("history.fundacao", "Fundação")
            chip_found_val = str(y_inc)

    # ---- UI ----
    with st.expander(title, expanded=default_open):
        st.markdown(f"**{country_label}**")

        # chips/resumo factual
        chips = []
        if y_ear:
            chips.append((_tr("history.primeira_mencao", "Primeira menção"), str(y_ear)))
        if chip_found_key and chip_found_val:
            chips.append((chip_found_key, chip_found_val))
        if nm_after:
            chips.append((_tr("history.etimologia_nome", "Etimologia / nome"), nm_after))
        if preds:
            chips.append((_tr("history.predecessores", "Predecessores"), preds))

        if chips:
            cols = st.columns(min(4, len(chips)))
            for i, (k, v) in enumerate(chips):
                with cols[i]:
                    st.caption(k)
                    st.markdown(
                        "<div style='padding:6px 10px;border-radius:8px;"
                        "border:1px solid rgba(255,255,255,.15);font-size:0.9rem'>"
                        f"{v}</div>",
                        unsafe_allow_html=True
                    )

        st.divider()

        if not txt:
            st.caption(sem_dados)
        else:
            # texto corrido (sem repetir chips)
            st.markdown(txt)

        st.divider()

        # downloads
        # md_text = f"# {country_label}\n\n{txt or ''}\n"
        # st.download_button(
        #     label=_tr("history.download_md", "💾 Descarregar texto (.md)"),
        #     data=md_text.encode("utf-8"),
        #     file_name=f"{iso3u}_origens.md",
        #     mime="text/markdown",
        #     use_container_width=True,
        # )
        # st.download_button(
        #     label=_tr("history.download_csv", "💾 Descarregar CSV"),
        #     data=row.to_csv(index=False, sep=";").encode("utf-8"),  # ; coerente com extração
        #     file_name=f"{iso3u}_origens.csv",
        #     mime="text/csv",
        #     use_container_width=True,
        # )
