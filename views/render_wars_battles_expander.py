from __future__ import annotations
from pathlib import Path
from typing import List
import re
import pandas as pd
import streamlit as st
from urllib.parse import quote_plus


F_ENRICHED = Path("data/conflicts_all_enriched.csv")

def _sniff_delimiter(path: Path) -> str:
    sample = path.read_text(encoding="utf-8", errors="ignore")[:4096]
    for d in (";", ",", "|", "\t"):
        if d in sample:
            return d
    return ";"

def _norm_str(x):
    return (str(x or "")).strip()

def _to_year(val: str) -> str:
    s = str(val or "").strip()
    if not s:
        return ""
    m = re.match(r"^(-?\d{1,4})", s)
    return m.group(1) if m else ""

def _pick_start_year(row: pd.Series) -> str:
    for c in ("start", "point_in_time", "earliest_year"):
        if c in row and row[c]:
            y = _to_year(row[c])
            if y:
                return y
    return ""

def _pick_end_year(row: pd.Series) -> str:
    for c in ("end", "latest_year"):
        if c in row and row[c]:
            y = _to_year(row[c])
            if y:
                return y
    return ""

def _sort_key_year(s: str) -> int:
    """chave para ordenar por início asc, vazios no fim"""
    s = (s or "").strip()
    if re.fullmatch(r"-?\d{1,4}", s):
        try:
            return int(s) + 100000  # desloca negativos para manter ordem relativa
        except Exception:
            pass
    return 10**9  # “∞” → vai para o fim

def load_conflicts_df() -> pd.DataFrame:
    if not F_ENRICHED.exists():
        st.warning(f"⚠️ Ficheiro não encontrado: {F_ENRICHED}")
        return pd.DataFrame()
    sep = _sniff_delimiter(F_ENRICHED)
    df = pd.read_csv(F_ENRICHED, sep=sep, dtype=str, keep_default_na=False, encoding="utf-8")
    for c in [
        "conflict_qid","conflict_label","entity_qid","entity_label","entity_type",
        "start","end","point_in_time","earliest_year","latest_year",
        "Iso3Start","FormationYear","is_human","is_military"
    ]:
        if c in df.columns:
            df[c] = df[c].map(_norm_str)
    return df

def render_wars_battles_expander(iso3: str, *, default_open: bool = False, max_rows: int = 3000) -> None:
    df = load_conflicts_df()
    if df.empty:
        return

    iso3u = (iso3 or "").upper().strip()

    required = {"entity_type","Iso3Start","conflict_qid"}
    if not required.issubset(df.columns):
        with st.expander("Guerras e Batalhas", expanded=default_open):
            st.info("Estrutura do CSV não tem as colunas esperadas (entity_type, Iso3Start, conflict_qid).")
        return

    # Linhas onde este país aparece como 'country'
    df_countries_self = df[
        (df["entity_type"].str.lower() == "country") &
        (df["Iso3Start"].str.upper() == iso3u)
    ].copy()

    with st.expander("Guerras e Batalhas", expanded=default_open):
        if df_countries_self.empty:
            st.info(f"Sem registos de conflitos para {iso3u}.")
            return

        # --- TAB 1: Conflitos do país (1 linha por conflito) ---
        cols_keep_country = ["conflict_qid"]
        if "conflict_label" in df.columns: cols_keep_country.append("conflict_label")
        for c in ("start","end","point_in_time","earliest_year","latest_year","is_military"):
            if c in df_countries_self.columns: cols_keep_country.append(c)

        tc = df_countries_self.drop_duplicates(subset=["conflict_qid"])[cols_keep_country].copy()
        tc["início"] = tc.apply(_pick_start_year, axis=1)
        tc["fim"]    = tc.apply(_pick_end_year,   axis=1)
        tc["militar"] = tc.get("is_military","").str.lower().map(lambda v: "yes" if v=="military" else ("no" if v=="non-military" else ""))

        if "conflict_label" in tc.columns:
            tc = tc.rename(columns={"conflict_label": "conflito"})
        else:
            tc["conflito"] = ""

        # ordenar por início (vazios no fim)
        tc["_ord"] = tc["início"].map(_sort_key_year)
        tc = tc.sort_values(["_ord","conflito"], kind="stable").drop(columns=["_ord"])
        tc = tc[["conflito","início","fim","militar"]].head(max_rows)

        conflitos: List[str] = df_countries_self["conflict_qid"].dropna().astype(str).unique().tolist()

        # --- Linhas (países + participantes) desses conflitos ---
        df_roles = df[
            (df["conflict_qid"].astype(str).isin(conflitos)) &
            (df["entity_type"].str.lower().isin(["country","participant"]))
        ].copy()

        # preparar flags
        has_is_human = "is_human" in df_roles.columns
        has_is_mil   = "is_military" in df_roles.columns
        if has_is_human:
            df_roles["is_human_flag"] = df_roles["is_human"].str.lower().eq("yes")
        else:
            df_roles["is_human_flag"] = False

        # --- separar humanos (para Personagens) ---
        mask_human = df_roles["entity_type"].str.lower().eq("participant") & df_roles["is_human_flag"]
        df_roles_humans = df_roles[mask_human].copy()

        # --- Participantes (apenas conflitos militares) ---
        if has_is_mil:
            # filtra apenas conflitos marcados como 'military'
            military_conflicts = set(df_countries_self[df_countries_self["is_military"].str.lower()=="military"]["conflict_qid"])
            df_roles_participants = df_roles[
                df_roles["entity_type"].str.lower().eq("participant") &
                (~df_roles["is_human_flag"]) &
                (df_roles["conflict_qid"].isin(military_conflicts))
            ].copy()
            df_roles_countries = df_roles[df_roles["entity_type"].str.lower().eq("country") &
                                          (df_roles["conflict_qid"].isin(military_conflicts))].copy()
        else:
            # se não houver coluna, mantém comportamento antigo (sem filtro)
            df_roles_participants = df_roles[df_roles["entity_type"].str.lower().eq("participant") & (~df_roles["is_human_flag"])].copy()
            df_roles_countries    = df_roles[df_roles["entity_type"].str.lower().eq("country")].copy()

        df_roles_others = pd.concat([df_roles_countries, df_roles_participants], ignore_index=True)

        # --- helpers tabela UI ---
        def _with_years(df_in: pd.DataFrame) -> pd.DataFrame:
            dfp = df_in.copy()
            dfp["início"] = dfp.apply(_pick_start_year, axis=1)
            dfp["fim"]    = dfp.apply(_pick_end_year,   axis=1)
            dfp["_ord"]   = dfp["início"].map(_sort_key_year)
            return dfp

        def _prep_ui(df_in: pd.DataFrame, *, rename_participant_to: str) -> pd.DataFrame:
            cols_keep = ["conflict_qid","conflict_label","entity_label","entity_qid","Iso3Start",
                         "start","end","point_in_time","earliest_year","latest_year","entity_type"]
            cols_keep = [c for c in cols_keep if c in df_in.columns]
            tp = df_in[cols_keep].copy()
            tp = _with_years(tp)
            if "conflict_label" in tp.columns:
                tp = tp.rename(columns={"conflict_label": "conflito"})
            else:
                tp["conflito"] = ""
            tp = tp.rename(columns={"entity_label": rename_participant_to, "Iso3Start": "ISO3"})
            if not tp.empty:
                tp = tp.sort_values(["_ord","conflict_qid", rename_participant_to], kind="stable").reset_index(drop=True)
                first_mask = ~tp["conflict_qid"].duplicated(keep="first")
                tp["conflito_first"] = tp["conflito"]
                tp.loc[~first_mask, "conflito_first"] = ""
                unified = tp[["conflito_first", rename_participant_to, "entity_qid","ISO3","início","fim","entity_type"]].rename(
                    columns={"conflito_first": "conflito"}
                )
            else:
                unified = pd.DataFrame(columns=["conflito", rename_participant_to, "entity_qid","ISO3","início","fim","entity_type"])
            return unified

        unified_nonhum = _prep_ui(df_roles_others, rename_participant_to="participante")
        unified_hum    = _prep_ui(df_roles_humans, rename_participant_to="personagem")

        total_conflitos_pais = tc.shape[0]
        conflitos_dist_roles = df_roles["conflict_qid"].nunique()

        # --- TABS ---
        tabs = ["Conflitos do país", "Participantes"]
        if has_is_human:
            tabs.append("Personagens")
        tab_objs = st.tabs(tabs)
        tab_conf = tab_objs[0]
        tab_part = tab_objs[1]
        tab_chars = tab_objs[2] if has_is_human else None

        
        # --- TAB 1: Conflitos do país (1 linha por conflito) ---
        cols_keep_country = [
            "conflict_qid", "conflict_label",
            "start","end","point_in_time","earliest_year","latest_year","is_military"
        ]
        cols_keep_country = [c for c in cols_keep_country if c in df_countries_self.columns]

        tc_base = (
            df_countries_self
            .drop_duplicates(subset=["conflict_qid"])[cols_keep_country]
            .copy()
        )

        # Datas derivadas
        tc_base["início"] = tc_base.apply(_pick_start_year, axis=1)
        tc_base["fim"]    = tc_base.apply(_pick_end_year,   axis=1)

        # Militar yes/no
        tc_base["militar"] = tc_base.get("is_military","").str.lower().map(
            lambda v: "yes" if v == "military" else ("no" if v == "non-military" else "")
        )

        # Nome do conflito
        if "conflict_label" in tc_base.columns:
            tc_base = tc_base.rename(columns={"conflict_label": "conflito"})
        else:
            tc_base["conflito"] = ""

        # Ordenação (vazios no fim)
        tc_base["_ord"] = tc_base["início"].map(_sort_key_year)
        tc_base = tc_base.sort_values(["_ord","conflito"], kind="stable").drop(columns=["_ord"])

        # Links (mantém sempre conflict_qid acessível)
        tc_base["wikidata"]      = tc_base["conflict_qid"].apply(lambda q: f"https://www.wikidata.org/wiki/{q}" if q else "")
        tc_base["wikipedia_pt"]  = tc_base["conflict_qid"].apply(lambda q: f"https://www.wikidata.org/wiki/Special:GoToLinkedPage/ptwiki/{q}" if q else "")
        tc_base["wikipedia_en"]  = tc_base["conflict_qid"].apply(lambda q: f"https://www.wikidata.org/wiki/Special:GoToLinkedPage/enwiki/{q}" if q else "")

        # DataFrame a apresentar
        tc_display = tc_base[["conflito","início","fim","militar","conflict_qid","wikidata","wikipedia_pt","wikipedia_en"]].head(max_rows)

        # Lista de conflitos para as outras tabs (sem merges, usa sempre o base que tem conflict_qid)
        # ... (depois de construir tc_base, ordenar e criar as 3 colunas de links)
        # tc_base contém: conflito, início, fim, militar, conflict_qid, wikidata, wikipedia_pt, wikipedia_en

        # Lista de conflitos para outras tabs
        conflitos: List[str] = tc_base["conflict_qid"].dropna().astype(str).unique().tolist()

        # Escolha de colunas de acordo com ISO3
        if iso3u == "PRT":
            # Portugal: mostrar WD + PT + QID
            tc_display = tc_base[
                ["conflito", "início", "fim", "militar", "conflict_qid", "wikidata", "wikipedia_pt"]
            ].head(max_rows)
            column_config = {
                "conflito":      st.column_config.TextColumn("conflito", width="large"),
                "início":        st.column_config.TextColumn("início", width="small"),
                "fim":           st.column_config.TextColumn("fim", width="small"),
                "militar":       st.column_config.TextColumn("militar", width="small"),
                "conflict_qid":  st.column_config.TextColumn("QID", width="small"),
                "wikidata":      st.column_config.LinkColumn("Wikidata", display_text="🔗 WD"),
                "wikipedia_pt":  st.column_config.LinkColumn("Wikipédia (PT)", display_text="🔗 PT"),
            }
        else:
            # Outros países: apenas Wikipedia EN
            tc_display = tc_base[
                ["conflito", "início", "fim", "militar", "wikipedia_en"]
            ].head(max_rows)
            column_config = {
                "conflito":      st.column_config.TextColumn("conflito", width="large"),
                "início":        st.column_config.TextColumn("início", width="small"),
                "fim":           st.column_config.TextColumn("fim", width="small"),
                "militar":       st.column_config.TextColumn("militar", width="small"),
                "wikipedia_en":  st.column_config.LinkColumn("Wikipedia (EN)", display_text="🔗 EN"),
            }

        with tab_conf:
            st.dataframe(
                tc_display,
                use_container_width=True,
                hide_index=True,
                column_config=column_config,
            )
            st.caption(f"{tc_display.shape[0]} conflito(s) listados para {iso3u}.")
                
        with tab_part:
            st.dataframe(
                unified_nonhum.drop(columns=["entity_type"], errors="ignore"),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "conflito":     st.column_config.TextColumn("conflito", width="large"),
                    "participante": st.column_config.TextColumn("participante", width="large"),
                    "entity_qid":   st.column_config.TextColumn("entity_qid", width="small"),
                    "ISO3":         st.column_config.TextColumn("ISO3", width="small"),
                    "início":       st.column_config.TextColumn("início", width="small"),
                    "fim":          st.column_config.TextColumn("fim", width="small"),
                },
            )
            st.caption(f"Conflitos militares apenas · conflitos distintos: {conflitos_dist_roles}")

        if has_is_human and tab_chars is not None:
            # --- Links para a tab Personagens ---
            uh = unified_hum.copy()  # garante que não perdemos 'entity_qid'
            if not uh.empty and "entity_qid" in uh.columns:

                uh = unified_hum.copy()  # mantém entity_qid e personagem

                # links via QID (funcionam quando existe sitelink no Wikidata)
                # links consistentes com os dos conflitos (via Wikidata → GoToLinkedPage)
                uh["wikidata"] = uh.get("entity_qid", "").apply(
                    lambda q: f"https://www.wikidata.org/wiki/{q}" if q else ""
                )
                uh["wikipedia_pt"] = uh.get("entity_qid", "").apply(
                    lambda q: f"https://www.wikidata.org/wiki/Special:GoToLinkedPage/ptwiki/{q}" if q else ""
                )
                uh["wikipedia_en"] = uh.get("entity_qid", "").apply(
                    lambda q: f"https://www.wikidata.org/wiki/Special:GoToLinkedPage/enwiki/{q}" if q else ""
                )

                # (opcional) fallbacks de pesquisa para quando não houver sitelink
                uh["wikipedia_pt_search"] = uh.get("personagem", "").apply(
                    lambda t: f"https://pt.wikipedia.org/w/index.php?search={quote_plus(str(t))}" if t else ""
                )
                uh["wikipedia_en_search"] = uh.get("personagem", "").apply(
                    lambda t: f"https://en.wikipedia.org/w/index.php?search={quote_plus(str(t))}" if t else ""
                )
            else:
                uh["wikidata"] = ""
                uh["wikipedia_pt"] = ""
                uh["wikipedia_en"] = ""

            # Seleção de colunas conforme ISO3
            if iso3u == "PRT":
                # Portugal: WD + PT + fallback de pesquisa em PT
                uh_display = uh[["conflito","personagem","início","fim","entity_qid","wikidata","wikipedia_pt","wikipedia_pt_search"]]
                uh_columns = {
                    "conflito":              st.column_config.TextColumn("conflito", width="large"),
                    "personagem":            st.column_config.TextColumn("personagem", width="large"),
                    "início":                st.column_config.TextColumn("início", width="small"),
                    "fim":                   st.column_config.TextColumn("fim", width="small"),
                    "entity_qid":            st.column_config.TextColumn("QID", width="small"),
                    "wikidata":              st.column_config.LinkColumn("Wikidata", display_text="🔗 WD"),
                    "wikipedia_pt":          st.column_config.LinkColumn("Wikipédia (PT)", display_text="🔗 PT"),
                    "wikipedia_pt_search":   st.column_config.LinkColumn("Wikipédia (PT) 🔎", display_text="🔎 procurar"),
                }
            else:
                # Outros países: EN + fallback de pesquisa em EN
                uh_display = uh[["conflito","personagem","início","fim","wikipedia_en","wikipedia_en_search"]]
                uh_columns = {
                    "conflito":              st.column_config.TextColumn("conflito", width="large"),
                    "personagem":            st.column_config.TextColumn("personagem", width="large"),
                    "início":                st.column_config.TextColumn("início", width="small"),
                    "fim":                   st.column_config.TextColumn("fim", width="small"),
                    "wikipedia_en":          st.column_config.LinkColumn("Wikipedia (EN)", display_text="🔗 EN"),
                    "wikipedia_en_search":   st.column_config.LinkColumn("Wikipedia (EN) 🔎", display_text="🔎 procurar"),
                }

            st.session_state.setdefault("_dummy", None)  # evita lint
            with tab_chars:
                st.dataframe(
                uh_display,
                use_container_width=True,
                hide_index=True,
                column_config=uh_columns,
            )
            st.caption(f"{uh_display.shape[0]} personagem(ns) humana(s)")

