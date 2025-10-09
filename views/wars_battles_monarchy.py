# views/history.py  (substitui/insere estas funções/linhas)

from __future__ import annotations
from pathlib import Path
import pandas as pd
import streamlit as st
from services.i18n import t as tr
import plotly.express as px
from datetime import datetime
import math

ROOT = Path(__file__).resolve().parents[1]
WAR_PATHS = [
    #ROOT / "data" / "history" / "wars_battles.enriched.csv",
    ROOT / "data" / "wars_battles.enriched.csv",
    #ROOT / "data" / "history" / "wars_battles.csv",
    #ROOT / "data" / "wars_battles.csv",
]

def _read_any(p: Path) -> pd.DataFrame | None:
    if not p.exists():
        return None
    try:
        return pd.read_csv(p, sep=None, engine="python", dtype=str)
    except Exception:
        for sep in (";", ","):
            try:
                return pd.read_csv(p, sep=sep, dtype=str)
            except Exception:
                continue
    return None

def _load_wars() -> pd.DataFrame:
    for p in WAR_PATHS:
        df = _read_any(p)
        if df is not None:
            return df
    return pd.DataFrame()

def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Torna os cabeçalhos previsíveis: minúsculas + renomes usuais."""
    if df is None or df.empty:
        return pd.DataFrame()

    d = df.copy()
    # tudo minúsculas
    d.columns = [c.strip().lower() for c in d.columns]

    # mapear 'iso3' a partir de variantes
    for alt in ["iso3", "readiso3", "iso_3", "country_iso3", "iso3_code", "iso"]:
        if alt in d.columns:
            if alt != "iso3":
                d = d.rename(columns={alt: "iso3"})
            break

    # alguns aliases úteis
    ren = {}
    if "conflict_type" in d.columns and "kind" not in d.columns:
        ren["conflict_type"] = "kind"
    if "winner" in d.columns and "result" not in d.columns:
        ren["winner"] = "result"
    if "deaths_num" in d.columns and "deaths" not in d.columns:
        ren["deaths_num"] = "deaths"
    if "place" in d.columns and "place" not in d.columns:
        pass  # já está
    if "start_year" not in d.columns and "start" in d.columns:
        # deixo a função criar _Inicio__/_Ano__ a partir de 'start'
        pass
    if ren:
        d = d.rename(columns=ren)

    return d

def _pick(df: pd.DataFrame, pt_list: list[str], en_list: list[str]) -> str | None:
    lang = (getattr(st.session_state, "lang", "pt") or "pt").lower()
    cands = (pt_list if lang == "pt" else en_list) + list({*pt_list, *en_list})
    for c in cands:
        if c in df.columns:
            return c
    return None

def render_wars_battles_expander(iso3: str, *, default_open: bool = False, max_rows: int = 300) -> None:
    from datetime import datetime
    
    iso3u = (iso3 or "").upper()

    raw = _load_wars()
    raw = _normalize_cols(raw)  # normalização

    title = tr("history.guerras_e_batalhas")
    if raw.empty or "iso3" not in raw.columns:
        with st.expander(title, expanded=default_open):
            st.caption(tr("history.sem_dados"))
        return

    base = raw.copy()
    base["iso3"] = base["iso3"].astype(str).str.upper().str.strip()
    base = base[base["iso3"] == iso3u]
    if base.empty:
        with st.expander(title, expanded=default_open):
            st.caption(tr("history.sem_dados"))
        return

    # ===== escolha de colunas =====
    col_conflict = _pick(
        base,
        ["conflict_label_pt", "conflictlabel_pt", "conflict_label", "conflict", "conflito"],
        ["conflict_label_en", "conflictlabel_en", "conflict_en", "conflict"],
    ) or "conflict"

    col_kind   = _pick(base, ["kind_label", "kind", "tipo", "conflict_type"], ["kind", "type", "conflict_type"])
    col_place  = _pick(base, ["place_label_pt", "place_label", "place"], ["place_label_en", "place_en", "place"])
    col_result = _pick(base, ["result_label", "result", "resultado", "winner"], ["result", "winner_en", "winner"])

    col_start = next((c for c in ["start_year", "startyear", "inicio", "start"] if c in base.columns), None)
    col_end   = next((c for c in ["end_year", "endyear", "fim", "end"] if c in base.columns), None)
    col_point = next((c for c in ["point_year", "pointyear", "ano", "pointintime"] if c in base.columns), None)

    col_deaths = next((c for c in ["deaths", "mortes", "casualties", "deaths_num"] if c in base.columns), None)

    sub = base.copy()

    # ===== Fallbacks de labels (evita linhas a "contar" sem aparecer) =====
    # Conflito: se label estiver vazio, cai para outras colunas ou para o QID.
    def _conflict_display(row) -> str:
        for c in [col_conflict, "conflict_label", "conflict_label_pt", "conflict_label_en",
                  "conflict", "conflict_en", "conflito"]:
            if c in sub.columns:
                v = str(row.get(c, "")).strip()
                if v:
                    return v
        return (row.get("conflict_qid") or row.get("conflictid") or "")  # último fallback

    sub["__CONFLITO__"] = sub.apply(_conflict_display, axis=1)

    # ===== Ano como TEXTO (sem separadores; plausível) =====
    def _year_text(row) -> str:
        # prioridade: point_year > start_year > end_year
        for c in (col_point, col_start, col_end):
            if c and pd.notna(row.get(c, None)):
                s = str(row[c]).strip()
                if not s:
                    continue
                s = s.replace(",", "").replace(".", "").replace(" ", "")
                if len(s) >= 5 and s[0] in "+-":  # ex: +01939-...
                    y = s[1:5]
                else:
                    y = s[:4]
                try:
                    yi = int(y)
                    this_year = datetime.now().year
                    if 700 <= yi <= this_year:   # elimina 2492 e afins
                        return str(yi)
                    return ""                    # fora do intervalo plausível
                except Exception:
                    return ""
        return ""

    sub["__ANO_TXT__"] = sub.apply(_year_text, axis=1)
    sub["__year_sort__"] = pd.to_numeric(sub["__ANO_TXT__"], errors="coerce")

    # ===== Filtrar linhas válidas (tem de haver nome de conflito) =====
    mask_valid = sub["__CONFLITO__"].astype(str).str.strip().ne("")
    sub_clean = sub.loc[mask_valid].copy()

    # ordenar por ano desc e conflito asc
    sub_clean = sub_clean.sort_values(
        by=["__year_sort__", "__CONFLITO__"],
        ascending=[False, True],
        na_position="last"
    )

    # ===== preparar tabela =====
    # Usamos a coluna normalizada "__CONFLITO__" para garantir sempre valor.
    cols_show = ["__CONFLITO__"]
    if col_kind:  cols_show.append(col_kind)
    cols_show.append("__ANO_TXT__")
    if col_place:  cols_show.append(col_place)
    if col_result: cols_show.append(col_result)
    if col_deaths:
        def _fmt_int(x):
            try:
                v = int(float(str(x).replace(" ", "").replace(",", "")))
                return f"{v:,}".replace(",", " ")
            except Exception:
                return "" if pd.isna(x) else str(x)
        sub_clean["__MORTES_TXT__"] = sub_clean[col_deaths].map(_fmt_int)
        cols_show.append("__MORTES_TXT__")

    # Subconjunto VISÍVEL (tabela) — o gráfico usa exatamente estas mesmas linhas
    show_raw = sub_clean.loc[:, cols_show].head(max_rows).copy()

    # renomes para UI
    rename_map = {"__CONFLITO__": tr("history.col.conflito")}
    if col_kind:  rename_map[col_kind] = tr("history.col.tipo")
    rename_map["__ANO_TXT__"] = tr("history.col.ano")
    if col_place:  rename_map[col_place]  = tr("history.col.local")
    if col_result: rename_map[col_result] = tr("history.col.resultado")
    if "__MORTES_TXT__" in show_raw.columns: rename_map["__MORTES_TXT__"] = tr("history.col.mortes")
    show_ui = show_raw.rename(columns=rename_map)

    total_all   = int(len(sub_clean))
    total_shown = int(len(show_ui))

    # ===== expander com tabela (esq) + gráfico (dir) =====
    with st.expander(f"{title}   (a mostrar {total_shown})", expanded=default_open):
        col_tbl, col_chart = st.columns([6, 6], gap="small")

        with col_tbl:
            st.dataframe(
                show_ui,
                use_container_width=True,
                hide_index=True,
                height=min(420, 40 + 28 * min(len(show_ui), 12)),
                column_config={
                    tr("history.col.ano"): st.column_config.TextColumn(tr("history.col.ano")),
                },
            )
            st.download_button(
                label=tr("history.download_csv"),
                data=show_ui.to_csv(index=False).encode("utf-8"),
                file_name=f"{iso3u}_wars_battles.csv",
                mime="text/csv",
                use_container_width=True,
            )

        with col_chart:
            

            # --- detetar colunas necessárias ---
            source_col        = next((c for c in ["source", "fonte"] if c in sub_clean.columns), None)
            conflict_qid_col  = next((c for c in ["conflict_qid", "conflictid"] if c in sub_clean.columns), None)
            kind_qid_col      = next((c for c in ["kind_qid", "kindid"] if c in sub_clean.columns), None)
            kind_label_col    = next((c for c in ["kind_label", "kind", "conflict_type", "tipo"] if c in sub_clean.columns), None)

            if not (kind_qid_col and conflict_qid_col):
                # fallback: se não houver QIDs, agrupa por label (menos robusto)
                if not kind_label_col:
                    st.caption("— Sem colunas suficientes para o gráfico (preciso de kind_qid/conflict_qid).")
                else:
                    ser = sub_clean[kind_label_col].astype(str).str.strip().replace({"": "Sem tipo", "—": "Sem tipo"})
                    top10 = (
                        ser.value_counts()
                        .head(10)
                        .rename_axis("Tipo")
                        .reset_index(name="Ocorrências")
                    )
                    fig = px.bar(top10, x="Ocorrências", y="Tipo", orientation="h", text="Ocorrências", template="plotly_dark")
                    fig.update_traces(textposition="outside")
                    fig.update_layout(height=420, margin=dict(l=6, r=6, t=8, b=8), xaxis_title="", yaxis_title="")
                    st.plotly_chart(fig, use_container_width=True)
            else:
                # 1) filtrar apenas PARTICIPANT
                dfp = sub_clean.copy()
                if source_col:
                    dfp = dfp[dfp[source_col].astype(str).str.lower().eq("participant")]

                # 2) contar conflitos distintos por kind_qid
                dfp["_kid"] = dfp[kind_qid_col].fillna("").astype(str).str.strip()
                dfp["_cid"] = dfp[conflict_qid_col].fillna("").astype(str).str.strip()
                dfp = dfp[dfp["_cid"].ne("")]  # precisa de conflict_qid válido

                grp = (
                    dfp.groupby("_kid")["_cid"]
                    .nunique()                           # conflitos distintos por tipo
                    .reset_index(name="Ocorrências")
                )

                # 3) mapear labels para cada kind_qid
                if kind_label_col:
                    lbl_map = (
                        dfp[[kind_qid_col, kind_label_col]]
                        .dropna()
                        .astype(str)
                        .drop_duplicates(subset=[kind_qid_col])
                        .set_index(kind_qid_col)[kind_label_col]
                        .to_dict()
                    )
                else:
                    lbl_map = {}

                grp["Tipo"] = grp["_kid"].map(lbl_map).fillna("Sem tipo")

                # 4) ordenar e Top-10
                top10 = grp.sort_values("Ocorrências", ascending=False).head(10)[["Tipo", "Ocorrências"]]

                if top10.empty:
                    st.caption("— Sem dados para o gráfico.")
                else:
                    fig = px.bar(
                        top10,
                        x="Ocorrências",
                        y="Tipo",
                        orientation="h",
                        text="Ocorrências",
                        template="plotly_dark",
                    )
                    fig.update_traces(textposition="outside")
                    fig.update_layout(height=420, margin=dict(l=6, r=6, t=8, b=8), xaxis_title="", yaxis_title="")
                    st.plotly_chart(fig, use_container_width=True)


# --- MONARCHY EXPANDER -------------------------------------------------------
def render_monarchy_expander(iso3: str, *, default_open: bool = False, max_rows: int = 300) -> None:
    from datetime import datetime
    import pandas as pd
    from pathlib import Path

    iso3u = (iso3 or "").upper()
    title = tr("history.monarquia") if "tr" in globals() else "Monarquia"

    # ---------- helpers ----------
    def _csv_path(*parts: str) -> Path:
        base = Path(__file__).resolve().parents[1] / "data" / "history"
        p = base.joinpath(*parts)
        return p if p.exists() else (Path.cwd() / "data" / "history" / Path(*parts))

    def _read_csv_smart(path: Path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame()
        for sep in (";", ","):
            try:
                df = pd.read_csv(path, dtype=str, sep=sep)
                if df.shape[1] > 1:
                    break
            except Exception:
                continue
        else:
            df = pd.read_csv(path, dtype=str, engine="python")
        df.columns = [c.replace("\ufeff", "").strip().lower().replace(" ", "_") for c in df.columns]
        return df

    def _clean(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""
        s = str(v).strip()
        return "" if s.lower() in ("", "nan", "none", "null") else s

    def _to_year_int(v):
        s = _clean(v)
        if not s:
            return None
        if len(s) >= 5 and s[0] in "+-":
            s = s[1:5]
        else:
            s = s[:4]
        try:
            yi = int(s)
            this_year = datetime.now().year
            return yi if 700 <= yi <= this_year else None
        except Exception:
            return None

    # ---------- load & filter ----------
    gov = _read_csv_smart(_csv_path("government_forms.enriched.csv"))
    mon = _read_csv_smart(_csv_path("monarchs.enriched.csv"))

    if gov.empty and mon.empty:
        with st.expander(title, expanded=default_open):
            st.caption(tr("history.sem_dados") if "tr" in globals() else "— sem dados —")
        return

    for df in (gov, mon):
        if not df.empty:
            if "iso3" not in df.columns:
                df["iso3"] = ""
            df["iso3"] = df["iso3"].astype(str).str.upper().str.strip()

    gov = gov[gov["iso3"] == iso3u].copy() if not gov.empty else pd.DataFrame()
    mon = mon[mon["iso3"] == iso3u].copy() if not mon.empty else pd.DataFrame()

    if mon.empty:
        with st.expander(title, expanded=default_open):
            st.caption(tr("history.sem_dados") if "tr" in globals() else "— sem dados —")
        return

    # ---------- compute monarchy periods from gov (used only to filter) ----------
    monarchy_ranges = []
    if not gov.empty and "is_monarchy" in gov.columns:
        gtmp = gov.copy()
        gtmp["__is_mon__"] = gtmp["is_monarchy"].astype(str).str.lower().isin(("1", "true", "yes", "sim"))
        gtmp = gtmp[gtmp["__is_mon__"]]
        for _, r in gtmp.iterrows():
            y1 = _to_year_int(r.get("start_year", r.get("start")))
            y2 = _to_year_int(r.get("end_year",   r.get("end")))
            monarchy_ranges.append((y1, y2))

    def _overlaps_any(y1, y2, ranges):
        for a, b in ranges:
            lo1 = y1 if y1 is not None else float("-inf")
            hi1 = y2 if y2 is not None else float("inf")
            lo2 = a  if a  is not None else float("-inf")
            hi2 = b  if b  is not None else float("inf")
            if lo1 <= hi2 and lo2 <= hi1:
                return True
        return False

    # ---------- build monarchs table ----------
    def _first(row, cols):
        for c in cols:
            if c in mon.columns:
                v = _clean(row.get(c, ""))
                if v:
                    return v
        return ""

    # name (PT→EN) – drop if empty
    mon["_MONARCA_"] = mon.apply(lambda r: _first(r, ["monarch_pt", "monarch_en"]), axis=1)
    mon = mon[mon["_MONARCA_"].ne("")].copy()

    # title & house (PT→EN; fallback "—")
    mon["_TITULO_"] = mon.apply(lambda r: _first(r, ["position_pt", "position_en"]), axis=1)
    mon["_CASA_"]   = mon.apply(lambda r: _first(r, ["house_pt", "house_en"]), axis=1)
    mon.loc[mon["_TITULO_"].eq(""), "_TITULO_"] = "—"
    mon.loc[mon["_CASA_"].eq(""),   "_CASA_"]   = "—"

    # years + filtering: only true monarchs
    mon["_Y1_"] = mon.apply(lambda r: _to_year_int(r.get("start_year", r.get("start"))), axis=1)
    mon["_Y2_"] = mon.apply(lambda r: _to_year_int(r.get("end_year",   r.get("end"))),   axis=1)
    def _fmt_year_safe(x) -> str:
    # vazio para None/NaN/""; caso contrário devolve inteiro em string
        if x is None:
            return ""
        try:
            # cobre floats NaN, pandas NA, etc.
            if pd.isna(x) or (isinstance(x, float) and math.isnan(x)):
                return ""
        except Exception:
            pass
        try:
            return str(int(x))
        except Exception:
            try:
                return str(int(float(str(x).strip())))
            except Exception:
                return ""

    mon["_INICIO_"] = mon["_Y1_"].apply(_fmt_year_safe)
    mon["_FIM_"]    = mon["_Y2_"].apply(_fmt_year_safe)

    republican_exclude = (
        "president", "prime minister", "chancellor", "minister", "governor",
        "presidente", "primeiro-ministro", "chanceler", "ministro", "governador"
    )
    monarchy_keywords = (
        "king","queen","emperor","empress","tsar","kaiser","sultan","emir","shah","caliph",
        "prince","princess","pharaoh","grand duke","duke","khan","sheikh",
        "rei","rainha","imperador","imperatriz","sultão","emir","xá","califa",
        "príncipe","princesa","faraó","grão-duque","duque","cã","xeique","monarca"
    )

    def _is_republic_office(title: str) -> bool:
        t = title.lower()
        return any(k in t for k in republican_exclude)

    def _looks_like_monarch(title: str) -> bool:
        t = title.lower()
        return any(k in t for k in monarchy_keywords)

    keep_mask = []
    for _, r in mon.iterrows():
        title_txt = r["_TITULO_"]
        if _is_republic_office(title_txt):
            keep_mask.append(False); continue
        if monarchy_ranges:
            keep_mask.append(_overlaps_any(r["_Y1_"], r["_Y2_"], monarchy_ranges))
        else:
            keep_mask.append(_looks_like_monarch(title_txt))
    mon = mon[pd.Series(keep_mask, index=mon.index)].copy()

    # order & final df
    mon["_SORT_"] = pd.to_numeric(mon["_INICIO_"].replace("", pd.NA), errors="coerce")
    mon = mon.sort_values(["_SORT_", "_MONARCA_"], ascending=[False, True], na_position="last")

    mon_show = mon.loc[:, ["_MONARCA_", "_TITULO_", "_CASA_", "_INICIO_", "_FIM_"]].copy().head(max_rows)
    # ensure plain strings (no '.0')
    for c in mon_show.columns:
        mon_show[c] = mon_show[c].astype(str).str.replace(".0", "", regex=False)

    mon_show = mon_show.rename(columns={
        "_MONARCA_": tr("history.col.monarca") if "tr" in globals() else "Monarca",
        "_TITULO_":  tr("history.col.titulo")  if "tr" in globals() else "Título",
        "_CASA_":    tr("history.col.casa")    if "tr" in globals() else "Dinastia/Casa",
        "_INICIO_":  tr("history.col.inicio")  if "tr" in globals() else "Início",
        "_FIM_":     tr("history.col.fim")     if "tr" in globals() else "Fim",
    })

    # ---------- UI (apenas 1 expander e 1 tabela, largura total) ----------
    with st.expander(title, expanded=default_open):
        # === Breakout para fora da coluna-pai (largura total do viewport) ===
        st.markdown("""
        <style>
        /* permitir que colunas/expander não cortem overflow */
        [data-testid="column"], [data-testid="stExpander"] > div, [data-testid="stVerticalBlock"] {
            overflow: visible !important;
        }
        /* wrapper em largura total, reposiciona a partir do centro */
        .wbm-breakout {
            position: relative;
            left: calc(-50vw + 50%);
            width: 100vw !important;
            max-width: 100vw !important;
            margin: 0; padding-right: 0;
        }
        /* garantir que o DataFrame usa 100% do wrapper */
        .wbm-breakout [data-testid="stDataFrame"] { width: 100% !important; }
        .wbm-breakout [data-testid="stDataFrame"] div[role="grid"] { width: 100% !important; }
        /* e nada de truncar no container do DataFrame */
        .wbm-breakout [data-testid="stDataFrame"] > div { overflow: visible !important; }

        /* em ecrãs estreitos, não forces o breakout */
        @media (max-width: 1024px) {
            .wbm-breakout { left: 0; width: 100% !important; max-width: 100% !important; }
        }
        </style>
        """, unsafe_allow_html=True)

        st.markdown('<div class="wbm-breakout">', unsafe_allow_html=True)

        st.markdown(f"**{tr('history.lista_monarcas') if 'tr' in globals() else 'Monarcas (histórico)'}**")
        if mon_show.empty:
            st.caption(tr("history.sem_dados") if "tr" in globals() else "— sem dados —")
        else:
            st.dataframe(
                mon_show,
                use_container_width=True,  # agora é 100% do viewport
                hide_index=True,
                height=min(640, 40 + 28 * min(len(mon_show), 18)),
                column_config={
                    (tr('history.col.inicio') if 'tr' in globals() else 'Início'): st.column_config.TextColumn(
                        tr('history.col.inicio') if 'tr' in globals() else 'Início'),
                    (tr('history.col.fim') if 'tr' in globals() else 'Fim'): st.column_config.TextColumn(
                        tr('history.col.fim') if 'tr' in globals() else 'Fim'),
                },
            )
            st.download_button(
                label=tr("history.download_csv") if "tr" in globals() else "💾 Descarregar CSV",
                data=mon_show.to_csv(index=False).encode("utf-8"),
                file_name=f"{iso3u}_monarchs.csv",
                mime="text/csv",
                use_container_width=True,
            )

        st.markdown('</div>', unsafe_allow_html=True)

