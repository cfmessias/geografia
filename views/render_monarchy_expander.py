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

def _tr(key: str, default: str) -> str:
    """Tradução com fallback: se a chave não existir, devolve default."""
    try:
        if "tr" in globals():
            val = tr(key)
            s = str(val).strip()
            # evita casos em que o i18n devolve a própria chave (ex.: "[history.x]")
            if s and s not in (key, f"[{key}]"):
                return s
    except Exception:
        pass
    return default

title = _tr("history.monarquia", "Monarquia")
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

    
def _pick(df: pd.DataFrame, pt_list: list[str], en_list: list[str]) -> str | None:
    lang = (getattr(st.session_state, "lang", "pt") or "pt").lower()
    cands = (pt_list if lang == "pt" else en_list) + list({*pt_list, *en_list})
    for c in cands:
        if c in df.columns:
            return c
    return None



# --- MONARCHY EXPANDER -------------------------------------------------------
def render_monarchy_expander(iso3: str, *, default_open: bool = False, max_rows: int = 300) -> None:
    # imports locais (evitam dependências no topo do ficheiro)
 

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

    def _fmt_year_safe(x) -> str:
        # vazio para None/NaN/""; caso contrário devolve inteiro em string
        if x is None:
            return ""
        try:
            if pd.isna(x):
                return ""
        except Exception:
            pass
        if isinstance(x, float):
            try:
                if math.isnan(x):
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

    # ---------- load ----------
    gov = _read_csv_smart(_csv_path("government_forms.enriched.csv"))
    mon = _read_csv_smart(_csv_path("monarchs.enriched.csv"))

    if gov.empty and mon.empty:
        with st.expander(title, expanded=default_open):
            st.caption(tr("history.sem_dados") if "tr" in globals() else "— sem dados —")
        return

    # normalizar ISO3 e filtrar país
    for df in (gov, mon):
        if not df.empty:
            if "iso3" not in df.columns:
                df["iso3"] = ""
            df["iso3"] = df["iso3"].astype(str).str.upper().str.strip()
    gov = gov[gov["iso3"] == iso3u].copy() if not gov.empty else pd.DataFrame()
    mon = mon[mon["iso3"] == iso3u].copy() if not mon.empty else pd.DataFrame()

    # ---------- períodos de monarquia (para filtrar) ----------
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

    # ---------- UI (expander único) ----------
    with st.expander(title, expanded=default_open):
    # ===== CSS: breakout para ocupar a largura total, mesmo dentro de st.columns =====
        st.markdown("""
        <style>
        [data-testid="column"], [data-testid="stExpander"] > div, [data-testid="stVerticalBlock"] { overflow: visible !important; }
        .wbm-breakout {
            position: relative; left: calc(-50vw + 50%); width: 100vw !important; max-width: 100vw !important;
            margin: 0; padding-right: 0;
        }
        .wbm-breakout [data-testid="stDataFrame"] { width: 100% !important; }
        .wbm-breakout [data-testid="stDataFrame"] div[role="grid"] { width: 100% !important; }
        .wbm-breakout [data-testid="stDataFrame"] > div { overflow: visible !important; }
        @media (max-width: 1024px) { .wbm-breakout { left: 0; width: 100% !important; max-width: 100% !important; } }
        </style>
        """, unsafe_allow_html=True)

        # ===== Labels com fallback (sem helpers) =====
        label_monarchy = tr("history.monarquia_legenda") if "tr" in globals() else "Monarquia"
        if not isinstance(label_monarchy, str) or not label_monarchy.strip() or label_monarchy in ("history.monarquia_legenda", "[history.monarquia_legenda]"):
            label_monarchy = "Monarquia"

        label_nonmonarchy = tr("history.nao_monarquia_legenda") if "tr" in globals() else "Não-monarquia"
        if not isinstance(label_nonmonarchy, str) or not label_nonmonarchy.strip() or label_nonmonarchy in ("history.nao_monarquia_legenda", "[history.nao_monarquia_legenda]"):
            label_nonmonarchy = "Não-monarquia"

        label_monarchs_title = tr("history.lista_monarcas") if "tr" in globals() else "Monarcas (histórico)"
        if not isinstance(label_monarchs_title, str) or not label_monarchs_title.strip() or label_monarchs_title in ("history.lista_monarcas", "[history.lista_monarcas]"):
            label_monarchs_title = "Monarcas (histórico)"

        label_download = tr("history.download_csv") if "tr" in globals() else "💾 Descarregar CSV"
        if not isinstance(label_download, str) or not label_download.strip() or label_download in ("history.download_csv", "[history.download_csv]"):
            label_download = "💾 Descarregar CSV"

        msg_empty = tr("history.sem_dados") if "tr" in globals() else "— sem dados —"
        if not isinstance(msg_empty, str) or not msg_empty.strip() or msg_empty in ("history.sem_dados", "[history.sem_dados]"):
            msg_empty = "— sem dados —"

        title_dyne = tr("history.top_dinastias") if "tr" in globals() else "Top-10 dinastias por anos no trono"
        if not isinstance(title_dyne, str) or not title_dyne.strip() or title_dyne in ("history.top_dinastias", "[history.top_dinastias]"):
            title_dyne = "Top-10 dinastias por anos no trono"

        # ===== Selector de modo (reinantes vs todos) =====
        mode = st.radio(
            label="",
            options=("reinantes", "todos"),
            format_func=lambda x: {"reinantes": "👑 Só reinantes", "todos": "👑+🎖️ Todos os títulos nobiliárquicos"}[x],
            horizontal=True,
        )

        # ===== Timeline das formas de governo (usar DATAS e tratar fim vazio como "em curso") =====
        if not gov.empty:
            # heurística para monarquia (texto ou QID)
            monarchy_qids = {"Q7269","Q43273","Q43702","Q310341","Q1788875","Q104463"}
            def _is_mon_form(row) -> bool:
                txt = f"{_clean(row.get('form_pt'))} {_clean(row.get('form_en'))}".lower()
                qid = str(row.get("form_qid", "")).rsplit("/", 1)[-1]
                return ("monarq" in txt or "monarch" in txt) or (qid in monarchy_qids)

            tl_rows = []
            this_year = datetime.now().year

            for _, r in gov.iterrows():
                label = (_clean(r.get("form_pt")) or _clean(r.get("form_en")) or _clean(r.get("form_qid")) or "—").strip()

                y1 = _to_year_int(r.get("start_year", r.get("start")))
                y2 = _to_year_int(r.get("end_year",   r.get("end")))

                # ignorar linhas totalmente sem anos
                if y1 is None and y2 is None:
                    continue

                # completar períodos:
                #  - se só há fim, usa-o como início;
                #  - se só há início (caso típico da Dinamarca), considera "em curso" até ao ano atual;
                if y1 is None and y2 is not None:
                    y1 = y2
                if y2 is None and y1 is not None:
                    y2 = max(y1, this_year)

                # corrigir intervalos invertidos
                if y2 < y1:
                    y1, y2 = y2, y1

                tl_rows.append({
                    "Forma": label,
                    "start_dt": pd.Timestamp(year=int(y1), month=1, day=1),
                    "end_dt":   pd.Timestamp(year=int(y2), month=12, day=31),
                    "classe": label_monarchy if _is_mon_form(r) else label_nonmonarchy,
                    "periodo": f"{y1}–{y2}",
                })

            if tl_rows:
                tldf = pd.DataFrame(tl_rows).sort_values(["start_dt","end_dt","Forma"])
                fig_tl = px.timeline(
                    tldf, x_start="start_dt", x_end="end_dt", y="Forma", color="classe",
                    template="plotly_dark",
                    hover_data={"periodo": True, "classe": True, "start_dt": False, "end_dt": False}
                )
                fig_tl.update_traces(hovertemplate="<b>%{y}</b><br>%{customdata[0]} — %{customdata[1]}<extra></extra>")
                fig_tl.update_layout(
                    height=200,
                    margin=dict(l=6, r=6, t=10, b=6),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1.0),
                    xaxis_title="", yaxis_title=""
                )
                fig_tl.update_xaxes(type="date", tickformat="%Y")
                st.plotly_chart(fig_tl, use_container_width=True)



        # ===== Construir & filtrar monarcas =====
        if mon.empty:
            st.caption(msg_empty)
            return

        def _first(row, cols):
            for c in cols:
                if c in mon.columns:
                    v = _clean(row.get(c, ""))
                    if v:
                        return v
            return ""

        # Nome (PT→EN); linhas sem nome são removidas
        mon["_MONARCA_"] = mon.apply(lambda r: _first(r, ["monarch_pt", "monarch_en"]), axis=1)
        mon = mon[mon["_MONARCA_"].ne("")].copy()

        # Título & Casa (PT→EN; fallback "—")
        mon["_TITULO_"] = mon.apply(lambda r: _first(r, ["position_pt", "position_en"]), axis=1)
        mon["_CASA_"]   = mon.apply(lambda r: _first(r, ["house_pt", "house_en"]), axis=1)
        mon.loc[mon["_TITULO_"].eq(""), "_TITULO_"] = "—"
        mon.loc[mon["_CASA_"].eq(""),   "_CASA_"]   = "—"

        # Anos e strings seguras
        mon["_Y1_"] = mon.apply(lambda r: _to_year_int(r.get("start_year", r.get("start"))), axis=1)
        mon["_Y2_"] = mon.apply(lambda r: _to_year_int(r.get("end_year",   r.get("end"))),   axis=1)
        mon["_INICIO_"] = mon["_Y1_"].apply(_fmt_year_safe)
        mon["_FIM_"]    = mon["_Y2_"].apply(_fmt_year_safe)

        # Filtros “monarca verdadeiro”
        republican_exclude = (
            "president","prime minister","chancellor","minister","governor",
            "presidente","primeiro-ministro","chanceler","ministro","governador"
        )
        sovereign_kw = (  # reinantes
            "king","queen","emperor","empress","tsar","czar","kaiser","sultan","emir","shah","caliph","pharaoh",
            "rei","rainha","imperador","imperatriz","czar","cáiser","sultão","emir","xá","califa","faraó","monarca"
        )
        nobility_extra_kw = (  # nobreza não reinante
            "prince","princess","grand duke","duke","archduke","khan","sheikh",
            "príncipe","princesa","grão-duque","duque","arquiduque","cã","xeique"
        )

        def _is_republic_office(title: str) -> bool:
            t = (title or "").lower()
            return any(k in t for k in republican_exclude)

        def _has_any(title: str, keys: tuple[str, ...]) -> bool:
            t = (title or "").lower()
            return any(k in t for k in keys)

        keep_mask = []
        for _, r in mon.iterrows():
            title_txt = r["_TITULO_"]
            if _is_republic_office(title_txt):
                keep_mask.append(False); continue
            overlaps = _overlaps_any(r["_Y1_"], r["_Y2_"], monarchy_ranges) if monarchy_ranges else True
            if mode == "reinantes":
                keep_mask.append(overlaps and _has_any(title_txt, sovereign_kw))
            else:
                keep_mask.append(overlaps and _has_any(title_txt, sovereign_kw + nobility_extra_kw))

        mon = mon[pd.Series(keep_mask, index=mon.index)].copy()

        # Ordenação e DF final
        mon["_SORT_"] = pd.to_numeric(mon["_INICIO_"].replace("", pd.NA), errors="coerce")
        mon = mon.sort_values(["_SORT_", "_MONARCA_"], ascending=[False, True], na_position="last")

        mon_show = mon.loc[:, ["_MONARCA_", "_TITULO_", "_CASA_", "_INICIO_", "_FIM_"]].copy().head(max_rows)
        for c in mon_show.columns:
            mon_show[c] = mon_show[c].astype(str).str.replace(".0", "", regex=False)
        mon_show = mon_show.rename(columns={
            "_MONARCA_": tr("history.col.monarca") if "tr" in globals() else "Monarca",
            "_TITULO_":  tr("history.col.titulo")  if "tr" in globals() else "Título",
            "_CASA_":    tr("history.col.casa")    if "tr" in globals() else "Dinastia/Casa",
            "_INICIO_":  tr("history.col.inicio")  if "tr" in globals() else "Início",
            "_FIM_":     tr("history.col.fim")     if "tr" in globals() else "Fim",
        })

        # ===== Wrapper de largura total =====
        # --- wrapper largura total ---
        st.markdown('<div class="wbm-breakout">', unsafe_allow_html=True)

        # ===== Tabela (esq.) + Top-3 monarcas (dir.) =====
        col_tbl, col_right = st.columns([7, 5], gap="small")
        with col_tbl:
            st.markdown(f"**{label_monarchs_title}**")
            if mon_show.empty:
                st.caption(msg_empty)
            else:
                st.dataframe(
                    mon_show,
                    use_container_width=True,
                    hide_index=True,
                    height=min(640, 40 + 28 * min(len(mon_show), 18)),
                    column_config={
                        (tr('history.col.inicio') if 'tr' in globals() else 'Início'): st.column_config.TextColumn(
                            tr('history.col.inicio') if 'tr' in globals() else 'Início'),
                        (tr('history.col.fim') if 'tr' in globals() else 'Fim'): st.column_config.TextColumn(
                            tr('history.col.fim') if 'tr' in globals() else 'Fim'),
                    },
                )
                # st.download_button(
                #     label=label_download,
                #     data=mon_show.to_csv(index=False).encode("utf-8"),
                #     file_name=f"{iso3u}_monarchs.csv",
                #     mime="text/csv",
                #     use_container_width=True,
                # )

        with col_right:
            # ===== NOVO: Top-3 por anos reinantes (por monarca) =====
            if not mon.empty:
                # anos seguros + reinado em curso => até ano atual
                y1 = pd.to_numeric(mon["_Y1_"], errors="coerce")
                y2 = pd.to_numeric(mon["_Y2_"], errors="coerce")
                current_year = datetime.now().year
                y1f = y1.fillna(y2).fillna(current_year)
                y2f = y2.fillna(y1).fillna(current_year)
                dur = (y2f - y1f + 1).clip(lower=0)

                top3 = (
                    pd.DataFrame({"Monarca": mon["_MONARCA_"].astype(str), "Anos": dur.astype(int)})
                    .groupby("Monarca", as_index=False)["Anos"].sum()
                    .sort_values("Anos", ascending=False)
                    .head(3)
                )
                if not top3.empty:
                    fig_top3 = px.bar(
                        top3, x="Anos", y="Monarca", orientation="h", text="Anos", template="plotly_dark"
                    )
                    fig_top3.update_traces(textposition="outside")
                    fig_top3.update_layout(
                        title_text="Top 3 monarcas por anos no trono",
                        height=360, margin=dict(l=6, r=6, t=28, b=6),
                        xaxis_title="", yaxis_title=""
                    )
                    st.plotly_chart(fig_top3, use_container_width=True)
                else:
                    st.caption(msg_empty)
            else:
                st.caption(msg_empty)

        # ===== (mantém) gráfico Top-10 dinastias por anos no trono — EM BAIXO =====
        if not mon.empty:
            tmp = mon.copy()
            tmp["_house"] = tmp["_CASA_"].replace("—", "").str.strip()
            tmp = tmp[tmp["_house"].ne("")]

            y1 = pd.to_numeric(tmp["_Y1_"], errors="coerce")
            y2 = pd.to_numeric(tmp["_Y2_"], errors="coerce")
            ymin = y1.where(~y1.isna(), y2)
            ymax = y2.where(~y2.isna(), y1)
            tmp["_anos"] = (ymax - ymin + 1).clip(lower=0).fillna(0).astype(int)

            by_house = (
                tmp.groupby("_house")["_anos"]
                .sum()
                .reset_index(name="Anos")
                .sort_values("Anos", ascending=False)
                .head(10)
            )
            if not by_house.empty:
                fig_house = px.bar(by_house, x="Anos", y="_house", orientation="h", text="Anos", template="plotly_dark")
                fig_house.update_traces(textposition="outside")
                fig_house.update_layout(
                    title_text=title_dyne,   # ex.: "Top-10 dinastias por anos no trono"
                    height=360, margin=dict(l=6, r=6, t=28, b=6),
                    xaxis_title="", yaxis_title=""
                )
                st.plotly_chart(fig_house, use_container_width=True)

        st.markdown('</div>', unsafe_allow_html=True)
