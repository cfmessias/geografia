# paises.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px
import plotly.graph_objects as go
from services.i18n import t as tr          # i18n
from services.i18n_boot import _ensure_lang_state
from utils.subnav import subnav
from views.migration_tables import render_country_migration_tables
from views.languages import render_country_languages_line, render_country_languages_expander
from views.render_monarchy_expander import render_monarchy_expander
from views.origins import render_origins_expander
from views.colonizacao import render_colonization_expander
from views.render_wars_battles_expander import render_wars_battles_expander
from views.economics import render_wdi_panel
from views.demography import render_demography_expander

# -------------------------- Helpers --------------------------

def _paises_submenu() -> str:
    """Submenu de Países. Retorna a key da aba selecionada."""
    try:
        label_ov    = tr("subnav.visao_global")
        label_demog = tr("subnav.demografia_pais")
        label_hist  = tr("subnav.historia")
        label_econ  = tr("subnav.economia")
    except Exception:
        # fallback simples caso tr() não esteja disponível
        label_ov, label_demog, label_hist, label_econ = (
            "Visão global", "Demografia", "História", "Economia"
        )

    tabs = [
        ("ov",    label_ov),
        ("demog", label_demog),
        ("hist",  label_hist),
        ("econ",  label_econ),   # novo, fica à direita
    ]

    keys   = [k for k, _ in tabs]
    labels = dict(tabs)

    cur = st.session_state.get("paises_mode", "ov")
    if cur not in keys:
        cur = "ov"

    mode = st.radio(
        label="",
        options=keys,
        index=keys.index(cur),
        format_func=lambda k: labels[k],
        horizontal=True,
    )
    st.session_state["paises_mode"] = mode
    return mode


def _colcfg_leadership():
    return {
        "Pessoa":        st.column_config.TextColumn(tr("cols.person")),
        "Partido":       st.column_config.TextColumn(tr("cols.party")),
        "Início":        st.column_config.TextColumn(tr("cols.start")),
        "Fim":           st.column_config.TextColumn(tr("cols.end")),
        "Causa do fim":  st.column_config.TextColumn(tr("cols.end_cause")),
    }

def _colcfg_cities():
    return {
        "Cidade":         st.column_config.TextColumn(tr("cols.city")),
        "Capital?":       st.column_config.TextColumn(tr("cols.capital_q")),
        "Região (P131)":  st.column_config.TextColumn(tr("cols.region_p131")),
        "População":      st.column_config.NumberColumn(tr("cols.population"), format="%d"),
        "Ano":            st.column_config.TextColumn(tr("cols.year")),
    }

def _colcfg_unesco():
    return {
        "Sítio":  st.column_config.TextColumn(tr("cols.site")),
        "Tipo":   st.column_config.TextColumn(tr("cols.type")),
        "Ano":    st.column_config.TextColumn(tr("cols.year")),
        "lat":    st.column_config.NumberColumn(tr("cols.lat"), format="%.4f"),
        "lon":    st.column_config.NumberColumn(tr("cols.lon"), format="%.4f"),
    }

def _colcfg_medals():
    return {
        "Ano":    st.column_config.TextColumn(tr("cols.year")),
        "Ouro":   st.column_config.NumberColumn(tr("cols.gold"), format="%d"),
        "Prata":  st.column_config.NumberColumn(tr("cols.silver"), format="%d"),
        "Bronze": st.column_config.NumberColumn(tr("cols.bronze"), format="%d"),
        "Total":  st.column_config.NumberColumn(tr("cols.total"), format="%d"),
    }

def _fmt_int(x) -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return ""
        return f"{int(float(x)):,}".replace(",", " ")
    except Exception:
        return str(x) if x is not None else ""

def _fmt_year(x) -> str:
    try:
        if pd.isna(x):
            return ""
        return str(int(x))
    except Exception:
        s = str(x)
        return s[:4] if len(s) >= 4 and s[:4].isdigit() else s


def _country_selector(countries_df: pd.DataFrame) -> tuple[str | None, str | None]:
    """
    Pesquisa + select + botão numa linha, sem mexer no session_state do select.
    'paises_iso3' é a única fonte de verdade. O índice do select é calculado a partir dela.
    """
    lang = st.session_state.get("lang", "pt").lower()

    df = countries_df.copy()
    df["iso3u"] = df["iso3"].astype(str).str.upper().str.strip()

    if lang == "pt" and "name_pt" in df.columns:
        df["label"] = df["name_pt"].astype(str)
    elif lang != "pt" and "name_en" in df.columns:
        df["label"] = df["name_en"].astype(str)
    elif "name" in df.columns:
        df["label"] = df["name"].astype(str)
    else:
        df["label"] = df["iso3u"]

    label_by_iso = dict(zip(df["iso3u"], df["label"]))
    iso_by_label = {v: k for k, v in label_by_iso.items()}

    placeholder_label = tr("labels.selecione_um_pais")

    with st.form("pais_form", clear_on_submit=False):
        col_q, col_sel, col_btn = st.columns([3, 7, 2], gap="small")

        # campo de pesquisa
        with col_q:
            q = st.text_input(
                tr("paises.pesquisar_nome_contem"),
                value=st.session_state.get("paises_search", ""),
                placeholder=tr("paises.placeholder_pesquisa"),
                key="paises_search",
                label_visibility="collapsed",
            )

        # filtra labels conforme pesquisa
        if q:
            opts = df[df["label"].str.contains(q, case=False, na=False)]
        else:
            opts = df

        labels = opts["label"].tolist()
        options_ui = [placeholder_label] + labels

        # índice calculado apenas a partir de paises_iso3 (fonte de verdade)
        cur_iso = st.session_state.get("paises_iso3")
        cur_lbl = label_by_iso.get(cur_iso) if cur_iso else None
        if cur_lbl and cur_lbl in options_ui:
            idx = options_ui.index(cur_lbl)
        else:
            idx = 0  # placeholder

        with col_sel:
            chosen_label_ui = st.selectbox(
                tr("labels.pa_s"),
                options=options_ui,
                index=idx,
                key="paises_country_select",   # nunca escrever este key manualmente
                label_visibility="collapsed",
            )

        with col_btn:
            submitted = st.form_submit_button(tr("paises.abrir"), use_container_width=True)

    # interpreta seleção
    chosen_label = None if chosen_label_ui == placeholder_label else chosen_label_ui

    if submitted:
        if chosen_label:
            chosen_iso3 = iso_by_label.get(chosen_label)
            st.session_state["pais_selected"] = chosen_label
            st.session_state["paises_iso3"]   = chosen_iso3
            return chosen_label, chosen_iso3
        else:
            # clicou sem escolher -> mantém o atual, se houver
            cur_iso = st.session_state.get("paises_iso3")
            return (label_by_iso.get(cur_iso), cur_iso) if cur_iso else (None, None)

    # sem submit: devolve o atual
    cur_iso = st.session_state.get("paises_iso3")
    if cur_iso:
        return (label_by_iso.get(cur_iso), cur_iso)

    # se já escolheu no select mas ainda não submeteu
    if chosen_label:
        return chosen_label, iso_by_label.get(chosen_label)

    return None, None


# -------------------------- Secções --------------------------

def render_migration_section(iso3: str) -> None:
    _ensure_lang_state()

    from services.offline_store import (
        load_migration_latest_for_iso3,
        load_migration_ts_for_iso3,
        load_migration_inout,     # UN DESA (full)
        MIG_INOUT_CSV,
    )

    with st.expander(tr("labels.migra_o")):
        # ───────── WDI (indicadores resumidos + série temporal) ─────────
        latest = load_migration_latest_for_iso3(iso3)
        ts     = load_migration_ts_for_iso3(iso3)

        # mapeia indicador → chave de tradução para o label
        kmap = {
            "SM.POP.NETM":          "paises.migracao_liquida_pessoas",
            "BX.TRF.PWKR.CD.DT":    "paises.remessas_recebidas_usd",
            "BX.TRF.PWKR.DT.GD.ZS": "paises.remessas_percent_pib",
        }
        unit_fmt = {
            "SM.POP.NETM": "int",
            "BX.TRF.PWKR.CD.DT": "money",
            "BX.TRF.PWKR.DT.GD.ZS": "pct",
        }

        def _fmt_value(v, kind, *, scale=None):
            try:
                v = float(v)
            except Exception:
                return "—"
            if kind == "pct":
                return f"{v:.1f}%"
            if kind == "money":
                if scale is None:
                    scale = "B" if abs(v) >= 1e9 else ("M" if abs(v) >= 1e6 else None)
                if scale == "B":
                    return f"{v/1e9:.2f} B"
                if scale == "M":
                    return f"{v/1e6:.2f} M"
                return f"{int(round(v)):,}".replace(",", " ")
            return f"{int(round(v)):,}".replace(",", " ")

        def _fmt_delta(delta, kind, *, ref_value=None):
            if kind == "pct":
                return f"{delta:+.1f} p.p."
            if kind == "money":
                ref_scale = "B" if (ref_value is not None and abs(ref_value) >= 1e9) else \
                            ("M" if (ref_value is not None and abs(ref_value) >= 1e6) else None)
                s = _fmt_value(delta, "money", scale=ref_scale)
                return ("+" if delta > 0 else "") + s
            return f"{delta:+,.0f}".replace(",", " ")

        def _latest_and_prev(df_iso: pd.DataFrame, code: str):
            d = (
                df_iso[df_iso["indicator"] == code]
                .dropna(subset=["value"])
                .sort_values("year")
            )
            if d.empty:
                return None, None
            last = d.iloc[-1]
            prev = d.iloc[-2] if len(d) > 1 else None
            return last, prev

        cols = st.columns(3)
        i = 0
        for code, label_key in kmap.items():
            src = latest if not latest.empty and (latest["indicator"] == code).any() else ts
            last, prev = _latest_and_prev(src, code)
            if last is None:
                continue
            year = int(last["year"])
            val  = float(last["value"])

            val_txt = _fmt_value(val, unit_fmt.get(code, "int"))

            delta_txt = ""
            if prev is not None and pd.notna(prev["value"]):
                delta = val - float(prev["value"])
                delta_txt = _fmt_delta(delta, unit_fmt.get(code, "int"), ref_value=val)

            cols[i % 3].metric(f"{tr(label_key)} · {year}", val_txt, delta=delta_txt)
            i += 1

        # ───────── Série temporal (WDI — desde 1990) ─────────
        _FRAG = getattr(st, "fragment", None)

        def _migration_wdi_timeseries(iso3: str, ts: pd.DataFrame, kmap: dict, unit_fmt: dict):
            series_opts = [(tr(kmap[k]), k) for k in kmap.keys()]
            labels = [x[0] for x in series_opts]
            code_by_label = dict(series_opts)

            sel_lbl = st.selectbox(
                tr("labels.s_rie_temporal_wdi_desde_1990"),
                labels,
                index=0,
                key=f"mig_wdi_sel_{iso3}",
            )
            code = code_by_label[sel_lbl]

            base = ts[(ts["iso3"] == iso3) & (ts["indicator"] == code)].copy()
            if base.empty:
                st.caption(tr("labels.sem_s_rie_temporal_para_o_indicador_selecionado"))
                return

            base["year"] = pd.to_numeric(base["year"], errors="coerce")
            base["value"] = pd.to_numeric(base["value"], errors="coerce")
            base = (
                base.dropna(subset=["year", "value"])
                    .sort_values("year")
                    .drop_duplicates(subset=["year"], keep="last")
            )
            base = base.loc[base["year"] >= 1990, ["year", "value"]]

            if base.empty:
                st.caption(tr("labels.sem_observa_es_desde_1990"))
                return

            y_min, y_max = int(base["year"].min()), int(base["year"].max())
            y_title = sel_lbl if unit_fmt.get(code) != "pct" else sel_lbl + " (%)"

            st.altair_chart(
                alt.Chart(base)
                .mark_line(point=True)
                .encode(
                    x=alt.X("year:Q", title=tr("climate_indicators.ano"),
                            scale=alt.Scale(domain=[y_min, y_max]),
                            axis=alt.Axis(format="d")),
                    y=alt.Y("value:Q", title=y_title),
                    tooltip=[
                        alt.Tooltip("year:Q", title=tr("climate_indicators.ano"), format="d"),
                        alt.Tooltip("value:Q", title=tr("paises.valor"), format=",.0f"),
                    ],
                )
                .properties(height=240),
                use_container_width=True
            )

        if _FRAG:
            _migration_wdi_timeseries = _FRAG(_migration_wdi_timeseries)
        _migration_wdi_timeseries(iso3, ts, kmap, unit_fmt)

        st.markdown(tr("labels.text"))

        # ───────── UN DESA (imigração/emigração) ─────────
        df_all = load_migration_inout()
        csv_name = getattr(MIG_INOUT_CSV, "name", "migration_inout.csv")

        if df_all.empty:
            st.caption(tr("labels.un_desa_dataset_vazio_n_o_encontrado_csv_name", csv_name=csv_name))
            return

        iso3u = str(iso3).upper()
        df = df_all.copy()
        df.columns = df.columns.str.replace("\ufeff", "", regex=False).str.strip()

        want = ["iso3", "year", "immigrants", "emigrants"]
        missing = [c for c in want if c not in df.columns]
        if missing:
            cols_txt = ", ".join(map(repr, df.columns))
            st.caption(tr("labels.un_desa_headers_unexpected",
                          csv_name=csv_name, missing=str(missing), columns=cols_txt))
            return

        df["iso3"] = df["iso3"].astype(str).str.upper()
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
        df["immigrants"] = pd.to_numeric(df["immigrants"], errors="coerce")
        df["emigrants"]  = pd.to_numeric(df["emigrants"],  errors="coerce")

        io_df = (
            df.loc[df["iso3"] == iso3u, want]
              .dropna(subset=["year"])
              .sort_values("year")
              .drop_duplicates("year", keep="last")
              .tail(30)
              .copy()
        )

        if io_df.empty:
            st.caption(tr("labels.sem_dados_un_desa_para_este_pa_s_no_csv_name_n_o_h_linhas_para_iso3_iso3u",
                          csv_name=csv_name, iso3u=iso3u))
            return

        long = (
            io_df.melt(
                id_vars="year",
                value_vars=["immigrants", "emigrants"],
                var_name="tipo",
                value_name="valor",
            )
            .assign(tipo=lambda d: d["tipo"].map({
                "immigrants": tr("paises.imigracao"),
                "emigrants":  tr("paises.emigracao"),
            }))
        )

        years_sorted = sorted(int(y) for y in long["year"].dropna().unique())
        color_enc = alt.Color(
            "tipo:N", title="",
            scale=alt.Scale(domain=[tr("paises.imigracao"), tr("paises.emigracao")],
                            range=["#2E7D32", "#E53935"]),
            legend=alt.Legend(orient="right"),
        )
        x_enc = alt.X("year:O", title=tr("climate_indicators.ano"), sort=years_sorted)

        ann = io_df.copy()
        ann["diff"]  = ann["emigrants"] - ann["immigrants"]
        ann["label"] = ann["diff"].apply(lambda x: f"{x/1_000:+.0f} K")
        ann["mid"]   = (ann["emigrants"] + ann["immigrants"]) / 2

        lines = (
            alt.Chart(long)
            .mark_line(point=True)
            .encode(
                x=x_enc,
                y=alt.Y("valor:Q", title=tr("paises.pessoas")),
                color=color_enc,
                tooltip=[
                    alt.Tooltip("year:O", title=tr("climate_indicators.ano")),
                    "tipo:N",
                    alt.Tooltip("valor:Q", title=tr("paises.pessoas"), format=",.0f"),
                ],
            )
        )
        labels = (
            alt.Chart(ann)
            .mark_text(size=11, color="#E0E0E0", baseline="middle")
            .encode(
                x=alt.X("year:O", sort=years_sorted, title=tr("climate_indicators.ano")),
                y=alt.Y("mid:Q"),
                text="label:N",
                tooltip=[
                    alt.Tooltip("year:O", title=tr("climate_indicators.ano")),
                    alt.Tooltip("diff:Q", title=tr("paises.δ_e_i"), format=",.0f"),
                ],
            )
        )

        st.altair_chart((lines + labels).properties(height=260), use_container_width=True)


def _profile_by_iso3(iso3: str) -> dict:
    from services.offline_store import load_profiles_master
    df = load_profiles_master()
    if not df.empty:
        row = df[df["iso3"].astype(str).str.upper() == str(iso3).upper()]
        if not row.empty:
            return row.iloc[0].to_dict()
    return {"iso3": iso3, "name": iso3}


def _mini_line(df: pd.DataFrame, ycol: str, ytitle: str):
    if df.empty or ycol not in df.columns or df[ycol].notna().sum() == 0:
        st.caption(tr("labels.sem_dados_de_ytitle_lower", ytitle=ytitle))
        return
    d = df.dropna(subset=["year", ycol]).copy()
    d["year"] = pd.to_numeric(d["year"], errors="coerce")

    chart = (
        alt.Chart(d)
        .mark_line()
        .encode(
            x=alt.X("year:Q", axis=alt.Axis(format="d", title=None)),
            y=alt.Y(f"{ycol}:Q", title=ytitle),
            tooltip=[
                alt.Tooltip("year:Q", title=tr("climate_indicators.ano"), format="d"),
                alt.Tooltip(f"{ycol}:Q", title=ytitle)
            ],
        )
        .properties(height=170)
    )
    st.altair_chart(chart, use_container_width=True)


# -------------------------- UI principal --------------------------

# --- Mini-view "economia (os 4 do Economics)" para o país ---------------------
# from views.economics import get_wdi_selection, fetch_wdi_dataset, render_wdi_charts_2x2

# def render_country_demography(iso3: str) -> None:
#     # mostra EXACTAMENTE os 4 escolhidos na página Economics (com fallback)
#     codes, (year_min, year_max) = get_wdi_selection()
#     df, labels_map = fetch_wdi_dataset(iso3, codes, year_min, year_max)
#     if df.empty:
#         st.caption(tr("labels.sem_s_ries_do_world_bank")); return
#     st.markdown(tr("labels.indicadores_economicos") if "tr" in globals() else "Economic indicators")
#     render_wdi_charts_2x2(df, codes, labels_map)

# --- SUBSTITUI a tua função por esta ----------------------------------------
def render_paises_tab():
    _ensure_lang_state()
    # 1) Submenu primeiro (aparece logo ao render)
    mode = _paises_submenu()

    from services.offline_store import (
        list_available_countries,
        wb_series_for_country,   # usado dentro da demografia
        cities_for_iso3,
        unesco_for_iso3,
        leaders_for_iso3,
        load_olympics_summer_csv,
        load_religion,
        load_flag_info,
        load_tourism_ts,
    )

    countries = list_available_countries()
    if countries.empty:
        st.error(tr("labels.sem_paises_disponiveis"))
        return

    country_name, iso3 = _country_selector(countries)
    if not country_name or not iso3:
        st.info(tr("labels.escolhe_um_pa_s_e_clica_abrir"))
        return

    prof = _profile_by_iso3(iso3) if iso3 else None

    if not iso3:
    # Mostra só a instrução; o submenu já está no topo
        st.info(tr("labels.escolhe_um_pa_s_e_clica_abrir"))
        return

    # ── Cabeçalho com informação essencial (uma coluna) ──────────────────────
    st.subheader(prof.get("name") or country_name)

    if mode == "ov":
        # ---------- CARTÃO / FACTOS EM DUAS COLUNAS ----------

        info = load_flag_info(prof.get("name") or country_name, iso3)

        facts = (info or {}).get("facts") or {}
        if info and info.get("flag_url"):
            st.image(info["flag_url"], width=96)

        def _fact_first(facts, *keys):
            for k in keys:
                v = facts.get(k)
                if v is not None and str(v).strip():
                    return str(v).strip()
            return None

        # Moeda: tentar site de bandeiras → fallback do profile
        moeda_txt = _fact_first(
            facts,
            "Moeda", "Moeda(s)", "Moeda (ISO)",
            "Currency", "Currency (ISO)", "Currency code", "Currency codes",
        )
        if not moeda_txt:
            name   = prof.get("currency_name") or prof.get("currency")
            code   = prof.get("currency_code") or prof.get("currency_iso")
            symbol = prof.get("currency_symbol")
            parts = [name, f"({symbol})" if symbol else None, f"{code}" if code else None]
            moeda_txt = " ".join([p for p in parts if p]).strip() or None

        # Campos “básicos”
        inc  = prof.get("inception") or prof.get("independence") or prof.get("inception_year")
        pres = prof.get("head_of_state") or ""
        pm   = prof.get("head_of_government") or ""
        pm_p = prof.get("hog_party") or ""
        pop  = prof.get("population")
        area = prof.get("area_km2")

        # Fun auxiliar (rótulo + valor com i18n)
        def _row(label_text: str, value: str | int | float | None):
            if value is None or str(value).strip() == "":
                value = "—"
            st.markdown(tr("labels.label_val", label=label_text, val=str(value)))

        # Duas colunas
        colL, colR = st.columns(2)

        with colL:
            # Ano de fundação/independência
            label = tr("labels.ano_de_fundacao_ou_independencia", year="")  # resolve {year} -> ""
            label = label.replace("**", "").strip()
            if label.endswith(":"):
                label = label[:-1].rstrip()
            _row(label, _fmt_year(inc) or "—")
            # Estado soberano (facts)
            _row(tr("paises.facts.estado_soberano"), facts.get("Estado soberano"))
            # Presidente / Chefe de governo
            if pres: _row(tr("labels.presidente"), pres)
            if pm:   _row(tr("labels.chefe_de_governo"), pm)
            # Linguas oficiais
            render_country_languages_line(iso3)
            # Capital
            _row(tr("country.capital"), prof.get("capital") or "—")
            # Continente (facts)
            _row(tr("paises.facts.continente"), facts.get("O Continente"))
            # Moeda
            _row(tr("labels.moeda"), moeda_txt or "—")
            # População
            _row(tr("labels.popula_o").replace("**","").replace(":",""),
                _fmt_int(pop) if pop is not None else "—")
            
        with colR:
           # Área
            _row(tr("labels.rea").replace("**","").replace(":",""),
                (_fmt_int(area) + " km²") if area is not None else "—")
            # Códigos dos países (facts)
            _row(tr("paises.facts.codigos_pais"), facts.get("Códigos dos países"))
            # Membro de (facts)
            _row(tr("paises.facts.membro_de"), facts.get("Membro de"))
            # Ponto mais alto / baixo (facts)
            _row(tr("paises.facts.ponto_mais_alto"),  facts.get("Ponto mais alto"))
            _row(tr("paises.facts.ponto_mais_baixo"), facts.get("Ponto mais baixo"))
            # PIB per capita (facts)
            _row(tr("paises.facts.pib_per_capita"),   facts.get("PIB per capita"))
            # Código de área telefónica (facts)
            _row(tr("paises.facts.codigo_area_tel"),  facts.get("Código de área telefónica"))
            # Domínio nacional (facts)
            _row(tr("paises.facts.dominio_nacional"), facts.get("Domínio nacional"))
        # ---------- /CARTÃO EM DUAS COLUNAS ----------

        render_country_languages_expander(iso3, default_open=False)
    
        # CIDADES
        with st.expander(tr("labels.principais_cidades")):
            cities = cities_for_iso3(iso3)
            if cities.empty:
                st.info(tr("labels.sem_cidades_gera_csv"))
            else:
                c = cities.copy()
                for k in ("city","admin","type","is_capital","population","year","lat","lon"):
                    if k not in c.columns:
                        c[k] = pd.NA

                def _clean_text(v):
                    s = str(v).strip()
                    return None if s.lower() in {"", "none", "nan", "empty"} else s

                c["city"]  = c["city"].apply(_clean_text)
                c["admin"] = c["admin"].apply(_clean_text)
                c["type"]  = c["type"].apply(_clean_text)

                c = c[c["city"].notna()]
                if c.empty:
                    st.info(tr("labels.sem_cidades_validas"))
                else:
                    c["__year"] = pd.to_numeric(c["year"], errors="coerce")
                    c["__pop"]  = pd.to_numeric(c["population"], errors="coerce")

                    def _join_unique(series: pd.Series) -> str:
                        vals = [str(x) for x in series.dropna().astype(str) if x]
                        return ", ".join(sorted(set(vals))) if vals else ""

                    idx_latest = (
                        c.sort_values(["city", "__year"], ascending=[True, True])
                        .groupby("city", observed=False)["__year"].idxmax()
                        .dropna().astype(int)
                    )
                    if idx_latest.empty:
                        idx_latest = (
                            c.sort_values(["city", "__pop"], ascending=[True, True])
                            .groupby("city", observed=False)["__pop"].idxmax()
                            .dropna().astype(int)
                        )
                    if idx_latest.empty:
                        idx_latest = c.groupby("city", observed=False).head(1).index

                    latest = c.loc[idx_latest, ["city","is_capital","population","__year"]].rename(
                        columns={"__year":"year"}
                    )
                    agg = (
                        c.groupby("city", as_index=False, observed=False)
                        .agg(admin=("admin", _join_unique), type=("type", _join_unique))
                    )
                    show = latest.merge(agg, on="city", how="left").rename(columns={
                        "city": "Cidade",
                        "admin": "Região (P131)",
                        "type": "Tipo",
                        "is_capital": "Capital?",
                        "population": "População",
                        "year": "Ano",
                    })

                    if "Capital?" in show.columns:
                        show["Capital?"] = show["Capital?"].map({1:"Sim",0:"Não",True:"Sim",False:"Não"}).fillna("")
                    if "Ano" in show.columns:
                        show["Ano"] = show["Ano"].apply(lambda x: "" if pd.isna(x) else str(int(x)))
                    if "População" in show.columns:
                        show["População"] = show["População"].apply(
                            lambda v: "" if pd.isna(v) else f"{int(v):,}".replace(",", " ")
                        )

                    show["_cap"] = show["Capital?"].eq("Sim") if "Capital?" in show.columns else False
                    show["_pop"] = (
                        pd.to_numeric(show.get("População", 0).astype(str).str.replace(" ","").str.replace(",",""),
                                      errors="coerce").fillna(0)
                    )
                    show = show.sort_values(["_cap","_pop","Cidade"], ascending=[False, False, True]) \
                            .drop(columns=["_cap","_pop","Tipo"], errors="ignore")
                    cols = [c for c in ["Cidade","Capital?","Região (P131)","População","Ano"] if c in show.columns]

                    colL, colR = st.columns([0.62, 0.38], gap="large")
                    with colL:
                        st.markdown(tr("labels.principais_cidades_munic_pios"))
                        st.dataframe(show[cols] if cols else show,
                                     use_container_width=True, hide_index=True,
                                     column_config=_colcfg_cities())
                    with colR:
                        st.markdown(tr("labels.mapa"))
                        for k in ("lat", "lon"):
                            if k not in c.columns:
                                c[k] = pd.NA
                        cc = c.copy()
                        cc["lat"] = pd.to_numeric(cc["lat"], errors="coerce")
                        cc["lon"] = pd.to_numeric(cc["lon"], errors="coerce")

                        pts = (
                            cc.dropna(subset=["lat","lon"])
                            .loc[cc["lat"].between(-90, 90) & cc["lon"].between(-180, 180),
                                 ["city","lat","lon"]]
                            .drop_duplicates(subset=["city"], keep="first")
                        )
                        if len(pts) > 0:
                            st.map(pts[["lat","lon"]], use_container_width=True)

        # UNESCO
        with st.expander(tr("labels.patrim_nio_mundial_unesco")):
            u = unesco_for_iso3(iso3)
            if not u.empty:
                u = u.copy()
                for k in ("site_qid","site","type","year","lat","lon"):
                    if k not in u.columns:
                        u[k] = pd.NA
                u["year"] = pd.to_numeric(u["year"], errors="coerce")

                def _agg_types(s: pd.Series) -> str:
                    vals = [str(x) for x in s.dropna().astype(str) if x and str(x).lower() != "none"]
                    return ", ".join(sorted(set(vals)))

                u = (
                    u.sort_values(["site_qid","year"])
                    .groupby("site_qid", as_index=False, observed=False)
                    .agg({
                        "site": "first",
                        "type": _agg_types,
                        "year": "min",
                        "lat": "first",
                        "lon": "first",
                        "country": "first",
                        "iso3": "first",
                    })
                )
                u = u.rename(columns={"site":"Sítio","type":"Tipo","year":"Ano"})
                u["Ano"] = u["Ano"].apply(_fmt_year)

                cols = ["Sítio","Tipo","Ano","lat","lon"]
                ROW_H, HDR_H, MAX_H = 28, 38, 420
                n = len(u)
                height = min(MAX_H, HDR_H + ROW_H * max(n, 1))

                st.data_editor(
                    u[cols],
                    use_container_width=True,
                    hide_index=True,
                    height=height,
                    disabled=True,
                    column_config=_colcfg_unesco(),
                )
                if {"lat","lon"}.issubset(u.columns):
                    st.map(u[["lat","lon"]].dropna(), use_container_width=True)
            else:
                st.caption(tr("paises.label"))

        # Medalhas
        with st.expander(tr("labels.medalhas_ol_mpicas_totais_e_por_edi_o")):
            cdf = load_olympics_summer_csv()
            if not cdf.empty:
                cdf = cdf[cdf["iso3"].astype(str).str.upper() == iso3].copy()

            if cdf.empty:
                st.caption(tr("labels.sem_dados_de_medalhas_de_ver_o_no_csv_manual"))
            else:
                vals = (
                    cdf.reindex(columns=["summer_gold", "summer_silver", "summer_bronze"])
                    .apply(pd.to_numeric, errors="coerce").fillna(0).astype(int)
                )
                g = int(vals["summer_gold"].sum())
                s = int(vals["summer_silver"].sum())
                b = int(vals["summer_bronze"].sum())

                L_G = tr("cols.gold"); L_S = tr("cols.silver"); L_B = tr("cols.bronze")
                L_MEDAL = tr("cols.medal"); L_COUNT = tr("cols.quantity")

                bar_df = pd.DataFrame({L_MEDAL: [L_G, L_S, L_B], L_COUNT: [g, s, b]})
                ymax = max(1, int(bar_df[L_COUNT].max() * 1.20))

                fig = px.bar(
                    bar_df, x=L_MEDAL, y=L_COUNT, text=L_COUNT,
                    category_orders={L_MEDAL: [L_G, L_S, L_B]},
                    color=L_MEDAL,
                    color_discrete_map={L_G: "#d4af37", L_S: "#c0c0c0", L_B: "#cd7f32"},
                )
                fig.update_traces(
                    texttemplate="<b>%{text:d}</b>", textposition="outside",
                    textfont=dict(size=20), hovertemplate="%{x}: %{y:d}<extra></extra>", cliponaxis=False
                )
                fig.update_layout(
                    showlegend=False, xaxis_title=None, yaxis_title=None,
                    yaxis=dict(range=[0, ymax], tickfont=dict(size=14)),
                    xaxis=dict(tickfont=dict(size=14)), bargap=0.35,
                    margin=dict(l=8, r=8, t=20, b=0), height=320,
                    uniformtext_minsize=16, uniformtext_mode="show",
                )

                col_tab, col_fig = st.columns([3, 2], gap="medium")
                with col_tab:
                    df_local = cdf.copy()
                    for c in ("year", "city", "host_country"):
                        if c not in df_local.columns:
                            df_local[c] = pd.NA
                    for c in ("summer_gold", "summer_silver", "summer_bronze"):
                        if c not in df_local.columns:
                            df_local[c] = 0
                        df_local[c] = pd.to_numeric(df_local[c], errors="coerce").fillna(0).astype(int)
                    if "summer_total" not in df_local.columns:
                        df_local["summer_total"] = (
                            df_local["summer_gold"] + df_local["summer_silver"] + df_local["summer_bronze"]
                        )
                    if "year" in df_local.columns:
                        df_local["__year_num"] = pd.to_numeric(df_local["year"], errors="coerce")
                        sort_cols = ["__year_num"]
                    else:
                        sort_cols = ["summer_total"]

                    show_cols = ["year", "city", "host_country", "summer_gold",
                                 "summer_silver", "summer_bronze", "summer_total"]
                    show = (
                        df_local[show_cols + (["__year_num"] if "__year_num" in df_local.columns else [])]
                        .sort_values(by=sort_cols, ascending=True, na_position="last")
                        .drop(columns=["__year_num"], errors="ignore").reset_index(drop=True)
                    )
                    st.dataframe(
                        show, use_container_width=True, hide_index=True,
                        column_config={
                            "year":          st.column_config.TextColumn(tr("cols.year")),
                            "city":          st.column_config.TextColumn(tr("cols.city")),
                            "host_country":  st.column_config.TextColumn(tr("cols.host_country")),
                            "summer_gold":   st.column_config.NumberColumn(tr("cols.gold"),   format="%d"),
                            "summer_silver": st.column_config.NumberColumn(tr("cols.silver"), format="%d"),
                            "summer_bronze": st.column_config.NumberColumn(tr("cols.bronze"), format="%d"),
                            "summer_total":  st.column_config.NumberColumn(tr("cols.total"),  format="%d"),
                        },
                    )
                with col_fig:
                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

        # Religiões
        with st.expander(tr("labels.religi_es")):
            try:
                rel = load_religion()
                rr = rel[rel["iso3"] == iso3]
            except Exception:
                rr = pd.DataFrame()

            if not rr.empty:
                r = rr.iloc[0]
                items = [
                    (tr("religions.christianity"), float(r.get("christian", 0))),
                    (tr("religions.islam"),        float(r.get("muslim", 0))),
                    (tr("religions.unaffiliated"), float(r.get("unaffiliated", 0))),
                    (tr("religions.hinduism"),     float(r.get("hindu", 0))),
                    (tr("religions.buddhism"),     float(r.get("buddhist", 0))),
                    (tr("religions.folk"),         float(r.get("folk_religions", 0))),
                    (tr("religions.other"),        float(r.get("other_religions", 0))),
                    (tr("religions.judaism"),      float(r.get("jewish", 0))),
                ]
                df_rel = pd.DataFrame(items, columns=["religion", "pct"])
                df_rel["pct"] = pd.to_numeric(df_rel["pct"], errors="coerce").fillna(0.0)
                df_rel = df_rel.sort_values("pct", ascending=False).reset_index(drop=True)

                df_rel["label"] = df_rel["pct"].map(lambda v: f"{v:.2f}")
                df_rel["label_pos"] = (df_rel["pct"] + 0.8).clip(upper=99.2)

                base = (
                    alt.Chart(df_rel)
                    .mark_bar()
                    .encode(
                        y=alt.Y("religion:N", sort="-x", title=""),
                        x=alt.X("pct:Q", title=tr("cols.population_pct"), scale=alt.Scale(domain=[0, 100])),
                        tooltip=[alt.Tooltip("religion:N", title=tr("cols.religion")),
                                 alt.Tooltip("pct:Q", title=tr("cols.population_pct"), format=".2f")],
                    )
                    .properties(height=300)
                )
                labels = (
                    alt.Chart(df_rel)
                    .mark_text(align="left", baseline="middle", dx=3, color="#e6e6e6")
                    .encode(y="religion:N", x="label_pos:Q", text="label:N")
                )
                _, c2, _ = st.columns([1, 8, 1])
                with c2:
                    st.altair_chart(base + labels, use_container_width=True)

                st.caption(tr("labels.ano_de_referencia",
                              year=int(pd.to_numeric(r.get('source_year', 2010), errors='coerce'))))
            else:
                st.caption(tr("labels.sem_dados_de_religi_o_em_data_religion_csv"))

        # Turismo (como tinhas)
        with st.expander(tr("labels.turismo")):
            t_ts = load_tourism_ts()
            kmap = {
                "ST.INT.ARVL":       tr("tourism.arrivals"),
                "ST.INT.DPRT":       tr("tourism.departures"),
                "ST.INT.RCPT.CD":    tr("tourism.receipts_usd"),
                "ST.INT.XPND.CD":    tr("tourism.expenditure_usd"),
                "ST.INT.RCPT.XP.ZS": tr("tourism.receipts_pct_exports"),
                "ST.INT.XPND.MP.ZS": tr("tourism.expenditure_pct_imports"),
            }
            unit = {
                "ST.INT.ARVL": "int",
                "ST.INT.DPRT": "int",
                "ST.INT.RCPT.CD": "money",
                "ST.INT.XPND.CD": "money",
                "ST.INT.RCPT.XP.ZS": "pct",
                "ST.INT.XPND.MP.ZS": "pct",
            }
            def _fmt_value(v, kind, *, scale=None):
                try: v = float(v)
                except Exception: return "—"
                if kind == "pct":   return f"{v:.1f}%"
                if kind == "money":
                    if scale is None:
                        scale = "B" if abs(v) >= 1e9 else ("M" if abs(v) >= 1e6 else None)
                    if scale == "B": return f"{v/1e9:.2f} B"
                    if scale == "M": return f"{v/1e6:.2f} M"
                    return f"{int(round(v)):,}".replace(",", " ")
                return f"{int(round(v)):,}".replace(",", " ")
            def _fmt_delta(delta, kind, *, ref_value=None):
                if kind == "pct":   return f"{delta:+.1f} p.p."
                if kind == "money":
                    ref_scale = "B" if (ref_value is not None and abs(ref_value) >= 1e9) else \
                                ("M" if (ref_value is not None and abs(ref_value) >= 1e6) else None)
                    s = _fmt_value(delta, "money", scale=ref_scale)
                    return ("+" if delta > 0 else "") + s
                return f"{delta:+,.0f}".replace(",", " ")
            def _latest_and_prev(df_all, code):
                d = (
                    df_all[(df_all["iso3"] == iso3) & (df_all["indicator"] == code)]
                    .dropna(subset=["value"]).sort_values("year")
                )
                if d.empty: return None, None
                last = d.iloc[-1]
                prev = d.iloc[-2] if len(d) > 1 else None
                return last, prev
            cols = st.columns(3)
            i = 0
            for code, label in kmap.items():
                last, prev = _latest_and_prev(t_ts, code)
                if last is None: continue
                year = int(last["year"]); val  = float(last["value"])
                val_txt = _fmt_value(val, unit.get(code, "int"))
                delta_txt = ""
                if prev is not None and pd.notna(prev["value"]):
                    delta = val - float(prev["value"])
                    delta_txt = _fmt_delta(delta, unit.get(code, "int"), ref_value=val)
                cols[i % 3].metric(f"{label} · {year}", val_txt, delta=delta_txt)
                i += 1

            _FRAG = getattr(st, "fragment", None)
            def _tourism_timeseries_compare(iso3: str, t_ts: pd.DataFrame, kmap: dict):
                VIEWS = {
                    "rcpt_vs_xpnd_usd": {
                        "label": tr("tourism.view.receipts_vs_expenditure_usd"),
                        "codes": ["ST.INT.RCPT.CD", "ST.INT.XPND.CD"],
                        "y_title": tr("tourism.ytitle.usd_current"),
                    },
                    "pct_rcpt_vs_pct_xpnd": {
                        "label": tr("tourism.view.pct_receipts_vs_expenditure"),
                        "codes": ["ST.INT.RCPT.XP.ZS", "ST.INT.XPND.MP.ZS"],
                        "y_title": tr("tourism.ytitle.percent"),
                    },
                    "arrivals_vs_departures": {
                        "label": tr("tourism.view.arrivals_vs_departures"),
                        "codes": ["ST.INT.ARVL", "ST.INT.DPRT"],
                        "y_title": tr("tourism.ytitle.people"),
                    },
                }
                options = [v["label"] for v in VIEWS.values()]
                label2key = {v["label"]: k for k, v in VIEWS.items()}

                view_label = st.selectbox(
                    tr("labels.s_rie_temporal_turismo_ltimos_20_anos"),
                    options, index=0, key=f"tour_series_cmp_{iso3}",
                )
                meta = VIEWS[label2key[view_label]]
                codes = meta["codes"]; y_title = meta["y_title"]

                base = (
                    t_ts[(t_ts["iso3"] == iso3) & (t_ts["indicator"].isin(codes))]
                    .dropna(subset=["value"]).copy()
                )
                if base.empty:
                    st.caption(tr("labels.sem_s_rie_temporal_para_os_indicadores_selecionados"))
                    return

                base["year"] = pd.to_numeric(base["year"], errors="coerce").astype("Int64")
                base = (base.dropna(subset=["year"])
                            .sort_values(["indicator", "year"])
                            .drop_duplicates(subset=["indicator", "year"], keep="last"))
                most_recent_years = base[["year"]].drop_duplicates().sort_values("year").tail(20)["year"].tolist()
                sub = base[base["year"].isin(most_recent_years)].copy()
                if sub.empty:
                    st.caption(tr("labels.sem_observa_es_nos_ltimos_20_anos"))
                    return

                y_min, y_max = int(min(most_recent_years)), int(max(most_recent_years))
                label_map = {c: kmap.get(c, c) for c in codes}
                sub["metric"] = sub["indicator"].map(label_map)

                st.altair_chart(
                    alt.Chart(sub)
                    .mark_line(point=True)
                    .encode(
                        x=alt.X("year:Q", title=tr("climate_indicators.ano"),
                                scale=alt.Scale(domain=[y_min, y_max]),
                                axis=alt.Axis(format="d")),
                        y=alt.Y("value:Q", title=y_title),
                        color=alt.Color("metric:N", title="", sort=list(label_map.values())),
                        tooltip=[
                            alt.Tooltip("metric:N", title=tr("paises.indicador")),
                            alt.Tooltip("year:Q", title=tr("climate_indicators.ano"), format="d"),
                            alt.Tooltip("value:Q", title=tr("paises.valor"), format=",.0f"),
                        ],
                    )
                    .properties(height=260),
                    use_container_width=True,
                )
            if _FRAG:
                _tourism_timeseries_compare = _FRAG(_tourism_timeseries_compare)
            _tourism_timeseries_compare(iso3, t_ts, kmap)

    # ---------- Demografia ----------
    elif mode == "demog":
        #render_country_demography(iso3)
        # supondo que tens iso3 e country_name definidos e uma função tr disponível:
        render_demography_expander(iso3=iso3, country_name=country_name, tr=tr)

        render_migration_section(iso3)
        render_country_migration_tables(iso3, year=2024, top=20)
    

    # ---------- História ----------
    elif mode == "hist":

        render_origins_expander(iso3, default_open=False)
        render_monarchy_expander(iso3, default_open=False)
        render_colonization_expander(iso3, default_open=False)  
        cur_df, hist_df = leaders_for_iso3(iso3)
        base = hist_df if (hist_df is not None and not hist_df.empty) else cur_df
        
        with st.expander(tr("labels.lideranca_atual_e_historica"), expanded=False):
            if base is not None and not base.empty:
                h = base.copy()
                role_map = {"head_of_state": tr("labels.presidente"), "head_of_government": tr("labels.chefe_de_governo")}
                h["Função"] = h.get("role").map(role_map).fillna(h.get("role"))
                h["__start_dt"] = pd.to_datetime(h.get("start"), errors="coerce")
                h["__end_dt"]   = pd.to_datetime(h.get("end"),   errors="coerce")
                h["Início"] = h["__start_dt"].dt.strftime("%Y-%m-%d").fillna("")
                h["Fim"]    = h["__end_dt"].dt.strftime("%Y-%m-%d").fillna("")
                h["Partido"] = h.get("party").fillna("").astype(str).str.strip()
                h["Causa do fim"] = h.get("end_cause").fillna("").astype(str)

                def _prep(df: pd.DataFrame) -> pd.DataFrame:
                    if df is None or df.empty:
                        return pd.DataFrame(columns=["Pessoa","Partido","Início","Fim","Causa do fim"])
                    show = pd.DataFrame({
                        "Pessoa": df.get("person"),
                        "Partido": h.loc[df.index, "Partido"],
                        "Início": h.loc[df.index, "Início"],
                        "Fim":    h.loc[df.index, "Fim"],
                        "Causa do fim": h.loc[df.index, "Causa do fim"],
                    })
                    return (show.assign(__ord=h.loc[show.index, "__start_dt"])
                                .sort_values(["__ord"], ascending=[False])
                                .drop(columns="__ord"))

                pres = _prep(h[h.get("role") == "head_of_state"])
                gov  = _prep(h[h.get("role") == "head_of_government"])

                c1, c2 = st.columns(2)
                with c1:
                    st.markdown(tr("labels.presidentes"))
                    st.dataframe(pres, use_container_width=True, hide_index=True,
                                column_config=_colcfg_leadership())
                with c2:
                    st.markdown(tr("labels.chefes_de_governo"))
                    st.dataframe(gov, use_container_width=True, hide_index=True,
                                column_config=_colcfg_leadership())
            else:
                st.caption(tr("paises.label"))
        
        render_wars_battles_expander(iso3, default_open=False)

    elif mode == "econ":
        # assume que já tens `iso3` e `country_name` definidos na página Países
        render_wdi_panel(iso3=iso3, country_name=country_name)