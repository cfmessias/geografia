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


# -------------------------- Helpers --------------------------
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
    names = countries_df["name"].astype(str).tolist()

    # valor inicial
    if "pais_selected" not in st.session_state:
        st.session_state["pais_selected"] = "Portugal" if "Portugal" in names else (names[0] if names else None)

    with st.form("pais_form", clear_on_submit=False):
        q = st.text_input(
            tr("paises.pesquisar_nome_contem"),
            value="",
            placeholder=tr("paises.placeholder_pesquisa"),
        )
        opts = [n for n in names if q.lower() in n.lower()] if q else names

        if not opts:
            st.warning(tr("labels.nenhum_pa_s_corresponde_ao_filtro"))
            st.form_submit_button(tr("paises.abrir"))
            return None, None

        idx = opts.index(st.session_state["pais_selected"]) if st.session_state["pais_selected"] in opts else 0

        c1, c2 = st.columns([4, 1])
        with c1:
            chosen = st.selectbox(
                tr("labels.pa_s"),
                options=opts,
                index=idx,
                label_visibility="collapsed",
            )
        with c2:
            submitted = st.form_submit_button(tr("paises.abrir"))

    if not submitted:
        return None, None

    st.session_state["pais_selected"] = chosen
    iso3 = countries_df.loc[countries_df["name"] == chosen, "iso3"].astype(str).str.upper().iloc[0]
    return chosen, iso3


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

def render_paises_tab():
    _ensure_lang_state()
    from services.offline_store import (
        list_available_countries,
        wb_series_for_country,
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

    prof = _profile_by_iso3(iso3)

    colL, colR = st.columns([1.5, 1.1], gap="large")

    with colL:
    # ── Título ────────────────────────────────────────────────────────────────
        st.subheader(prof.get("name") or country_name)

        # ── Bandeira + facts (NÃO renderiza moeda aqui) ───────────────────────────
        info = load_flag_info(prof.get("name") or country_name, iso3)
        facts = (info or {}).get("facts") or {}
        if info and info.get("flag_url"):
            st.image(info["flag_url"], width=100)

        def _fact_first(facts: dict, *keys: str) -> str | None:
            """Devolve o primeiro valor não vazio entre várias chaves possíveis."""
            for k in keys:
                v = facts.get(k)
                if v is not None and str(v).strip():
                    return str(v).strip()
            return None

        # 1) tentar apanhar a moeda a partir dos 'facts' (várias variantes)
        moeda_txt = _fact_first(
            facts,
            "Moeda", "Moeda(s)", "Moeda (ISO)",           # PT (site)
            "Currency", "Currency (ISO)", "Currency code", "Currency codes"  # EN/var.
        )
        # 2) fallback a partir do profile
        if not moeda_txt:
            name   = prof.get("currency_name") or prof.get("currency")
            code   = prof.get("currency_code") or prof.get("currency_iso")
            symbol = prof.get("currency_symbol")
            parts = [name, f"({symbol})" if symbol else None, f"{code}" if code else None]
            moeda_txt = " ".join([p for p in parts if p]).strip() or None

        # ── Factos básicos ───────────────────────────────────────────────────────
        st.markdown(tr("labels.label_val", label=tr("country.capital"),
                    val=prof.get("capital") or "—"))

        inc = prof.get("inception") or prof.get("independence") or prof.get("inception_year")
        st.markdown(tr("labels.ano_de_fundacao_ou_independencia", year=_fmt_year(inc) or "—"))

        # ── Liderança atual (com fallback ao profile) ─────────────────────────────
        from services.offline_store import leaders_for_iso3
        pres_name = pm_name = pm_party = None
        try:
            cur_df, hist_df = leaders_for_iso3(iso3)

            if cur_df is not None and not cur_df.empty:
                r = cur_df[cur_df["role"] == "head_of_state"]
                if not r.empty:
                    pres_name = (r.iloc[0].get("person") or "").strip() or None
            if pres_name is None and hist_df is not None and not hist_df.empty:
                h = hist_df[hist_df["role"] == "head_of_state"].copy()
                if not h.empty:
                    h["__start"] = pd.to_datetime(h.get("start"), errors="coerce")
                    pres_name = (h.sort_values("__start", ascending=False).iloc[0].get("person") or "").strip() or None

            if cur_df is not None and not cur_df.empty:
                r = cur_df[cur_df["role"] == "head_of_government"]
                if not r.empty:
                    r = r.iloc[0]
                    pm_name = (r.get("person") or "").strip() or None
                    pm_party = (
                        (r.get("party_label") or r.get("party_pt") or r.get("party") or "").strip()
                        or None
                    )
            if pm_name is None and hist_df is not None and not hist_df.empty:
                h = hist_df[hist_df["role"] == "head_of_government"].copy()
                if not h.empty:
                    h["__start"] = pd.to_datetime(h.get("start"), errors="coerce")
                    r = h.sort_values("__start", ascending=False).iloc[0]
                    pm_name = (r.get("person") or "").strip() or None
                    pm_party = (
                        (r.get("party_label") or r.get("party_pt") or r.get("party") or "").strip()
                        or None
                    )
        except Exception:
            pass

        if not pres_name:
            pres_name = prof.get("head_of_state") or ""
        if not pm_name:
            pm_name = prof.get("head_of_government") or ""
        if not pm_party:
            pm_party = prof.get("hog_party") or ""

        if pres_name:
            st.markdown(tr("labels.presidente_pres_name", pres_name=pres_name))
        if pm_name:
            st.markdown(tr("labels.chefe_de_governo_pm_name", pm_name=pm_name))
        if pm_party:
            st.markdown(tr("labels.partido_do_chefe_de_governo_pm_party", pm_party=pm_party))

        # ── População / Área / Moeda (moeda só AQUI) ─────────────────────────────
        pop = prof.get("population")
        area = prof.get("area_km2")
        if pop is not None:
            st.markdown(tr("labels.popula_o") + _fmt_int(pop))
        if area is not None:
            st.markdown(tr("labels.rea") + (_fmt_int(area) + " km²"))
        st.markdown(tr("labels.label_val", label=tr("labels.moeda"), val=moeda_txt or "—"))

        # ── Factos adicionais do site das bandeiras (com i18n nos rótulos) ───────
        FACT_LABELS = {
            "Estado soberano":            "paises.facts.estado_soberano",
            "Códigos dos países":         "paises.facts.codigos_pais",
            "O Continente":               "paises.facts.continente",
            "Membro de":                  "paises.facts.membro_de",
            "Ponto mais alto":            "paises.facts.ponto_mais_alto",
            "Ponto mais baixo":           "paises.facts.ponto_mais_baixo",
            "PIB per capita":             "paises.facts.pib_per_capita",
            "Código de área telefónica":  "paises.facts.codigo_area_tel",
            "Domínio nacional":           "paises.facts.dominio_nacional",
        }
        EXCLUDE = {
            "Capital", "População", "Área",
            "Moeda", "Moeda(s)", "Moeda (ISO)",
            "Currency", "Currency (ISO)", "Currency code", "Currency codes"
        }

        for site_key, i18n_key in FACT_LABELS.items():
            if site_key in EXCLUDE:
                continue
            val = facts.get(site_key)
            if val:
                st.markdown(tr("labels.label_val", label=tr(i18n_key), val=val))



    with colR:
        wb = wb_series_for_country(iso3)
        if not wb.empty:
            wb = wb.copy()
            wb["year"] = pd.to_numeric(wb["year"], errors="coerce")

            st.markdown(tr("labels.popula_o_total"))
            _mini_line(wb, "pop_total", tr("paises.pessoas"))

            st.markdown(tr("labels.densidade_hab_km"))
            _mini_line(wb, "pop_density", tr("crescimento_populacional.habitantes_por_km2"))

            st.markdown(tr("labels.popula_o_urbana"))
            _mini_line(wb, "urban_pct", "%")
        else:
            st.caption(tr("labels.sem_s_ries_do_world_bank"))

    st.markdown(tr("labels.text"))

    with st.container():
        render_migration_section(iso3)

    # -------- Histórico de liderança
    with st.expander(tr("labels.hist_rico_de_lideran_a")):
        from services.offline_store import leaders_for_iso3
        cur_df, hist_df = leaders_for_iso3(iso3)
        base = hist_df if (hist_df is not None and not hist_df.empty) else cur_df

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

    # -------- Cidades
    with st.expander(tr("labels.principais_cidades")):
        from services.offline_store import cities_for_iso3
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
                    .dropna()
                    .astype(int)
                )
                if idx_latest.empty:
                    idx_latest = (
                        c.sort_values(["city", "__pop"], ascending=[True, True])
                        .groupby("city", observed=False)["__pop"].idxmax()
                        .dropna()
                        .astype(int)
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

                    n_total = len(c)
                    n_has_any_lat = int(cc["lat"].notna().sum())
                    n_has_any_lon = int(cc["lon"].notna().sum())
                    n_pts = len(pts)

                    if n_pts > 0:
                        st.map(pts[["lat","lon"]], use_container_width=True)
                    else:
                        st.caption(
                            tr("labels.sem_coordenadas_para_mapear") +
                            f"(linhas: {n_total}, com lat: {n_has_any_lat}, com lon: {n_has_any_lon}, válidas: {n_pts})"
                        )
                        if st.checkbox(tr("labels.ver_amostra_das_coords_brutas"), key=f"dbg_map_{iso3}"):
                            st.dataframe(cc[["city","lat","lon"]].head(20), use_container_width=True, hide_index=True)

    # -------- UNESCO
    with st.expander(tr("labels.patrim_nio_mundial_unesco")):
        from services.offline_store import unesco_for_iso3
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

            if "Tipo" in u.columns:
                u = u.sort_values("Tipo", ascending=True, kind="mergesort")

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
            ),
            

            if {"lat","lon"}.issubset(u.columns):
                st.map(u[["lat","lon"]].dropna(), use_container_width=True)
        else:
            st.caption(tr("paises.label"))

    # -------- Medalhas olímpicas
   
    with st.expander(tr("labels.medalhas_ol_mpicas_totais_e_por_edi_o")):
        from services.offline_store import load_olympics_summer_csv
        cdf = load_olympics_summer_csv()
        if not cdf.empty:
            cdf = cdf[cdf["iso3"].astype(str).str.upper() == iso3].copy()

        if cdf.empty:
            st.caption(tr("labels.sem_dados_de_medalhas_de_ver_o_no_csv_manual"))
        else:
            vals = (
                cdf.reindex(columns=["summer_gold", "summer_silver", "summer_bronze"])
                .apply(pd.to_numeric, errors="coerce")
                .fillna(0).astype(int)
            )
            g = int(vals["summer_gold"].sum())
            s = int(vals["summer_silver"].sum())
            b = int(vals["summer_bronze"].sum())

            # labels localizados
            L_G = tr("cols.gold"); L_S = tr("cols.silver"); L_B = tr("cols.bronze")
            L_MEDAL = tr("cols.medal"); L_COUNT = tr("cols.quantity")

            bar_df = pd.DataFrame({L_MEDAL: [L_G, L_S, L_B], L_COUNT: [g, s, b]})
            ymax = max(1, int(bar_df[L_COUNT].max() * 1.20))

            fig = px.bar(
                bar_df,
                x=L_MEDAL,
                y=L_COUNT,
                text=L_COUNT,
                category_orders={L_MEDAL: [L_G, L_S, L_B]},
                color=L_MEDAL,
                color_discrete_map={L_G: "#d4af37", L_S: "#c0c0c0", L_B: "#cd7f32"},
            )
            fig.update_traces(
                texttemplate="<b>%{text:d}</b>",
                textposition="outside",
                textfont=dict(size=20),
                hovertemplate="%{x}: %{y:d}<extra></extra>",
                cliponaxis=False
            )
            fig.update_layout(
                showlegend=False,
                xaxis_title=None,
                yaxis_title=None,
                yaxis=dict(range=[0, ymax], tickfont=dict(size=14)),
                xaxis=dict(tickfont=dict(size=14)),
                bargap=0.35,
                margin=dict(l=8, r=8, t=20, b=0),
                height=320,
                uniformtext_minsize=16,
                uniformtext_mode="show",
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
                    df_local["summer_total"] = df_local["summer_gold"] + df_local["summer_silver"] + df_local["summer_bronze"]
                if "year" in df_local.columns:
                    df_local["__year_num"] = pd.to_numeric(df_local["year"], errors="coerce")
                    sort_cols = ["__year_num"]
                else:
                    sort_cols = ["summer_total"]

                show_cols = ["year", "city", "host_country", "summer_gold", "summer_silver", "summer_bronze", "summer_total"]
                show = (
                    df_local[show_cols + (["__year_num"] if "__year_num" in df_local.columns else [])]
                    .sort_values(by=sort_cols, ascending=True, na_position="last")
                    .drop(columns=["__year_num"], errors="ignore")
                    .reset_index(drop=True)
                )

                # mantemos os nomes canónicos e traduzimos via column_config
                st.dataframe(
                    show,
                    use_container_width=True,
                    hide_index=True,
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

    # -------- Religiões
    with st.expander(tr("labels.religi_es")):
        from services.offline_store import load_religion
        try:
            rel = load_religion()
            rr = rel[rel["iso3"] == iso3]
        except Exception:
            rr = pd.DataFrame()

        if not rr.empty:
            r = rr.iloc[0]
            items = [
                (tr("religions.christianity"),     float(r.get("christian",        0))),
                (tr("religions.islam"),            float(r.get("muslim",           0))),
                (tr("religions.unaffiliated"),     float(r.get("unaffiliated",     0))),
                (tr("religions.hinduism"),         float(r.get("hindu",            0))),
                (tr("religions.buddhism"),         float(r.get("buddhist",         0))),
                (tr("religions.folk"),             float(r.get("folk_religions",   0))),
                (tr("religions.other"),            float(r.get("other_religions",  0))),
                (tr("religions.judaism"),          float(r.get("jewish",           0))),
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

    # -------- Turismo
    with st.expander(tr("labels.turismo")):
        from services.offline_store import load_tourism_ts
        t_ts = load_tourism_ts()

        # textos dos indicadores (i18n)
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
            if d.empty:
                return None, None
            last = d.iloc[-1]
            prev = d.iloc[-2] if len(d) > 1 else None
            return last, prev

        cols = st.columns(3)
        i = 0
        for code, label in kmap.items():
            last, prev = _latest_and_prev(t_ts, code)
            if last is None:
                continue
            year = int(last["year"])
            val  = float(last["value"])
            val_txt = _fmt_value(val, unit.get(code, "int"))
            delta_txt = ""
            if prev is not None and pd.notna(prev["value"]):
                delta = val - float(prev["value"])
                delta_txt = _fmt_delta(delta, unit.get(code, "int"), ref_value=val)
            cols[i % 3].metric(f"{label} · {year}", val_txt, delta=delta_txt)
            i += 1

        _FRAG = getattr(st, "fragment", None)

        def _tourism_timeseries_compare(iso3: str, t_ts: pd.DataFrame, kmap: dict):
            # opções localizadas
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
                options,
                index=0,
                key=f"tour_series_cmp_{iso3}",
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
            base = (
                base.dropna(subset=["year"])
                    .sort_values(["indicator", "year"])
                    .drop_duplicates(subset=["indicator", "year"], keep="last")
            )

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

        st.markdown(tr("labels.text"))
