# paises.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px
import plotly.graph_objects as go


# -------------------------- Helpers --------------------------

def _fmt_int(x) -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return ""
        return f"{int(float(x)):,}".replace(",", " ")
    except Exception:
        return str(x) if x is not None else ""

def _fmt_year(x) -> str:
    try:
        if pd.isna(x): return ""
        return str(int(x))
    except Exception:
        s = str(x)
        return s[:4] if len(s) >= 4 and s[:4].isdigit() else s

def _country_selector(countries_df: pd.DataFrame) -> tuple[str | None, str | None]:
    names = countries_df["name"].astype(str).tolist()
    if "pais_selected" not in st.session_state:
        st.session_state["pais_selected"] = "Portugal" if "Portugal" in names else (names[0] if names else None)

    with st.form("pais_form", clear_on_submit=False):
        q = st.text_input("Pesquisar (nome contém…)", value="", placeholder="ex.: Por, Bra, Ang…")
        opts = [n for n in names if q.lower() in n.lower()] if q else names
        if not opts:
            st.warning("Nenhum país corresponde ao filtro.")
            st.form_submit_button("🔎 Abrir")  # mantém o layout da form
            return None, None

        idx = opts.index(st.session_state["pais_selected"]) if st.session_state["pais_selected"] in opts else 0

        # --- select + botão na MESMA LINHA ---
        c1, c2 = st.columns([4, 1])
        with c1:
            chosen = st.selectbox(
                "País",
                options=opts,
                index=idx,
                key="pais_selectbox",
                label_visibility="collapsed"  # para alinhar melhor com o botão
            )
        with c2:
            submitted = st.form_submit_button("🔎 Abrir")

    if not submitted:
        return None, None

    st.session_state["pais_selected"] = chosen
    iso3 = countries_df.loc[countries_df["name"] == chosen, "iso3"].astype(str).str.upper().iloc[0]
    return chosen, iso3

def render_migration_section(iso3: str) -> None:
    

    # ✔ usar as versões filtradas
    from services.offline_store import (
        load_migration_latest_for_iso3,
        load_migration_ts_for_iso3,
        load_migration_inout_for_iso3,   # nova
        # (opcional) versões "tudo" ficam disponíveis só para debug:
        load_migration_inout,            # existe no teu código
    )

    with st.expander("Migração"):
        # ——— WDI (apenas do país) ———
        latest = load_migration_latest_for_iso3(iso3)
        ts     = load_migration_ts_for_iso3(iso3)

        kmap = {
            "SM.POP.TOTL":         "Migrantes (stock, pessoas)",
            "SM.POP.TOTL.ZS":      "Migrantes (% população)",
            "SM.POP.NETM":         "Migração líquida (pessoas)",
            "SM.POP.REFG":         "Refugiados (asilo, pessoas)",
            "BX.TRF.PWKR.CD.DT":   "Remessas recebidas (US$)",
            "BX.TRF.PWKR.DT.GD.ZS":"Remessas (% PIB)",
        }
        unit_fmt = {
            "SM.POP.TOTL": "int",
            "SM.POP.TOTL.ZS": "pct",
            "SM.POP.NETM": "int",
            "SM.POP.REFG": "int",
            "BX.TRF.PWKR.CD.DT": "int",
            "BX.TRF.PWKR.DT.GD.ZS": "pct",
        }

        def _fmt(v, kind):
            try:
                v = float(v)
            except Exception:
                return "—"
            return f"{v:.1f}%" if kind == "pct" else f"{int(round(v)):,}".replace(",", " ")

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

        # ─ Cards WDI
        cols = st.columns(3)
        i = 0
        for code, label in kmap.items():
            last, prev = _latest_and_prev(latest if not latest.empty else ts, code)
            if last is None:
                continue
            year = int(last["year"])
            val  = last["value"]
            delta_txt = ""
            if prev is not None and pd.notna(prev["value"]):
                delta = val - prev["value"]
                delta_txt = f"{delta:+.1f} p.p." if unit_fmt.get(code)=="pct" else f"{delta:+,.0f}".replace(",", " ")
            cols[i%3].metric(f"{label} · {year}", _fmt(val, unit_fmt.get(code,"int")), delta=delta_txt)
            i += 1

        # ─ Série temporal (últimos 30 anos) — só do país
        series_opts = [(kmap[k], k) for k in kmap.keys()]
        sel_lbl = st.selectbox("Série temporal (WDI — últimos 30 anos)",
                               [x[0] for x in series_opts], index=0, key=f"mig_wdi_{iso3}")
        code = dict(series_opts)[sel_lbl]

        base = (
            ts[ts["indicator"]==code]
            .dropna(subset=["value"])
            .copy()
            .sort_values("year")
        )
        if not base.empty:
            base["year"] = pd.to_numeric(base["year"], errors="coerce").astype("Int64")
            base = base.dropna(subset=["year"]).drop_duplicates(subset=["year"], keep="last")
            sub  = base.tail(30).copy()
            y_min, y_max = int(sub["year"].min()), int(sub["year"].max())
            st.altair_chart(
                alt.Chart(sub)
                .mark_line(point=True)
                .encode(
                    x=alt.X("year:Q", title="Ano", scale=alt.Scale(domain=[y_min, y_max]), axis=alt.Axis(format="d")),
                    y=alt.Y("value:Q", title=sel_lbl if unit_fmt.get(code)!="pct" else sel_lbl + " (%)"),
                    tooltip=[alt.Tooltip("year:Q", title="Ano", format="d"),
                             alt.Tooltip("value:Q", title="Valor", format=",.0f")]
                )
                .properties(height=240),
                use_container_width=True
            )

        st.markdown("---")

        # ——— UN DESA (apenas do país) ———
        st.subheader("Emigração vs. Imigração (UN DESA) — últimos 30 anos")

        # Carrega só as linhas do país (filtrado a montante)
        io_df = load_migration_inout_for_iso3(iso3)

        # (opcional) carregar o dataset completo SÓ se assinalares debug
        if st.checkbox("debug UN DESA (carregar dataset completo)", key=f"dbg_mig_{iso3}"):
            df_all = load_migration_inout()
            st.write("UN DESA total linhas:", len(df_all), list(df_all.columns))
            st.write(f"Linhas {iso3}:", int((df_all["iso3"] == str(iso3).upper()).sum()))
            st.dataframe(
                df_all[df_all["iso3"] == str(iso3).upper()].tail(10),
                use_container_width=True,
            )

        if io_df.empty:
            st.caption("— sem dados UN DESA para este país —")
        else:
            io_df = io_df.copy()
            io_df["year"] = pd.to_numeric(io_df["year"], errors="coerce").astype("Int64")
            io_df["immigrants"] = pd.to_numeric(io_df["immigrants"], errors="coerce")
            io_df["emigrants"]  = pd.to_numeric(io_df["emigrants"],  errors="coerce")
            io_df = io_df.dropna(subset=["year"]).sort_values("year").tail(30)

            long = io_df.melt(
                id_vars="year",
                value_vars=["immigrants", "emigrants"],
                var_name="tipo",
                value_name="valor",
            )
            long["tipo"] = long["tipo"].map({"immigrants": "Imigração", "emigrants": "Emigração"})

            color_enc = alt.Color(
                "tipo:N",
                title="",
                scale=alt.Scale(domain=["Imigração", "Emigração"], range=["#2E7D32", "#E53935"]),
                legend=alt.Legend(orient="right"),
            )
            years_sorted = sorted(int(y) for y in long["year"].dropna().unique())
            x_enc = alt.X("year:O", title="Ano", sort=years_sorted)

            st.altair_chart(
                alt.Chart(long).mark_line(point=True).encode(
                    x=x_enc,
                    y=alt.Y("valor:Q", title="Pessoas"),
                    color=color_enc,
                    tooltip=[alt.Tooltip("year:O", title="Ano"), "tipo:N",
                             alt.Tooltip("valor:Q", title="Pessoas", format=",.0f")],
                ).properties(height=260),
                use_container_width=True,
            )

            last_year = int(io_df["year"].max())
            last_row  = io_df[io_df["year"] == last_year].iloc[0]
            c1, c2 = st.columns(2)
            c1.metric(f"Imigração · {last_year}", f"{last_row['immigrants']:,.0f}".replace(",", " "))
            c2.metric(f"Emigração · {last_year}", f"{last_row['emigrants']:,.0f}".replace(",", " "))

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
        st.caption(f"— sem dados de {ytitle.lower()} —")
        return
    d = df.dropna(subset=["year", ycol]).copy()
    d["year"] = pd.to_numeric(d["year"], errors="coerce")

    chart = (
        alt.Chart(d)
        .mark_line()
        .encode(
            # ❌ título do eixo X removido
            x=alt.X("year:Q", axis=alt.Axis(format="d", title=None)),
            y=alt.Y(f"{ycol}:Q", title=ytitle),
            tooltip=[
                alt.Tooltip("year:Q", title="Ano", format="d"),
                alt.Tooltip(f"{ycol}:Q", title=ytitle)
            ],
        )
        .properties(height=170)
    )
    st.altair_chart(chart, use_container_width=True)


# -------------------------- UI principal --------------------------

def render_paises_tab():
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
        load_tourism_latest,
        tourism_origin_for_iso3,
        tourism_purpose_for_iso3,
    )
    #st.caption("Ficha de país (dados locais CSV)")

    countries = list_available_countries()
    if countries.empty:
        st.error("Sem países disponíveis. Gera primeiro os CSVs agregados (profiles, etc.).")
        return

    country_name, iso3 = _country_selector(countries)
    if not country_name or not iso3:
        st.info("Escolhe um país e clica **🔎 Abrir**.")
        return

    prof = _profile_by_iso3(iso3)

    # Topo: texto à ESQ, métricas+gráficos à DIR
    colL, colR = st.columns([1.5, 1.1], gap="large")

    with colL:
        # --- TÍTULO ---
        st.subheader(prof.get("name") or country_name)

        # --- BANDEIRA IMEDIATA (após o nome) ---
        info = load_flag_info(prof.get("name") or country_name, iso3)
        moeda_txt = None
        if info:
            if info.get("flag_url"):
                st.image(info["flag_url"], width=100)
            # guardar moeda para mostrar depois da Área
            facts = info.get("facts", {}) or {}
            moeda_txt = facts.get("Moeda")

        # --- FACTOS BÁSICOS ---
        st.markdown(f"**Capital:** {prof.get('capital') or '—'}")
        inc = prof.get("inception") or prof.get("independence") or prof.get("inception_year")
        st.markdown(f"**Ano de fundação/independência:** {_fmt_year(inc) or '—'}")

        # ----- LIDERANÇA ATUAL (apenas para mostrar nomes no topo) -----
        pres_name = None
        pm_name = None
        pm_party = None
        try:
            cur_df, hist_df = leaders_for_iso3(iso3)
            # Presidente (head_of_state)
            if cur_df is not None and not cur_df.empty:
                r = cur_df[cur_df["role"] == "head_of_state"]
                if not r.empty:
                    pres_name = (r.iloc[0].get("person") or "").strip() or None
            if pres_name is None and hist_df is not None and not hist_df.empty:
                h = hist_df[hist_df["role"] == "head_of_state"].copy()
                if not h.empty:
                    h["__start"] = pd.to_datetime(h.get("start"), errors="coerce")
                    pres_name = (h.sort_values("__start", ascending=False).iloc[0].get("person") or "").strip() or None

            # Chefe de governo
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

        # fallbacks do profiles_master
        if not pres_name:
            pres_name = prof.get("head_of_state") or ""
        if not pm_name:
            pm_name = prof.get("head_of_government") or ""
        if not pm_party:
            pm_party = prof.get("hog_party") or ""

        # ORDEM PEDIDA: Presidente → Chefe de governo → Partido
        if pres_name:
            st.markdown(f"**Presidente:** {pres_name}")
        if pm_name:
            st.markdown(f"**Chefe de governo:** {pm_name}")
        if pm_party:
            st.markdown(f"**Partido do chefe de governo:** {pm_party}")

        # ----- População / Área / Moeda -----
        pop = prof.get("population")
        area = prof.get("area_km2")
        if pop is not None:
            st.markdown("**População:** " + _fmt_int(pop))
        if area is not None:
            st.markdown("**Área:** " + (_fmt_int(area) + " km²"))
        # Moeda (logo a seguir à Área)
        st.markdown(f"**Moeda:** {moeda_txt or '—'}")
        
        facts = (info or {}).get("facts") or {}

        ordered = [
            ("Estado soberano",            "Estado soberano"),
            ("Códigos dos países",         "Códigos dos países"),
            ("O Continente",               "Continente"),            # renomeado
            ("Membro de",                  "Membro de"),
            ("Ponto mais alto",            "Ponto mais alto"),
            ("Ponto mais baixo",           "Ponto mais baixo"),
            ("PIB per capita",             "PIB per capita"),
            ("Código de área telefónica",  "Código de área telefónica"),
            ("Domínio nacional",           "Domínio nacional"),
        ]

        # Já mostrados acima — não repetir
        exclude = {"Capital", "População", "Área", "Moeda"}

        for site_key, label in ordered:
            val = facts.get(site_key)
            if val and site_key not in exclude:
                st.markdown(f"**{label}:** {val}")

                        # fonte do site de bandeiras
                #if info and info.get("site_url"):
                #    st.caption(f"Fonte: bandeirasnacionais.com — {info['site_url']}")

    with colR:
        wb = wb_series_for_country(iso3)
        if not wb.empty:
            wb = wb.copy()
            wb["year"] = pd.to_numeric(wb["year"], errors="coerce")

            st.markdown("**População total**")
            _mini_line(wb, "pop_total", "habitantes")

            st.markdown("**Densidade (hab/km²)**")
            _mini_line(wb, "pop_density", "hab/km²")

            st.markdown("**População urbana (%)**")
            _mini_line(wb, "urban_pct", "%")
        else:
            st.caption("— sem séries do World Bank —")

    st.markdown("---")

    anchor = st.container()
    with anchor:
        render_migration_section(iso3)

    
    # -------- Histórico de liderança
    with st.expander("Histórico de liderança"):
        cur_df, hist_df = leaders_for_iso3(iso3)

        # Usa histórico se existir; caso contrário, mostra os atuais para não ficar vazio
        base = hist_df if (hist_df is not None and not hist_df.empty) else cur_df

        if base is not None and not base.empty:
            h = base.copy()

            # Mapeamento das funções para PT
            role_map = {
                "head_of_state": "Presidente",
                "head_of_government": "Chefe de governo",
            }
            h["Função"] = h.get("role").map(role_map).fillna(h.get("role"))

            # Datas em AAAA-MM-DD (string), sem "None"/NaT
            h["__start_dt"] = pd.to_datetime(h.get("start"), errors="coerce")
            h["__end_dt"]   = pd.to_datetime(h.get("end"),   errors="coerce")
            h["Início"] = h["__start_dt"].dt.strftime("%Y-%m-%d").fillna("")
            h["Fim"]    = h["__end_dt"].dt.strftime("%Y-%m-%d").fillna("")

            # Seleção/renomeação de colunas para PT
            show = pd.DataFrame({
                "ISO3":   h.get("iso3"),
                "País":   h.get("country"),
                "Função": h["Função"],
                "Pessoa": h.get("person"),
                "QID":    h.get("person_qid"),
                "Início": h["Início"],
                "Fim":    h["Fim"],
            })

            # Ordenar por função e início (desc)
            show = show.assign(__ord=h["__start_dt"]).sort_values(
                ["Função", "__ord"], ascending=[True, False]
            ).drop(columns="__ord")

            st.dataframe(show, use_container_width=True, hide_index=True)

        else:
            st.caption("—")


    # -------- Cidades
    with st.expander("Principais cidades"):
        cities = cities_for_iso3(iso3)
        if cities.empty:
            st.info("Sem cidades. Corre `scripts/fetch_cities.py`.")
        else:
            c = cities.copy()

            # garantir colunas esperadas (incluindo lat/lon)
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
                st.info("Sem cidades válidas após limpeza.")
            else:
                # —— tabela (como já tinhas) —— #
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

                # —— layout: tabela à esquerda, mapa à direita —— #
                colL, colR = st.columns([0.62, 0.38], gap="large")

                with colL:
                    st.markdown("**Principais cidades / municípios (Wikidata – 1 linha por cidade)**")
                    st.dataframe(show[cols] if cols else show, use_container_width=True, hide_index=True)

                with colR:
                    st.markdown("**Mapa**")

                    # garantir colunas e converter para float
                    for k in ("lat", "lon"):
                        if k not in c.columns:
                            c[k] = pd.NA
                    cc = c.copy()
                    cc["lat"] = pd.to_numeric(cc["lat"], errors="coerce")
                    cc["lon"] = pd.to_numeric(cc["lon"], errors="coerce")

                    # 1º par válido por cidade + valores plausíveis
                    pts = (
                        cc.dropna(subset=["lat","lon"])
                        .loc[cc["lat"].between(-90, 90) & cc["lon"].between(-180, 180),
                            ["city","lat","lon"]]
                        .drop_duplicates(subset=["city"], keep="first")
                    )

                    # diagnóstico rápido
                    n_total = len(c)
                    n_has_any_lat = int(cc["lat"].notna().sum())
                    n_has_any_lon = int(cc["lon"].notna().sum())
                    n_pts = len(pts)

                    if n_pts > 0:
                        st.map(pts[["lat","lon"]], use_container_width=True)
                    else:
                        st.caption(
                            f"— Sem coordenadas para mapear — "
                            f"(linhas: {n_total}, com lat: {n_has_any_lat}, com lon: {n_has_any_lon}, válidas: {n_pts})"
                        )
                        if st.checkbox("ver amostra das coords brutas", key=f"dbg_map_{iso3}"):
                            st.dataframe(cc[["city","lat","lon"]].head(20), use_container_width=True, hide_index=True)

    # -------- UNESCO
    

    with st.expander("Património Mundial (UNESCO)"):
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

            # viewport fixo -> scroll vertical automático
            ROW_H, HDR_H, MAX_H = 28, 38, 420
            n = len(u)
            height = min(MAX_H, HDR_H + ROW_H * max(n, 1))

            st.data_editor(
                u[cols],
                use_container_width=True,   # ocupa toda a largura do contentor
                hide_index=True,
                height=height,              # scroll vertical
                disabled=True,              # read-only
                column_config={
                    "Sítio": st.column_config.TextColumn("Sítio"),
                    "Tipo": st.column_config.TextColumn("Tipo"),
                    "Ano": st.column_config.TextColumn("Ano"),
                    "lat": st.column_config.NumberColumn("lat", format="%.4f"),
                    "lon": st.column_config.NumberColumn("lon", format="%.4f"),
                },
            )

            if {"lat","lon"}.issubset(u.columns):
                st.map(u[["lat","lon"]].dropna(), use_container_width=True)
        else:
            st.caption("—")


        

    # -------- Medalhas olímpicas
    with st.expander("Medalhas olímpicas (Totais e por edição)"):
        cdf = load_olympics_summer_csv()
        if not cdf.empty:
            cdf = cdf[cdf["iso3"].astype(str).str.upper() == iso3].copy()

        if cdf.empty:
            st.caption("— sem dados de medalhas de Verão no CSV manual —")
        else:
            # ---- totais e gráfico ----
            vals = (
                cdf.reindex(columns=["summer_gold", "summer_silver", "summer_bronze"])
                .apply(pd.to_numeric, errors="coerce")
                .fillna(0).astype(int)
            )
            g = int(vals["summer_gold"].sum())
            s = int(vals["summer_silver"].sum())
            b = int(vals["summer_bronze"].sum())

            bar_df = pd.DataFrame(
                {"Medalha": ["Ouro", "Prata", "Bronze"], "Quantidade": [int(g), int(s), int(b)]}
            )
            ymax = max(1, int(bar_df["Quantidade"].max() * 1.20))

            fig = px.bar(
                bar_df,
                x="Medalha",
                y="Quantidade",
                text="Quantidade",
                category_orders={"Medalha": ["Ouro", "Prata", "Bronze"]},
                color="Medalha",
                color_discrete_map={
                    "Ouro":   "#d4af37",
                    "Prata":  "#c0c0c0",
                    "Bronze": "#cd7f32",
                },
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

            # ---- tabela por edição ----
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

            show_cols = ["year", "city", "host_country",
                        "summer_gold", "summer_silver", "summer_bronze", "summer_total"]
            show = (
                df_local[show_cols + (["__year_num"] if "__year_num" in df_local.columns else [])]
                .sort_values(by=sort_cols, ascending=True, na_position="last")
                .drop(columns=["__year_num"], errors="ignore")
                .reset_index(drop=True)
            )
            show_pt = show.rename(columns={
                "year": "Ano",
                "city": "Cidade",
                "host_country": "País anfitrião",
                "summer_gold": "Ouro",
                "summer_silver": "Prata",
                "summer_bronze": "Bronze",
                "summer_total": "Total",
            })
            if "Ano" in show_pt.columns:
                show_pt["Ano"] = show_pt["Ano"].apply(
                    lambda v: "" if pd.isna(v) or str(v).strip()=="" else f"{int(pd.to_numeric(v, errors='coerce'))}"
                )

            # ---- layout lado a lado: tabela (esq) + gráfico (dir) ----
            col_tab, col_fig = st.columns([3, 2], gap="medium")
            with col_tab:
                st.dataframe(
                    show_pt,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Ano":   st.column_config.TextColumn("Ano"),
                        "Ouro":  st.column_config.NumberColumn("Ouro",   format="%d"),
                        "Prata": st.column_config.NumberColumn("Prata",  format="%d"),
                        "Bronze":st.column_config.NumberColumn("Bronze", format="%d"),
                        "Total": st.column_config.NumberColumn("Total",  format="%d"),
                    },
                )
            with col_fig:
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


    # -------- Religiões
    with st.expander("Religiões"):
        try:
            rel = load_religion()
            rr = rel[rel["iso3"] == iso3]
        except Exception:
            rr = pd.DataFrame()

        if not rr.empty:
            r = rr.iloc[0]
            items = [
                ("Cristianismo",      float(r.get("christian",        0))),
                ("Islamismo",         float(r.get("muslim",           0))),
                ("Sem religião",      float(r.get("unaffiliated",     0))),
                ("Hinduísmo",         float(r.get("hindu",            0))),
                ("Budismo",           float(r.get("buddhist",         0))),
                ("Religiões étnicas", float(r.get("folk_religions",   0))),
                ("Outras",            float(r.get("other_religions",  0))),
                ("Judaísmo",          float(r.get("jewish",           0))),
            ]
            df_rel = pd.DataFrame(items, columns=["Religião", "% População"])
            df_rel["% População"] = pd.to_numeric(df_rel["% População"], errors="coerce").fillna(0.0)

            c1, c2 ,c3= st.columns([0.65, 0.3,1.05])
            with c1:
                st.dataframe(
                    df_rel,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Religião": st.column_config.TextColumn("Religião"),
                        "% População": st.column_config.NumberColumn("% População", format="%.2f"),
                    },
                )
            with c3:
                maxv = max(1.0, float(df_rel["% População"].max()))
                TOP_PAD = 32  # ↑ espaço extra no topo (px)

                base = (
                    alt.Chart(df_rel)
                    .mark_bar()
                    .encode(
                        y=alt.Y("Religião:N", sort="-x", title=""),
                        x=alt.X("% População:Q", title="% população",
                                scale=alt.Scale(domain=[0, maxv * 1.1])),
                        tooltip=["Religião", alt.Tooltip("% População:Q", format=".2f")],
                    )
                    .properties(height=290)
                )
                labels = (
                    alt.Chart(df_rel)
                    .mark_text(align="left", baseline="middle", dx=3)
                    .encode(
                        y="Religião:N",
                        x="% População:Q",
                        text=alt.Text("% População:Q", format=".2f"),
                    )
                )

                chart = base + labels

                # Tenta padding nativo; se não existir (Altair mais antigo), usa spacer invisível
                try:
                    chart = chart.properties(padding={"top": TOP_PAD, "left": 5, "right": 10, "bottom": 5})
                except TypeError:
                    spacer = alt.Chart(pd.DataFrame({"_":[0]})).mark_text(text="").properties(height=TOP_PAD)
                    chart = spacer & chart  # vconcat cria “margem” acima do gráfico

                st.altair_chart(chart, use_container_width=True)


            st.caption(f"Ano de referência: {int(pd.to_numeric(r.get('source_year', 2010), errors='coerce'))}")
        else:
            st.caption("— sem dados de religião em data/religion.csv —")

   
    
    # -------- Turismo
   
    with st.expander("Turismo"):
        # Carrega dados (World Bank WDI + Eurostat quando existir)
        t_latest = load_tourism_latest()
        t_ts     = load_tourism_ts()

        # Mapas de rótulos e “tipos” para formatação
        kmap = {
            "ST.INT.ARVL":       "Chegadas (turistas internacionais)",
            "ST.INT.DPRT":       "Partidas (turistas internacionais)",
            "ST.INT.RCPT.CD":    "Receitas do turismo (US$ correntes)",
            "ST.INT.XPND.CD":    "Despesas do turismo (US$ correntes)",
            "ST.INT.RCPT.XP.ZS": "Receitas do turismo (% exportações)",
            "ST.INT.XPND.MP.ZS": "Despesas do turismo (% importações)",
        }
        unit = {
            "ST.INT.ARVL": "int",
            "ST.INT.DPRT": "int",
            "ST.INT.RCPT.CD": "money",
            "ST.INT.XPND.CD": "money",
            "ST.INT.RCPT.XP.ZS": "pct",
            "ST.INT.XPND.MP.ZS": "pct",
        }

        def _fmt_tour(v, kind):
            try:
                v = float(v)
            except Exception:
                return "—"
            if kind == "pct":
                return f"{v:.1f}%"
            if kind == "money":
                if abs(v) >= 1e9:  return f"{v/1e9:.2f} B"
                if abs(v) >= 1e6:  return f"{v/1e6:.2f} M"
                return f"{int(round(v)):,}".replace(",", " ")
            return f"{int(round(v)):,}".replace(",", " ")

        def _latest_and_prev(df_all, code):
            """Devolve (último, anterior) para um indicador."""
            d = (
                df_all[(df_all["iso3"]==iso3) & (df_all["indicator"]==code)]
                .dropna(subset=["value"])
                .sort_values("year")
            )
            if d.empty:
                return None, None
            last = d.iloc[-1]
            prev = d.iloc[-2] if len(d) > 1 else None
            return last, prev

        # ───────────────────────── Cards (mostram o ANO e delta vs ano anterior)
        cols = st.columns(3)
        i = 0
        for code, label in kmap.items():
            last, prev = _latest_and_prev(t_ts, code)
            if last is None:
                continue
            year = int(last["year"])
            val  = last["value"]
            delta_txt = ""
            if prev is not None and pd.notna(prev["value"]):
                delta = val - prev["value"]
                if unit.get(code) == "pct":
                    delta_txt = f"{delta:+.1f} p.p."
                else:
                    delta_txt = f"{delta:+,.0f}".replace(",", " ")
            cols[i%3].metric(f"{label} · {year}", _fmt_tour(val, unit.get(code,"int")), delta=delta_txt)
            i += 1

        # ───────────────────────── Série temporal (MOSTRAR SEMPRE os últimos 30 anos)
        series_opts = [(kmap[k], k) for k in kmap.keys()]
        sel_lbl = st.selectbox("Série temporal (turismo — últimos 30 anos)", [x[0] for x in series_opts], index=0)
        code = dict(series_opts)[sel_lbl]

        base = (
            t_ts[(t_ts["iso3"]==iso3) & (t_ts["indicator"]==code)]
            .dropna(subset=["value"])
            .copy()
            .sort_values("year")
        )

        if not base.empty:
            # garantir numérico e único por ano
            base["year"] = pd.to_numeric(base["year"], errors="coerce").astype("Int64")
            base = base.dropna(subset=["year"]).drop_duplicates(subset=["year"], keep="last")

            # recortar estritamente para no máx. 30 pontos (anos mais recentes)
            sub = base.sort_values("year").tail(30).copy()

            y_min = int(sub["year"].min())
            y_max = int(sub["year"].max())

            st.altair_chart(
                alt.Chart(sub)
                .mark_line(point=True)
                .encode(
                    x=alt.X("year:Q", title="Ano",
                            scale=alt.Scale(domain=[y_min, y_max]),
                            axis=alt.Axis(format="d")),
                    y=alt.Y("value:Q", title=sel_lbl),
                    tooltip=[alt.Tooltip("year:Q", title="Ano", format="d"),
                            alt.Tooltip("value:Q", title="Valor", format=",.0f")]
                )
                .properties(height=260),
                use_container_width=True
            )
        else:
            st.caption("— sem série temporal para o indicador selecionado —")

        st.markdown("---")

        # ───────────────────────── Origem dos turistas (UE/EFTA apenas)
        orig = tourism_origin_for_iso3(iso3)
        if not orig.empty:
            yy = pd.to_numeric(orig["year"], errors="coerce")
            if yy.notna().any():
                last = int(yy.max())
                top = (
                    orig[orig["year"]==last]
                    .groupby("origin", as_index=False, observed=False)["arrivals"].sum()
                    .sort_values("arrivals", ascending=False)
                    .head(10)
                    .rename(columns={"origin":"Origem (ISO2)","arrivals":"Chegadas"})
                )
                c1, c2 = st.columns([1,1])
                with c1:
                    st.markdown(f"**Origem dos turistas (Top 10, {last})**")
                    st.dataframe(
                        top, use_container_width=True, hide_index=True,
                        column_config={"Chegadas": st.column_config.NumberColumn("Chegadas", format="%.0f")}
                    )
                with c2:
                    st.altair_chart(
                        alt.Chart(top).mark_bar().encode(
                            x=alt.X("Chegadas:Q", title="Chegadas"),
                            y=alt.Y("Origem (ISO2):N", sort="-x", title=""),
                            tooltip=["Origem (ISO2)", alt.Tooltip("Chegadas:Q", format=",.0f")]
                        ).properties(height=300),
                        use_container_width=True
                    )
        else:
            #st.caption("— sem detalhe de origem (apenas disponível para UE/EFTA) —")
            st.caption("")

        # ───────────────────────── Propósito das viagens (residentes UE/EFTA)
        purp = tourism_purpose_for_iso3(iso3)
        if not purp.empty:
            yy = pd.to_numeric(purp["year"], errors="coerce")
            if yy.notna().any():
                last = int(yy.max())
                p = purp[purp["year"]==last].copy()
                p = p.rename(columns={"purpose":"Propósito","destination":"Destino","trips":"Viagens"})
                labels_p = {"BUS":"Negócios", "HOL":"Férias/lazer"}
                p["Propósito"] = p["Propósito"].map(labels_p).fillna(p["Propósito"])
                st.markdown(f"**Propósito das viagens dos residentes ({last})**")
                st.altair_chart(
                    alt.Chart(p).mark_bar().encode(
                        x=alt.X("Viagens:Q", title="Viagens"),
                        y=alt.Y("Propósito:N", title=""),
                        color="Destino:N",
                        tooltip=["Propósito","Destino", alt.Tooltip("Viagens:Q", format=",.0f")]
                    ).properties(height=300),
                    use_container_width=True
                )
        else:
            #st.caption("— sem detalhe de propósito (apenas disponível para UE/EFTA) —")
            st.caption("")
        st.caption(
            "Fonte: World Bank — WDI (chegadas/partidas/receitas/despesas anuais). "
            "Os números podem divergir de estatísticas nacionais (p.ex., 'hóspedes' e 'dormidas' do Turismo de Portugal), "
            "pois medem universos/metodologias distintas."
        )



