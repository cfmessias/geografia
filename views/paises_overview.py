# views/paises_overview.py
from __future__ import annotations
import pandas as pd
import altair as alt
import plotly.express as px
import streamlit as st
from services.i18n import t as tr
from services.formatting import fmt_int, fmt_year

def _colcfg_leadership():
    return {
        "Pessoa":        st.column_config.TextColumn(tr("cols.person")),
        "Partido":       st.column_config.TextColumn(tr("cols.party")),
        "Início":        st.column_config.TextColumn(tr("cols.start")),
        "Fim":           st.column_config.TextColumn(tr("cols.end")),
        "Causa do fim":  st.column_config.TextColumn(tr("cols.end_cause")),
    }

def _colcfg_gastronomy():
    return {
        "Item":           st.column_config.TextColumn("Item"),
        "World ranking":  st.column_config.NumberColumn("World ranking", format="%d"),
        "Score":          st.column_config.NumberColumn("Score", format="%.2f"),
        "Reviews":        st.column_config.NumberColumn("Reviews", format="%d"),
        "Link":           st.column_config.TextColumn("Link"),
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

def _profile_by_iso3(iso3: str) -> dict:
    from services.offline_store import load_profiles_master
    df = load_profiles_master()
    if not df.empty:
        row = df[df["iso3"].astype(str).str.upper() == str(iso3).upper()]
        if not row.empty:
            return row.iloc[0].to_dict()
    return {"iso3": iso3, "name": iso3}

def render_overview_panel(iso3: str, country_name: str):
    from services.offline_store import (
        cities_for_iso3, unesco_for_iso3, load_olympics_summer_csv,
        load_religion, load_flag_info, load_tourism_ts,leaders_for_iso3,gastronomy_for_iso3
    )

    from services.offline_store import (
        cities_for_iso3, unesco_for_iso3, load_olympics_summer_csv,
        load_religion, load_flag_info, load_tourism_ts,leaders_for_iso3
    )

    prof = _profile_by_iso3(iso3)
    info = load_flag_info(prof.get("name") or country_name, iso3)
    facts = (info or {}).get("facts") or {}

    if info and info.get("flag_url"):
        st.image(info["flag_url"], width=96)

    def _first(facts, *keys):
        for k in keys:
            v = facts.get(k)
            if v is not None and str(v).strip():
                return str(v).strip()
        return None
    
    # Líderes
       
    def _get_current_heads(prof: dict, iso3: str) -> tuple[str, str]:
        """
        Devolve (presidente, chefe_de_governo):
        1) tenta primeiro usar os campos do countries_profiles.csv
        2) se estiverem vazios, tenta leaders_current.csv via leaders_for_iso3()
        """

        pres = (prof.get("head_of_state") or "").strip()
        pm   = (prof.get("head_of_government") or "").strip()

        # Se já tivermos os dois a partir do profiles, não fazemos mais nada
        if pres and pm:
            return pres, pm

        # Fallback: leaders_current.csv
        try:
            cur, _ = leaders_for_iso3(iso3)
        except Exception:
            return pres, pm

        if cur is None or cur.empty:
            return pres, pm

        cur = cur.copy()
        # normaliza role para lower case (mantendo '_' se existir)
        cur["role_norm"] = cur["role"].astype(str).str.lower()

        def _pick(role_patterns: str, include_party: bool = False) -> str:
            """
            Escolhe a pessoa mais recente cuja 'role_norm' corresponda à regex.
            Se include_party=True, acrescenta o partido entre parêntesis (se existir).
            """
            m = cur[cur["role_norm"].str.contains(role_patterns, regex=True, na=False)]
            if m.empty:
                return ""
            if "start" in m.columns:
                m = m.sort_values("start")
            row = m.iloc[-1]

            person = str(row.get("person") or "").strip()
            if not person:
                return ""

            if include_party:
                party = str(row.get("party") or "").strip()
                if party:
                    return f"{person} ({party})"
            return person

        # Presidente: só o nome (sem partido)
        if not pres:
            pres = _pick(
                r"head_of_state|head of state|president|chefe_de_estado|chefe de estado",
                include_party=False,
            )

        # Chefe de governo: nome + partido, se existir
        if not pm:
            pm = _pick(
                r"head_of_government|head of government|prime_minister|prime minister|chefe_de_governo|chefe de governo",
                include_party=True,
            )

        return pres, pm


    # Moeda
    moeda_txt = _first(
        facts, "Moeda", "Moeda(s)", "Moeda (ISO)", "Currency", "Currency (ISO)",
        "Currency code", "Currency codes",
    )
    if not moeda_txt:
        name   = prof.get("currency_name") or prof.get("currency")
        code   = prof.get("currency_code") or prof.get("currency_iso")
        symbol = prof.get("currency_symbol")
        parts = [name, f"({symbol})" if symbol else None, f"{code}" if code else None]
        moeda_txt = " ".join([p for p in parts if p]).strip() or None

    inc  = prof.get("inception") or prof.get("independence") or prof.get("inception_year")
    pres, pm = _get_current_heads(prof, iso3)


    pop  = prof.get("population")
    area = prof.get("area_km2")

    def _row(label_text: str, value: str | int | float | None):
        if value is None or str(value).strip() == "":
            value = "—"
        st.markdown(tr("labels.label_val", label=label_text, val=str(value)))

    colL, colR = st.columns(2)
    with colL:
        label = tr("labels.ano_de_fundacao_ou_independencia", year="").replace("**","").strip()
        if label.endswith(":"): label = label[:-1].rstrip()
        _row(label, fmt_year(inc) or "—")
        _row(tr("paises.facts.estado_soberano"), facts.get("Estado soberano"))
        if pres: _row(tr("labels.presidente"), pres)
        if pm:   _row(tr("labels.chefe_de_governo"), pm)
        from views.languages import render_country_languages_line
        render_country_languages_line(iso3)
        _row(tr("country.capital"), prof.get("capital") or "—")
        _row(tr("paises.facts.continente"), facts.get("O Continente"))
        _row(tr("labels.moeda"), moeda_txt or "—")
        _row(tr("labels.popula_o").replace("**","").replace(":",""), fmt_int(pop) if pop is not None else "—")

    with colR:
        _row(tr("labels.rea").replace("**","").replace(":",""), (fmt_int(area) + " km²") if area is not None else "—")
        _row(tr("paises.facts.codigos_pais"), facts.get("Códigos dos países"))
        _row(tr("paises.facts.membro_de"), facts.get("Membro de"))
        _row(tr("paises.facts.ponto_mais_alto"),  facts.get("Ponto mais alto"))
        _row(tr("paises.facts.ponto_mais_baixo"), facts.get("Ponto mais baixo"))
        _row(tr("paises.facts.pib_per_capita"),   facts.get("PIB per capita"))
        _row(tr("paises.facts.codigo_area_tel"),  facts.get("Código de área telefónica"))
        _row(tr("paises.facts.dominio_nacional"), facts.get("Domínio nacional"))

    # Línguas — expander
    from views.languages import render_country_languages_expander
    render_country_languages_expander(iso3, default_open=False)

    # Cidades
    with st.expander(tr("labels.principais_cidades")):
        cities = cities_for_iso3(iso3)
        if cities.empty:
            st.info(tr("labels.sem_cidades_gera_csv"))
        else:
            c = cities.copy()
            for k in ("city", "admin", "type", "is_capital", "population", "year", "lat", "lon"):
                if k not in c.columns:
                    c[k] = pd.NA

            def _clean_text(v):
                s = str(v).strip()
                return None if s.lower() in {"", "none", "nan", "empty"} else s

            c["city"] = c["city"].apply(_clean_text)
            c["admin"] = c["admin"].apply(_clean_text)
            c["type"] = c["type"].apply(_clean_text)
            c = c[c["city"].notna()]
            if c.empty:
                st.info(tr("labels.sem_cidades_validas"))
            else:
                c["__year"] = pd.to_numeric(c["year"], errors="coerce")
                c["__pop"] = pd.to_numeric(c["population"], errors="coerce")

                def _join_unique(series: pd.Series) -> str:
                    vals = [str(x) for x in series.dropna().astype(str) if x]
                    return ", ".join(sorted(set(vals))) if vals else ""

                # Índice do registo com ano mais recente por cidade
                idx_latest = (
                    c.sort_values(["city", "__year"], ascending=[True, True])
                    .groupby("city", observed=False)["__year"].idxmax()
                    .dropna()
                    .astype(int)
                )
                # Se não houver ano, usar o registo com maior população
                if idx_latest.empty:
                    idx_latest = (
                        c.sort_values(["city", "__pop"], ascending=[True, True])
                        .groupby("city", observed=False)["__pop"].idxmax()
                        .dropna()
                        .astype(int)
                    )
                # Último fallback: primeiro registo por cidade
                if idx_latest.empty:
                    idx_latest = c.groupby("city", observed=False).head(1).index

                latest = c.loc[idx_latest, ["city", "is_capital", "population", "__year"]].rename(
                    columns={"__year": "year"}
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

                # Mapeia Capital? para Sim/Não, mas só para uso interno (ordenar)
                if "Capital?" in show.columns:
                    show["Capital?"] = show["Capital?"].map(
                        {1: "Sim", 0: "Não", True: "Sim", False: "Não"}
                    ).fillna("")

                if "Ano" in show.columns:
                    show["Ano"] = show["Ano"].apply(
                        lambda x: "" if pd.isna(x) else str(int(x))
                    )
                if "População" in show.columns:
                    show["População"] = show["População"].apply(
                        lambda v: "" if pd.isna(v) else f"{int(v):,}".replace(",", " ")
                    )

                # Usar a info de capital e população apenas para ordenar
                show["_cap"] = show["Capital?"].eq("Sim") if "Capital?" in show.columns else False
                show["_pop"] = (
                    pd.to_numeric(
                        show.get("População", 0).astype(str)
                        .str.replace(" ", "")
                        .str.replace(",", ""),
                        errors="coerce",
                    ).fillna(0)
                )

                show = (
                    show.sort_values(["_cap", "_pop", "Cidade"], ascending=[False, False, True])
                    .drop(columns=["_cap", "_pop", "Tipo", "Capital?"], errors="ignore")
                )

                # Colunas visíveis — sem "Capital?"
                cols = [c for c in ["Cidade",  "População", "Ano"] if c in show.columns]

                colL, colR = st.columns([0.42, 0.58], gap="large")
                with colL:
                    st.markdown(tr("labels.principais_cidades_munic_pios"))
                    st.dataframe(
                        show[cols] if cols else show,
                        use_container_width=True,
                        hide_index=True,
                        column_config=_colcfg_cities(),
                    )
                with colR:
                    st.markdown(tr("labels.mapa"))
                    for k in ("lat", "lon"):
                        if k not in c.columns:
                            c[k] = pd.NA
                    cc = c.copy()
                    cc["lat"] = pd.to_numeric(cc["lat"], errors="coerce")
                    cc["lon"] = pd.to_numeric(cc["lon"], errors="coerce")
                    pts = (
                        cc.dropna(subset=["lat", "lon"])
                        .loc[
                            cc["lat"].between(-90, 90) & cc["lon"].between(-180, 180),
                            ["city", "lat", "lon"],
                        ]
                        .drop_duplicates(subset=["city"], keep="first")
                    )
                    if len(pts) > 0:
                        st.map(pts[["lat", "lon"]], use_container_width=True)

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
                    "site": "first", "type": _agg_types, "year": "min",
                    "lat": "first", "lon": "first", "country": "first", "iso3": "first",
                })
            ).rename(columns={"site":"Sítio","type":"Tipo","year":"Ano"})
            u["Ano"] = u["Ano"].apply(fmt_year)

            cols = ["Sítio","Tipo","Ano","lat","lon"]
            ROW_H, HDR_H, MAX_H = 28, 38, 420
            n = len(u); height = min(MAX_H, HDR_H + ROW_H * max(n, 1))
            st.data_editor(
                u[cols], use_container_width=True, hide_index=True, height=height,
                disabled=True, column_config=_colcfg_unesco(),
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

    # Gastronomia
    with st.expander(tr("labels.gastronomia")):
        try:
            g = gastronomy_for_iso3(iso3)
        except Exception:
            g = pd.DataFrame()

        if g.empty:
            st.caption("No gastronomy data available for this country.")
        else:
            g = g.copy()

            # --- 1) Lista de exclusões (podes ir afinando) ---
            EXCLUDED_GASTRO_ITEMS = {
                "beer",
                "beers",
                "biscuit",
                "biscuits",
                "cookie",
                "cookies",
                "bread",
                "white bread",
                "rice",
                "water",
                "tea",
                "coffee",
            }

            g["item_norm"] = (
                g["item"]
                .astype(str)
                .str.strip()
                .str.lower()
            )
            g = g[~g["item_norm"].isin(EXCLUDED_GASTRO_ITEMS)]

            if g.empty:
                st.caption("Only very generic items (beer, biscuits, etc.) were found.")
            else:
                base_url = "https://www.tasteatlas.com"

                def _mk_url(slug: str) -> str:
                    s = str(slug or "").strip()
                    if not s:
                        return ""
                    if s.startswith("http://") or s.startswith("https://"):
                        return s
                    if not s.startswith("/"):
                        s = "/" + s
                    return base_url + s

                # ordenar por nome para ficar estável
                g = g.sort_values("item")

                for idx, row in g.iterrows():
                    item_name = str(row.get("item") or "").strip()
                    slug      = str(row.get("url_slug") or "").strip()
                    url       = _mk_url(slug)

                    # --- 2) Nome "mascarado" com o link TasteAtlas ---
                    if url:
                        st.markdown(f"**[{item_name}]({url})**")
                    else:
                        st.markdown(f"**{item_name}**")

                    # --- 3) Única inputbox para receita em PT ---
                    # key inclui iso3 e índice para não colidir entre países
                    recipe_key = f"recipe_{iso3}_{idx}"

                    st.text_area(
                        "Recipe (pt-PT)",
                        key=recipe_key,
                        height=120,
                        label_visibility="collapsed",
                        placeholder="Escreve aqui a receita em português (ingredientes + modo de preparação)…",
                    )

                    st.markdown("---")
    
        try:
            g = gastronomy_for_iso3(iso3)
        except Exception:
            g = pd.DataFrame()

        if g.empty:
            st.caption("No gastronomy data available for this country.")
        else:
            g = g.copy()
            # garantir colunas esperadas
            for col in ["item", "ranking", "score", "critics", "url_slug"]:
                if col not in g.columns:
                    g[col] = pd.NA

            # construir URL completo para TasteAtlas
            base_url = "https://www.tasteatlas.com"

            def _mk_url(slug: str) -> str:
                s = str(slug or "").strip()
                if not s:
                    return ""
                if s.startswith("http://") or s.startswith("https://"):
                    return s
                if not s.startswith("/"):
                    s = "/" + s
                return base_url + s

            g["url"] = g["url_slug"].apply(_mk_url)

            # ordenar por ranking numérico (se existir) e limitar a 30 itens
            g["ranking_num"] = pd.to_numeric(g["ranking"], errors="coerce")
            g["score_num"]   = pd.to_numeric(g["score"], errors="coerce")
            g["critics_num"] = pd.to_numeric(g["critics"], errors="coerce")

            g = g.sort_values(
                ["ranking_num", "score_num", "item"],
                ascending=[True, False, True],
                na_position="last",
            ).head(300)

            # preparar DataFrame para mostrar
            show = pd.DataFrame({
                "Item":          g["item"],
                "World ranking": g["ranking_num"],
                "Score":         g["score_num"],
                "Reviews":       g["critics_num"],
                "Link":          g["url"].apply(
                    lambda u: f"[TasteAtlas]({u})" if u else ""
                ),
            })

            # altura dinâmica semelhante ao UNESCO
            ROW_H, HDR_H, MAX_H = 28, 38, 420
            n = len(show)
            height = min(MAX_H, HDR_H + ROW_H * max(n, 1))

            st.data_editor(
                show,
                use_container_width=True,
                hide_index=True,
                disabled=True,
                height=height,
                column_config=_colcfg_gastronomy(),
            )

    # Turismo (métricas + série)
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

        cols = st.columns(3); i = 0
        for code, label in kmap.items():
            last, prev = _latest_and_prev(t_ts, code)
            if last is None: continue
            year = int(last["year"]); val = float(last["value"])
            val_txt = _fmt_value(val, unit.get(code, "int"))
            delta_txt = ""
            if prev is not None and pd.notna(prev["value"]):
                delta = val - float(prev["value"])
                delta_txt = _fmt_delta(delta, unit.get(code, "int"), ref_value=val)
            cols[i % 3].metric(f"{label} · {year}", val_txt, delta=delta_txt); i += 1

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
                color=alt.Color("metric:N", title="", sort=list(label_map.values()),
                                legend=alt.Legend(orient="bottom")),
                tooltip=[
                    alt.Tooltip("metric:N", title=tr("paises.indicador")),
                    alt.Tooltip("year:Q", title=tr("climate_indicators.ano"), format="d"),
                    alt.Tooltip("value:Q", title=tr("paises.valor"), format=",.0f"),
                ],
            )
            .properties(height=260),
            use_container_width=True,
        )
