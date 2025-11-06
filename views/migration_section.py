# views/migration_section.py
from __future__ import annotations
import pandas as pd
import altair as alt
import streamlit as st
from services.i18n import t as tr

def render_migration_section(iso3: str) -> None:
    from services.offline_store import (
        load_migration_latest_for_iso3,
        load_migration_ts_for_iso3,
        load_migration_inout,
        MIG_INOUT_CSV,
    )

    with st.expander(tr("labels.migra_o")):
        latest = load_migration_latest_for_iso3(iso3)
        ts     = load_migration_ts_for_iso3(iso3)

        kmap = {
            "SM.POP.NETM":          "paises.migracao_liquida_pessoas",
            "BX.TRF.PWKR.CD.DT":    "paises.remessas_recebidas_usd",
            "BX.TRF.PWKR.DT.GD.ZS": "paises.remessas_percent_pib",
        }
        unit_fmt = {"SM.POP.NETM": "int", "BX.TRF.PWKR.CD.DT": "money", "BX.TRF.PWKR.DT.GD.ZS": "pct"}

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
                if scale == "B": return f"{v/1e9:.2f} B"
                if scale == "M": return f"{v/1e6:.2f} M"
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

        # Série temporal UN DESA (imigração/emigração)
        df_all = load_migration_inout()
        csv_name = getattr(MIG_INOUT_CSV, "name", "migration_inout.csv")
        if df_all.empty:
            st.caption(tr("labels.un_desa_dataset_vazio_n_o_encontrado_csv_name", csv_name=csv_name))
            return

        iso3u = str(iso3).upper()
        df = df_all.copy()
        df.columns = df.columns.str.replace("\ufeff", "", regex=False).str.strip()
        want = ["iso3", "year", "immigrants", "emigrants"]
        if not set(want).issubset(df.columns):
            st.caption(tr("labels.un_desa_headers_unexpected",
                          csv_name=csv_name, missing=str([c for c in want if c not in df.columns]),
                          columns=", ".join(map(repr, df.columns))))
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
            st.caption(tr("labels.sem_dados_un_desa_para_este_pais_no_csv_name_n_o_h_linhas_para_iso3_iso3u",
                          csv_name=csv_name, iso3u=iso3u))
            return

        long = (
            io_df.melt(id_vars="year", value_vars=["immigrants", "emigrants"],
                       var_name="tipo", value_name="valor")
            .assign(tipo=lambda d: d["tipo"].map({
                "immigrants": tr("paises.imigracao"),
                "emigrants":  tr("paises.emigracao"),
            }))
        )
        years_sorted = sorted(int(y) for y in long["year"].dropna().unique())

        lines = (
            alt.Chart(long)
            .mark_line(point=True)
            .encode(
                x=alt.X("year:O", title=tr("climate_indicators.ano"), sort=years_sorted),
                y=alt.Y("valor:Q", title=tr("paises.pessoas")),
                color=alt.Color("tipo:N", title="", legend=alt.Legend(orient="bottom")),
                tooltip=[
                    alt.Tooltip("year:O", title=tr("climate_indicators.ano")),
                    "tipo:N",
                    alt.Tooltip("valor:Q", title=tr("paises.pessoas"), format=",.0f"),
                ],
            )
            .properties(height=260)
        )
        st.altair_chart(lines, use_container_width=True)
