# views/climate_scenarios.py
from __future__ import annotations
import io
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from services.open_meteo import geocode
from services.cmip6 import (
    list_model_members, default_members_for_models,
    fetch_series, anomalies,
)
def _hex_to_rgba(hex_color: str, alpha: float) -> str:
    """Converte '#rrggbb' para 'rgba(r,g,b,a)'."""
    try:
        h = hex_color.lstrip("#")
        r = int(h[0:2], 16)
        g = int(h[2:4], 16)
        b = int(h[4:6], 16)
        return f"rgba({r},{g},{b},{alpha})"
    except Exception:
        # fallback azul plotly
        return f"rgba(31,119,180,{alpha})"

def _hex_to_rgba(hex_color: str, alpha: float) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"

def _pretty_scn(s: str) -> str:
    return {
        "historical": "Histórico",
        "ssp126": "SSP1-2.6",
        "ssp245": "SSP2-4.5",
        "ssp370": "SSP3-7.0",
        "ssp585": "SSP5-8.5",
    }.get(s, s)

def _warming_label(smooth_df: pd.DataFrame, scn: str) -> str:
    """média ΔT na década 2091–2100 (ou últimos 10 pontos se faltar)."""
    g = smooth_df[smooth_df["scenario"] == scn].copy()
    if g.empty:
        return "n/a"
    g = g.sort_values("time")
    end = g[(g["year"] >= 2091) & (g["year"] <= 2100)]
    if end.empty:
        end = g.tail(10)
    val = float(end["ΔT (°C)"].mean())
    return f"~+{val:.1f} °C"

# ---------- util -----------------
def _pick_point(q: str) -> pd.DataFrame:
    df = geocode(q)
    if df is None or df.empty:
        return pd.DataFrame()
    df["place"] = df["label"].str.split("—").str[0].str.strip()
    return df[["place", "latitude", "longitude"]].head(5)

_SCENARIO_COLORS = {
    "historical": "#6c757d",  # cinza
    "ssp126":     "#2ca02c",  # verde
    "ssp245":     "#ff7f0e",  # laranja
    "ssp370":     "#9467bd",  # roxo
    "ssp585":     "#d62728",  # vermelho
}

def _hex_to_rgba(hex_color: str, alpha: float) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"

def _pretty_scn(s: str) -> str:
    return {
        "historical": "Histórico",
        "ssp126": "SSP1-2.6",
        "ssp245": "SSP2-4.5",
        "ssp370": "SSP3-7.0",
        "ssp585": "SSP5-8.5",
    }.get(s, s)

def _warming_tail_value(smooth_df: pd.DataFrame, scn: str) -> float | None:
    """Média ΔT (°C) na janela 2091–2100; se não houver, usa os últimos 10 pontos."""
    g = smooth_df[smooth_df["scenario"] == scn]
    if g.empty:
        return None
    g = g.sort_values("time")
    end = g[(g["year"] >= 2091) & (g["year"] <= 2100)]
    if end.empty:
        end = g.tail(10)
    if end.empty or end["ΔT (°C)"].isna().all():
        return None
    return float(end["ΔT (°C)"].mean())

def _smooth_rolling(df: pd.DataFrame, y: str, win: int = 5) -> pd.DataFrame:
    # média móvel centrada (mantém o mesmo length)
    df = df.sort_values("time").copy()
    df[y] = df[y].rolling(win, center=True, min_periods=1).mean()
    return df

def _warming_tail_value(smooth_df: pd.DataFrame, scn: str) -> float | None:
    """
    ΔT médio (°C):
      - histórico: 2005–2014
      - SSPs: 2091–2100 (ou últimos 10 pontos se faltar)
    """
    g = smooth_df[smooth_df["scenario"] == scn].sort_values("time")
    if g.empty:
        return None
    if scn == "historical":
        win = g[(g["year"] >= 2005) & (g["year"] <= 2014)]
        if win.empty:
            win = g.tail(10)
    else:
        win = g[(g["year"] >= 2091) & (g["year"] <= 2100)]
        if win.empty:
            win = g.tail(10)
    if win.empty or win["ΔT (°C)"].isna().all():
        return None
    return float(win["ΔT (°C)"].mean())

# ---------- aba ------------------
def render_climate_tab():
    st.subheader("📅 Projeções climáticas (CMIP6) — média e incerteza por cenário")

    # Local
    c1, c2, c3 = st.columns([1.7, 1, 1])
    with c1:
        q = st.text_input("Local (ponto)", "Lisboa")
        if st.button("🔎 Procurar", key="cmip_search"):
            st.session_state["cmip_places"] = _pick_point(q)
    with c2:
        baseline = st.selectbox("Baseline (anomalias)", ["1991–2020", "1981–2010", "1961–1990"], index=0)
    with c3:
        win = st.number_input("Suavização (média móvel, anos)", 1, 11, 5, 2)

    base_map = {"1991–2020": (1991, 2020), "1981–2010": (1981, 2010), "1961–1990": (1961, 1990)}
    baseline_yrs = base_map[baseline]

    res = st.session_state.get("cmip_places")
    if isinstance(res, pd.DataFrame) and not res.empty:
        sel = st.selectbox("Resultados", list(res.index),
                           format_func=lambda i: f"{res.loc[i,'place']} ({res.loc[i,'latitude']:.2f}, {res.loc[i,'longitude']:.2f})")
        row = res.loc[sel]
    else:
        tmp = _pick_point("Lisboa")
        row = tmp.iloc[0] if not tmp.empty else pd.Series({"place": "Lisboa", "latitude": 38.72, "longitude": -9.14})

    location = {"type": "point", "lat": float(row["latitude"]), "lon": float(row["longitude"])}

    # Modelos & cenários
    avail = list_model_members()
    suggested = [m for m in ["EC-Earth3", "MPI-ESM1-2-HR", "CMCC-ESM2", "UKESM1-0-LL"] if m in set(avail["source_id"])]
    models = st.multiselect("Modelos", options=sorted(avail["source_id"].unique()), default=suggested[:3])

    scenarios = st.multiselect(
        "Cenários",
        options=["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        default=["historical", "ssp126", "ssp245", "ssp370", "ssp585"]
    )

    if st.button("▶️ Calcular", type="primary"):
        if not models or not scenarios:
            st.warning("Escolhe pelo menos 1 modelo e 1 cenário.")
            return

        members = default_members_for_models(models)
        if members.empty:
            st.warning("Sem membros para os modelos escolhidos.")
            return

        # recolha: para cada modelo+cenário gera série anual (°C) e anomalia
        rows = []
        with st.spinner("A carregar CMIP6 (pode demorar na 1ª vez)…"):
            for _, mrow in members.iterrows():
                model = mrow["source_id"]; member = mrow["member_id"]; grid = mrow["grid_label"]
                for exp in scenarios:
                    s = fetch_series(model, member, grid, exp, location=location, annual=True)
                    if s.empty:
                        continue
                    sa = anomalies(s, baseline=baseline_yrs)
                    df = pd.DataFrame({"time": s.index, "tas (°C)": s.values})
                    df["ΔT (°C)"] = sa.reindex(s.index).values
                    df["model"] = model
                    df["scenario"] = exp
                    rows.append(df)

        if not rows:
            st.info("Sem dados devolvidos para estas escolhas.")
            return

        full = pd.concat(rows, ignore_index=True)
        full["year"] = full["time"].dt.year
        full = full[full["year"] >= 1950].copy()

        # =========================
        #   MÉDIA e FAIXA por SSP
        # =========================
        # suavização (opcional) por cenário+modelo antes de agregar
        smooth = (
            full.groupby(["model", "scenario"], group_keys=False)
                .apply(lambda g: _smooth_rolling(g, "ΔT (°C)", win))
        )

        # stats por cenário/ano
        grp = smooth.groupby(["scenario", "time"])
        stat = grp["ΔT (°C)"].agg(["mean", "min", "max"]).reset_index()
        st.markdown(
            "<p style='font-size:14px'><b>O que são os SSP?</b> Cenários socioeconómicos combinados com trajetórias de emissões usados nas projeções CMIP6.</p>",
            unsafe_allow_html=True
        )

        st.markdown(
            """
            <div style='font-size:13px'>
            - <b>SSP1-2.6 (Sustentabilidade):</b> forte mitigação; aquecimento estabiliza perto de ~1.5–2 °C.<br>
            - <b>SSP2-4.5 (Cenário intermédio):</b> continuação das tendências atuais, sem esforços extraordinários nem colapso.<br>
            - <b>SSP3-7.0 (Rivalidade regional):</b> mundo fragmentado; mitigação fraca; emissões elevadas.<br>
            - <b>SSP5-8.5 (Fóssil-intensivo):</b> dependência forte de combustíveis fósseis; emissões muito altas.<br>
            - <b>Histórico:</b> simulações do passado; não é cenário futuro.
            </div>
            """,
            unsafe_allow_html=True
        )



        # gráfico
        fig = go.Figure()

        for scn in [s for s in ["historical","ssp126","ssp245","ssp370","ssp585"]
                    if s in stat["scenario"].unique()]:
            g = stat[stat["scenario"] == scn].sort_values("time")
            color = _SCENARIO_COLORS.get(scn, "#1f77b4")
            fill_rgba = _hex_to_rgba(color, 0.18)

            # 1) faixa (mín–máx)
            fig.add_traces([
                go.Scatter(
                    x=pd.concat([g["time"], g["time"][::-1]]),
                    y=pd.concat([g["max"],  g["min"][::-1]]),
                    fill="toself",
                    fillcolor=fill_rgba,
                    line=dict(width=0),
                    hoverinfo="skip",
                    name=f"{_pretty_scn(scn)} (incerteza)",
                    showlegend=False,
                )
            ])

            # 2) linha média (sem legendas — vamos rotular no fim da linha)
            fig.add_trace(
                go.Scatter(
                    x=g["time"], y=g["mean"],
                    mode="lines+markers",
                    line=dict(color=color, width=2),
                    marker=dict(size=4),
                    name=_pretty_scn(scn),
                    showlegend=False,
                )
            )

            # 3) rótulo no fim da linha com o aquecimento estimado
            tail_text = _pretty_scn(scn)
            w = _warming_tail_value(smooth, scn)
            if w is not None:
                tail_text += f" · ~+{w:.1f} °C"

            # último ponto válido da média
            
            g_valid = g.dropna(subset=["mean"])

            if scn == "historical":
                # posicionar o rótulo no meio do período histórico (~2012)
                t_anno = pd.to_datetime("2012-07-01")
                # valor no entorno de 5 anos para ficar estável
                around = g_valid[g_valid["time"].between(t_anno - pd.Timedelta(days=365*5),
                                                        t_anno + pd.Timedelta(days=365*5))]
                y_anno = around["mean"].mean() if not around.empty else (g_valid["mean"].iloc[-1] if not g_valid.empty else None)
                if y_anno is not None:
                    w = _warming_tail_value(smooth, scn)
                    txt = f"{_pretty_scn(scn)}" + (f" · ~+{w:.1f} °C" if w is not None else "")
                    # caixa opaca na cor do cenário para garantir legibilidade
                    fig.add_annotation(
                        x=t_anno, y=y_anno,
                        text=txt,
                        xanchor="left", yanchor="middle",
                        xshift=8, showarrow=False,
                        font=dict(size=12, color="white"),
                        bgcolor=_hex_to_rgba(_SCENARIO_COLORS["historical"], 0.70),
                        bordercolor=_SCENARIO_COLORS["historical"], borderwidth=0.5,
                    )
            else:
                # rótulo normal no fim da linha (SSPs)
                if not g_valid.empty:
                    x_last = g_valid["time"].iloc[-1]
                    y_last = g_valid["mean"].iloc[-1]
                    w = _warming_tail_value(smooth, scn)
                    txt = f"{_pretty_scn(scn)}" + (f" · ~+{w:.1f} °C" if w is not None else "")
                    fig.add_annotation(
                        x=x_last, y=y_last,
                        text=txt,
                        xanchor="left", yanchor="middle",
                        xshift=8, showarrow=False,
                        font=dict(size=12),
                        bgcolor="rgba(0,0,0,0)",
                    )




        fig.update_layout(
            height=360,
            margin=dict(l=6, r=6, t=40, b=0),
            xaxis_title="Ano",
            yaxis_title="Δ°C",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        )
        fig.update_yaxes(gridcolor="rgba(160,160,160,0.35)", gridwidth=1.2)
        fig.update_xaxes(gridcolor="rgba(160,160,160,0.18)", gridwidth=0.8)

        st.markdown(f"**Anomalias vs {baseline}** — média dos modelos (linha) e incerteza (faixa). Local: **{row['place']}**")
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("**Resumo por década (média dos modelos)**")
        smooth["decada"] = (smooth["year"] // 10) * 10
        
        dec = (
            smooth[smooth["year"] >= 1950]
            .groupby(["scenario", "decada"])["ΔT (°C)"]
            .mean()
            .reset_index()
            .pivot(index="decada", columns="scenario", values="ΔT (°C)")
            .sort_index()
        )
          

        # dec: pivot com uma linha por década (>=1950) e colunas por cenário
        dec = (
            smooth[smooth["year"] >= 1950]
            .groupby(["scenario", "decada"])["ΔT (°C)"]
            .mean()
            .reset_index()
            .pivot(index="decada", columns="scenario", values="ΔT (°C)")
            .sort_index()
            .reset_index()
        )

        # garantir ordem/nomes de colunas (só usa as que existirem)
        order = ["decada", "historical", "ssp126", "ssp245", "ssp370", "ssp585"]
        cols  = [c for c in order if c in dec.columns]
        dec   = dec[cols].copy()

        # cabeçalhos bonitos
        rename_hdr = {
            "decada": "Década",
            "historical": "Histórico",
            "ssp126": "SSP1-2.6",
            "ssp245": "SSP2-4.5",
            "ssp370": "SSP3-7.0",
            "ssp585": "SSP5-8.5",
        }
        headers = [rename_hdr.get(c, c) for c in dec.columns]

        # formatos:
        # - Década como string (evita separador de milhares)
        # - restantes: 4 casas decimais, NaN -> vazio
        dec["decada"] = dec["decada"].astype(int).astype(str)
        for c in dec.columns:
            if c != "decada":
                dec[c] = dec[c].apply(lambda v: "" if pd.isna(v) else f"{float(v):.4f}")

        # construir tabela Plotly (centrada)
        cell_vals = [dec[c].tolist() for c in dec.columns]
        fig_tbl = go.Figure(
            data=[go.Table(
                header=dict(values=headers, align="center"),
                cells=dict(values=cell_vals, align="center"),
            )]
        )
        fig_tbl.update_layout(margin=dict(l=0, r=0, t=8, b=0), height=420)
        st.plotly_chart(fig_tbl, use_container_width=True)


        # Download CSV dos pontos da média/min/máx
        csv_df = stat.rename(columns={"mean": "media", "min": "min", "max": "max"})
        buf = io.StringIO(); csv_df.to_csv(buf, index=False)
        st.download_button(
            "💾 Download CSV (média e faixa por cenário)",
            data=buf.getvalue(),
            file_name="cmip6_scenarios_mean_band.csv",
            mime="text/csv",
            use_container_width=True,
            key="dl_cmip6_mean_band",
        )
