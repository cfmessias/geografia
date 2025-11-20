# views/economia.py
# -*- coding: utf-8 -*-
"""
Painel de Indicadores Económicos (WDI / World Bank)
Traduzido dinamicamente (PT/EN) e compatível com múltiplos países.
"""

from __future__ import annotations
import pandas as pd
import streamlit as st
import altair as alt
from functools import lru_cache
from services.i18n_boot import _ensure_lang_state
from services.i18n import t as tr
from services.countries_names import country_display_name
import sys
import time
from pathlib import Path 

WB_WDI_FILES_BASE = "https://data360files.worldbank.org/data360-data/data/WB_WDI"

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


@lru_cache(maxsize=1)
def _load_wdi_econ_offline() -> pd.DataFrame:
    """Lê data/wdi_economics.csv gerado pelo fetch_wdi_economics.py."""
    path = DATA_DIR / "wdi_economics.csv"
    if not path.exists():
        return pd.DataFrame(columns=["iso3", "year", "code", "value"])
    df = pd.read_csv(path, sep=";", dtype={"iso3": str, "code": str})
    df["iso3"] = df["iso3"].str.upper().str.strip()
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df


@lru_cache(maxsize=1)
def _load_wdi_sectors_offline() -> pd.DataFrame:
    """Lê data/wdi_sectors_wide.csv gerado pelo fetch_wdi_sectors.py."""
    path = DATA_DIR / "wdi_sectors_wide.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, sep=";")
    df["iso3"] = df["iso3"].astype(str).str.upper().str.strip()
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    return df

def _wdi_indicator_to_file_id(ind_code: str) -> str:
    """
    Converte código WDI clássico (NY.GDP.MKTP.CD) no ID do ficheiro CSV da Data360.
    Ex.: NY.GDP.MKTP.CD -> WB_WDI_NY_GDP_MKTP_CD
    """
    return f"WB_WDI_{ind_code.replace('.', '_')}"

def _read_wdi_csv_with_retries(ind_code: str, max_retries: int = 3) -> pd.DataFrame:
    """
    Lê o CSV da Data360 com algumas tentativas extra para evitar falhas
    intermitentes (WinError 10054).
    """
    file_id = _wdi_indicator_to_file_id(ind_code)
    url = f"{WB_WDI_FILES_BASE}/{file_id}.csv"

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            df = pd.read_csv(url)
            return df
        except Exception as e:
            last_err = e
            print(
                f"[warn] tentativa {attempt}/{max_retries} a ler CSV {ind_code} ({url}) falhou: {e}",
                file=sys.stderr,
            )
            # espera crescente entre tentativas
            time.sleep(0.8 * attempt)

    print(
        f"[error] falhou definitivamente CSV para {ind_code} ({url}): {last_err}",
        file=sys.stderr,
    )
    # devolve DataFrame vazio para não rebentar o resto da app
    return pd.DataFrame()

# --- Configurações gerais ---
# Atenção: o endpoint antigo https://api.worldbank.org/v2 deixou de responder (502).
# Em vez disso, passamos a usar diretamente os ficheiros CSV da plataforma Data360 (WB_WDI).

FALLBACKS = {
    # Pobreza a 2.15 USD/dia → se não houver, tentar LMIC / UMIC
    "SI.POV.DDAY": ["SI.POV.LMIC", "SI.POV.UMIC"],
}


@st.cache_data(show_spinner=False)
def _wdi_fetch_indicator(iso3: str, ind_code: str, date_range: str) -> pd.DataFrame:
    """
    Lê um indicador WDI para um país a partir dos CSVs Data360 (WB_WDI),
    aplicando o intervalo de anos indicado em `date_range` (ex: "2000:2024").

    Retorna um DataFrame com colunas ['year', 'value'].
    """
    iso3 = iso3.upper().strip()

    # 1) Converter "2000:2024" em year_min / year_max
    year_min, year_max = None, None
    if isinstance(date_range, str) and ":" in date_range:
        a, b = date_range.split(":", 1)
        try:
            year_min = int(a)
            year_max = int(b)
        except ValueError:
            pass

    # Fallback caso algo venha estranho
    if year_min is None or year_max is None:
        year_min, year_max = 1960, 2100

    # 2) Ler CSV com retries
    raw = _read_wdi_csv_with_retries(ind_code)
    if raw.empty:
        # Falhou download ou CSV mesmo vazio
        return pd.DataFrame(columns=["year", "value"])

    # 3) Normalizar país
    if "REF_AREA" not in raw.columns or "OBS_VALUE" not in raw.columns:
        # formato inesperado
        return pd.DataFrame(columns=["year", "value"])

    df = raw.copy()
    df["REF_AREA"] = df["REF_AREA"].astype(str).str.upper().str.strip()
    df = df[df["REF_AREA"] == iso3].copy()
    if df.empty:
        return pd.DataFrame(columns=["year", "value"])

    # 4) Extrair ano
    if "TIME_PERIOD" in df.columns:
        df["year"] = pd.to_numeric(df["TIME_PERIOD"], errors="coerce")
    elif "TIME_PERIOD_int" in df.columns:
        df["year"] = pd.to_numeric(df["TIME_PERIOD_int"], errors="coerce")
    else:
        df["year"] = pd.NA

    # 5) Valor numérico
    df["value"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")

    # 6) Filtrar intervalo de anos e limpar
        # 6) Filtrar intervalo de anos e limpar
    df = df[
        (df["year"].notna())
        & (df["value"].notna())
        & (df["year"] >= year_min)
        & (df["year"] <= year_max)
    ].copy()

    df = df.sort_values("year")[["year", "value"]]

    # adicionar iso3 (necessário para algumas funções que esperam esta coluna)
    df["iso3"] = iso3

    # ordenar colunas para consistência
    return df[["iso3", "year", "value"]].reset_index(drop=True)


def _render_pib_sectors_table(iso3: str, year_min: int, year_max: int):
    path = DATA_DIR / "wdi_pib_sectors_table.csv"
    if not path.exists():
        st.info("Sem dados offline para setores do PIB.")
        return

    df = pd.read_csv(path, sep=";")
    iso = (iso3 or "").upper().strip()
    df = df[df["iso3"] == iso].copy()

    df = df[(df["Ano"] >= year_min) & (df["Ano"] <= year_max)]
    df = df.drop(columns=["iso3"]).reset_index(drop=True)

    # aqui podes traduzir cabeçalhos se quiseres, mas já vem com nomes descritivos
    st.subheader(tr("economics.table_title"))
    st.dataframe(df, use_container_width=True)

def _first_series_with_data(
    iso3: str, candidates: list[str], date_range: str
) -> tuple[pd.DataFrame, str]:
    """Tenta várias alternativas de código e devolve a primeira com dados."""
    for code in candidates:
        df = _wdi_fetch_indicator(iso3, code, date_range)
        if not df.empty and df["value"].notna().any():
            return df, code
    # Se nenhuma das alternativas tiver dados, devolve DF vazio e o primeiro código
    return pd.DataFrame(columns=["iso3", "year", "value", "code"]), candidates[0]

@st.cache_data(show_spinner=False)
def fetch_wdi_dataset(
    iso3: str,
    codes: list[str],
    year_min: int,
    year_max: int,
    IND: dict,
) -> tuple[pd.DataFrame, dict[str, str]]:
    """
    Versão OFFLINE: usa apenas data/wdi_economics.csv gerado pelo
    scripts/fetch_wdi_economics.py, com suporte a FALLBACKS (ex. pobreza).

    Devolve um DataFrame longo com colunas:
      - iso3
      - year
      - value
      - code       → código efetivamente usado (pode ser fallback)
      - orig_code  → código "pedido" no preset (para os labels)
    """
    iso = (iso3 or "").upper().strip()
    econ = _load_wdi_econ_offline()

    frames: list[pd.DataFrame] = []
    labels_map: dict[str, str] = {}

    if econ.empty:
        return pd.DataFrame(columns=["iso3", "year", "value", "code", "orig_code"]), labels_map

    for orig_code in codes[:4]:
        candidates = [orig_code] + FALLBACKS.get(orig_code, [])

        used_code = None
        df_code = pd.DataFrame()

        # 1) procurar no CSV offline
        sub = econ[(econ["iso3"] == iso) & (econ["code"].isin(candidates))]
        if not sub.empty:
            for cand in candidates:
                tmp = sub[sub["code"] == cand]
                if not tmp.empty:
                    used_code = cand
                    df_code = tmp.copy()
                    break

        if used_code is None:
            # não temos dados para este indicador
            labels_map[orig_code] = IND.get(orig_code, {}).get("label", orig_code)
            continue

        # filtrar intervalo de anos do slider
        df_code = df_code[
            df_code["year"].notna()
            & (df_code["year"] >= year_min)
            & (df_code["year"] <= year_max)
        ]

        if df_code.empty:
            # havia dados fora do intervalo, mas não dentro
            labels_map[orig_code] = IND.get(used_code, {}).get("label", used_code)
            continue

        df_code = df_code[["iso3", "year", "value"]].copy()
        # 'code' tem de bater certo com os códigos usados no UI (orig_code)
        df_code["code"] = orig_code
        df_code["orig_code"] = orig_code

        frames.append(df_code)
        # mas o texto do label pode vir do código efetivamente usado (fallback)
        labels_map[orig_code] = IND.get(used_code, {}).get("label", used_code)


    if frames:
        out = pd.concat(frames, ignore_index=True).sort_values(["year", "code"])
    else:
        out = pd.DataFrame(columns=["iso3", "year", "value", "code", "orig_code"])

    return out, labels_map


def get_wdi_selection(default_codes: list[str], default_years: tuple[int, int] = (2000, 2024)) -> tuple[list[str], tuple[int, int]]:
    """Lê seleção atual (ou usa defaults)."""
    codes = st.session_state.get("econ_selected_codes", default_codes)
    years = st.session_state.get("econ_year_range", default_years)
    return list(codes), tuple(years)


def _is_percent(code: str, label: str) -> bool:
    return "%" in label or code.endswith(".ZG")


def _chart_one(df: pd.DataFrame, code: str, label: str) -> alt.Chart:
    """Desenha um gráfico Altair para um indicador."""
    if not label:
        label = code

    # Se não há dados nenhuns no dataset, mostra logo mensagem
    if df.empty:
        empty = pd.DataFrame({"msg": [tr("economics.table_no_data")]})
        return (
            alt.Chart(empty, height=260)
            .mark_text(align="center", baseline="middle")
            .encode(text="msg:N")
            .properties(title=label, width="container")
        )

    # Filtrar só o indicador pedido
    sub = df[df["code"] == code].dropna(subset=["value"]).copy()

    # Se não houver dados para ESTE indicador, mostrar "sem dados"
    if sub.empty:
        empty = pd.DataFrame({"msg": [tr("economics.table_no_data")]})
        return (
            alt.Chart(empty, height=260)
            .mark_text(align="center", baseline="middle")
            .encode(text="msg:N")
            .properties(title=label, width="container")
        )

    # Formatação do eixo Y / tooltip
    y_fmt = ",.1f" if _is_percent(code, label) else ",.0f"
    tip = ",.2f"

    return (
        alt.Chart(sub, height=260)
        .mark_line(point=True)
        .encode(
            x=alt.X(
                "year:Q",
                title=tr("economics.chart_tooltip.year"),
                axis=alt.Axis(format="d"),
            ),
            y=alt.Y(
                "value:Q",
                title=None,
                scale=alt.Scale(zero=False),
                axis=alt.Axis(format=y_fmt),
            ),
            tooltip=[
                alt.Tooltip("year:Q", format="d", title=tr("economics.chart_tooltip.year")),
                alt.Tooltip("value:Q", format=tip, title=tr("economics.chart_tooltip.value")),
            ],
        )
        .properties(title=label, width="container")
    )

def render_wdi_charts_2x2(df: pd.DataFrame, codes: list[str], labels_map: dict[str, str]) -> None:
    """Renderiza quatro gráficos (2x2)."""
    grid = [st.columns(2), st.columns(2)]
    for i, code in enumerate(codes[:4]):
        r, c = divmod(i, 2)
        label = labels_map.get(code, code)
        with grid[r][c]:
            st.altair_chart(_chart_one(df, code, label), use_container_width=True)


# --- views/economia.py (secção Setores) ------------------------------------
from pathlib import Path

SECTORS_VAB = ["NV.AGR.TOTL.ZS", "NV.IND.TOTL.ZS", "NV.SRV.TOTL.ZS"]
SECTORS_EMP = ["SL.AGR.EMPL.ZS", "SL.IND.EMPL.ZS", "SL.SRV.EMPL.ZS"]

# Mapas de rótulos (i18n)
def _sector_label_map():
    return {
        "NV.AGR.TOTL.ZS": tr("economics.ind.agri_vab"),
        "NV.IND.TOTL.ZS": tr("economics.ind.ind_vab"),
        "NV.SRV.TOTL.ZS": tr("economics.ind.srv_vab"),
        "SL.AGR.EMPL.ZS": tr("economics.ind.agri_emp"),
        "SL.IND.EMPL.ZS": tr("economics.ind.ind_emp"),
        "SL.SRV.EMPL.ZS": tr("economics.ind.srv_emp"),
    }


def _sector_palette(lbl: dict[str, str]) -> dict[str, str]:
    """
    Paleta única e coerente por label/setor:
      Serviços -> azul-claro, Indústria -> azul, Agricultura -> rosa
    """
    COLORS = {
        "srv": "#9ecae1",  # light blue
        "ind": "#2c7fb8",  # blue
        "agr": "#fcbba1",  # pink
    }
    return {
        # VAB
        lbl["NV.SRV.TOTL.ZS"]: COLORS["srv"],
        lbl["NV.IND.TOTL.ZS"]: COLORS["ind"],
        lbl["NV.AGR.TOTL.ZS"]: COLORS["agr"],
        # Emprego
        lbl["SL.SRV.EMPL.ZS"]: COLORS["srv"],
        lbl["SL.IND.EMPL.ZS"]: COLORS["ind"],
        lbl["SL.AGR.EMPL.ZS"]: COLORS["agr"],
    }


@lru_cache(maxsize=1)
def _try_read_sectors_csv() -> pd.DataFrame:
    """
    Lê data/wdi_sectors_wide.csv (se existir; sep=';').

    Estrutura esperada:
      iso3;year;agr_vab;ind_vab;srv_vab;agr_emp;ind_emp;srv_emp
    """
    candidates = []

    here = Path(__file__).resolve()
    # 1) Geografia/data/wdi_sectors_wide.csv  (estrutura do teu projeto)
    candidates.append(here.parent.parent / "data" / "wdi_sectors_wide.csv")
    # 2) fallback relativo (caso corras a partir da raiz do projeto)
    candidates.append(Path("data") / "wdi_sectors_wide.csv")

    for p in candidates:
        try:
            if p.exists():
                df = pd.read_csv(
                    p,
                    sep=";",
                    dtype={"iso3": str, "year": str},
                    encoding="utf-8",
                    keep_default_na=False,
                )
                # normalizar
                df["iso3"] = df["iso3"].astype(str).str.upper()
                df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

                for col in ["agr_vab", "ind_vab", "srv_vab", "agr_emp", "ind_emp", "srv_emp"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")

                print(f"[sectors] carregado {p} | linhas={len(df)}")
                return df
        except Exception as e:
            print(f"[sectors] erro a ler {p}: {e}")

    print("[sectors] wdi_sectors_wide.csv não encontrado ou vazio")
    return pd.DataFrame()

def _fetch_sector_series_online(iso3: str, codes: list[str]) -> pd.DataFrame:
    """Vai buscar séries ao WDI usando a função já existente _wdi_fetch_indicator."""
    frames = []
    for code in codes:
        df = _wdi_fetch_indicator(iso3, code, "1990:2024")
        if not df.empty:
            # df já vem com iso3, year, value
            frames.append(df[["iso3", "year", "value"]].assign(code=code))
    if not frames:
        return pd.DataFrame(columns=["iso3", "year", "code", "value"])
    return pd.concat(frames, ignore_index=True)



def _load_sectors_for_iso3(iso3: str) -> dict:
    """
    Devolve:
      - 'vab_long' (year, code, value) e 'vab_wide' (year, agr/ind/srv) para VAB
      - 'emp_long' e 'emp_wide' para Emprego
    Usando CSV local se existir; fallback online só para o país.
    """
    iso3u = (iso3 or "").upper().strip()
    csv_wide = _try_read_sectors_csv()

    out = {"vab_long": pd.DataFrame(), "vab_wide": pd.DataFrame(),
           "emp_long": pd.DataFrame(), "emp_wide": pd.DataFrame()}

    def _from_wide(csv: pd.DataFrame, cols: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
        if csv.empty:
            return pd.DataFrame(), pd.DataFrame()
        need = ["iso3", "year"] + cols
        if not set(need).issubset(csv.columns):
            return pd.DataFrame(), pd.DataFrame()
        sub = csv.loc[csv["iso3"] == iso3u, need].copy()
        if sub.empty:
            return pd.DataFrame(), pd.DataFrame()
        long = sub.melt(id_vars=["iso3", "year"], value_vars=cols, var_name="var", value_name="value")
        long = long.rename(columns={"var": "code"})
        return long.dropna(subset=["year"]), sub

    # 1) tentar CSV local
    vab_long, vab_wide = _from_wide(csv_wide, ["agr_vab", "ind_vab", "srv_vab"])
    emp_long, emp_wide = _from_wide(csv_wide, ["agr_emp", "ind_emp", "srv_emp"])

    # 2) se faltar, buscar online só para o país
    # if vab_long.empty:
    #     on = _fetch_sector_series_online(iso3u, SECTORS_VAB)
    #     if not on.empty:
    #         lbl = _sector_label_map()
    #         on["label"] = on["code"].map(lbl)
    #         vab_long = on.copy()
    #         vab_wide = (
    #             on.pivot_table(index=["iso3", "year"], columns="code", values="value", aggfunc="last")
    #             .reset_index()
    #             .rename(
    #                 columns={
    #                     "NV.AGR.TOTL.ZS": "agr_vab",
    #                     "NV.IND.TOTL.ZS": "ind_vab",
    #                     "NV.SRV.TOTL.ZS": "srv_vab",
    #                 }
    #             )
    #         )

    # if emp_long.empty:
    #     on = _fetch_sector_series_online(iso3u, SECTORS_EMP)
    #     if not on.empty:
    #         lbl = _sector_label_map()
    #         on["label"] = on["code"].map(lbl)
    #         emp_long = on.copy()
    #         emp_wide = (
    #             on.pivot_table(index=["iso3", "year"], columns="code", values="value", aggfunc="last")
    #             .reset_index()
    #             .rename(
    #                 columns={
    #                     "SL.AGR.EMPL.ZS": "agr_emp",
    #                     "SL.IND.EMPL.ZS": "ind_emp",
    #                     "SL.SRV.EMPL.ZS": "srv_emp",
    #                 }
    #             )
    #         )

    # normalizar tipos
    for w in (vab_wide, emp_wide):
        if not w.empty:
            w["year"] = pd.to_numeric(w["year"], errors="coerce").astype("Int64")
            for c in w.columns:
                if c not in {"iso3", "year"}:
                    w[c] = pd.to_numeric(w[c], errors="coerce")

    return {"vab_long": vab_long, "vab_wide": vab_wide, "emp_long": emp_long, "emp_wide": emp_wide}


def _latest_complete_row(wide: pd.DataFrame, cols: list[str]) -> pd.Series | None:
    """Devolve a última linha onde todas as cols existem e têm valor."""
    if wide is None or wide.empty:
        return None

    cols_present = [c for c in cols if c in wide.columns]
    if len(cols_present) < len(cols):
        return None

    g = wide.dropna(subset=cols_present, how="any").sort_values("year")
    if g.empty:
        return None
    return g.iloc[-1]


def _donut_fig(
    labels: list[str],
    values: list[float],
    title: str,
    color_map: dict[str, str] | None = None,
    category_order: list[str] | None = None,
):
    # Donut robusto com ordem fixa usando graph_objects (evita bugs do px + narwhals)
    import plotly.graph_objects as go

    if category_order:
        order_idx = {lab: i for i, lab in enumerate(category_order)}
        pairs = sorted(zip(labels, values), key=lambda p: order_idx.get(p[0], 1e9))
        labels, values = [p[0] for p in pairs], [p[1] for p in pairs]

    if color_map:
        marker_colors = [color_map.get(l, None) for l in labels]
    else:
        marker_colors = None

    fig = go.Figure(
        go.Pie(
            labels=labels,
            values=values,
            hole=0.55,
            sort=False,
            marker=dict(colors=marker_colors) if marker_colors else None,
            textinfo="percent",
            hovertemplate="%{label}: %{value:.2f}%<extra></extra>",
        )
    )
    fig.update_layout(
        title=title,
        showlegend=True,
        legend=dict(orientation="h", y=-0.12),
        margin=dict(l=10, r=10, t=30, b=10),
        height=320,
    )
    return fig


def render_sectors_panel(iso3: str):
    data = _load_sectors_for_iso3(iso3)
    lbl = _sector_label_map()
    palette = _sector_palette(lbl)

    # Ordem fixa global: Agricultura -> Indústria -> Serviços
    order_vab = [lbl["NV.AGR.TOTL.ZS"], lbl["NV.IND.TOTL.ZS"], lbl["NV.SRV.TOTL.ZS"]]
    order_emp = [lbl["SL.AGR.EMPL.ZS"], lbl["SL.IND.EMPL.ZS"], lbl["SL.SRV.EMPL.ZS"]]

    st.markdown(f"### {tr('economics.charts.sectors_latest_title')}")
    c1, c2 = st.columns(2, gap="large")

    # --- VAB (último ano completo) ---
    vab_wide = data.get("vab_wide", pd.DataFrame())
    vab_last = _latest_complete_row(vab_wide, ["agr_vab", "ind_vab", "srv_vab"])
    if vab_last is not None:
        y = int(vab_last["year"])
        vals = [
            float(vab_last["agr_vab"]),
            float(vab_last["ind_vab"]),
            float(vab_last["srv_vab"]),
        ]
        labels = [lbl["NV.AGR.TOTL.ZS"], lbl["NV.IND.TOTL.ZS"], lbl["NV.SRV.TOTL.ZS"]]
        with c1:
            st.caption(tr("economics.presets.sectors_vab") + f" — {y}")
            st.plotly_chart(
                _donut_fig(
                    labels=labels,
                    values=vals,
                    title="",
                    color_map=palette,
                    category_order=order_vab,
                ),
                use_container_width=True,
                config={"displayModeBar": False},
            )

    # --- Emprego (último ano completo) ---
    emp_wide = data.get("emp_wide", pd.DataFrame())
    emp_last = _latest_complete_row(emp_wide, ["agr_emp", "ind_emp", "srv_emp"])
    if emp_last is not None:
        y = int(emp_last["year"])
        vals = [
            float(emp_last["agr_emp"]),
            float(emp_last["ind_emp"]),
            float(emp_last["srv_emp"]),
        ]
        labels = [lbl["SL.AGR.EMPL.ZS"], lbl["SL.IND.EMPL.ZS"], lbl["SL.SRV.EMPL.ZS"]]
        with c2:
            st.caption(tr("economics.presets.sectors_emp") + f" — {y}")
            st.plotly_chart(
                _donut_fig(
                    labels=labels,
                    values=vals,
                    title="",
                    color_map=palette,
                    category_order=order_emp,
                ),
                use_container_width=True,
                config={"displayModeBar": False},
            )

    # --- Série temporal empilhada (toggle VAB/Emprego) ---
    view = st.radio(
        tr("economics.charts.sectors_ts_title"),
        (tr("economics.presets.sectors_vab"), tr("economics.presets.sectors_emp")),
        horizontal=True,
        key=f"sectors_ts_view_{iso3}",
    )

    long = pd.DataFrame()
    domain, rng = [], []

    # Série VAB
    if view == tr("economics.presets.sectors_vab"):
        w = vab_wide
        required = ["year", "agr_vab", "ind_vab", "srv_vab"]
        if not w.empty and all(c in w.columns for c in required):
            w = w.sort_values("year").tail(30).copy()
            long = (
                w.melt(
                    id_vars=["year"],
                    value_vars=["agr_vab", "ind_vab", "srv_vab"],
                    var_name="code",
                    value_name="value",
                )
                .assign(
                    code=lambda d: d["code"].map(
                        {
                            "agr_vab": lbl["NV.AGR.TOTL.ZS"],
                            "ind_vab": lbl["NV.IND.TOTL.ZS"],
                            "srv_vab": lbl["NV.SRV.TOTL.ZS"],
                        }
                    )
                )
            )
            domain = order_vab[:]
            rng = [palette[d] for d in domain]
            long["code"] = pd.Categorical(long["code"], categories=domain, ordered=True)

    # Série Emprego
    elif view == tr("economics.presets.sectors_emp"):
        w = emp_wide
        required = ["year", "agr_emp", "ind_emp", "srv_emp"]
        if not w.empty and all(c in w.columns for c in required):
            w = w.sort_values("year").tail(30).copy()
            long = (
                w.melt(
                    id_vars=["year"],
                    value_vars=["agr_emp", "ind_emp", "srv_emp"],
                    var_name="code",
                    value_name="value",
                )
                .assign(
                    code=lambda d: d["code"].map(
                        {
                            "agr_emp": lbl["SL.AGR.EMPL.ZS"],
                            "ind_emp": lbl["SL.IND.EMPL.ZS"],
                            "srv_emp": lbl["SL.SRV.EMPL.ZS"],
                        }
                    )
                )
            )
            domain = order_emp[:]
            rng = [palette[d] for d in domain]
            long["code"] = pd.Categorical(long["code"], categories=domain, ordered=True)

    # Desenhar ou mostrar mensagem
    if not long.empty:
        ch = (
            alt.Chart(long)
            .mark_area(opacity=0.85)
            .encode(
                x=alt.X(
                    "year:Q",
                    axis=alt.Axis(format="d", title=tr("economics.metrics.year")),
                ),
                y=alt.Y(
                    "value:Q",
                    stack="normalize",
                    axis=alt.Axis(format=".0%", title=None),
                ),
                color=alt.Color(
                    "code:N",
                    title="",
                    legend=alt.Legend(orient="bottom"),
                    scale=alt.Scale(domain=domain, range=rng),
                ),
                tooltip=[
                    alt.Tooltip("code:N", title=tr("paises.indicador")),
                    alt.Tooltip("year:Q", title=tr("economics.metrics.year"), format="d"),
                    alt.Tooltip("value:Q", title=tr("paises.valor"), format=".2f"),
                ],
            )
            .properties(height=300, width="container")
        )
        st.altair_chart(ch, use_container_width=True)
    else:
        st.caption(
            tr(
                "labels.sem_s_rie_temporal_para_os_indicadores_selecionados"
            )
        )



# ==========================================================
# Catálogos traduzidos
# ==========================================================

def _catalog_i18n() -> dict[str, dict[str, str]]:
    """Mapeia indicadores para labels traduzidos."""
    return {
        # --- Core existentes ---
        "NY.GDP.MKTP.CD":    {"short": tr("economics.metrics.gdp"),          "label": tr("economics.metrics.gdp")},
        "NY.GDP.MKTP.KD":    {"short": tr("economics.metrics.gdp_const"),     "label": tr("economics.metrics.gdp_const")},
        "NY.GDP.MKTP.KD.ZG": {"short": tr("economics.metrics.gdp_growth"),    "label": tr("economics.metrics.gdp_growth")},
        "NY.GDP.PCAP.CD":    {"short": tr("economics.metrics.gdp_pc"),        "label": tr("economics.metrics.gdp_pc")},
        "NY.GDP.PCAP.KD.ZG": {"short": tr("economics.metrics.gdp_pc_growth"), "label": tr("economics.metrics.gdp_pc_growth")},
        "SI.POV.DDAY":       {"short": tr("economics.metrics.poverty_215"),   "label": tr("economics.metrics.poverty_215")},
        "SI.POV.LMIC":       {"short": tr("economics.metrics.poverty_365"),   "label": tr("economics.metrics.poverty_365")},
        "SI.POV.UMIC":       {"short": tr("economics.metrics.poverty_685"),   "label": tr("economics.metrics.poverty_685")},
        "SI.POV.GINI":       {"short": tr("economics.metrics.gini"),          "label": tr("economics.metrics.gini")},

        # --- Novos: Setores (VAB % PIB) ---
        "NV.AGR.TOTL.ZS":    {"short": tr("economics.ind.agri_vab"),          "label": tr("economics.ind.agri_vab")},
        "NV.IND.TOTL.ZS":    {"short": tr("economics.ind.ind_vab"),           "label": tr("economics.ind.ind_vab")},
        "NV.SRV.TOTL.ZS":    {"short": tr("economics.ind.srv_vab"),           "label": tr("economics.ind.srv_vab")},

        # --- Novos: Setores (Emprego % total) ---
        "SL.AGR.EMPL.ZS":    {"short": tr("economics.ind.agri_emp"),          "label": tr("economics.ind.agri_emp")},
        "SL.IND.EMPL.ZS":    {"short": tr("economics.ind.ind_emp"),           "label": tr("economics.ind.ind_emp")},
        "SL.SRV.EMPL.ZS":    {"short": tr("economics.ind.srv_emp"),           "label": tr("economics.ind.srv_emp")},
    }


def _presets_i18n() -> dict[str, list[str]]:
    """Presets traduzidos."""
    return {
        tr("economics.presets.core4"):              ["NY.GDP.MKTP.KD.ZG","NY.GDP.MKTP.CD","SI.POV.DDAY","NY.GDP.PCAP.CD"],
        tr("economics.presets.growth_income"):      ["NY.GDP.MKTP.KD.ZG","NY.GDP.MKTP.CD","NY.GDP.PCAP.CD","NY.GDP.PCAP.KD.ZG"],
        tr("economics.presets.poverty_inequality"): ["SI.POV.DDAY","SI.POV.LMIC","SI.POV.UMIC","SI.POV.GINI"],

        # --- Novos presets setoriais ---
        tr("economics.presets.sectors_vab"):        ["NV.AGR.TOTL.ZS","NV.IND.TOTL.ZS","NV.SRV.TOTL.ZS"],
        tr("economics.presets.sectors_emp"):        ["SL.AGR.EMPL.ZS","SL.IND.EMPL.ZS","SL.SRV.EMPL.ZS"],
    }


# ==========================================================
# Painel principal
# ==========================================================

def render_wdi_panel(iso3: str, country_name: str | None = None) -> None:
    _ensure_lang_state()

    IND = _catalog_i18n()
    PRESETS = _presets_i18n()

    display_name = country_display_name(iso3, country_name)

    st.subheader(f"{tr('economics.header')} — {display_name}")

    # --- seleção de preset e indicadores ---
    preset = st.selectbox(
        tr("economics.preset_label"),
        options=list(PRESETS.keys()),
        index=0,
        key=f"preset_{iso3}"
    )

    short_options  = [IND[k]["short"] for k in IND.keys()]
    code_by_short  = {IND[k]["short"]: k for k in IND.keys()}
    default_shorts = [IND[c]["short"] for c in PRESETS[preset]]

    selected_shorts = st.multiselect(
        "",
        options=short_options,
        default=default_shorts,
        key=f"indicators_{iso3}"
    )
    codes = [code_by_short[s] for s in selected_shorts] or PRESETS[preset]

    # --- slider de anos ---
    years = st.slider(
        tr("economics.years_label"),
        min_value=1960,
        max_value=2024,
        value=(2000, 2024),
        key=f"years_{iso3}"
    )
    year_min, year_max = years

    # --- carregar dados ---    
    with st.spinner(tr("economics.loading")):
        df, labels_map = fetch_wdi_dataset(iso3, codes, year_min, year_max, IND)

    # --- gráficos 2x2 + tabela (só se houver dados macro) ---
    if df.empty:
        st.info(tr("economics.table_no_data"))
    else:
        render_wdi_charts_2x2(df, codes, labels_map)

        # --- tabela ---
        df_tbl = df.copy()
        df_tbl["disp_label"] = df_tbl["orig_code"].map(labels_map)
        wide = (
            df_tbl.pivot_table(
                index="year", columns="disp_label", values="value", aggfunc="last"
            ).sort_index()
        )
        disp = wide.copy()

        def fmt(col, v):
            if pd.isna(v):
                return "–"
            return (
                f"{float(v):,.2f}".replace(",", " ")
                if "%" in (col or "")
                else f"{float(v):,.0f}".replace(",", " ")
            )

        for col in disp.columns:
            disp[col] = disp[col].apply(lambda x, c=col: fmt(c, x))

        disp.index = disp.index.map(lambda y: str(int(y)))
        out = disp.reset_index()
        out = out.rename(
            columns={
                "year": tr("economics.metrics.year"),
                "index": tr("economics.metrics.year"),
            }
        )

        st.subheader(tr("economics.table_title"))
        st.dataframe(out, use_container_width=True)

    # --- painel setorial com paleta + ordem coerentes (SEMPRE) ---
    render_sectors_panel(iso3)


    #render_wdi_charts_2x2(df, codes, labels_map)

    # --- DEBUG opcional: ver dados brutos do primeiro indicador selecionado ---
    # if codes:
    #     debug_code = codes[0]
    #     with st.expander(f"DEBUG – dados para {debug_code}"):
    #         sub = df[df["code"] == debug_code].sort_values("year")
    #         st.dataframe(sub, use_container_width=True)

    # # --- tabela ---
    # df_tbl = df.copy()
    # df_tbl["disp_label"] = df_tbl["orig_code"].map(labels_map)
    # wide = (
    #     df_tbl.pivot_table(index="year", columns="disp_label", values="value", aggfunc="last")
    #     .sort_index()
    # )
    # disp = wide.copy()

    # def fmt(col, v):
    #     if pd.isna(v):
    #         return "–"
    #     return f"{float(v):,.2f}".replace(",", " ") if "%" in (col or "") else f"{float(v):,.0f}".replace(",", " ")

    # for col in disp.columns:
    #     disp[col] = disp[col].apply(lambda x, c=col: fmt(c, x))

    # disp.index = disp.index.map(lambda y: str(int(y)))
    # out = disp.reset_index()
    # out = out.rename(columns={"year": tr("economics.metrics.year"), "index": tr("economics.metrics.year")})

    # st.subheader(tr("economics.table_title"))
    # st.dataframe(out, use_container_width=True)

    # # --- painel setorial com paleta + ordem coerentes ---
    # #render_sectors_panel(iso3)
    # _render_pib_sectors_table(iso3, year_min, year_max)
