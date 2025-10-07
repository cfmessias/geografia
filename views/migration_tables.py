# views/migration_tables.py
from __future__ import annotations
from pathlib import Path
import pandas as pd
import streamlit as st

# i18n (fallback simples se não houver services.i18n)
try:
    from services.i18n import t as tr
except Exception:  # pragma: no cover
    def tr(key: str, **kw):
        return {
            "paises.migracoes_stocks_title": "Migrações — stocks (UN DESA)",
            "paises.fonte_un_desa": "Fonte: UN DESA — International migrant stock (por sexo)",
            "paises.mulheres_origem": "Mulheres — Origem dos imigrantes",
            "paises.mulheres_destino": "Mulheres — Destino dos emigrantes",
            "paises.homens_origem": "Homens — Origem dos imigrantes",
            "paises.homens_destino": "Homens — Destino dos emigrantes",
            "paises.ficheiros_em_falta": "Ficheiros em falta",
            "paises.sem_m49_para_iso3": "Sem mapeamento m49 para ISO3={iso3}.",
        }.get(key, key).format(**kw)

# ---------- HELPERS (únicas, a nível de módulo) ----------
def _project_root() -> Path:
    """Descobre a raiz do projeto (onde existe /data)."""
    here = Path(__file__).resolve()
    for p in [here, *here.parents]:
        if (p / "data").exists():
            return p
    return Path.cwd()

def _fmt_int(n: int) -> str:
    return f"{int(n):,}".replace(",", " ")

def _find_col(cols, needle: str) -> str:
    """Encontra coluna que contém 'needle' (lida com cabeçalhos duplicados tipo '.1')."""
    if needle in cols:
        return needle
    nlow = needle.lower()
    for c in cols:
        if nlow in str(c).lower():
            return c
    raise KeyError(needle)

def _load_stock_csv(path: Path, year: int) -> pd.DataFrame:
    """Lê CSV UN DESA (sem separadores nos números) e normaliza colunas."""
    df = pd.read_csv(path, sep=";", dtype="string")
    dest_name = _find_col(df.columns, "Region, development group, country or area of destination")
    dest_m49  = _find_col(df.columns, "Location code of destination")
    orig_name = _find_col(df.columns, "Region, development group, country or area of origin")
    orig_m49  = _find_col(df.columns, "Location code of origin")
    year_col  = _find_col(df.columns, str(year))

    df = df[[dest_name, dest_m49, orig_name, orig_m49, year_col]].copy()
    df.columns = ["dest_name", "dest_m49", "orig_name", "orig_m49", "value"]

    df["dest_m49"] = pd.to_numeric(df["dest_m49"], errors="coerce").astype("Int64")
    df["orig_m49"] = pd.to_numeric(df["orig_m49"], errors="coerce").astype("Int64")
    df["value"]    = pd.to_numeric(df["value"],    errors="coerce").fillna(0).astype("int64")
    return df

def _load_seed_labels(base_dir: Path, lang: str = "pt") -> tuple[dict[str, str], str]:
    """
    Lê data/countries_seed.csv (ou data/country_seed.csv) e devolve:
      labels_map (ISO3 -> nome PT/EN) e cabeçalho da coluna (“País”/“Country”).
    """
    data_dir = base_dir / "data"
    candidates = [data_dir / "countries_seed.csv", data_dir / "country_seed.csv"]
    csv_path = next((p for p in candidates if p.exists()), None)
    labels_map: dict[str, str] = {}

    if csv_path:
        for sep in (",", ";"):
            try:
                df = pd.read_csv(csv_path, sep=sep, dtype="string")
                if {"iso3", "name_pt", "name_en"}.issubset(df.columns):
                    df["iso3"] = df["iso3"].str.upper().str.strip()
                    pick = "name_pt" if lang == "pt" else "name_en"
                    labels_map = dict(zip(df["iso3"], df[pick].fillna(df["iso3"]).astype(str)))
                    break
            except Exception:
                pass

    country_col = "País" if lang == "pt" else "Country"
    return labels_map, country_col

def _iso_to_label(iso3: str | None, labels_map: dict[str, str], fallback_name: str | None) -> str:
    if not iso3:
        return fallback_name or "—"
    iso3u = str(iso3).upper()
    return labels_map.get(iso3u, fallback_name or iso3u)

# ---------- PÚBLICO ----------
def render_country_migration_tables(
    iso3: str,
    *,
    year: int = 2024,
    top: int = 20,
    height: int = 360,
    only_countries: bool = True,
    base_dir: str | Path | None = None,
    wrap_expander: bool = True,
) -> None:
    """
    Mostra 4 tabelas: Mulheres/Homens × (Origem dos imigrantes | Destino dos emigrantes).
    Join por m49 (data/un_m49_iso.csv). Sem coluna ISO3; nomes PT/EN via countries_seed.csv.
    """
    base = Path(base_dir) if base_dir else _project_root()
    data_dir = base / "data"
    raw_dir  = data_dir / "raw"

    map_path  = data_dir / "un_m49_iso.csv"
    fem_path  = raw_dir / "2024_ims_stock_female_destination_and_origin_sem_sep.csv"
    male_path = raw_dir / "2024_ims_stock_male_destination_and_origin_sem_sep.csv"

    missing = [p for p in (map_path, fem_path, male_path) if not p.exists()]
    if missing:
        st.warning(tr("paises.ficheiros_em_falta") + ":\n" + "\n".join(f"• {p}" for p in missing))
        return

    lang = st.session_state.get("lang", "pt")
    seed_labels_map, country_col_header = _load_seed_labels(base, lang=lang)

    # mapa m49 ↔ iso3 ↔ nome fallback (UN DESA)
    map_df = pd.read_csv(map_path, sep=";", dtype={"m49": "Int64", "iso3": "string"})
    map_df["iso3"] = map_df["iso3"].str.upper().str.strip()
    map_df["name"] = map_df["Country or Area"].astype(str).str.strip()
    map_df = map_df[["m49", "iso3", "name"]].dropna(subset=["m49"]).copy()

    sel_row = map_df[map_df["iso3"] == iso3.upper()]
    if sel_row.empty:
        st.error(tr("paises.sem_m49_para_iso3", iso3=iso3))
        return
    sel_m49 = int(sel_row["m49"].iloc[0])

    fem  = _load_stock_csv(fem_path, year)
    male = _load_stock_csv(male_path, year)

    def _top_origins(df: pd.DataFrame, m49: int) -> pd.DataFrame:
        sub = df[df["dest_m49"] == m49].groupby("orig_m49", as_index=False)["value"].sum()
        sub = sub.merge(map_df, left_on="orig_m49", right_on="m49", how="left")
        if only_countries:
            sub = sub[sub["iso3"].notna()]
        sub["__label"] = sub.apply(lambda r: _iso_to_label(r.get("iso3"), seed_labels_map, r.get("name")), axis=1)
        out = sub.rename(columns={"value": f"Stock {year}"}).loc[:, ["__label", f"Stock {year}"]]
        out = out.sort_values(f"Stock {year}", ascending=False).head(top).reset_index(drop=True)
        out[f"Stock {year}"] = out[f"Stock {year}"].map(_fmt_int)
        out.rename(columns={"__label": country_col_header}, inplace=True)
        return out

    def _top_destinations(df: pd.DataFrame, m49: int) -> pd.DataFrame:
        sub = df[df["orig_m49"] == m49].groupby("dest_m49", as_index=False)["value"].sum()
        sub = sub.merge(map_df, left_on="dest_m49", right_on="m49", how="left")
        if only_countries:
            sub = sub[sub["iso3"].notna()]
        sub["__label"] = sub.apply(lambda r: _iso_to_label(r.get("iso3"), seed_labels_map, r.get("name")), axis=1)
        out = sub.rename(columns={"value": f"Stock {year}"}).loc[:, ["__label", f"Stock {year}"]]
        out = out.sort_values(f"Stock {year}", ascending=False).head(top).reset_index(drop=True)
        out[f"Stock {year}"] = out[f"Stock {year}"].map(_fmt_int)
        out.rename(columns={"__label": country_col_header}, inplace=True)
        return out

    fem_orig  = _top_origins(fem,  sel_m49)
    fem_dest  = _top_destinations(fem,  sel_m49)
    male_orig = _top_origins(male, sel_m49)
    male_dest = _top_destinations(male, sel_m49)

    container = st.expander(tr("paises.migracoes_stocks_title"), expanded=False) if wrap_expander else st.container()
    with container:
        st.caption(f"{tr('paises.fonte_un_desa')} · {year}")
        cfg = {country_col_header: st.column_config.TextColumn(width="large")}

        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"**{tr('paises.mulheres_origem')}**")
            st.dataframe(fem_orig, hide_index=True, use_container_width=True, height=height, column_config=cfg)
        with c2:
            st.markdown(f"**{tr('paises.mulheres_destino')}**")
            st.dataframe(fem_dest, hide_index=True, use_container_width=True, height=height, column_config=cfg)

        c3, c4 = st.columns(2)
        with c3:
            st.markdown(f"**{tr('paises.homens_origem')}**")
            st.dataframe(male_orig, hide_index=True, use_container_width=True, height=height, column_config=cfg)
        with c4:
            st.markdown(f"**{tr('paises.homens_destino')}**")
            st.dataframe(male_dest, hide_index=True, use_container_width=True, height=height, column_config=cfg)
