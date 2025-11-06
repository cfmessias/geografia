# services/geo_store.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
from functools import lru_cache
import re
import pandas as pd

# Caminhos base
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR     = PROJECT_ROOT / "data"
print (f"[geo_store] 📂 DATA_DIR = {DATA_DIR}")
# ---------------------------------------------------------------------
# Utilitários locais (independentes para evitar ciclo de importações)
# ---------------------------------------------------------------------
def _read_csv_safe(path: str | Path, expected_cols: list[str] | None = None) -> pd.DataFrame:
    """
    Lê CSV de forma resiliente:
    - tenta primeiro com sep=";"
    - fallback para ',' e autodetecção (sep=None, engine="python")
    - garante colunas esperadas e ordem estável
    """
    p = Path(path)
    if not p.exists():
        return pd.DataFrame(columns=list(expected_cols) if expected_cols else [])

    df: pd.DataFrame | None = None
    for sep in (";", ",", None):
        try:
            if sep is None:
                df = pd.read_csv(p, sep=None, engine="python", dtype=str, encoding="utf-8", keep_default_na=False)
            else:
                df = pd.read_csv(p, sep=sep, dtype=str, encoding="utf-8", keep_default_na=False)
            # se ficou tudo numa coluna, tentar a próxima hipótese
            if df.shape[1] == 1 and sep is not None:
                continue
            break
        except Exception:
            df = None
    if df is None:
        return pd.DataFrame(columns=list(expected_cols) if expected_cols else [])

    if expected_cols:
        for c in expected_cols:
            if c not in df.columns:
                df[c] = ""
        extras = [c for c in df.columns if c not in expected_cols]
        df = df[[*expected_cols, *extras]]

    return df

def _to_number(s: str | None) -> float | None:
    if s is None or str(s).strip() == "":
        return None
    txt = str(s)
    num = re.sub(r"[^0-9,.\-]+", "", txt)
    if num.count(",") == 1 and num.count(".") == 0:
        num = num.replace(",", ".")
    try:
        return float(num)
    except Exception:
        return None

# ---------------------------------------------------------------------
# Ficheiros-alvo
# ---------------------------------------------------------------------
BORDERS_CSV          = DATA_DIR / "borders.csv"
TIMEZONES_CSV        = DATA_DIR / "timezones.csv"
GEOGRAFIA_PAISES_CSV = DATA_DIR / "geografia_paises.csv"
KOPPEN_CSV           = DATA_DIR / "koppen.csv"          # opcional
BIOMES_CSV           = DATA_DIR / "biomes.csv"          # opcional
COASTLINES_CSV       = DATA_DIR / "coastlines.csv"      # opcional
PORTS_ROUTES_CSV     = DATA_DIR / "ports_and_routes.csv"# opcional
COUNTRIES_PROFILES   = DATA_DIR / "countries_profiles.csv"
# ── Rivers (Natural Earth + enriquecimento Wikidata) ────────────────────────────
RIVERS_ENR_CSV = DATA_DIR / "rivers_enriched.csv"
RIVERS_BASE_CSV = DATA_DIR / "rivers.csv"

# ── Rivers (Natural Earth + enriquecimento Wikidata) ────────────────────────────
@lru_cache(maxsize=1)
def load_rivers(path_enriched: str | None = None, path_base: str | None = None) -> pd.DataFrame:
    """
    Lê data/rivers_enriched.csv (se existir) ou, em fallback, data/rivers.csv.
    Normaliza colunas e calcula 'length_best_km' com prioridade a length_wd.
    """
    p_enr  = Path(path_enriched) if path_enriched else RIVERS_ENR_CSV
    p_base = Path(path_base)     if path_base     else RIVERS_BASE_CSV

    # CSVs deste projeto usam ';'
    expected_base = ["iso3","river_name","length_km","scalerank","featurecla","source"]
    expected_enr  = expected_base + ["source_label","source_qid","mouth_label","mouth_qid","basin_label","basin_qid","length_wd"]

    if p_enr.exists():
        df = _read_csv_safe(p_enr, expected_cols=expected_enr)
    else:
        df = _read_csv_safe(p_base, expected_cols=expected_base)
        # preencher colunas enriquecidas vazias para compatibilidade
        for c in ["source_label","source_qid","mouth_label","mouth_qid","basin_label","basin_qid","length_wd"]:
            if c not in df.columns:
                df[c] = pd.NA

    if df.empty:
        return df

    # Normalizações
    for c in ("iso3","river_name","featurecla","source","source_label","mouth_label","basin_label"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    df["iso3"] = df["iso3"].astype(str).str.upper()

    # Comprimento “best”: WD se houver, senão length_km
    def _to_num(x):
        try:
            return float(str(x).replace(",", "."))
        except Exception:
            return float("nan")

    wd = df.get("length_wd")
    km = df.get("length_km")
    wd_num = wd.map(_to_num) if wd is not None else pd.Series([float("nan")]*len(df))
    km_num = km.map(_to_num) if km is not None else pd.Series([float("nan")]*len(df))
    df["length_best_km"] = wd_num.fillna(km_num)

    return df


def rivers_for_iso3(iso3: str, top_n: int = 12, min_km: float = 50.0) -> pd.DataFrame:
    """
    Top N rios por ISO3, ordenados por comprimento desc., filtrando < min_km.
    Devolve colunas prontas para UI.
    """
    iso3u = (iso3 or "").upper().strip()
    df = load_rivers()
    if df.empty:
        return pd.DataFrame(columns=["river_name","length_km","source_label","mouth_label","basin_label","scalerank","featurecla"])

    sub = df[df["iso3"] == iso3u].copy()
    if sub.empty:
        return pd.DataFrame(columns=["river_name","length_km","source_label","mouth_label","basin_label","scalerank","featurecla"])

    sub = sub[sub["length_best_km"].fillna(-1) >= float(min_km)]
    sub = sub.sort_values(["length_best_km","scalerank"], ascending=[False, True]).head(int(top_n)).copy()
    sub["length_km"] = sub["length_best_km"].round(0).astype("Int64")

    keep = ["river_name","length_km","source_label","mouth_label","basin_label","scalerank","featurecla"]
    for c in keep:
        if c not in sub.columns:
            sub[c] = pd.NA
    return sub[keep]
# ---------------------------------------------------------------------
# Borders
# ---------------------------------------------------------------------
@lru_cache(maxsize=1)
def load_borders(path: str | None = None) -> pd.DataFrame:
    p = Path(path) if path else BORDERS_CSV
    df = _read_csv_safe(p, expected_cols=[
        "country_iso3","country_qid","country",
        "neighbor_qid","neighbor_name",
        "neighbor_country_qid","neighbor_country_name","neighbor_iso3",
        "land_km"
    ])
    if df.empty:
        return df
    df["country_iso3"] = df["country_iso3"].astype(str).str.upper()
    if "neighbor_iso3" in df.columns:
        df["neighbor_iso3"] = df["neighbor_iso3"].astype(str).str.upper()
    df["land_km_num"] = df.get("land_km", pd.Series(dtype=str)).map(_to_number)
    return df

def borders_for_iso3(iso3: str) -> pd.DataFrame:
    iso = (iso3 or "").upper().strip()
    borders = load_borders()
    if borders.empty:
        return pd.DataFrame(columns=["neighbor_iso3","neighbor_name","land_km"])

    profiles = _read_csv_safe(COUNTRIES_PROFILES)
    name_col = next((c for c in ["country","Country","nome","Nome","country_name"] if c in profiles.columns), None) if not profiles.empty else None
    name_map = {}
    if name_col:
        for _, r in profiles.iterrows():
            i = str(r.get("iso3","")).strip().upper()
            nm = str(r.get(name_col,"")).strip()
            if i and nm:
                name_map[i] = nm

    sub = borders[borders["country_iso3"].astype(str).str.upper() == iso].copy()
    if "neighbor_name" not in sub.columns:
        sub["neighbor_name"] = ""
    sub["neighbor_name"] = sub["neighbor_iso3"].map(name_map).fillna(sub["neighbor_name"]).replace({"None": ""}).fillna("")
    mask = sub["neighbor_name"].eq("")
    sub.loc[mask, "neighbor_name"] = sub.loc[mask, "neighbor_iso3"]
    return sub.reindex(columns=["neighbor_iso3","neighbor_name","land_km"], fill_value="").sort_values("neighbor_iso3").reset_index(drop=True)

# ---------------------------------------------------------------------
# Timezones
# ---------------------------------------------------------------------
@lru_cache(maxsize=1)
def load_timezones_new(path: str | None = None) -> pd.DataFrame:
    """
    Lê data/timezones.csv (sep=';') e normaliza nomes:
    - aceita 'iso3' → renomeia para 'country_iso3'
    - aceita 'qid'  → renomeia para 'country_qid'
    Mantém colunas úteis: timezones, tz_with_offsets_now, utc_offsets_now, tz_count.
    """
    p = Path(path) if path else TIMEZONES_CSV
    df = _read_csv_safe(p)  # sem expected_cols para aceitar ambos os esquemas

    if df.empty:
        return df

    # normalizar nomes
    rename_map = {}
    if "iso3" in df.columns and "country_iso3" not in df.columns:
        rename_map["iso3"] = "country_iso3"
    if "qid" in df.columns and "country_qid" not in df.columns:
        rename_map["qid"] = "country_qid"
    if rename_map:
        df = df.rename(columns=rename_map)

    # upper em ISO
    if "country_iso3" in df.columns:
        df["country_iso3"] = df["country_iso3"].astype(str).str.upper()
    if "iso2" in df.columns and "country_iso2" not in df.columns:
        df = df.rename(columns={"iso2": "country_iso2"})
    if "country_iso2" in df.columns:
        df["country_iso2"] = df["country_iso2"].astype(str).str.upper()

    return df



# (alias mantido para retro-compat se alguém importava load_timezones)
def load_timezones(path: str | None = None) -> pd.DataFrame:
    return load_timezones_new(path)

def timezones_for_iso3(iso3: str) -> pd.DataFrame:
    """
    Devolve offsets atuais por país como coluna 'tz_label',
    usando SEMPRE 'utc_offsets_now' do CSV (ex.: 'UTC+01:00, UTC-01:00').
    """
    iso = (iso3 or "").upper().strip()
    df = load_timezones_new()
    if df.empty or "country_iso3" not in df.columns:
        return pd.DataFrame(columns=["tz_label"])

    sub = df[df["country_iso3"] == iso].copy()
    if sub.empty:
        return pd.DataFrame(columns=["tz_label"])

    # extrair offsets; aceitar vírgula, ponto-e-vírgula ou pipe como separador
    col = "utc_offsets_now"
    if col in sub.columns:
        vals = (
            sub[col].astype(str)
            .str.replace("−", "-", regex=False)     # normalizar sinal unicode
            .str.split(r"[|;,]", regex=True)
            .explode()
            .map(str.strip)
        )
        vals = vals[vals != ""]
        # garantir prefixo 'UTC'
        def _fmt(v: str) -> str:
            v = v.upper()
            return v if v.startswith("UTC") else ("UTC" + (v if v.startswith(("+","-")) else f"+{v}"))
        uniq = sorted({_fmt(v) for v in vals})
        if uniq:
            return pd.DataFrame({"tz_label": uniq})

    # fallback: se não houver 'utc_offsets_now', tenta os IDs IANA sem offset
    for alt in ("tz_with_offsets_now", "timezones"):
        if alt in sub.columns:
            vals = (
                sub[alt].astype(str)
                .str.replace(r"\s*\([^)]*\)", "", regex=True)  # remover "(UTC…)" se existir
                .str.split(r"[|;,]", regex=True)
                .explode()
                .map(str.strip)
            )
            vals = vals[vals != ""]
            if not vals.empty:
                return pd.DataFrame({"tz_label": sorted(vals.unique().tolist())})

    return pd.DataFrame(columns=["tz_label"])


# ---------------------------------------------------------------------
# Resumo geografia: capital/estações/contagens
# ---------------------------------------------------------------------
@lru_cache(maxsize=1)
def load_geografia_paises(path: str | None = None) -> pd.DataFrame:
    p = Path(path) if path else GEOGRAFIA_PAISES_CSV
    df = _read_csv_safe(p, expected_cols=[
        "iso3","country","neighbors_iso3","neighbors_count","border_km_total",
        "timezones","timezones_count","capital_lat","capital_lon","seasons_estimate"
    ])
    if df.empty:
        return df
    df["iso3"] = df["iso3"].astype(str).str.upper()
    for c in ("neighbors_count","timezones_count","seasons_estimate"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    for c in ("capital_lat","capital_lon"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def geografia_for_iso3(iso3: str) -> pd.Series | None:
    df = load_geografia_paises()
    if df.empty:
        return None
    row = df[df["iso3"] == str(iso3).upper()].copy()
    return None if row.empty else row.iloc[0]

# ---------------------------------------------------------------------
# Köppen & Biomas (opcionais)
# ---------------------------------------------------------------------
@lru_cache(maxsize=1)
def load_koppen(path: str | None = None) -> pd.DataFrame:
    p = Path(path) if path else KOPPEN_CSV
    if not p.exists():
        return pd.DataFrame()
    df = _read_csv_safe(p)
    if df.empty:
        return df
    if "country_iso3" not in df.columns:
        for cand in ("iso3","ISO3","pais","country"):
            if cand in df.columns:
                df = df.rename(columns={cand: "country_iso3"})
                break
    df["country_iso3"] = df.get("country_iso3", pd.Series(dtype=str)).astype(str).str.upper()
    return df

def koppen_for_iso3(iso3: str) -> pd.DataFrame:
    df = load_koppen()
    if df.empty:
        return df
    iso = str(iso3).upper()
    sub = df[df.get("country_iso3","") == iso].copy()
    keep = [c for c in sub.columns if c.lower() in {"country_iso3","class","share_pct","koppen"}]
    return sub[keep] if keep else sub

@lru_cache(maxsize=1)
def load_biomes(path: str | None = None) -> pd.DataFrame:
    p = Path(path) if path else BIOMES_CSV
    if not p.exists():
        return pd.DataFrame()
    df = _read_csv_safe(p)
    if df.empty:
        return df
    if "country_iso3" not in df.columns:
        for cand in ("iso3","ISO3","pais","country"):
            if cand in df.columns:
                df = df.rename(columns={cand: "country_iso3"})
                break
    df["country_iso3"] = df.get("country_iso3", pd.Series(dtype=str)).astype(str).str.upper()
    return df

def biomes_for_iso3(iso3: str) -> pd.DataFrame:
    df = load_biomes()
    if df.empty:
        return df
    iso = str(iso3).upper()
    sub = df[df.get("country_iso3","") == iso].copy()
    keep = [c for c in sub.columns if c.lower() in {"country_iso3","biome","share_pct"}]
    return sub[keep] if keep else sub

# ---------------------------------------------------------------------
# Coastlines / Ports & Routes (opcionais)
# ---------------------------------------------------------------------
@lru_cache(maxsize=1)
def load_coastlines(path: str | None = None) -> pd.DataFrame:
    """
    Lê data/coastlines.csv (sep=';') aceitando esquemas:
    - country_iso3 / country_iso2 / country_qid
    - iso3 / qid
    Normaliza para a coluna 'iso3' e garante:
      has_coast, coast_km, adjacent_seas_pt, adjacent_seas_en
    """
    p = Path(path) if path else COASTLINES_CSV
    df = _read_csv_safe(p)  # não forçar expected_cols para aceitar ambas variantes
    if df.empty:
        return df

    # Normalizar nomes → trabalhar sempre com 'iso3' e 'qid'
    if "iso3" not in df.columns:
        if "country_iso3" in df.columns:
            df = df.rename(columns={"country_iso3": "iso3"})
        elif "ISO3" in df.columns:
            df = df.rename(columns={"ISO3": "iso3"})
    if "qid" not in df.columns and "country_qid" in df.columns:
        df = df.rename(columns={"country_qid": "qid"})

    # Garantir colunas esperadas
    for c in ("has_coast", "coast_km", "adjacent_seas_pt", "adjacent_seas_en"):
        if c not in df.columns:
            df[c] = ""

    # Normalizações
    df["iso3"] = df["iso3"].astype(str).str.upper()
    # coast_km numérico (se vier vazio fica NaN)
    df["coast_km"] = pd.to_numeric(df["coast_km"], errors="coerce")

    # Normalizar 'has_coast' para Sim/Não (ou True/False se preferires)
    df["has_coast"] = (
        df["has_coast"].astype(str).str.strip().str.lower()
        .map({"sim": "Sim", "yes": "Sim", "true": "Sim", "não": "Não", "nao": "Não", "no": "Não", "false": "Não"})
        .fillna(df["has_coast"])
    )

    return df


def coastlines_for_iso3(iso3: str) -> pd.DataFrame:
    iso3 = (iso3 or "").upper().strip()
    df = load_coastlines()
    if df.empty or "iso3" not in df.columns:
        return pd.DataFrame(columns=["has_coast","coast_km","adjacent_seas_pt","adjacent_seas_en"])
    sub = df[df["iso3"] == iso3].copy()
    return sub.reindex(columns=["has_coast","coast_km","adjacent_seas_pt","adjacent_seas_en"])

@lru_cache(maxsize=1)
def load_ports_and_routes(path: str | None = None) -> pd.DataFrame:
    p = Path(path) if path else PORTS_ROUTES_CSV
    df = _read_csv_safe(p, expected_cols=[
        "iso3","qid","country_pt","country_en",
        "ports_pt","ports_en","waters_pt","waters_en",
        "has_ports","has_routes"
    ])
    if df.empty:
        return df
    df["iso3"] = df["iso3"].astype(str).str.upper()
    return df

def ports_and_routes_for_iso3(iso3: str) -> pd.DataFrame:
    iso = (iso3 or "").upper().strip()
    df = load_ports_and_routes()
    if df.empty:
        return pd.DataFrame(columns=["ports_pt","ports_en","waters_pt","waters_en"])
    sub = df[df["iso3"] == iso].copy()
    return sub.reindex(columns=["ports_pt","ports_en","waters_pt","waters_en"])

def ports_routes_for_iso3(iso3: str) -> pd.DataFrame:
    return ports_and_routes_for_iso3(iso3)

_lakes_cache   : pd.DataFrame | None = None
_reliefs_cache : pd.DataFrame | None = None

def _read_semicolon(path: Path) -> pd.DataFrame:
    if not path.exists(): return pd.DataFrame()
    return pd.read_csv(path, sep=";", dtype=str, keep_default_na=False, encoding="utf-8")

def load_lakes_all() -> pd.DataFrame:
    global _lakes_cache
    if _lakes_cache is None:
        _lakes_cache = _read_semicolon(DATA_DIR / "lakes.csv")
        if not _lakes_cache.empty:
            _lakes_cache["iso3"] = _lakes_cache["iso3"].str.upper().str.strip()
    return _lakes_cache.copy() if _lakes_cache is not None else pd.DataFrame()

def lakes_for_iso3(iso3: str, *, min_area_km2: float = 0.0, top_n: int | None = 20) -> pd.DataFrame:
    df = load_lakes_all()
    if df.empty: return df
    iso = str(iso3).upper()
    if "area_km2" in df.columns:
        df["__area"] = pd.to_numeric(df["area_km2"], errors="coerce")
    else:
        df["__area"] = None
    out = df[df["iso3"] == iso]
    if min_area_km2 > 0:
        out = out[out["__area"].fillna(0) >= float(min_area_km2)]
    out = out.sort_values(["__area","lake_label"], ascending=[False, True]).drop(columns="__area")
    return out.head(top_n) if top_n else out

def load_reliefs_all() -> pd.DataFrame:
    global _reliefs_cache
    if _reliefs_cache is None:
        _reliefs_cache = _read_semicolon(DATA_DIR / "reliefs.csv")
        if not _reliefs_cache.empty:
            _reliefs_cache["iso3"] = _reliefs_cache["iso3"].str.upper().str.strip()
    print (_reliefs_cache)
    return _reliefs_cache.copy() if _reliefs_cache is not None else pd.DataFrame()

def reliefs_for_iso3(iso3: str, *, kinds: list[str] | None = None, top_n: int | None = 30) -> pd.DataFrame:
    df = load_reliefs_all()
    if df.empty:
        return df

    iso = str(iso3).upper().strip()
    out = df[df.get("iso3", "").str.upper().str.strip() == iso].copy()

    if kinds and "kind_qid" in out.columns:
        kset = set(kinds)
        out = out[out["kind_qid"].isin(kset)]

    if "elevation_m" in out.columns:
        out["__elev"] = pd.to_numeric(out["elevation_m"], errors="coerce")
        out = out.sort_values(["__elev", "feature_label"], ascending=[False, True]).drop(columns="__elev", errors="ignore")

    return out.head(top_n) if top_n else out


# Export explícito (opcional)
__all__ = [
    "load_borders","borders_for_iso3",
    "load_geografia_paises","geografia_for_iso3",
    "load_koppen","koppen_for_iso3",
    "load_biomes","biomes_for_iso3",
    "load_timezones_new","timezones_for_iso3","load_timezones",
    "load_coastlines","coastlines_for_iso3",
    "load_ports_and_routes","ports_and_routes_for_iso3",
]

if __name__ == "__main__":
    print("[geo_store] 🚀 Teste manual de carregamento...")

    try:
        df1 = load_borders()
        print(f"[geo_store] borders → {len(df1)} linhas")

        df2 = load_timezones_new()
        print(f"[geo_store] timezones → {len(df2)} linhas")

        df3 = load_coastlines()
        print(f"[geo_store] coastlines → {len(df3)} linhas")

        df4 = load_ports_and_routes()
        print(f"[geo_store] ports_and_routes → {len(df4)} linhas")

    except Exception as e:
        import traceback
        print("[geo_store] ❌ Erro ao carregar dados:", e)
        traceback.print_exc()

    print("[geo_store] ✅ Teste concluído.")
