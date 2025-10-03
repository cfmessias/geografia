# services/offline_store.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
from typing import Iterable, Optional, Dict, Tuple
import re
import pandas as pd
from functools import lru_cache
import os
import io
import  unicodedata, requests
from bs4 import BeautifulSoup
from collections import namedtuple
import streamlit as st
# ---- Paths base -------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR     = PROJECT_ROOT / "data"

# ficheiros agregados canónicos
countries_seed_path      = DATA_DIR / "countries_seed.csv"
countries_profiles_path  = DATA_DIR / "countries_profiles.csv"
worldbank_timeseries_path= DATA_DIR / "wb_timeseries.csv"
cities_path              = DATA_DIR / "cities_all.csv"
unesco_path              = DATA_DIR / "unesco_all.csv"
leaders_current_path     = DATA_DIR / "leaders_current.csv"
leaders_history_path     = DATA_DIR / "leaders_history.csv"
olympics_summer_path     = DATA_DIR / "olympics_summer_manual.csv"
tourism_timeseries_path  = DATA_DIR / "tourism_timeseries.csv"
tourism_latest_path      = DATA_DIR / "tourism_latest.csv"
tourism_origin_eu_path   = DATA_DIR / "tourism_origin_eu.csv"
tourism_purpose_eu_path  = DATA_DIR / "tourism_purpose_eu.csv"
migration_inout_path     = DATA_DIR / "migration_inout.csv"


# ---- Utils -----------------------------------------------------------------
def _slugify(s: str) -> str:
    s = re.sub(r"[^\w\-]+", "_", s, flags=re.U)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "pais"

def _read_csv_safe(path: Path, expected_cols: Optional[Iterable[str]] = None) -> pd.DataFrame:
    """Lê CSV; se não existir, devolve DF vazio (com colunas esperadas). Tenta separador padrão e ';'."""
    if not path.exists():
        return pd.DataFrame(columns=list(expected_cols) if expected_cols else None)
    try:
        df = pd.read_csv(path)
    except Exception:
        try:
            df = pd.read_csv(path, sep=";")
        except Exception:
            return pd.DataFrame(columns=list(expected_cols) if expected_cols else None)
    if expected_cols:
        for c in expected_cols:
            if c not in df.columns:
                df[c] = pd.NA
        df = df[list(expected_cols)]
    return df

# ---- Profiles (master) ------------------------------------------------------
def have_master_profiles() -> bool:
    return countries_profiles_path.exists()

def load_profiles_master() -> pd.DataFrame:
    df = _read_csv_safe(countries_profiles_path)
    if df.empty:
        return df
    for c in ("population","area_km2","population_year"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def list_available_countries() -> pd.DataFrame:
    """
    Lista países a partir do profiles agregado; se não existir, cai para a seed.
    """
    if have_master_profiles():
        df = load_profiles_master()
        if df.empty:
            return pd.DataFrame(columns=["name","iso3","qid"])
        out = pd.DataFrame({
            "name": df.get("name", pd.Series(dtype=str)),
            "iso3": df.get("iso3", pd.Series(dtype=str)).astype(str).str.upper(),
            "qid":  df.get("qid",  pd.Series(dtype=str)),
        })
        return (
            out.dropna(subset=["name"])
               .drop_duplicates(subset=["iso3"])
               .sort_values("name")
               .reset_index(drop=True)
        )

    # fallback: seed
    seed = _read_csv_safe(countries_seed_path)
    if seed.empty:
        return pd.DataFrame(columns=["name","iso3","qid"])
    out = pd.DataFrame({
        "name": seed.get("name_pt", seed.get("name_en", pd.Series(dtype=str))),
        "iso3": seed.get("iso3", pd.Series(dtype=str)).astype(str).str.upper(),
        "qid":  pd.Series([], dtype=str),  # desconhecido aqui
    })
    return (
        out.dropna(subset=["name"])
           .drop_duplicates(subset=["iso3"])
           .sort_values("name")
           .reset_index(drop=True)
    )
# --- retro-compat ---
def list_countries() -> pd.DataFrame:
    """Alias compatível: devolve o mesmo que list_available_countries()."""
    return list_available_countries()

def get_profile_by_name(name: str) -> Optional[pd.Series]:
    df = load_profiles_master()
    if df.empty:
        return None
    row = df[df["name"] == name]
    return None if row.empty else row.iloc[0]

# ---- World Bank -------------------------------------------------------------
def load_worldbank_timeseries() -> pd.DataFrame:
    df = _read_csv_safe(worldbank_timeseries_path, expected_cols=["iso3","year","pop_total","pop_density","urban_pct"])
    if df.empty:
        return df
    df["iso3"] = df["iso3"].astype(str).str.upper()
    for c in ("year","pop_total","pop_density","urban_pct"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def wb_series_for_country(iso3: str) -> pd.DataFrame:
    df = load_worldbank_timeseries()
    if df.empty:
        return df
    return (
        df[df["iso3"] == str(iso3).upper()]
        .dropna(subset=["year"])
        .sort_values("year")
        .reset_index(drop=True)
    )

# ---- Cities -----------------------------------------------------------------
def load_cities_all() -> pd.DataFrame:
    df = _read_csv_safe(cities_path, expected_cols=["iso3","country","city","city_qid","admin","is_capital","population","year"])
    if df.empty:
        return df
    df["iso3"] = df["iso3"].astype(str).str.upper()
    for c in ("population","year","is_capital"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def cities_for_iso3(iso3: str) -> pd.DataFrame:
    """Wrapper que passa o mtime do ficheiro para invalidar cache quando muda."""
    mtime_ns = cities_path.stat().st_mtime_ns
    return _cities_for_iso3_cached(str(cities_path), mtime_ns, str(iso3).upper())

@st.cache_data(show_spinner=False)
def _cities_for_iso3_cached(path: str, _mtime_ns: int, iso3u: str) -> pd.DataFrame:
    # BOM-safe + separador auto (suporta ',' e ';')
    df = pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")
    # garantir colunas esperadas
    for k in ("iso3","city","admin","is_capital","population","year","lat","lon"):
        if k not in df.columns:
            df[k] = pd.NA
    # normalizar tipos
    df["iso3"] = df["iso3"].astype(str).str.upper()
    df["lat"]  = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"]  = pd.to_numeric(df["lon"], errors="coerce")
    df["population"] = pd.to_numeric(df["population"], errors="coerce")
    df["year"]       = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    return df[df["iso3"] == iso3u].reset_index(drop=True)

def country_has_cities(iso3: str) -> bool:
    df = load_cities_all()
    if df.empty:
        return False
    return df["iso3"].astype(str).str.upper().eq(str(iso3).upper()).any()

# ---- UNESCO -----------------------------------------------------------------
def load_unesco_all() -> pd.DataFrame:
    df = _read_csv_safe(unesco_path, expected_cols=["iso3","country","site","site_qid","type","year","lat","lon"])
    if df.empty:
        return df
    df["iso3"] = df["iso3"].astype(str).str.upper()
    for c in ("year","lat","lon"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def unesco_for_iso3(iso3: str) -> pd.DataFrame:
    df = load_unesco_all()
    if df.empty:
        return df
    return (
        df[df["iso3"] == str(iso3).upper()]
        .sort_values(["year","site"])
        .reset_index(drop=True)
    )

# ---- Leaders ----------------------------------------------------------------
def load_leaders_current() -> pd.DataFrame:
    df = _read_csv_safe(
        leaders_current_path,
        expected_cols=["iso3","country","role","person","person_qid","start","end",
                       "end_cause","party","party_qid"]
    )
    if df.empty:
        return df
    df["iso3"] = df["iso3"].astype(str).str.upper()
    return df

def load_leaders_history() -> pd.DataFrame:
    df = _read_csv_safe(
        leaders_history_path,
        expected_cols=["iso3","country","role","person","person_qid","start","end",
                       "end_cause","party","party_qid"]
    )
    if df.empty:
        return df
    df["iso3"] = df["iso3"].astype(str).str.upper()
    return df

def leaders_for_iso3(iso3: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    iso3u = str(iso3).upper()
    cur  = load_leaders_current()
    hist = load_leaders_history()
    return (
        cur[cur["iso3"] == iso3u].copy(),
        hist[hist["iso3"] == iso3u].copy(),
    )

# ---- Gastronomia ------------------------------------------------------------
Gastro = namedtuple("Gastro", ["dishes", "beverages"])

# Colunas esperadas (as antigas + as novas de referências; as que faltarem são criadas vazias)
_GASTRO_COLS = [
    "iso3","country","kind","item","item_qid","description","admin",
    "instance_of","image","wikipedia_pt","wikipedia_en","commons","website"
]


# mapeamentos comuns NOC→ISO3 (só usado se o ficheiro tiver NOC e não ISO3)
_NOC_TO_ISO3_FIX = {
    "POR":"PRT","GRE":"GRC","GER":"DEU","NED":"NLD","SUI":"CHE","CZE":"CZE","SVK":"SVK",
    "KOR":"KOR","PRK":"PRK","TPE":"TWN","HKG":"HKG","MAC":"MAC","UKR":"UKR","BLR":"BLR",
    "ROU":"ROU","MDA":"MDA","MNE":"MNE","SRB":"SRB","BIH":"BIH","MKD":"MKD","KOS":"XKX",
    "RUS":"RUS","USA":"USA","GBR":"GBR","IRL":"IRL","FRA":"FRA","ESP":"ESP","ITA":"ITA",
    "SWE":"SWE","NOR":"NOR","FIN":"FIN","DEN":"DNK","ISL":"ISL","NGR":"NGA","CIV":"CIV",
    "ARE":"ARE","KSA":"SAU","NZL":"NZL","AUS":"AUS","CAN":"CAN","MEX":"MEX","ARG":"ARG",
    "URU":"URY","CHI":"CHL","COL":"COL","PER":"PER","BOL":"BOL","PAR":"PRY","ECU":"ECU",
    "VEN":"VEN","BRA":"BRA","CHN":"CHN","JPN":"JPN","VIE":"VNM","PHI":"PHL","MAS":"MYS",
    "INA":"IDN","IND":"IND","PAK":"PAK","BAN":"BGD","NPL":"NPL","SRI":"LKA","QAT":"QAT",
    "KWT":"KWT","BRN":"BRN","OMN":"OMN","RSA":"ZAF","ETH":"ETH","EGY":"EGY","ALG":"DZA",
    "MAR":"MAR","TUN":"TUN","TUR":"TUR","SUD":"SDN",
}

def _pick_col(df: pd.DataFrame, options: list[str]) -> str | None:
    for opt in options:
        if opt in df.columns:
            return opt
        for col in df.columns:
            if re.sub(r"\W+","", col.lower()) == re.sub(r"\W+","", opt.lower()):
                return col
    return None

def _ensure_iso3_for_olympics(df: pd.DataFrame) -> pd.DataFrame:
    # já tem iso3?
    for c in ("iso3","ISO3","Iso3"):
        if c in df.columns:
            df = df.rename(columns={c:"iso3"})
            df["iso3"] = df["iso3"].astype(str).str.upper().str.strip()
            return df
    # tenta NOC/COI
    noc_col = _pick_col(df, ["NOC","COI","noc","código COI","codigo COI"])
    if noc_col:
        df["iso3"] = (
            df[noc_col].astype(str).str.upper().str.strip()
              .map(lambda x: _NOC_TO_ISO3_FIX.get(x, x))
        )
        return df
    # sem iso3 e sem NOC: deixa em branco (linhas serão filtradas)
    df["iso3"] = None
    return df


def _read_csv_safe_any(path: Path) -> pd.DataFrame:
    """Lê CSVs 'problemáticos' (Excel, UTF-16, BOM, ;/,/tab) devolvendo DF ou vazio."""
    if not path or not Path(path).exists():
        return pd.DataFrame()

    raw = Path(path).read_bytes()
    if not raw.strip():
        return pd.DataFrame()

    # Heurística simples de encoding
    encs: list[str] = []
    # BOM UTF-16 ou presença de NUL -> tratar como UTF-16 primeiro
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff") or b"\x00" in raw[:4096]:
        encs.extend(["utf-16", "utf-16le", "utf-16be"])
    encs.extend(["utf-8-sig", "utf-8", "cp1252", "latin1"])

    seps = [",", ";", "\t"]

    import io
    for enc in encs:
        for sep in seps:
            try:
                return pd.read_csv(io.BytesIO(raw), encoding=enc, sep=sep, engine="python")
            except Exception:
                pass

    # Última tentativa: deixar o pandas inferir o separador
    try:
        return pd.read_csv(io.BytesIO(raw), encoding="utf-8", sep=None, engine="python")
    except Exception:
        return pd.DataFrame()


def load_olympics_summer_csv(path: str | None = None) -> pd.DataFrame:
    """
    Lê o CSV manual data/olympics_summer_manual.csv (se existir) e devolve:
      iso3, country_pt, summer_gold, summer_silver, summer_bronze, summer_total,
      year, city, host_country

    Cabeçalhos aceites (case-insensitive, com/sem acentos):
      - ISO3 | NOC | COI
      - País | Country
      - Ouro | Gold
      - Prata | Silver
      - Bronze
      - Total
      - Ano | Year
      - Cidade | City
      - País (anfitrião) | Host | Host Country
    """
    p = Path(path) if path else olympics_summer_path
    if not p.exists():
        return pd.DataFrame(columns=[
            "iso3","country_pt","summer_gold","summer_silver","summer_bronze",
            "summer_total","year","city","host_country"
        ])

    # 1) ler com autodetecção; se vier numa só coluna, reler com ';'
    try:
        df = pd.read_csv(p, dtype=str, keep_default_na=False, sep=None, engine="python")
    except Exception:
        df = pd.read_csv(p, dtype=str, keep_default_na=False)

    if df.shape[1] == 1:
        only_col = str(df.columns[0])
        if ";" in only_col:
            df = pd.read_csv(p, dtype=str, keep_default_na=False, sep=";")
        else:
            # tentativa extra: vírgula
            if "," in only_col:
                df = pd.read_csv(p, dtype=str, keep_default_na=False, sep=",")

    # utilidades de picking/normalização
    def norm(s: str) -> str:
        return re.sub(r"\W+", "", s, flags=re.U).lower()

    colmap = {c: norm(str(c)) for c in df.columns}

    def pick(*names: str) -> str | None:
        # match normalizado
        want = {norm(n) for n in names}
        for c, n in colmap.items():
            if n in want:
                return c
        return None

    def as_int(series: pd.Series | None) -> pd.Series:
        if series is None:
            return pd.Series([0]*len(df), index=df.index)
        return (
            series.astype(str)
                  .str.replace(r"[^\d\-]", "", regex=True)
                  .replace("", "0")
                  .astype(int)
        )

    # 2) identificar colunas
    c_code = pick("iso3","noc","coi","codigo","codigoocoi")
    c_ctry = pick("pais","país","country")
    c_gold = pick("ouro","gold")
    c_silv = pick("prata","silver")
    c_bron = pick("bronze")
    c_tot  = pick("total","totais","medalhas")
    c_year = pick("ano","year")
    c_city = pick("cidade","city")
    c_host = pick("paisanfitriao","paísanfitrião","host","hostcountry","paisanfitriao","pais")

    # 3) construir ISO3 (aceita ISO3 direto ou NOC → ISO3 via mapa)
    s_code = df[c_code].astype(str).str.strip().str.upper() if c_code else pd.Series([""]*len(df))
    s_iso = s_code.map(lambda x: x if re.fullmatch(r"[A-Z]{3}", x) and x not in _NOC_TO_ISO3_FIX
                               else _NOC_TO_ISO3_FIX.get(x, None))
    s_iso = s_iso.where(s_iso.str.fullmatch(r"[A-Z]{3}"), None)

    # 4) montar dataframe final
    out = pd.DataFrame({
        "iso3": s_iso,
        "country_pt": (df[c_ctry] if c_ctry else pd.Series([""]*len(df))).astype(str).str.strip(),
        "summer_gold":   as_int(df[c_gold] if c_gold else None),
        "summer_silver": as_int(df[c_silv] if c_silv else None),
        "summer_bronze": as_int(df[c_bron] if c_bron else None),
    }, index=df.index)

    if c_tot:
        out["summer_total"] = as_int(df[c_tot])
    else:
        out["summer_total"] = out["summer_gold"] + out["summer_silver"] + out["summer_bronze"]

    # meta (opcionais)
    out["year"]         = as_int(df[c_year] if c_year else None)
    out["city"]         = (df[c_city].astype(str).str.strip() if c_city else pd.Series([None]*len(df)))
    out["host_country"] = (df[c_host].astype(str).str.strip() if c_host else pd.Series([None]*len(df)))

    # limpar inválidos
    out = out[out["iso3"].notna()].copy()

    # tipos finais
    for c in ("summer_gold","summer_silver","summer_bronze","summer_total","year"):
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).astype(int)

    return out


@lru_cache(maxsize=1)
def load_religion(path: str | None = None) -> pd.DataFrame:
    """
    Lê data/religion.csv (produzido por scripts/fetch_religion.py ou manual).
    Colunas esperadas:
      iso3, country, christian, muslim, unaffiliated, hindu, buddhist,
      folk_religions, other_religions, jewish, source_year
    Unidades: PERCENTAGEM da população (0-100).
    """
    p = Path(path) if path else (DATA_DIR / "religion.csv")
    df = pd.read_csv(p)
    df["iso3"] = df["iso3"].astype(str).str.upper()
    for c in ["christian","muslim","unaffiliated","hindu","buddhist","folk_religions","other_religions","jewish"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    return df

@lru_cache(maxsize=1)
def load_migration_latest(path: str | None = None) -> pd.DataFrame:
    """
    Lê data/migration_latest.csv (scripts/fetch_migration.py).
    Colunas:
      iso3, country, indicator, indicator_name, year, value
    """
    p = Path(path) if path else (DATA_DIR / "migration_latest.csv")
    df = pd.read_csv(p)
    df["iso3"] = df["iso3"].astype(str).str.upper()
    return df

@lru_cache(maxsize=1)
def load_migration_ts(path: str | None = None) -> pd.DataFrame:
    """
    Lê data/migration_timeseries.csv (séries anuais).
    Colunas:
      iso3, country, indicator, indicator_name, year, value
    """
    p = Path(path) if path else (DATA_DIR / "migration_timeseries.csv")
    df = pd.read_csv(p)
    df["iso3"] = df["iso3"].astype(str).str.upper()
    return df

_BN_HEADERS = {"User-Agent": "GeoApp/1.0 (+https://github.com/)"}
_BN_BASE = "https://www.bandeirasnacionais.com"

# Exceções de slug quando o nome PT “normalizado” não bate certo com o site
_BN_SLUG_FIX = {
    "Côte d'Ivoire": "costa-do-marfim",
    "Costa do Marfim": "costa-do-marfim",
    "Cabo Verde": "cabo-verde",
    "São Tomé e Príncipe": "sao-tome-e-principe",
    "Guiné-Bissau": "guine-bissau",
    "Timor-Leste": "timor-leste",
    "Micronésia": "micronesia",
    "Eswatini": "essuatini",
    "Suazilândia": "essuatini",
    "Reino Unido": "reino-unido",
    "Estados Unidos": "estados-unidos",
    "República Democrática do Congo": "republica-democratica-do-congo",
}

def _slugify_pt(name: str) -> str:
    s = unicodedata.normalize("NFD", str(name))
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")       # remove acentos
    s = re.sub(r"&", " e ", s).lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s

def _bn_pick_table_facts(soup: BeautifulSoup) -> dict:
    facts = {}
    # pegar na(s) tabela(s) maior(es) com pares chave/valor
    for tbl in soup.select("table"):
        for tr in tbl.select("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th","td"])]
            if len(cells) >= 2:
                k, v = cells[0], cells[1]
                # filtra chaves demasiado genéricas
                if 1 <= len(k) <= 40:
                    facts[k] = v
    return facts

def load_flag_info(country_pt: str, iso3: str | None = None) -> dict | None:
    """
    Vai ao bandeirasnacionais.com e devolve:
      {"flag_url": <img>, "site_url": <url>, "facts": dict}
    Devolve None se falhar.
    """
    if not country_pt:
        return None

    # mapeamentos conhecidos primeiro
    slug = _BN_SLUG_FIX.get(country_pt) or _slugify_pt(country_pt)
    url = f"{_BN_BASE}/{slug}"

    try:
        resp = requests.get(url, headers=_BN_HEADERS, timeout=15)
        if resp.status_code == 404 and iso3:  # tentativa com o ISO em fallback (raro)
            alt = _slugify_pt(iso3)
            url = f"{_BN_BASE}/{alt}"
            resp = requests.get(url, headers=_BN_HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # 1) imagem da bandeira (meta og:image é a mais estável)
    flag = None
    og = soup.select_one('meta[property="og:image"]')
    if og and og.get("content"):
        flag = og["content"]
    if not flag:
        img = soup.select_one("figure img, .flag img, img")
        if img and img.get("src"):
            flag = img["src"]
            if flag.startswith("/"):
                flag = _BN_BASE + flag

    # 2) factos (pega na(s) tabela(s) da página)
    facts = _bn_pick_table_facts(soup)

    if not flag and not facts:
        return None
    return {"flag_url": flag, "site_url": url, "facts": facts}

# ---- Turismo (World Bank WDI) ----------------------------------------------
@lru_cache(maxsize=1)
def load_tourism_ts(path: str | None = None) -> pd.DataFrame:
    """
    Lê data/tourism_timeseries.csv
    colunas: iso3; country; indicator; indicator_name; year; value
    """
    p = Path(path) if path else tourism_timeseries_path
    df = _read_csv_safe(p, expected_cols=["iso3","country","indicator","indicator_name","year","value"])
    if df.empty:
        return df
    df["iso3"] = df["iso3"].astype(str).str.upper()
    for c in ("year","value"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

@lru_cache(maxsize=1)
def load_tourism_latest(path: str | None = None) -> pd.DataFrame:
    """
    Lê data/tourism_latest.csv (último valor por iso3+indicator)
    """
    p = Path(path) if path else tourism_latest_path
    df = _read_csv_safe(p, expected_cols=["iso3","country","indicator","indicator_name","year","value"])
    if df.empty:
        return df
    df["iso3"] = df["iso3"].astype(str).str.upper()
    for c in ("year","value"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def tourism_series_for_iso3(iso3: str) -> pd.DataFrame:
    df = load_tourism_ts()
    if df.empty:
        return df
    return df[df["iso3"] == str(iso3).upper()].sort_values("year").reset_index(drop=True)

# --- mapas simples ISO3<->ISO2 para UE/EFTA (suficiente para filtrar por país)
_ISO3_TO_ISO2_EU = {
    "AUT":"AT","BEL":"BE","BGR":"BG","HRV":"HR","CYP":"CY","CZE":"CZ","DNK":"DK","EST":"EE",
    "FIN":"FI","FRA":"FR","DEU":"DE","GRC":"EL","HUN":"HU","IRL":"IE","ITA":"IT","LVA":"LV",
    "LTU":"LT","LUX":"LU","MLT":"MT","NLD":"NL","POL":"PL","PRT":"PT","ROU":"RO","SVK":"SK",
    "SVN":"SI","ESP":"ES","SWE":"SE", "ISL":"IS","LIE":"LI","NOR":"NO","CHE":"CH"
}
_ISO2_TO_ISO3_EU = {v:k for k,v in _ISO3_TO_ISO2_EU.items()}

def _read_csv_semicolon(path: Path, expected_cols: list[str]) -> pd.DataFrame:
    return _read_csv_safe(path, expected_cols=expected_cols)

@lru_cache(maxsize=1)
def load_tourism_origin_eu(path: str | None = None) -> pd.DataFrame:
    """
    Lê data/tourism_origin_eu.csv (destino 'geo' e origem 'origin' em ISO2; ano; arrivals)
    """
    p = Path(path) if path else tourism_origin_eu_path
    df = _read_csv_semicolon(p, expected_cols=["geo","origin","year","arrivals","unit"])
    if df.empty:
        return df
    for c in ("year","arrivals"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["geo"] = df["geo"].astype(str).str.upper()
    df["origin"] = df["origin"].astype(str).str.upper()
    return df

def tourism_origin_for_iso3(iso3: str) -> pd.DataFrame:
    """
    Filtra por país de destino (iso3→iso2). Devolve dataframe com colunas:
      origin (ISO2), year, arrivals, unit
    """
    df = load_tourism_origin_eu()
    if df.empty:
        return df
    iso2 = _ISO3_TO_ISO2_EU.get(str(iso3).upper())
    if not iso2:
        return pd.DataFrame(columns=df.columns)
    sub = df[df["geo"] == iso2].copy()
    return sub[["origin","year","arrivals","unit"]].sort_values(["year","arrivals"], ascending=[True, False])

@lru_cache(maxsize=1)
def load_tourism_purpose_eu(path: str | None = None) -> pd.DataFrame:
    """
    Lê data/tourism_purpose_eu.csv (geo ISO2; purpose; destination; year; trips)
    """
    p = Path(path) if path else tourism_purpose_eu_path
    df = _read_csv_semicolon(p, expected_cols=["geo","purpose","destination","year","trips","unit"])
    if df.empty:
        return df
    for c in ("year","trips"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["geo"] = df["geo"].astype(str).str.upper()
    df["purpose"] = df["purpose"].astype(str).str.upper()
    df["destination"] = df["destination"].astype(str).str.upper()
    return df

def tourism_purpose_for_iso3(iso3: str) -> pd.DataFrame:
    """
    Filtra por país (residentes do país geo=iso2). Devolve purpose/destination/year/trips.
    """
    df = load_tourism_purpose_eu()
    if df.empty:
        return df
    iso2 = _ISO3_TO_ISO2_EU.get(str(iso3).upper())
    if not iso2:
        return pd.DataFrame(columns=df.columns)
    sub = df[df["geo"] == iso2].copy()
    return sub[["purpose","destination","year","trips","unit"]].sort_values(["year","purpose"])


try:
    migration_inout_path
except NameError:
    migration_inout_path = DATA_DIR / "migration_inout.csv"


@lru_cache(maxsize=1)
def load_migration_inout(path: str | Path | None = None) -> pd.DataFrame:
    """
    Lê data/migration_inout.csv como UTF-8 com separador ';' e normaliza colunas.
    Devolve: iso3, year, immigrants, emigrants (tipados).
    """
    p = Path(path) if path else migration_inout_path
    if not p.exists():
        return pd.DataFrame(columns=["iso3", "year", "immigrants", "emigrants"])

    # Lê como texto e remove BOM, depois parse com ';'
    text = p.read_text(encoding="utf-8", errors="replace")
    if text.startswith("\ufeff"):
        text = text.lstrip("\ufeff")

    df = pd.read_csv(io.StringIO(text), sep=";", engine="python")

    # Caso patológico: 1 coluna chamada 'iso3;year;immigrants;emigrants'
    if df.shape[1] == 1 and ";" in (df.columns[0] or ""):
        df = pd.read_csv(io.StringIO(text), sep=";", engine="python")

    # Normalizar nomes de colunas (tira BOM/espacos)
    df.columns = [str(c).strip().strip("\ufeff") for c in df.columns]

    # Mapear variações (se existirem)
    low = {c.lower(): c for c in df.columns}
    aliases = {
        "iso3":        ["iso3", "country", "pais", "code", "codigo"],
        "year":        ["year", "ano", "time"],
        "immigrants":  ["immigrants", "imigrantes", "immig"],
        "emigrants":   ["emigrants", "emigrantes", "emig"],
    }
    rename = {}
    for std, opts in aliases.items():
        hit = next((low[k] for k in opts if k in low), None)
        if hit:
            rename[hit] = std
    if rename:
        df = df.rename(columns=rename)
        low = {c.lower(): c for c in df.columns}

    needed = {"iso3", "year", "immigrants", "emigrants"}
    if not needed.issubset({c.lower() for c in df.columns}):
        return pd.DataFrame(columns=list(needed))

    # Renomear exatamente e tipar
    df = df.rename(columns={
        next(c for c in df.columns if c.lower() == "iso3"): "iso3",
        next(c for c in df.columns if c.lower() == "year"): "year",
        next(c for c in df.columns if c.lower() == "immigrants"): "immigrants",
        next(c for c in df.columns if c.lower() == "emigrants"): "emigrants",
    })
    df["iso3"] = df["iso3"].astype(str).str.upper().str.strip()
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["immigrants"] = pd.to_numeric(df["immigrants"], errors="coerce")
    df["emigrants"]  = pd.to_numeric(df["emigrants"],  errors="coerce")

    return (df.dropna(subset=["year"])
              .loc[:, ["iso3", "year", "immigrants", "emigrants"]]
              .sort_values(["iso3", "year"])
              .reset_index(drop=True))


from pathlib import Path
import pandas as pd
import unicodedata, re

DATA_DIR = Path(__file__).resolve().parents[1] / "data"

# se já tiveres esta variável, mantém a tua
try:
    countries_seed_path
except NameError:
    countries_seed_path = DATA_DIR / "countries_seed.csv"

def _normkey(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFD", str(s))
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"&", " e ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()
    # normalizações frequentes (UN DESA vs seed)
    s = (s.replace("united states of america", "united states")
           .replace("viet nam", "vietnam")
           .replace("iran islamic republic of", "iran")
           .replace("syrian arab republic", "syria")
           .replace("russian federation", "russia")
           .replace("bolivia plurinational state of", "bolivia")
           .replace("tanzania united republic of", "tanzania")
           .replace("korea republic of", "south korea")
           .replace("korea democratic people s republic of", "north korea")
           .replace("cote d ivoire", "cote d ivoire")  # já sem acento
    )
    return re.sub(r"\s+", " ", s).strip()

def migration_inout_for_iso3(iso3: str) -> pd.DataFrame:
    """
    Série imigração/emigração para um ISO3.
    1) Tenta diretamente em migration_inout.csv (iso3).
    2) Se vazio, reconstrói a partir de migration_inout_m49.csv por nome do país.
    """
    iso = str(iso3).upper()

    # 1) caminho normal: já em ISO3
    df = load_migration_inout()
    sub = df[df["iso3"] == iso].sort_values("year").copy()
    if not sub.empty:
        return sub

    # 2) fallback: procurar por nome no ficheiro M49
    m49_path = DATA_DIR / "migration_inout_m49.csv"
    if not m49_path.exists():
        return pd.DataFrame(columns=["iso3", "year", "immigrants", "emigrants"])

    m49 = _read_csv_safe_any(m49_path)
    if m49.empty:
        return pd.DataFrame(columns=["iso3", "year", "immigrants", "emigrants"])

    # colunas esperadas: country, year, immigrants, emigrants
    low = {c.lower(): c for c in m49.columns}
    c_country = low.get("country") or low.get("pais") or low.get("name")
    c_year    = low.get("year") or low.get("ano") or "year"
    c_imm     = low.get("immigrants") or low.get("imigrantes") or "immigrants"
    c_emg     = low.get("emigrants")  or low.get("emigrantes")  or "emigrants"
    if not all([c_country, c_year, c_imm, c_emg]):
        return pd.DataFrame(columns=["iso3", "year", "immigrants", "emigrants"])

    # obter os nomes (PT/EN) para o ISO3 a partir da seed
    seed = _read_csv_safe_any(countries_seed_path)
    if seed.empty or "iso3" not in seed.columns:
        return pd.DataFrame(columns=["iso3", "year", "immigrants", "emigrants"])

    row = seed[seed["iso3"].astype(str).str.upper() == iso]
    if row.empty:
        return pd.DataFrame(columns=["iso3", "year", "immigrants", "emigrants"])

    names = set()
    for k in ("name_pt", "name_en", "name"):
        if k in row.columns:
            v = row.iloc[0].get(k)
            if pd.notna(v) and str(v).strip():
                names.add(_normkey(v))

    # normalizar nomes do M49 e filtrar por match
    m49 = m49.rename(columns={c_country: "country", c_year: "year", c_imm: "immigrants", c_emg: "emigrants"})
    m49["__key"] = m49["country"].astype(str).map(_normkey)
    subm = m49[m49["__key"].isin(names)].copy()
    if subm.empty:
        # último recurso: tenta match pelo início (alguns países vêm com “Republic of …”)
        key = next(iter(names)) if names else ""
        subm = m49[m49["__key"].str.startswith(key)].copy()

    if subm.empty:
        return pd.DataFrame(columns=["iso3", "year", "immigrants", "emigrants"])

    subm["year"] = pd.to_numeric(subm["year"], errors="coerce").astype("Int64")
    subm["immigrants"] = pd.to_numeric(subm["immigrants"], errors="coerce")
    subm["emigrants"]  = pd.to_numeric(subm["emigrants"],  errors="coerce")
    subm = (
        subm.dropna(subset=["year"])
            .loc[:, ["year", "immigrants", "emigrants"]]
            .sort_values("year")
    )
    subm.insert(0, "iso3", iso)
    return subm.reset_index(drop=True)

# services/offline_store.py
# services/offline_store.py  (secção de migração)


MIG_TS_CSV          = DATA_DIR / "migration_timeseries.csv"
MIG_LATEST_CSV      = DATA_DIR / "migration_latest.csv"
MIG_INOUT_CSV       = DATA_DIR / "migration_inout.csv"
MIG_INOUT_M49_CSV   = DATA_DIR / "migration_inout_m49.csv"
COUNTRIES_SEED_CSV  = DATA_DIR / "countries_seed.csv"   # só para fallback M49→ISO3 (se existir)

# ── helpers ───────────────────────────────────────────────────────────────────
def _empty(cols: list[str]) -> pd.DataFrame:
    return pd.DataFrame({c: pd.Series(dtype="float" if c in {"value","immigrants","emigrants"} else "object")
                         for c in cols})

def _filter_iso3_csv(path: Path, iso3: str, usecols: list[str], dtypes: dict[str,str] | None = None,
                     chunksize: int = 200_000) -> pd.DataFrame:
    """Lê em chunks e devolve só as linhas do ISO3 pedido. Requer coluna 'iso3' no ficheiro."""
    if not path.exists():
        st.warning(f"Ficheiro não encontrado: {path}")
        return _empty(usecols)
    iso3u = str(iso3).upper()
    frames = []
    for ch in pd.read_csv(path, usecols=usecols, dtype=dtypes, chunksize=chunksize, low_memory=False):
        ch["iso3"] = ch["iso3"].astype(str).str.upper()
        frames.append(ch[ch["iso3"] == iso3u])
    return pd.concat(frames, ignore_index=True) if frames else _empty(usecols)

# ── loaders WDI: filtrados por país ───────────────────────────────────────────
@st.cache_data(show_spinner=False)
def load_migration_ts_for_iso3(iso3: str) -> pd.DataFrame:
    # cols reais: iso3,country,indicator,indicator_name,year,value
    usecols = ["iso3","indicator","year","value"]
    dtypes  = {"iso3":"string","indicator":"string","year":"Int64","value":"float"}
    df = _filter_iso3_csv(MIG_TS_CSV, iso3, usecols+[], dtypes)
    return df

@st.cache_data(show_spinner=False)
def load_migration_latest_for_iso3(iso3: str) -> pd.DataFrame:
    usecols = ["iso3","indicator","year","value"]
    dtypes  = {"iso3":"string","indicator":"string","year":"Int64","value":"float"}
    df = _filter_iso3_csv(MIG_LATEST_CSV, iso3, usecols+[], dtypes)
    return df

# ── loader UN DESA (in/out): filtrado por país ────────────────────────────────
@st.cache_data(show_spinner=False)
def load_migration_inout_for_iso3(iso3: str) -> pd.DataFrame:
    iso3u = str(iso3).upper()
    cols_out = ["iso3","year","immigrants","emigrants"]

    if not MIG_INOUT_CSV.exists():
        st.warning(f"Ficheiro não encontrado: {MIG_INOUT_CSV}")
        return pd.DataFrame(columns=cols_out)

    frames = []
    # 👇 separador autodetectado + BOM-safe
    it = pd.read_csv(
        MIG_INOUT_CSV,
        sep=None,              # autodetecta ',' ';' '\t' …
        engine="python",       # necessário para sep=None
        chunksize=200_000,
        encoding="utf-8-sig",  # remove BOM se existir
    )


    for ch in it:
        # normalizar cabeçalho
        ch.columns = ch.columns.str.strip()
        needed = {"iso3","year","immigrants","emigrants"}

        # se ainda assim faltar algo, mostra o cabeçalho real para debug
        if not needed.issubset(set(ch.columns)):
            st.warning(
                f"Cabeçalho inesperado em {MIG_INOUT_CSV.name}: {list(ch.columns)} "
                f"(esperado: {sorted(needed)})"
            )
            # tenta mapear por variantes triviais
            rename = {}
            for c in ch.columns:
                k = c.strip().lower()
                if k in {"ï»¿iso3","﻿iso3"}: rename[c] = "iso3"
                if k == "time": rename[c] = "year"
            if rename:
                ch = ch.rename(columns=rename)

        # se mesmo assim não houver as 4, ignora o chunk
        if not needed.issubset(set(ch.columns)):
            continue

        ch = ch[["iso3","year","immigrants","emigrants"]].copy()
        ch["iso3"] = ch["iso3"].astype(str).str.upper()
        ch = ch[ch["iso3"] == iso3u]

        if ch.empty:
            continue

        ch["year"] = pd.to_numeric(ch["year"], errors="coerce").astype("Int64")
        ch["immigrants"] = pd.to_numeric(ch["immigrants"], errors="coerce")
        ch["emigrants"]  = pd.to_numeric(ch["emigrants"],  errors="coerce")

        frames.append(ch)

    if not frames:
        return pd.DataFrame(columns=cols_out)

    df = pd.concat(frames, ignore_index=True)
    df = df.dropna(subset=["year"]).sort_values("year").drop_duplicates(subset=["year"], keep="last")
    return df[cols_out].reset_index(drop=True)


# -----------------------
# Consolidation footer
# -----------------------

# Ensure migration per-ISO3 function is available under both names.
try:
    load_migration_inout_for_iso3  # type: ignore[name-defined]
except NameError:
    # Fallback wrapper using the full loader if specific one isn't present
    import pandas as pd
    import streamlit as st
    @st.cache_data(show_spinner=False)
    def load_migration_inout_for_iso3(iso3: str) -> pd.DataFrame:
        df = load_migration_inout() if 'load_migration_inout' in globals() else pd.DataFrame(columns=['iso3','year','immigrants','emigrants'])
        if df.empty:
            return df
        df = df.copy()
        df.columns = df.columns.str.replace('\ufeff','', regex=False).str.strip()
        if 'iso3' not in df.columns or 'year' not in df.columns:
            return pd.DataFrame(columns=['iso3','year','immigrants','emigrants'])
        df['iso3'] = df['iso3'].astype(str).str.upper()
        df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
        for c in ('immigrants','emigrants'):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce')
        out = df[df['iso3'] == str(iso3).upper()].dropna(subset=['year']).sort_values('year').drop_duplicates('year', keep='last')
        return out[['iso3','year','immigrants','emigrants']] if set(['immigrants','emigrants']).issubset(out.columns) else out

# Back-compat alias:
migration_inout_for_iso3 = load_migration_inout_for_iso3

# Curated export list (optional; comment out if you prefer wildcard imports)
try:
    __all__
except NameError:
    __all__ = [
        # Cities / UNESCO / Leaders (if present in this module)
        "load_cities_all","cities_for_iso3","country_has_cities",
        "load_unesco_all","unesco_for_iso3",
        "load_leaders_current","load_leaders_history","leaders_for_iso3",
        # Tourism
        "load_tourism_ts","load_tourism_latest","tourism_series_for_iso3",
        "load_tourism_origin_eu","tourism_origin_for_iso3",
        "load_tourism_purpose_eu","tourism_purpose_for_iso3",
        # Migration (full and per-ISO3)
        "load_migration_latest","load_migration_ts","load_migration_inout",
        "load_migration_ts_for_iso3","load_migration_latest_for_iso3",
        "load_migration_inout_for_iso3","migration_inout_for_iso3",
        # World Bank
        "load_worldbank_timeseries","wb_series_for_country",
        # Misc profiles/countries (if present)
        "list_available_countries","load_profiles_master"
    ]
