# scripts/fetch_migration_inout.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import io, re, sys, time, random, zipfile, unicodedata
from typing import Optional, Iterable, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ======================== Config =========================
BASE_URL = "https://www.un.org/development/desa/pd/content/international-migrant-stock"
UA = "GeoMigration/1.4 (+https://example.org)"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

SEED_PATH = DATA_DIR / "countries_seed.csv"
OUT_INOUT_ISO3 = DATA_DIR / "migration_inout.csv"
OUT_INOUT_M49  = DATA_DIR / "migration_inout_m49.csv"
CACHE_XLSX     = DATA_DIR / "migration_inout_source.xlsx"   # cache opcional
MAP_UN_OFFICIAL = DATA_DIR / "un_m49_iso.csv" 

S = requests.Session()
S.headers.update({"User-Agent": UA})

# ======================== Util ===========================
def _log(msg: str) -> None:
    print(msg, flush=True)

def _fetch(url: str, timeout: int = 120) -> Optional[bytes]:
    for _ in range(4):
        try:
            r = S.get(url, timeout=timeout)
            r.raise_for_status()
            return r.content
        except Exception:
            time.sleep(0.8 + random.random())
    return None

def _get_html(url: str) -> Optional[BeautifulSoup]:
    blob = _fetch(url)
    if not blob:
        return None
    for parser in ("lxml","html.parser"):
        try:
            return BeautifulSoup(blob, parser)
        except Exception:
            pass
    return None

def _abs(base: str, href: str) -> str:
    from urllib.parse import urljoin
    return urljoin(base, href)

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"\s+", " ", s.lower()).strip()
    return s

# ==================== Link finder ========================
def _find_download_url(start_url: str, max_depth: int = 2) -> Optional[str]:
    visited = set([start_url])
    frontier = [start_url]
    best = None

    def _score(text: str, href: str) -> int:
        t = _norm(text + " " + href)
        has_dao = "destination and origin" in t
        sc = 0
        if has_dao:
            sc += 80                   # queremos este
        if "destination" in t and "origin" in t:
            sc += 10
        # só penaliza 'sex' e 'total' quando NÃO é o 'destination and origin'
        if "sex" in t and not has_dao:
            sc -= 50
        if "total" in t and not has_dao:
            sc -= 50
        if href.lower().endswith((".xlsx", ".xls", ".zip", ".csv")):
            sc += 15
        m = re.search(r"(20\d{2})", t)
        if m:
            sc += int(m.group(1)) - 1990  # prefere ano mais recente
        return sc

    depth = 0
    while frontier and depth <= max_depth:
        next_frontier = []
        for url in frontier:
            soup = _get_html(url)
            if not soup:
                continue
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                txt  = a.get_text(" ", strip=True)
                url_abs = _abs(url, href)
                sc = _score(txt, href)
                if best is None or sc > best[0]:
                    best = (sc, url_abs, txt)
            # seguir páginas prováveis
            for a in soup.find_all("a", href=True):
                txt = _norm(a.get_text(" ", strip=True))
                h   = a["href"].lower()
                if any(k in txt for k in ("data","international migrant stock","destination","origin")) \
                   and not re.search(r"\.(pdf|xlsx|xls|zip|csv)$", h):
                    url_abs = _abs(url, a["href"])
                    if url_abs not in visited:
                        visited.add(url_abs)
                        next_frontier.append(url_abs)
        # devolve assim que achar um .xlsx/.zip/.csv cujo texto contenha Destination and origin
        if best and best[1].lower().endswith((".xlsx",".xls",".zip",".csv")) \
           and "destination and origin" in _norm(best[2]):
            return best[1]
        frontier = next_frontier
        depth += 1

    # fallback: melhor link que não seja “Total …”
    if best and best[1].lower().endswith((".xlsx",".xls",".zip",".csv")) \
       and "total" not in _norm(best[2]):
        return best[1]
    return None


# ================ Leitura XLSX/ZIP/CSV ===================
def _read_container(blob: bytes) -> Tuple[Optional[bytes], Optional[bytes]]:
    """
    Devolve (xlsx_bytes, csv_bytes).
    - Se o blob for um XLSX (é um ZIP com 'xl/...'), devolve (blob, None)
    - Se for ZIP “contentor”, tenta .xlsx/.xls; senão tenta .csv
    - Se for CSV direto, devolve (None, blob)
    """
    # Tenta abrir como ZIP (XLSX também é ZIP)
    try:
        with zipfile.ZipFile(io.BytesIO(blob)) as z:
            names = z.namelist()
            # Caso 1: é um XLSX “puro” (o próprio ficheiro tem 'xl/...' lá dentro)
            if any(n.startswith("xl/") or n == "[Content_Types].xml" for n in names):
                return blob, None
            # Caso 2: ZIP “contentor” com um XLSX lá dentro
            for n in names:
                if n.lower().endswith((".xlsx", ".xls")):
                    return z.read(n), None
            # Caso 3: ZIP com CSV
            for n in names:
                if n.lower().endswith(".csv"):
                    return None, z.read(n)
            raise RuntimeError("ZIP sem XLSX/CSV utilizável")
    except zipfile.BadZipFile:
        pass

    # Não era um ZIP “abrível”: testa se parece CSV de texto
    head_txt = blob[:4096].decode("utf-8", errors="ignore")
    if "," in head_txt and "\n" in head_txt:
        return None, blob

    # Por eliminação: trata como XLSX “normal”
    return blob, None


# =============== Extração (XLSX e CSV) ===================
def _looks_long(df: pd.DataFrame) -> bool:
    """
    Verificação leve para o CSV 'Destination and origin' em formato longo.
    Procuramos colunas com origem, destino, ano e valor.
    """
    cols = [str(c).strip().lower() for c in df.columns]
    has_origin      = any("origin"      in c for c in cols)
    has_destination = any("destin"      in c for c in cols)  # 'destination'
    has_year        = any(c == "year" or "time" in c for c in cols)
    has_value       = any(c == "value" or "migrant" in c or "stock" in c for c in cols)
    return has_origin and has_destination and has_year and has_value


def _extract_inout_from_csv(csv_bytes: bytes) -> pd.DataFrame:
    """
    Lê o CSV longo da UN DESA (Destination and origin) e devolve m49/year/immigrants/emigrants.
    Aceita ; ou , como separador.
    """
    # detetar separador automaticamente
    df = pd.read_csv(io.BytesIO(csv_bytes), sep=None, engine="python")
    if not _looks_long(df):
        raise ValueError("CSV não parece 'Destination and origin' (formato longo).")

    cols = {str(c).strip().lower(): c for c in df.columns}

    # PRIORIDADE aos códigos M49; se não existirem, cai para os campos genericos
    oc = (
        cols.get("location code of origin")
        or cols.get("origin code")
        or cols.get("origin m49")
        or cols.get("origin")
        or "Origin"
    )
    dc = (
        cols.get("location code of destination")
        or cols.get("destination code")
        or cols.get("destination m49")
        or cols.get("destination")
        or "Destination"
    )
    yc = cols.get("year") or cols.get("time") or "Year"
    vc = cols.get("value") or cols.get("migrants") or cols.get("stock") or "Value"

    # códigos M49 podem vir como números OU strings; normaliza
    def _to_m49(x):
        try:
            return int(str(x).strip())
        except Exception:
            return pd.NA

    df = df.copy()
    df["o"] = df[oc].apply(_to_m49)
    df["d"] = df[dc].apply(_to_m49)
    df["y"] = pd.to_numeric(df[yc], errors="coerce").astype("Int64")
    df["v"] = pd.to_numeric(df[vc], errors="coerce").fillna(0)

    WORLD = 900
    imm = (
        df[df["o"] == WORLD]
        .groupby(["d", "y"], as_index=False, observed=False)["v"].sum()
        .rename(columns={"d": "m49", "y": "year", "v": "immigrants"})
    )
    emi = (
        df[df["d"] == WORLD]
        .groupby(["o", "y"], as_index=False, observed=False)["v"].sum()
        .rename(columns={"o": "m49", "y": "year", "v": "emigrants"})
    )

    out = pd.merge(imm, emi, on=["m49", "year"], how="outer")
    out["m49"]  = pd.to_numeric(out["m49"], errors="coerce").astype("Int64")
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    out["immigrants"] = pd.to_numeric(out["immigrants"], errors="coerce")
    out["emigrants"]  = pd.to_numeric(out["emigrants"],  errors="coerce")
    return out.sort_values(["m49", "year"])

# ============ M49 → ISO3 (seed opcional) =================
def _normalize_name(s: str) -> str:
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()
    return s

NAME_FIX = {
    "cote d ivoire": "cote d ivoire",
    "palestine state of": "state of palestine",
    "syrian arab republic": "syrian arab republic",
    "congo democratic republic of the": "democratic republic of the congo",
    "congo republic of the": "republic of the congo",
    "korea republic of": "south korea",
    "korea democratic people s republic of": "north korea",
    "russian federation": "russia",
    "iran islamic republic of": "iran",
    "viet nam": "vietnam",
    "eswatini": "eswatini",
}
def _extract_inout_from_xlsx(xlsx_bytes: bytes) -> pd.DataFrame:
    """
    Lê o XLSX da UN DESA (Destination and origin) e devolve m49/year/immigrants/emigrants.
    Usa a Table 1 (1990..último; ambos os sexos).
    """
    x = pd.ExcelFile(io.BytesIO(xlsx_bytes))

    # escolher a folha (preferir "Table 1")
    sheet = next((s for s in x.sheet_names if re.search(r"table\s*1", str(s), re.I)), x.sheet_names[0])

    # detetar a linha do header (procura pelos dois "Location code ...")
    probe = pd.read_excel(x, sheet_name=sheet, header=None, nrows=30)
    header_row = None
    for i in range(min(25, probe.shape[0])):
        vals = probe.iloc[i].astype(str).tolist()
        if any("Location code of destination" in v for v in vals) and any("Location code of origin" in v for v in vals):
            header_row = i
            break
    if header_row is None:
        header_row = 10  # fallback

    df = pd.read_excel(x, sheet_name=sheet, header=header_row)

    # localizar nomes das colunas (tolerante)
    def _find(col_pattern: str) -> str:
        for c in df.columns:
            if col_pattern.lower() in str(c).lower():
                return c
        raise KeyError(f"Coluna não encontrada: {col_pattern}")

    dest_code_col = _find("Location code of destination")
    orig_code_col = _find("Location code of origin")

    # anos 'both sexes' = colunas que são inteiros (1990, 1995, …, 2024)
    year_cols = [c for c in df.columns if isinstance(c, int)]
    WORLD = 900

    # Imigrantes: destino = país, origem = Mundo
    imm = (
        df[df[orig_code_col] == WORLD]
        .rename(columns={
            "Region, development group, country or area of destination": "country",
            dest_code_col: "m49",
        })
        [["m49", "country"] + year_cols]
        .melt(id_vars=["m49", "country"], var_name="year", value_name="immigrants")
    )

    # Emigrantes: destino = Mundo, origem = país
    emi = (
        df[df[dest_code_col] == WORLD]
        .rename(columns={
            "Region, development group, country or area of origin": "country",
            orig_code_col: "m49",
        })
        [["m49", "country"] + year_cols]
        .melt(id_vars=["m49", "country"], var_name="year", value_name="emigrants")
    )

    out = pd.merge(imm, emi, on=["m49", "country", "year"], how="outer")
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype(int)
    out["m49"] = pd.to_numeric(out["m49"], errors="coerce").astype("Int64")
    out["immigrants"] = pd.to_numeric(out["immigrants"], errors="coerce")
    out["emigrants"]  = pd.to_numeric(out["emigrants"],  errors="coerce")
    return out.sort_values(["m49", "year"])

def _pick_col(df, *cands):
    low = {str(c).strip().lower(): c for c in df.columns}
    for n in cands:
        k = str(n).strip().lower()
        if k in low:
            return low[k]
    # fallback: remove não-alfanum (ex.: "ISO 3166-1 alpha-3 code")
    def _norm(s): return "".join(ch for ch in str(s).lower() if ch.isalnum())
    low2 = {_norm(c): c for c in df.columns}
    for n in cands:
        k = _norm(n)
        if k in low2:
            return low2[k]
    return None

def _load_un_official_map(path: Path) -> dict[int, str]:
    if not path.exists():
        return {}
    df = pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")
    df.columns = df.columns.str.replace("\ufeff","", regex=False).str.strip()

    # tenta vários cabeçalhos comuns da UN
    m49c = _pick_col(df, "M49 code", "M49", "UN M49", "m49", "code")
    i3c  = _pick_col(df, "ISO-alpha3 code", "ISO 3166-1 alpha-3 code",
                        "alpha-3", "iso3", "ISO3", "Alpha-3 code")
    if not m49c or not i3c:
        _log(f"⚠️ Mapeamento UN sem colunas reconhecíveis. Colunas: {list(df.columns)}")
        return {}

    df[m49c] = pd.to_numeric(df[m49c], errors="coerce").astype("Int64")
    df[i3c]  = df[i3c].astype(str).str.upper().str.strip()
    df = df.dropna(subset=[m49c, i3c]).drop_duplicates(subset=[m49c])
    return dict(df[[m49c, i3c]].itertuples(index=False, name=None))


# ========================= Main ==========================
def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="força novo download (ignora cache)")
    ap.add_argument("--keep-xlsx", action="store_true", help="guarda o xlsx/zip lido em data/")
    ap.add_argument("--source", type=str, default="", help="ficheiro local (xlsx/zip/csv) a usar")
    ap.add_argument("--no-download", action="store_true", help="não tentar internet (usar apenas --source)")
    args = ap.parse_args()

    xlsx_bytes: Optional[bytes] = None
    csv_bytes: Optional[bytes]  = None

    if args.source:
        p = Path(args.source)
        if not p.exists():
            print(f"❌ --source não encontrado: {p}"); sys.exit(2)
        _log(f"[LOCAL] {p.name}")
        blob = p.read_bytes()
        xlsx_bytes, csv_bytes = _read_container(blob)
    else:
        if CACHE_XLSX.exists() and not args.force:
            _log(f"[cache] a usar {CACHE_XLSX.name}")
            xlsx_bytes = CACHE_XLSX.read_bytes()
        else:
            if args.no_download:
                print("❌ Sem --source e com --no-download. Fornece um ficheiro local."); sys.exit(2)
            _log("[web] a procurar link de download na UN DESA…")
            dl = _find_download_url(BASE_URL, max_depth=2)
            if not dl:
                print("❌ Não encontrei o link 'Destination and origin'. Abre a página e descarrega manualmente esse ficheiro."); sys.exit(2)
            _log(f"[web] a descarregar: {dl}")
            blob = _fetch(dl, timeout=180)
            if not blob:
                print("❌ Download falhou. Usa --source com o ficheiro local."); sys.exit(2)
            xlsx_bytes, csv_bytes = _read_container(blob)
            if args.keep_xlsx and xlsx_bytes:
                CACHE_XLSX.write_bytes(xlsx_bytes)
                _log(f"[cache] guardei cópia em {CACHE_XLSX}")

    # Extrai (aceita xlsx ou csv 'long')
    if xlsx_bytes:
        df = _extract_inout_from_xlsx(xlsx_bytes)
    elif csv_bytes:
        df = _extract_inout_from_csv(csv_bytes)
    else:
        print("❌ Conteúdo descarregado não é XLSX nem CSV utilizável."); sys.exit(2)

    # escreve m49
    OUT_INOUT_M49.write_text("", encoding="utf-8")
    df.to_csv(OUT_INOUT_M49, index=False, sep=";", encoding="utf-8")
    _log(f"✔️ Escrevi {OUT_INOUT_M49} ({len(df):,} linhas)")

    # mapear para ISO3 (robusto): 1) M49→ISO3, 2) fallback por NOME
    # — construir mapping ISO3 (prioridade: UN oficial) —
    m49_to_iso3 = _load_un_official_map(MAP_UN_OFFICIAL)
    if m49_to_iso3:
        _log(f"[map] a usar {MAP_UN_OFFICIAL.name} (UN oficial M49→ISO3)")
    else:
        # fallbacks (se quiseres manter)
        _log(f"[map] {MAP_UN_OFFICIAL.name} não encontrado/sem colunas — a tentar iso_m49_map.csv / seed")
        try:
            m49_to_iso3, name_to_iso3 = _load_iso_m49_map(MAP_PATH)          # se tiveres este
        except NameError:
            m49_to_iso3, name_to_iso3 = ({}, {})
        if not m49_to_iso3:
            try:
                m49_to_iso3, name_to_iso3 = _m49_to_iso3_from_seed(SEED_PATH)  # e este
            except NameError:
                m49_to_iso3, name_to_iso3 = ({}, {})
            if not m49_to_iso3:
                _log("⚠️ Sem mapeamento M49→ISO3. Fica apenas o migration_inout_m49.csv.")
                return

    # — aplicar mapping (apenas M49→ISO3) —
    merged = df.copy()  # df: [m49,country,year,immigrants,emigrants]
    merged["iso3"] = merged["m49"].map(m49_to_iso3)

    # — escrever ISO3 —
    out = (
        merged.dropna(subset=["iso3"])[["iso3", "year", "immigrants", "emigrants"]]
        .assign(
            iso3=lambda d: d["iso3"].astype(str).str.upper().str.strip(),
            year=lambda d: pd.to_numeric(d["year"], errors="coerce"),
        )
        .dropna(subset=["year"])
        .sort_values(["iso3", "year"])
    )
    OUT_INOUT_ISO3.write_text("", encoding="utf-8")
    out.to_csv(OUT_INOUT_ISO3, index=False, sep=";", encoding="utf-8")
    _log(f"✔️ Escrevi {OUT_INOUT_ISO3} ({len(out):,} linhas)")

    faltas = merged[merged["iso3"].isna()][["m49","country"]].drop_duplicates()
    if not faltas.empty:
        _log(f"ℹ️ M49 sem ISO3 no mapa UN: {len(faltas)} (p.ex.)")
        _log(faltas.head(10).to_string(index=False))

    else:
        _log("⚠️ Sem mapeamento M49→ISO3 (countries_seed.csv). Usa migration_inout_m49.csv ou fornece seed com m49/iso3.")

if __name__ == "__main__":
    main()

