# -*- coding: utf-8 -*-
"""
fetch_gastronomia.py

Extrai itens de gastronomia a partir de ficheiros HTML locais do TasteAtlas
(um por país) e gera data/gastronomia.csv.

Lógica:
- Percorre TODOS os ficheiros .html em DATA_DIR/htmls
- Para cada HTML:
    * extrai o "nome do país" a partir do <h1> (ou <title> como fallback)
      (faz html.unescape para resolver entidades tipo Cura&#231;ao, S&#227;o Tom&#233;)
    * tenta encontrar o país em countries_profiles_with_en.csv:
        - primeiro pela coluna 'name_pt'
        - depois pela coluna 'name_en'
        - depois por alguns casos especiais (Turkye, East Timor, the United States of America, ...)
        - depois, opcionalmente, por heurística fuzzy muito conservadora
    * se encontrar, obtém iso3 e name_pt (nome canónico em PT)
    * se não encontrar, ignora esse HTML (não grava linhas sem iso3)
    * extrai blocos JSON com "Name" (itens de gastronomia) e respetiva info
      (UrlLink, ranking, score, critics)
- Gera data/gastronomia.csv com as colunas:
    iso3;country;item;url_slug;ranking;score;critics;source

Requisitos:
    pip install pandas
"""

from __future__ import annotations

import csv
import json
import re
import unicodedata
import html
from pathlib import Path
from typing import List, Dict, Any, Optional

import pandas as pd


# ------------------------------------------------------------
# Caminhos base
# ------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"

# Ajusta aqui se o ficheiro tiver outro nome no teu projeto
PROFILES_PATH = DATA_DIR / "countries_profiles_with_en.csv"
HTML_DIR      = DATA_DIR / "htmls"
OUTPUT_PATH   = DATA_DIR / "gastronomia.csv"


# ------------------------------------------------------------
# Helpers de normalização e carregamento
# ------------------------------------------------------------

def _normalize(s: str) -> str:
    """Normaliza string para comparação frouxa (sem acentos, minúsculas, só alfanumérico)."""
    if not isinstance(s, str):
        s = str(s or "")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def _load_profiles() -> pd.DataFrame:
    """
    Carrega countries_profiles_with_en.csv e prepara colunas normalizadas
    para matching (name_pt, name_en).
    """
    if not PROFILES_PATH.exists():
        raise SystemExit(f"[erro] Ficheiro {PROFILES_PATH} não existe.")

    df = pd.read_csv(PROFILES_PATH, sep=";", dtype=str)

    missing = [c for c in ("iso3", "name_pt", "name_en") if c not in df.columns]
    if missing:
        raise SystemExit(f"[erro] {PROFILES_PATH.name} não tem as colunas obrigatórias: {missing}")

    df["iso3"]    = df["iso3"].fillna("").str.upper()
    df["name_pt"] = df["name_pt"].fillna("")
    df["name_en"] = df["name_en"].fillna("")

    df["name_norm_pt"] = df["name_pt"].apply(_normalize)
    df["name_norm_en"] = df["name_en"].apply(_normalize)

    return df


def _build_profiles_index(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Cria índices de lookup a partir de countries_profiles_with_en:
      - por nome PT normalizado
      - por nome EN normalizado
      - por iso3
      - lista de linhas para eventual fuzzy

    Retorna:
        {
          "pt":   {norm_pt: {...}},
          "en":   {norm_en: {...}},
          "iso3": {iso3: {...}},
          "rows": [ {...}, ... ]
        }
    """
    idx_pt: Dict[str, Dict[str, str]] = {}
    idx_en: Dict[str, Dict[str, str]] = {}
    idx_iso3: Dict[str, Dict[str, str]] = {}
    rows: List[Dict[str, str]] = []

    for _, row in df.iterrows():
        iso3     = row["iso3"]
        name_pt  = row["name_pt"]
        name_en  = row["name_en"]
        norm_pt  = row["name_norm_pt"]
        norm_en  = row["name_norm_en"]

        base = {
            "iso3": iso3,
            "country_pt": name_pt,
            "name_en": name_en,
            "name_norm_pt": norm_pt,
            "name_norm_en": norm_en,
        }
        rows.append(base)

        if iso3 and iso3 not in idx_iso3:
            idx_iso3[iso3] = base

        if norm_pt:
            if norm_pt in idx_pt and idx_pt[norm_pt]["iso3"] != iso3:
                print(f"[warn] name_pt normalizado '{norm_pt}' com iso3 duplicado: "
                      f"{idx_pt[norm_pt]['iso3']} vs {iso3}")
            idx_pt.setdefault(norm_pt, base)

        if norm_en:
            if norm_en in idx_en and idx_en[norm_en]["iso3"] != iso3:
                print(f"[warn] name_en normalizado '{norm_en}' com iso3 duplicado: "
                      f"{idx_en[norm_en]['iso3']} vs {iso3}")
            idx_en.setdefault(norm_en, base)

    return {"pt": idx_pt, "en": idx_en, "iso3": idx_iso3, "rows": rows}


# Casos especiais onde o nome do HTML não bate com name_en diretamente
# (podes acrescentar/ajustar esta tabela consoante as tuas necessidades)
SPECIAL_MAP_NORM_TO_ISO3: Dict[str, str] = {
    # TasteAtlas usa "Turkye" neste dataset
    "turkye": "TUR",
    # TasteAtlas usa "East Timor"
    "easttimor": "TLS",
    # TasteAtlas usa "the United States of America"
    "theunitedstatesofamerica": "USA",
    # Se quiseres mapear explicitamente Palestine -> PSE, podes descomentar:
    # "palestine": "PSE",
}


# ------------------------------------------------------------
# Ler HTMLs e extrair país + gastronomia
# ------------------------------------------------------------

def _extract_country_from_html(path: Path) -> str:
    """
    Extrai o nome do país a partir do HTML:
      1) Primeiro <h1>
      2) Se falhar, <title>
    Aplica heurística para casos "Eat Local in Portugal" -> "Portugal".
    Faz html.unescape para converter entidades numéricas (&amp;#231; -> ç, etc.).
    """
    txt = path.read_text(encoding="utf-8", errors="ignore")

    # tentar <h1>
    m = re.search(r"<h1[^>]*>(.*?)</h1>", txt, re.IGNORECASE | re.DOTALL)
    if m:
        inner = re.sub(r"<.*?>", "", m.group(1))
        raw = inner.strip()
    else:
        # fallback: <title>
        m = re.search(r"<title>(.*?)</title>", txt, re.IGNORECASE | re.DOTALL)
        if not m:
            return ""
        inner = re.sub(r"<.*?>", "", m.group(1))
        raw = inner.strip()

    # Desfazer entidades HTML (Cura&#231;ao -> Curaçao)
    raw = html.unescape(raw)

    # heurística simples: se tiver " in X", usa o que vem depois do último " in "
    # ex: "Eat Local in Portugal" -> "Portugal"
    parts = raw.rsplit(" in ", 1)
    if len(parts) == 2 and parts[1].strip():
        return parts[1].strip()

    return raw


def _match_country(profiles_index: Dict[str, Any], country_raw: str) -> Optional[Dict[str, str]]:
    """
    Tenta casar o nome do país do HTML com countries_profiles_with_en.

    Estratégia:
      1) SPECIAL_MAP (Turkye, East Timor, the United States of America, ...)
      2) matching direto por name_pt (PT)
      3) matching direto por name_en (EN)
      4) fallback fuzzy muito conservador:
           - se norm_html for sufixo de name_norm_en OU
             name_norm_en for sufixo de norm_html
           - OU se partilharem um prefixo >= 4 caracteres e o name_en tiver
             termos como 'republic', 'federation', 'kingdom', 'state'
         e o match for único.

    Devolve:
        {"iso3": ..., "country_pt": ..., "name_en": ..., ...}
    ou None se nada for encontrado.
    """
    idx_pt  = profiles_index["pt"]
    idx_en  = profiles_index["en"]
    idx_i3  = profiles_index["iso3"]
    rows    = profiles_index["rows"]

    norm_html = _normalize(country_raw)

    # 1) Casos especiais
    if norm_html in SPECIAL_MAP_NORM_TO_ISO3:
        iso3 = SPECIAL_MAP_NORM_TO_ISO3[norm_html]
        base = idx_i3.get(iso3)
        if base:
            print(f"[info] match SPECIAL_MAP: '{country_raw}' -> {base['name_en']} ({iso3})")
            return base
        # se não encontrarmos iso3 no CSV, continua para as outras estratégias

    # 2) match direto PT
    if norm_html in idx_pt:
        return idx_pt[norm_html]

    # 3) match direto EN
    if norm_html in idx_en:
        return idx_en[norm_html]

    # 4) fallback fuzzy em EN (muito conservador)
    def _common_prefix_len(a: str, b: str) -> int:
        n = 0
        for ch1, ch2 in zip(a, b):
            if ch1 == ch2:
                n += 1
            else:
                break
        return n

    candidates: List[Dict[str, str]] = []
    for r in rows:
        en_norm = r["name_norm_en"]
        if not en_norm:
            continue

        # 4.1) sufixos (ex: yemen ↔ republicofyemen, se tivesses esse caso)
        cond_suffix = en_norm.endswith(norm_html) or norm_html.endswith(en_norm)

        # 4.2) prefixo comum >= 4 + termos políticos (ex: italy ↔ italianrepublic)
        common_prefix_len = _common_prefix_len(norm_html, en_norm)
        cond_prefix = (
            common_prefix_len >= 4
            and any(word in en_norm for word in ("republic", "federation", "kingdom", "state"))
        )

        if cond_suffix or cond_prefix:
            candidates.append(r)

    if len(candidates) == 1:
        c = candidates[0]
        print(f"[info] match fuzzy EN: '{country_raw}' -> {c['name_en']} ({c['iso3']})")
        return c

    if len(candidates) > 1:
        # demasiado ambíguo (ex: 'Congo' poderia dar 2 países se tivesses COD e COG)
        print(f"[warn] match fuzzy EN ambíguo para '{country_raw}', {len(candidates)} candidatos; a ignorar.")
        return None

    return None


def _extract_arrays_with_name(text: str, max_arrays: int = 200) -> List[str]:
    """
    Procura blocos JSON do tipo:

        [{"Name":"...", "UrlLink":"...", ...}, {...}, ...]

    dentro do HTML e devolve a string JSON de cada array.
    Faz contagem de colchetes para apanhar o ']' correto.
    """
    arrays: List[str] = []
    pos = 0
    n = len(text)
    pattern = re.compile(r'\[\s*\{"Name"\s*:', re.MULTILINE)

    while len(arrays) < max_arrays and pos < n:
        m = pattern.search(text, pos)
        if not m:
            break
        start = m.start()
        depth = 0
        end = None
        for i, ch in enumerate(text[start:], start):
            if ch == "[":
                depth += 1
            elif ch == "]":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        if end is None:
            break
        arr_str = text[start:end + 1]
        arrays.append(arr_str)
        pos = end + 1

    return arrays


def _extract_items_from_html(path: Path) -> List[Dict[str, str]]:
    """
    Extrai itens de gastronomia de um HTML local do TasteAtlas.
    Devolve lista de dicts:
        {"item", "url_slug", "ranking", "score", "critics"}
    """
    txt = path.read_text(encoding="utf-8", errors="ignore")
    arrays = _extract_arrays_with_name(txt)

    items: List[Dict[str, str]] = []

    for arr in arrays:
        try:
            data = json.loads(arr)
        except Exception:
            continue
        if not isinstance(data, list) or not data or not isinstance(data[0], dict):
            continue
        if "Name" not in data[0]:
            continue

        for obj in data:
            if not isinstance(obj, dict):
                continue
            name = (obj.get("Name") or "").strip()
            if not name:
                continue

            d = {
                "item": name,
                "url_slug": str(obj.get("UrlLink") or ""),
                "ranking": str(obj.get("PrecalculatedWorldRanking") or ""),
                "score": str(obj.get("DividedScore") or ""),
                "critics": str(obj.get("NoOfCritics") or ""),
            }
            items.append(d)

    # deduplicar por nome (case-insensitive)
    dedup: Dict[str, Dict[str, str]] = {}
    for d in items:
        key = d["item"].lower()
        if key not in dedup:
            dedup[key] = d

    return list(dedup.values())


# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------

def main() -> None:
    # 1) Carregar perfis de países e índices
    df_profiles = _load_profiles()
    profiles_index = _build_profiles_index(df_profiles)

    # 2) Percorrer TODOS os HTMLs em HTML_DIR
    if not HTML_DIR.exists():
        raise SystemExit(f"[erro] Pasta de HTMLs não existe: {HTML_DIR}")

    html_files = sorted(HTML_DIR.glob("*.html"))
    print(f"[info] Encontrados {len(html_files)} ficheiros HTML em {HTML_DIR}")

    all_rows: Dict[tuple, Dict[str, str]] = {}  # (iso3, item_lower) -> row

    for path in html_files:
        # ignorar páginas globais tipo "100 Best Dishes..."
        if not path.name.lower().startswith("eat local in".lower()):
            print(f"[skip] A ignorar ficheiro não-país: {path.name}")
            continue

        country_raw = _extract_country_from_html(path)
        if not country_raw:
            print(f"[warn] Não foi possível extrair nome de país de {path.name}")
            continue

        match = _match_country(profiles_index, country_raw)
        if not match:
            print(f"[warn] País '{country_raw}' (HTML: {path.name}) não encontrado em {PROFILES_PATH.name}; a ignorar este ficheiro.")
            continue

        iso3        = match["iso3"]
        country_pt  = match["country_pt"]

        if not iso3:
            print(f"[warn] País '{country_raw}' (HTML: {path.name}) com iso3 vazio; a ignorar itens deste ficheiro.")
            continue

        print(f"[country] {iso3} {country_pt}  ← {path.name}")

        items = _extract_items_from_html(path)
        if not items:
            print(f"    [info] Nenhum item de gastronomia encontrado em {path.name}")
            continue

        print(f"    [info] {len(items)} itens extraídos")

        for d in items:
            item_name = d["item"].strip()
            if not item_name:
                continue
            key_row = (iso3, item_name.lower())
            all_rows[key_row] = {
                "iso3": iso3,
                "country": country_pt,
                "item": item_name,
                "url_slug": d["url_slug"],
                "ranking": d["ranking"],
                "score": d["critics"],
                "critics": d["critics"],
                "source": "tasteatlas_html",
            }

    # 3) Converter para lista ordenada
    final_rows = list(all_rows.values())
    final_rows.sort(key=lambda r: (r["iso3"], r["country"], r["item"].lower()))

    # 4) Gravar CSV
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["iso3", "country", "item", "url_slug", "ranking", "score", "critics", "source"])
        for r in final_rows:
            writer.writerow([
                r["iso3"],
                r["country"],
                r["item"],
                r["url_slug"],
                r["ranking"],
                r["score"],
                r["critics"],
                r["source"],
            ])

    print(f"\n✔ Concluído. Escreveu {len(final_rows)} registos em {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
