# -*- coding: utf-8 -*-
from __future__ import annotations
from functools import lru_cache

_BN_HEADERS = {"User-Agent": "GeoApp/1.0 (+https://github.com/)"}
_BN_BASE = "https://www.bandeirasnacionais.com"
_BN_SLUG_FIX = {
    "Côte d'Ivoire": "costa-do-marfim","Costa do Marfim": "costa-do-marfim",
    "Cabo Verde": "cabo-verde","São Tomé e Príncipe": "sao-tome-e-principe",
    "Guiné-Bissau": "guine-bissau","Timor-Leste": "timor-leste","Micronésia": "micronesia",
    "Eswatini": "essuatini","Suazilândia": "essuatini","Reino Unido": "reino-unido",
    "Estados Unidos": "estados-unidos",
}

def _slugify_pt(name_pt: str) -> str:
    import re, unicodedata
    s = unicodedata.normalize("NFKD", name_pt).encode("ascii","ignore").decode("ascii")
    s = re.sub(r"[^\w\s-]", "", s, flags=re.U).strip().lower()
    s = re.sub(r"[\s_]+", "-", s)
    return _BN_SLUG_FIX.get(name_pt, s)

@lru_cache(maxsize=256)
def load_flag_info(name_pt: str) -> dict:
    """
    Lê a página de um país em bandeirasnacionais.com e devolve meta-informação.
    Imports de rede feitos localmente (evita custo global).
    """
    import requests
    from bs4 import BeautifulSoup

    slug = _slugify_pt(name_pt or "")
    url  = f"{_BN_BASE}/{slug}"
    try:
        r = requests.get(url, headers=_BN_HEADERS, timeout=10)
        r.raise_for_status()
    except Exception:
        return {"url": url, "ok": False, "html": ""}

    soup = BeautifulSoup(r.text, "html.parser")
    # devolve só o essencial; o resto da extração fica como quiseres evoluir
    h1 = (soup.find("h1") or {}).get_text(strip=True) if soup.find("h1") else ""
    return {"url": url, "ok": True, "title": h1}
