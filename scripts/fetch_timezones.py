# -*- coding: utf-8 -*-
"""
fetch_timezones.py
Gera data/timezones.csv com labels PT/EN + lista de timezones por país e offsets UTC atuais.

Fontes:
- Wikidata (labels, qid, ISO2/ISO3) — em batches, com SERVICE wikibase:label "pt,en"
- IANA tzdb zone1970.tab — mapeamento ISO2 -> TZ IDs
- zoneinfo (stdlib) — cálculo do UTC offset atual por TZ

Saída: data/timezones.csv (sep=';')
"""

from __future__ import annotations
from pathlib import Path
import csv
import sys
import time
import requests
from typing import Dict, List, Tuple
from datetime import datetime, timezone, timedelta
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None  # fallback simples (não deverá acontecer no teu setup)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
PROFILES_CSV = DATA_DIR / "countries_profiles.csv"
OUT_CSV      = DATA_DIR / "timezones1.csv"

UA = "GeografiaApp/1.0 (+https://cfmessias.pt)"
WDQS_URL = "https://query.wikidata.org/sparql"
IANA_URL_PRIMARY = "https://data.iana.org/time-zones/tzdb/zone1970.tab"
IANA_URL_FALLBACK = "https://raw.githubusercontent.com/eggert/tz/main/zone1970.tab"

CHUNK = 40   # tamanho dos batches para WDQS
RETRIES = 5
TIMEOUT = 90

def read_profiles(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        print(f"[erro] Não encontrei {path}", file=sys.stderr)
        sys.exit(1)
    sep = ";"
    sample = path.read_text(encoding="utf-8", errors="ignore")[:4096]
    if "," in sample and sample.count(",") > sample.count(";"):
        sep = ","
    rows = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=sep)
        for r in reader:
            rows.append({k: (v or "").strip() for k, v in r.items()})
    return rows

def chunked(iterable: List, n: int) -> List[List]:
    return [iterable[i:i+n] for i in range(0, len(iterable), n)]

def wdqs_labels_for_iso3(iso3_list: List[str]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for batch in chunked([x for x in iso3_list if x], CHUNK):
        values = " ".join(f'"{x}"' for x in batch)
        query = f"""
SELECT ?country ?iso2 ?iso3 ?countryLabel ?countryLabel_pt ?countryLabel_en WHERE {{
  VALUES ?iso3 {{ {values} }}
  ?country wdt:P298 ?iso3 .
  OPTIONAL {{ ?country wdt:P297 ?iso2 . }}

  SERVICE wikibase:label {{
    bd:serviceParam wikibase:language "pt,en".
    ?country rdfs:label ?countryLabel .
  }}
  OPTIONAL {{
    ?country rdfs:label ?countryLabel_pt .
    FILTER(LANG(?countryLabel_pt) = "pt")
  }}
  OPTIONAL {{
    ?country rdfs:label ?countryLabel_en .
    FILTER(LANG(?countryLabel_en) = "en")
  }}
}}
"""
        for attempt in range(1, RETRIES+1):
            try:
                r = requests.get(
                    WDQS_URL,
                    params={"query": query, "format": "json"},
                    headers={"User-Agent": UA},
                    timeout=TIMEOUT
                )
                r.raise_for_status()
                data = r.json()
                for b in data.get("results", {}).get("bindings", []):
                    country_uri = b.get("country", {}).get("value", "")
                    qid = country_uri.split("/")[-1] if country_uri else ""
                    iso3 = (b.get("iso3", {}) or {}).get("value", "").upper()
                    iso2 = (b.get("iso2", {}) or {}).get("value", "").upper()
                    lbl_pt = (b.get("countryLabel_pt", {}) or {}).get("value", "")
                    lbl_en = (b.get("countryLabel_en", {}) or {}).get("value", "")
                    generic = (b.get("countryLabel", {}) or {}).get("value", "")
                    if not lbl_pt and b.get("countryLabel", {}).get("xml:lang") == "pt":
                        lbl_pt = generic
                    if not lbl_en and b.get("countryLabel", {}).get("xml:lang") == "en":
                        lbl_en = generic
                    if iso3:
                        out[iso3] = {
                            "qid": qid,
                            "iso3": iso3,
                            "iso2": iso2,
                            "country_pt": lbl_pt or "",
                            "country_en": lbl_en or "",
                        }
                break
            except requests.RequestException as e:
                if attempt < RETRIES:
                    print(f"[warn] WDQS batch falhou ({attempt}/{RETRIES}): {e}", file=sys.stderr)
                    time.sleep(2 * attempt)
                    continue
                else:
                    print(f"[erro] WDQS batch falhou em definitivo: {e}", file=sys.stderr)
    return out

def fetch_iana_zone1970() -> str:
    for url in (IANA_URL_PRIMARY, IANA_URL_FALLBACK):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT)
            r.raise_for_status()
            return r.text
        except requests.RequestException as e:
            print(f"[warn] Falha ao obter {url}: {e}", file=sys.stderr)
    print("[erro] Não foi possível obter o zone1970.tab da IANA.", file=sys.stderr)
    sys.exit(1)

def parse_zone1970_tab(text: str) -> Dict[str, List[str]]:
    cc_to_tz: Dict[str, List[str]] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        cc_field, _coords, tzid = parts[0], parts[1], parts[2]
        for cc in cc_field.split(","):
            cc = cc.strip().upper()
            if not cc:
                continue
            cc_to_tz.setdefault(cc, [])
            if tzid not in cc_to_tz[cc]:
                cc_to_tz[cc].append(tzid)
    return cc_to_tz

def format_offset(td: timedelta | None) -> str:
    if td is None:
        return "UTC±00:00"
    # arredonda a minutos (dst/half-hour zones incluídas)
    total_seconds = int(td.total_seconds())
    sign = "+" if total_seconds >= 0 else "-"
    total_seconds = abs(total_seconds)
    hours, rem = divmod(total_seconds, 3600)
    minutes, _ = divmod(rem, 60)
    return f"UTC{sign}{hours:02d}:{minutes:02d}"

def offsets_now_for_tzids(tzids: List[str]) -> Tuple[str, str]:
    """
    Devolve:
    - offsets únicos (ordenados pela primeira ocorrência): 'UTC+00:00, UTC+01:00'
    - pares tz=offset: 'Europe/Lisbon=UTC+00:00, Atlantic/Azores=UTC-01:00'
    """
    if not tzids:
        return "", ""
    if ZoneInfo is None:
        # fallback: sem zoneinfo não calculamos; devolvemos vazio
        return "", ""
    now_utc = datetime.now(timezone.utc)
    seen = []
    pairs = []
    for tz in tzids:
        try:
            loc = now_utc.astimezone(ZoneInfo(tz))
            off = format_offset(loc.utcoffset())
        except Exception:
            off = ""
        if off and off not in seen:
            seen.append(off)
        pairs.append(f"{tz}={off}" if off else f"{tz}=")
    return ", ".join(seen), ", ".join(pairs)

def main() -> None:
    print("[fetch_timezones] A preparar perfis…")
    profs = read_profiles(PROFILES_CSV)
    iso3_list = []
    for r in profs:
        iso3 = (r.get("iso3") or r.get("ISO3") or r.get("Iso3") or "").strip().upper()
        if iso3:
            iso3_list.append(iso3)
    iso3_list = sorted(set(iso3_list))
    if not iso3_list:
        print("[erro] Não encontrei ISO3 em countries_profiles.csv", file=sys.stderr)
        sys.exit(1)

    print(f"[fetch_timezones] A obter labels PT/EN da Wikidata… (n={len(iso3_list)})")
    meta_by_iso3 = wdqs_labels_for_iso3(iso3_list)

    print("[fetch_timezones] A descarregar IANA zone1970.tab…")
    ztext = fetch_iana_zone1970()
    cc_to_tz = parse_zone1970_tab(ztext)

    rows_out: List[Dict[str, str]] = []
    for iso3 in iso3_list:
        meta = meta_by_iso3.get(iso3, {})
        qid   = meta.get("qid", "")
        iso2  = meta.get("iso2", "")
        c_pt  = meta.get("country_pt", "")
        c_en  = meta.get("country_en", "")

        tz_list = cc_to_tz.get(iso2, []) if iso2 else []
        tz_list = sorted(tz_list)
        tz_str = ", ".join(tz_list)

        utc_list, tz_pairs = offsets_now_for_tzids(tz_list)

        rows_out.append({
            "iso3": iso3,
            "qid": qid,
            "country_pt": c_pt,
            "country_en": c_en,
            "iso2": iso2,
            "timezones": tz_str,                 # IDs IANA
            "tz_with_offsets_now": tz_pairs,     # Europe/Lisbon=UTC+00:00, Atlantic/Azores=UTC-01:00
            "utc_offsets_now": utc_list,         # offsets únicos atuais
            "tz_count": str(len(tz_list)),
        })

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "iso3","qid","country_pt","country_en","iso2",
                "timezones","tz_with_offsets_now","utc_offsets_now","tz_count"
            ],
            delimiter=";"
        )
        writer.writeheader()
        writer.writerows(rows_out)

    print(f"[ok] Escrito {OUT_CSV} | países: {len(rows_out)}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[info] Interrompido pelo utilizador.")
        sys.exit(130)
