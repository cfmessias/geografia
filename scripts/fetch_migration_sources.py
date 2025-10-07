# scripts/fetch_migration_sources.py
from __future__ import annotations
import os, sys, re, io, time, json, zipfile
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, List
import pandas as pd
import requests

BASE = Path(__file__).resolve().parents[1]
RAW  = BASE / "data" / "raw"
OUT  = BASE / "data" / "migration"
RAW.mkdir(parents=True, exist_ok=True)
OUT.mkdir(parents=True, exist_ok=True)

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Python-requests/2.x"}

def save_csv(df: pd.DataFrame, path: Path) -> None:
    if df is None or df.empty:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")

def safe_get(url: str, *, accept: Optional[str]=None, params: Dict[str, Any]|None=None, stream=False, retries=2, timeout=60):
    headers = dict(UA)
    if accept:
        headers["Accept"] = accept
    last_err = None
    for i in range(retries+1):
        try:
            r = requests.get(url, headers=headers, params=params, stream=stream, timeout=timeout)
            if r.status_code == 403:
                # tenta com um Referer genérico
                headers["Referer"] = "https://www.google.com/"
                time.sleep(1.0)
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            time.sleep(1.0)
    raise last_err

# ---------------- UN DESA (stock bilateral) ----------------
def try_un_desa_download() -> Optional[Path]:
    """
    Tenta descarregar o Excel de 2020 (OD matrix). Muitos servidores do UN bloqueiam requests.
    Se falhar, devolve None e o utilizador pode colocar manualmente em data/raw/UN_DESA_2020_OD.xlsx.
    """
    urls = [
        # ligações que por vezes dão 403 em requests:
        "https://www.un.org/development/desa/pd/sites/default/files/2020-12/International%20Migration%202020%20Origin%20Destination%20Matrix.xlsx",
        "https://www.un.org/development/desa/pd/sites/default/files/2020-12/International%20Migration%202020%20Origin%20Destination%20Matrix%20(Users).xlsx",
    ]
    dest = RAW / "UN_DESA_2020_OD.xlsx"
    for u in urls:
        try:
            r = safe_get(u, accept="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", stream=True)
            with open(dest, "wb") as f:
                for chunk in r.iter_content(1<<20):
                    f.write(chunk)
            print(f"[ok] UN DESA Excel guardado em: {dest}")
            return dest
        except Exception as e:
            print(f"[warn] UN DESA falhou ({e}); a tentar próximo URL…")
    if dest.exists():
        return dest
    print(f"[info] Não foi possível descarregar automaticamente.\n      Coloca manualmente o Excel em: {dest}")
    return None

# --------------- UNICEF SDMX (stock por destino / por origem) ---------------
def unicef_migrant_stock():
    """
    Usa o SDMX da UNICEF (espelha o UN DESA 2024) para obter:
      - MG_INTNL_MG_CNTRY_DEST (migrantes internacionais por país de destino)
      - MG_INTNL_MG_CNTRY_ORIGIN (… por país de origem)
    Exporta 2 CSV em data/migration/.
    Docs gerais SDMX/UNICEF: https://sdmx.data.unicef.org/
    """
    base = "https://sdmx.data.unicef.org/ws/public/sdmxapi/rest/data"
    flows = [
        # (dataflow, filename)
        ("UNICEF,MG,1.0/MG_INTNL_MG_CNTRY_DEST", OUT / "unicef_migrant_stock_by_destination.csv"),
        ("UNICEF,MG,1.0/MG_INTNL_MG_CNTRY_ORIGIN", OUT / "unicef_migrant_stock_by_origin.csv"),
    ]
    for dfid, outpath in flows:
        # tentamos CSV “with labels”; se não der, tentamos CSV simples e por fim JSON→CSV
        q_common = {
            "dimensionAtObservation": "AllDimensions",
        }
        tried = []
        for fmt, accept in [
            ("csvfilewithlabels", "text/csv"),
            ("csvfile", "text/csv"),
        ]:
            url = f"{base}/{dfid}/.all"
            try:
                r = safe_get(url, params={**q_common, "format": fmt}, accept=accept, timeout=120)
                df = pd.read_csv(io.StringIO(r.text))
                if not df.empty:
                    save_csv(df, outpath)
                    print(f"[ok] UNICEF SDMX → {outpath.name}  (linhas={len(df)})")
                    break
            except Exception as e:
                tried.append(f"{fmt}: {e}")
        else:
            # fallback JSON
            try:
                url = f"{base}/{dfid}/.all"
                r = safe_get(url, params={**q_common, "format": "jsondata"}, accept="application/json", timeout=120)
                j = r.json()
                # tentar achatar estrutura SDMX-JSON -> DataFrame
                # as chaves variam; tentamos localizar 'dataSets' e 'series/observations'
                # abordagem simples: procurar arrays de observações; se falhar, gravar bruto
                data = []
                obs = j.get("dataSets", [{}])[0].get("observations", {})
                # precisamos dos mapeamentos dos índices -> labels
                dims = j.get("structure", {}).get("dimensions", {})
                series_dims = dims.get("series", [])
                obs_dims = dims.get("observation", [])
                series_keys = [d["id"] for d in series_dims]
                for s_key, s_vals in j.get("dataSets", [{}])[0].get("series", {}).items():
                    s_idx = [int(i) for i in s_key.split(":")] if s_key else []
                    s_map = {series_keys[i]: series_dims[i]["values"][s_idx[i]]["name"] for i in range(len(s_idx))}
                    for o_key, o_val in s_vals.get("observations", {}).items():
                        # o_key é o índice do período; o_val[0] é o valor
                        data.append({**s_map, "time": obs_dims[0]["values"][int(o_key)]["id"], "value": o_val[0]})
                if data:
                    df = pd.DataFrame(data)
                    save_csv(df, outpath)
                    print(f"[ok] UNICEF SDMX(JSON) → {outpath.name}  (linhas={len(df)})")
                else:
                    print(f"[warn] UNICEF SDMX(JSON) sem dados parseáveis para {dfid}. Tentativas: {tried}")
            except Exception as e:
                print(f"[warn] UNICEF SDMX falhou para {dfid}: {e}. Tentativas: {tried}")

# ---------------- OECD (fluxos) ----------------
def oecd_mig_b11() -> Optional[Path]:
    """
    OECD Data Explorer API (SDMX). B11 = inflows por nacionalidade/cidadania.
    Dimensões relevantes típicas: VAR=B11, GEN=TOT, YEA=*, COU=destino (país OECD), CO2=origem (nacionalidade).
    Exemplo de formato documentado: use 'format=csvfilewithlabels'.  (ver doc) 
    """
    # Dataflow + DSD (mapeamento novo OECD Data Explorer)
    base = "https://sdmx.oecd.org/public/rest/data"
    # Key minimalista: .B11.TOT...A  (deixamos COU e CO2 sem filtro para puxar tudo; pode ser pesado)
    key = ".B11.TOT...A"
    url = f"{base}/OECD.ELS.IMD,DSD_MIG@DF_MIG/{key}"
    params = {
        "dimensionAtObservation": "AllDimensions",
        "format": "csvfilewithlabels",
        # "lastNObservations": 10,  # se quiseres limitar os últimos N anos
    }
    try:
        r = safe_get(url, params=params, accept="text/csv", timeout=180)
        df = pd.read_csv(io.StringIO(r.text))
        out = OUT / "oecd_IMD_B11_inflows_by_nationality.csv"
        save_csv(df, out)
        print(f"[ok] OECD IMD(B11) → {out}  (linhas={len(df)})")
        return out
    except Exception as e:
        print(f"[warn] OECD IMD falhou ({e}). A API mudou em 2024; confirmar chaves no Data Explorer.")
        print("      Doc API & exemplo de CSV com labels: https://sdmx.oecd.org/public/rest/...  (ver guia)")
        return None

# ---------------- UNHCR (fluxos de asilo) ----------------
def unhcr_asylum_applications(year_from=2000, year_to=None):
    """
    UNHCR Refugee Statistics API — endpoint 'asylum-applications', com CSV via ?download=true.
    Campos principais: year, coo (country of origin), coa (country of asylum), value.
    Docs: https://api.unhcr.org/docs/refugee-statistics.html
    """
    if year_to is None:
        year_to = pd.Timestamp.today().year
    url = "https://api.unhcr.org/population/v1/asylum-applications/"
    params = {
        "yearFrom": year_from,
        "yearTo": year_to,
        "coo_all": "true",
        "coa_all": "true",
        "cf_type": "iso3",   # usa ISO3 em vez de códigos próprios UNHCR
        "download": "true",
    }
    try:
        r = safe_get(url, params=params, accept="text/csv", timeout=120)
        df = pd.read_csv(io.StringIO(r.text))
        out = OUT / "unhcr_asylum_applications_by_origin_destination.csv"
        save_csv(df, out)
        print(f"[ok] UNHCR asylum-applications → {out}  (linhas={len(df)})")
    except Exception as e:
        print(f"[warn] UNHCR asylum-applications falhou: {e}")

# ---------------- Eurostat (imigração por cidadania) ----------------
def eurostat_migr_imm1ctz():
    """
    Eurostat SDMX 2.1 — dataset migr_imm1ctz (immigration by age/sex/citizenship).
    A API SDMX nova está documentada; em alguns momentos pode devolver 503/erros temporários.
    Se falhar, indico o link para download manual do SDMX-CSV a partir do catálogo data.europa.eu.
    """
    # Padrão geral (SDMX 2.1): https://ec.europa.eu/eurostat/api/discoveries/...  (documentação)
    # Nem sempre está estável; tentamos a rota de SDMX-CSV via 'format=sdmx-csv' quando disponível.
    # Como fallback final, instruímos o download manual.
    try_urls = [
        # Exemplos típicos; podem variar conforme a instância:
        # Observação: muitos consumidores têm usado packages (eurostat em R) que escondem esta URL.
        "https://ec.europa.eu/eurostat/api/discoveries/tgm/table?code=migr_imm1ctz&format=sdmx-csv",
    ]
    for u in try_urls:
        try:
            r = safe_get(u, accept="text/csv", timeout=120)
            # Alguns endpoints devolvem SDMX-CSV gz/zip — detetar isso:
            ct = r.headers.get("Content-Type", "")
            if "zip" in ct or u.lower().endswith(".zip"):
                zf = zipfile.ZipFile(io.BytesIO(r.content))
                for name in zf.namelist():
                    if name.lower().endswith(".csv"):
                        df = pd.read_csv(zf.open(name))
                        out = OUT / "eurostat_migr_imm1ctz.csv"
                        save_csv(df, out)
                        print(f"[ok] Eurostat (zip) → {out}  (linhas={len(df)})")
                        return
            else:
                df = pd.read_csv(io.StringIO(r.text))
                out = OUT / "eurostat_migr_imm1ctz.csv"
                save_csv(df, out)
                print(f"[ok] Eurostat → {out}  (linhas={len(df)})")
                return
        except Exception as e:
            print(f"[warn] Eurostat falhou ({e}); a tentar próximo URL…")

    print("[info] Eurostat SDMX instável/alterado.")
    print("       Download manual (SDMX-CSV) a partir do catálogo:")
    print("       'Immigration by age group, sex and citizenship' (migr_imm1ctz) – botão 'Download dataset in SDMX-CSV format'.")
    print("       https://data.europa.eu/data/datasets/sqeeof5vrq0cxpbvmgr0a?locale=en")

def main():
    print("== MIGRATION SOURCES ==")
    # 1) UN DESA (Excel bilateral — tentativa + instruções)
    try_un_desa_download()

    # 2) UNICEF SDMX (stocks 2024 por destino/origem)
    unicef_migrant_stock()

    # 3) OECD IMD B11 (fluxos por nacionalidade)
    oecd_mig_b11()

    # 4) UNHCR (asylum applications flows)
    unhcr_asylum_applications(year_from=2000)

    # 5) Eurostat (imigração por cidadania)
    eurostat_migr_imm1ctz()

    print("\n[done] CSVs criados (quando possível) em:", OUT)

if __name__ == "__main__":
    main()
