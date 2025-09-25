# -*- coding: utf-8 -*-
import time
import requests

HTTP_HEADERS = {
    "User-Agent": "ClimaHistoricoApp/1.0 (contacto: user@example.com)"
}

def safe_get_json(url: str, params: dict, max_retries: int = 3, timeout: int = 45):
    """GET com retries e validação JSON. Lança RuntimeError com informação útil."""
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, params=params, headers=HTTP_HEADERS, timeout=timeout)
            r.raise_for_status()
            ctype = r.headers.get("Content-Type", "")
            if "application/json" not in ctype:
                snippet = (r.text or "")[:400]
                raise ValueError(f"Resposta não-JSON ({ctype}). Trecho: {snippet}")
            return r.json()
        except Exception as e:
            last_err = e
            if attempt < max_retries:
                time.sleep(0.6 * attempt)
            else:
                break
    raise RuntimeError(f"Falha ao obter JSON de {url} com params={params}. Erro: {last_err}")
