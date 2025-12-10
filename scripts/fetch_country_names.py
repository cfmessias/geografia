#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
fetch_country_names.py

Lê um countries_profiles.csv com pelo menos:
    qid;name;...

Vai à Wikidata buscar o label em inglês para cada qid e gera
um novo CSV com uma coluna adicional 'name_en'.

Por omissão:
    input : data/countries_profiles.csv
    output: data/countries_profiles_with_en.csv

Também podes indicar caminhos na linha de comandos:
    python -u scripts/fetch_country_names.py [input_csv [output_csv]]
"""

import sys
import csv
from pathlib import Path
from typing import Dict, List

import requests


WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"


def build_sparql_query(qids: List[str]) -> str:
    """Constrói query SPARQL para obter o label em inglês para uma lista de QIDs."""
    values = " ".join(f"wd:{qid}" for qid in qids)
    query = f"""
    SELECT ?country ?label_en WHERE {{
      VALUES ?country {{ {values} }}

      OPTIONAL {{
        ?country rdfs:label ?label_en .
        FILTER (LANG(?label_en) = "en")
      }}
    }}
    """
    return query


def run_sparql(query: str) -> dict:
    """Executa query SPARQL no endpoint da Wikidata e devolve o JSON."""
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "GeoMundiCountryNames/1.0 (cfmessias.pt)"
    }
    resp = requests.get(
        WIKIDATA_SPARQL_URL,
        params={"query": query},
        headers=headers,
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_en_labels(qids: List[str], chunk_size: int = 80) -> Dict[str, str]:
    """
    Devolve um dicionário {qid: label_en} com base numa lista de QIDs.

    Faz a query em blocos (chunk_size) para evitar queries demasiado grandes.
    """
    labels: Dict[str, str] = {}
    # remover duplicados + vazios preservando ordem
    qids_unique = [q for q in dict.fromkeys(qids) if q]

    for i in range(0, len(qids_unique), chunk_size):
        chunk = qids_unique[i:i + chunk_size]
        print(
            f"[info] A obter labels EN para QIDs "
            f"{i+1}-{i+len(chunk)} de {len(qids_unique)}..."
        )
        query = build_sparql_query(chunk)
        data = run_sparql(query)

        for b in data.get("results", {}).get("bindings", []):
            uri = b["country"]["value"]
            qid = uri.rsplit("/", 1)[-1]
            label_en = b.get("label_en", {}).get("value", "")
            if label_en:
                labels[qid] = label_en

    return labels


def main():
    # Caminhos por omissão
    default_in = Path("data") / "countries_profiles.csv"
    default_out = Path("data") / "countries_profiles_with_en.csv"

    # Permitir overrides pela linha de comandos, mas opcionais
    if len(sys.argv) >= 2:
        in_path = Path(sys.argv[1])
    else:
        in_path = default_in

    if len(sys.argv) >= 3:
        out_path = Path(sys.argv[2])
    else:
        out_path = default_out

    if not in_path.exists():
        print(f"[erro] Ficheiro de entrada não encontrado: {in_path}")
        sys.exit(1)

    print(f"[info] A ler ficheiro de entrada: {in_path}")

    # 1) Ler todas as linhas e recolher QIDs
    rows = []
    qids = []

    with in_path.open("r", encoding="utf-8", newline="") as f_in:
        reader = csv.DictReader(f_in, delimiter=";")
        fieldnames_in = reader.fieldnames or []

        if "qid" not in fieldnames_in:
            print("[erro] Ficheiro não tem coluna 'qid'.")
            sys.exit(1)

        for row in reader:
            rows.append(row)
            qids.append((row.get("qid") or "").strip())

    print(f"[info] Linhas lidas: {len(rows)}")

    # 2) Buscar labels em inglês na Wikidata
    labels_en = fetch_en_labels(qids)
    print(f"[info] Labels EN obtidos: {len(labels_en)}")

    # 3) Preparar cabeçalho com coluna name_en
    original_fields = rows[0].keys() if rows else []
    fieldnames = []
    has_name_en = "name_en" in original_fields

    for fn in original_fields:
        fieldnames.append(fn)
        # Se não existir name_en, vamos inseri-lo logo a seguir a 'name'
        if fn == "name" and not has_name_en:
            fieldnames.append("name_en")

    if not has_name_en and "name_en" not in fieldnames:
        fieldnames.append("name_en")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"[info] A escrever ficheiro de saída: {out_path}")

    with out_path.open("w", encoding="utf-8", newline="") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()

        for row in rows:
            qid = (row.get("qid") or "").strip()

            # Se já houver name_en preenchido, mantemos (não estragamos correções manuais)
            current_en = (row.get("name_en") or "").strip()

            if not current_en and qid in labels_en:
                row["name_en"] = labels_en[qid]
            else:
                # garantir que a chave existe
                row.setdefault("name_en", current_en)

            writer.writerow(row)

    print("[ok] Terminado.")


if __name__ == "__main__":
    main()
