# scripts/diag_wdqs_q198.py
# -*- coding: utf-8 -*-
import requests, time, json
from textwrap import indent

SPARQL = "https://query.wikidata.org/sparql"
UA = "GeoMundi-Diag/1.0 (+cfmessias@gmail.com)"
H = {"User-Agent": UA, "Accept": "application/sparql-results+json"}
TO = 120

Q = {
    # 0) COUNT simples — deve ser > 0
    "count_P31P279_Q198": """
SELECT (COUNT(*) AS ?n) WHERE {
  ?c wdt:P31/wdt:P279* wd:Q198 .
}
""",
    # 1) COUNT com P710/P17 — deve ser > 0
    "count_with_links": """
SELECT (COUNT(*) AS ?n) WHERE {
  ?c wdt:P31/wdt:P279* wd:Q198 .
  { ?c wdt:P710 [] } UNION { ?c wdt:P17 [] }
}
""",
    # 2) 10 QIDs crus — deve listar alguns
    "list_10_ids": """
SELECT DISTINCT ?c WHERE {
  ?c wdt:P31/wdt:P279* wd:Q198 .
  { ?c wdt:P710 [] } UNION { ?c wdt:P17 [] }
} LIMIT 10
""",
    # 3) 20 com labels (sem filtrar por label)
    "list_20_with_labels": """
SELECT DISTINCT ?c ?cLabel WHERE {
  ?c wdt:P31/wdt:P279* wd:Q198 .
  { ?c wdt:P710 [] } UNION { ?c wdt:P17 [] }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en,pt". }
} LIMIT 20
""",
    # 4) Variante 2-pernas (mais estável no WDQS) com labels
    "two_leg_with_labels": """
SELECT DISTINCT ?c ?cLabel WHERE {
  ?c wdt:P31 ?t .
  ?t wdt:P279* wd:Q198 .
  { ?c wdt:P710 [] } UNION { ?c wdt:P17 [] }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en,pt". }
} LIMIT 20
""",
    # 5) A mesma do catálogo (2-pernas + label obrigatório)
    "catalog_style_min": """
SELECT DISTINCT ?conflict ?conflictLabel WHERE {
  ?conflict wdt:P31 ?t .
  ?t wdt:P279* wd:Q198 .
  { ?conflict wdt:P710 [] } UNION { ?conflict wdt:P17 [] }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],pt,en". }
  FILTER(BOUND(?conflictLabel) && STRLEN(STR(?conflictLabel)) > 0)
} LIMIT 50
""",
}

def try_call(name, query, method="GET"):
    try:
        if method=="GET":
            r = requests.get(SPARQL, params={"query": query}, headers=H, timeout=TO)
        else:
            r = requests.post(SPARQL, data={"query": query}, headers=H, timeout=TO)
        status = r.status_code
        txt = r.text
        try:
            js = r.json()
        except Exception as e:
            print(f"[{name}][{method}] HTTP {status} — resposta não JSON ({type(e).__name__}):")
            print(indent(txt[:500], "  "))
            return

        # resultados
        if "boolean" in js:
            print(f"[{name}][{method}] HTTP {status}  boolean={js['boolean']}")
            return
        binds = js.get("results",{}).get("bindings", [])
        # métricas
        print(f"[{name}][{method}] HTTP {status}  bindings={len(binds)}")
        # amostra
        sample = []
        for b in binds[:5]:
            for k in ("c","conflict"):
                if k in b:
                    uri = b[k]["value"]
                    qid = uri.rsplit("/",1)[-1]
                    label = b.get(k+"Label",{}).get("value","")
                    sample.append(f"{qid} — {label}")
                    break
        if sample:
            print(indent("amostra:\n"+"\n".join("• "+s for s in sample), "  "))
    except requests.RequestException as e:
        print(f"[{name}][{method}] ERRO de rede: {e}")

def main():
    print("== DIAGNÓSTICO WDQS para Q198 ==")
    for name, query in Q.items():
        for method in ("GET","POST"):
            try_call(name, query, method)
        time.sleep(1)

if __name__ == "__main__":
    main()
