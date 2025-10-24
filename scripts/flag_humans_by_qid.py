# scripts/flag_humans_by_qid.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import sys, time, argparse
from pathlib import Path
from typing import List, Dict, Set
import pandas as pd
import requests

IN_CSV  = Path("data/conflict_participants.csv")
OUT_CSV = IN_CSV  # por defeito, sobrescreve

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT      = "GeoMundi-FlagHumans/1.0 (+your-email@example.com)"
BATCH_SIZE      = 200
RETRY_MAX       = 4
RETRY_SLEEP_S   = 2.0

def read_csv_semicolon(p: Path) -> pd.DataFrame:
    if not p.exists():
        raise FileNotFoundError(f"Ficheiro não encontrado: {p}")
    return pd.read_csv(p, sep=";", dtype=str, encoding="utf-8", keep_default_na=False)

def canon(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        df[c] = df[c].astype(str).fillna("").str.strip()
    return df

def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    # Tem de existir participant_qid; renomeamos variantes comuns se preciso
    if "participant_qid" not in df.columns:
        low = {c.lower(): c for c in df.columns}
        if "entity_qid" in low:
            df = df.rename(columns={low["entity_qid"]: "participant_qid"})
        elif "qid" in low:
            df = df.rename(columns={low["qid"]: "participant_qid"})
        else:
            raise ValueError("O CSV precisa da coluna 'participant_qid'.")
    if "is_human" not in df.columns:
        df["is_human"] = ""
    return df

def chunked(seq: List[str], n: int) -> List[List[str]]:
    return [seq[i:i+n] for i in range(0, len(seq), n)]

def run_sparql(query: str) -> dict:
    headers = {"Accept": "application/sparql-results+json", "User-Agent": USER_AGENT}
    for attempt in range(1, RETRY_MAX+1):
        try:
            r = requests.post(SPARQL_ENDPOINT, data={"query": query}, headers=headers, timeout=90)
            if r.status_code == 200:
                return r.json()
        except requests.RequestException:
            pass
        time.sleep(RETRY_SLEEP_S * attempt)
    raise RuntimeError("Falha ao consultar o endpoint SPARQL após várias tentativas.")

def qids_that_are_human(qids: List[str]) -> Set[str]:
    """
    Devolve os QIDs que satisfazem: ?item wdt:P31 / wdt:P279* wd:Q5
    """
    values = " ".join(f"wd:{q}" for q in qids)
    query = f"""
    SELECT ?itemQ WHERE {{
      VALUES ?item {{ {values} }}
      ?item wdt:P31 / wdt:P279* wd:Q5 .
      BIND(STRAFTER(STR(?item), "entity/") AS ?itemQ)
    }}
    """
    js = run_sparql(query)
    hits: Set[str] = set()
    for b in js.get("results", {}).get("bindings", []):
        q = b.get("itemQ", {}).get("value", "")
        if q:
            hits.add(q.upper())
    return hits

def compute_map(qids: List[str]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for batch in chunked(qids, BATCH_SIZE):
        hits = qids_that_are_human(batch)
        for q in batch:
            result[q] = "yes" if q in hits else "no"
    return result

def main():
    ap = argparse.ArgumentParser(
        description="Marca is_human=yes/no em conflict_participants.csv via P31/P279*→Q5 (Wikidata)."
    )
    ap.add_argument("--in", dest="inp", default=str(IN_CSV), help=f"CSV de entrada (default: {IN_CSV})")
    ap.add_argument("--out", dest="out", default=str(OUT_CSV), help=f"CSV de saída (default: sobrescreve o de entrada)")
    ap.add_argument("--only-empty", action="store_true", help="Só calcula para linhas com is_human vazio (preserva valores existentes).")
    ap.add_argument("--force", action="store_true", help="Recalcula e sobrescreve is_human para TODOS os QIDs.")
    ap.add_argument("--dry-run", action="store_true", help="Não escreve – apenas mostra o resumo.")
    args = ap.parse_args()

    if args.only_empty and args.force:
        print("[erro] Use --only-empty OU --force, não ambos.", file=sys.stderr)
        sys.exit(2)

    inp = Path(args.inp); out = Path(args.out)

    try:
        df = canon(read_csv_semicolon(inp))
        df = ensure_cols(df)
        df["participant_qid"] = df["participant_qid"].str.upper()

        mask_valid = df["participant_qid"].str.match(r"^Q\d+$")
        if not mask_valid.all():
            bad = df.loc[~mask_valid, "participant_qid"].unique().tolist()
            if bad:
                print(f"[aviso] QIDs inválidos ignorados: {bad[:10]}{'...' if len(bad)>10 else ''}")

        df_valid = df[mask_valid].copy()

        if args.force:
            qids = sorted(df_valid["participant_qid"].unique())
        elif args.only_empty:
            qids = sorted(df_valid.loc[df_valid["is_human"] == "", "participant_qid"].unique())
        else:
            # modo padrão: calcula para todos mas só preenche vazios (preserva 'yes'/'no' existentes)
            qids = sorted(df_valid["participant_qid"].unique())

        if not qids:
            print("Nada para fazer.")
            sys.exit(0)

        print(f"[info] a verificar {len(qids)} QIDs (batch={BATCH_SIZE}) ...")
        mapping = compute_map(qids)

        before_yes = int((df["is_human"] == "yes").sum())
        before_no  = int((df["is_human"] == "no").sum())
        before_blank = int((df["is_human"] == "").sum())

        def decide(current: str, qid: str) -> str:
            cur = (current or "").lower()
            if args.force:
                return mapping.get(qid, cur)
            if args.only_empty:
                return mapping.get(qid, cur) if cur == "" else cur
            # padrão: só preenche vazios; mantém decisões existentes
            return mapping.get(qid, cur) if cur == "" else cur

        df["is_human"] = [decide(cur, q) for cur, q in zip(df["is_human"], df["participant_qid"])]

        after_yes = int((df["is_human"] == "yes").sum())
        after_no  = int((df["is_human"] == "no").sum())
        after_blank = int((df["is_human"] == "").sum())

        print(f"[resumo] is_human: yes {before_yes}→{after_yes} | no {before_no}→{after_no} | vazios {before_blank}→{after_blank}")

        if args.dry_run:
            print("[dry-run] não escrevi ficheiro.")
            sys.exit(0)

        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, sep=";", index=False, encoding="utf-8")
        print(f"✔️ escrito: {out}")

    except Exception as e:
        print(f"Erro: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
