#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
remove_duplicados_csv.py
Remover linhas duplicadas de um CSV, com opções:
- escolher colunas para deduplicar
- manter a 1.ª ou a última ocorrência
- ignorar maiúsculas/minúsculas, aparar espaços, normalizar unicode
- autodetectar separador (ou forçar ; , \t)
- modo pandas ou streaming (baixa memória)

Exemplos:
  python remove_duplicados_csv.py input.csv -o output.csv
  python remove_duplicados_csv.py input.csv -o output.csv -c Nome,Data --keep last --ignore-case --trim
  python remove_duplicados_csv.py input.csv -o output.csv --sep ";" --streaming
"""

import argparse
import csv
import sys
import unicodedata
from pathlib import Path

def autodetect_sep(path, encoding="utf-8", sample_bytes=1024*64):
    with open(path, "rb") as f:
        sample = f.read(sample_bytes)
    try:
        sample_text = sample.decode(encoding, errors="ignore")
        dialect = csv.Sniffer().sniff(sample_text, delimiters=[",",";","\t","|"])
        return dialect.delimiter
    except Exception:
        # fallback razoável: se houver muitos ';' na 1.ª linha, usa ';'
        first_line = sample_text.splitlines()[0] if sample_text else ""
        return ";" if first_line.count(";") >= first_line.count(",") else ","

def build_normalizer(ignore_case=False, trim=False, normalize=False):
    def norm(v):
        if v is None:
            return ""
        s = str(v)
        if trim:
            s = s.strip()
        if normalize:
            # compatível com acentos/variações de unicode
            s = unicodedata.normalize("NFKC", s)
        if ignore_case:
            s = s.casefold()
        return s
    return norm

def dedupe_streaming(in_path, out_path, sep, encoding, keep, cols, ignore_case, trim, normalize):
    norm = build_normalizer(ignore_case, trim, normalize)

    with open(in_path, "r", encoding=encoding, newline="") as fin, \
         open(out_path, "w", encoding=encoding, newline="") as fout:

        reader = csv.reader(fin, delimiter=sep)
        writer = csv.writer(fout, delimiter=sep)

        try:
            header = next(reader)
        except StopIteration:
            return 0, 0  # ficheiro vazio

        col_idx = None
        if cols:
            wanted = [c.strip() for c in cols.split(",") if c.strip()]
            missing = [c for c in wanted if c not in header]
            if missing:
                print(f"[erro] Colunas não encontradas: {missing}", file=sys.stderr)
                sys.exit(2)
            col_idx = [header.index(c) for c in wanted]

        # Estratégia para keep:
        # - keep=first: escrevemos a primeira vez que vemos a chave.
        # - keep=last: precisamos acumular todas as linhas (memória). Para evitar isso,
        #   fazemos 2 passagens: 1) contar última posição da chave; 2) escrever só quando
        #   a posição for a última. Mantemos um índice de posições.
        if keep == "first":
            writer.writerow(header)
            seen = set()
            total = 0
            kept = 0
            for row in reader:
                total += 1
                key_elems = row if col_idx is None else [row[i] for i in col_idx]
                key = tuple(norm(x) for x in key_elems)
                if key not in seen:
                    writer.writerow(row)
                    kept += 1
                    seen.add(key)
            return total, kept
        else:  # keep == "last"
            # 1ª passagem: mapear chave -> última posição
            positions = {}
            total = 0
            fin.seek(0)
            reader = csv.reader(fin, delimiter=sep)
            _ = next(reader)  # header
            for pos, row in enumerate(reader):
                total += 1
                key_elems = row if col_idx is None else [row[i] for i in col_idx]
                key = tuple(norm(x) for x in key_elems)
                positions[key] = pos  # vai sendo substituído: fica a última

            # 2ª passagem: escrever apenas onde pos == positions[key]
            fin.seek(0)
            reader = csv.reader(fin, delimiter=sep)
            writer.writerow(next(reader))  # header
            kept = 0
            for pos, row in enumerate(reader):
                key_elems = row if col_idx is None else [row[i] for i in col_idx]
                key = tuple(norm(x) for x in key_elems)
                if positions.get(key, -1) == pos:
                    writer.writerow(row)
                    kept += 1
            return total, kept

def dedupe_pandas(in_path, out_path, sep, encoding, keep, cols, ignore_case, trim, normalize):
    try:
        import pandas as pd
    except ImportError:
        print("[info] pandas não instalado. Use --streaming ou instale pandas.", file=sys.stderr)
        sys.exit(3)

    if sep is None:
        sep = autodetect_sep(in_path, encoding=encoding)

    df = pd.read_csv(in_path, sep=sep, dtype=str, encoding=encoding, keep_default_na=False)
    total = len(df)

    # normalizações (só aplicadas às colunas de dedupe, para não estragar restantes)
    if cols:
        col_list = [c.strip() for c in cols.split(",") if c.strip()]
        missing = [c for c in col_list if c not in df.columns]
        if missing:
            print(f"[erro] Colunas não encontradas: {missing}", file=sys.stderr)
            sys.exit(2)
        work_cols = col_list
    else:
        work_cols = list(df.columns)

    def norm_series(s):
        s2 = s.astype(str)
        if trim:
            s2 = s2.str.strip()
        if normalize:
            s2 = s2.apply(lambda x: unicodedata.normalize("NFKC", x))
        if ignore_case:
            s2 = s2.str.casefold()
        return s2

    df_keys = pd.DataFrame({c: norm_series(df[c]) for c in work_cols})

    # duplicated: keep='first' ou 'last'
    mask = ~df_keys.duplicated(keep=keep)
    df_final = df.loc[mask]

    df_final.to_csv(out_path, sep=sep, index=False, encoding=encoding)
    kept = len(df_final)
    return total, kept

def main():
    p = argparse.ArgumentParser(description="Remover linhas duplicadas de um CSV.")
    p.add_argument("input", help="Caminho do CSV de entrada")
    p.add_argument("-o", "--output", help="Caminho do CSV de saída (default: <input>.dedup.csv)")
    p.add_argument("--sep", help="Separador (por ex. ';' ou ',' ). Se omitido, tenta autodetectar.")
    p.add_argument("--encoding", default="utf-8", help="Encoding (default: utf-8)")
    p.add_argument("-c", "--columns", help="Lista de colunas para deduplicar (ex: 'Nome,Data'). Se omitido, usa todas.")
    p.add_argument("--keep", choices=["first","last"], default="first", help="Manter a primeira ou a última ocorrência (default: first).")
    p.add_argument("--ignore-case", action="store_true", help="Ignorar maiúsculas/minúsculas ao comparar.")
    p.add_argument("--trim", action="store_true", help="Aparar espaços nos extremos ao comparar.")
    p.add_argument("--normalize", action="store_true", help="Normalizar unicode (NFKC) ao comparar.")
    p.add_argument("--streaming", action="store_true", help="Usar modo streaming (baixa memória, mais lento).")
    args = p.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        print(f"[erro] Ficheiro não encontrado: {in_path}", file=sys.stderr)
        sys.exit(1)

    out_path = Path(args.output) if args.output else in_path.with_suffix(".dedup.csv")
    sep = args.sep
    if sep is None:
        sep = autodetect_sep(in_path, encoding=args.encoding)

    if args.streaming:
        total, kept = dedupe_streaming(
            str(in_path), str(out_path), sep, args.encoding,
            keep=args.keep, cols=args.columns,
            ignore_case=args.ignore_case, trim=args.trim, normalize=args.normalize
        )
    else:
        total, kept = dedupe_pandas(
            str(in_path), str(out_path), sep, args.encoding,
            keep=args.keep, cols=args.columns,
            ignore_case=args.ignore_case, trim=args.trim, normalize=args.normalize
        )

    removed = total - kept
    print(f"[ok] Entrada: {total} | Saída (únicas): {kept} | Removidas: {removed}")
    print(f"[save] {out_path}")

if __name__ == "__main__":
    main()
