#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Detecta chaves duplicadas em ficheiros JSON (suporta nested).
- Aceita ficheiros e/ou diretórios (procura *.json recursivamente).
- Sai com código 1 se encontrar duplicados.
- Não altera ficheiros (apenas reporta).
"""
import sys, json, argparse, pathlib
from collections import Counter

class Node:
    """Nó de objeto JSON preservando a ordem e pares repetidos."""
    __slots__ = ("pairs",)
    def __init__(self, pairs): self.pairs = list(pairs)
    def to_dict(self):
        # devolve um dicionário “normal” (última ocorrência ganha)
        d = {}
        for k, v in self.pairs:
            d[k] = v.to_dict() if isinstance(v, Node) else (
                [x.to_dict() if isinstance(x, Node) else x for x in v] if isinstance(v, list) else v
            )
        return d

def _object_hook(pairs):
    # cria Node para cada objeto; permite inspecionar duplicados depois
    return Node(pairs)

def _parse_json_as_nodes(path: pathlib.Path) -> Node:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f, object_pairs_hook=_object_hook)

def _find_dupes(node, path="$", out=None):
    if out is None: out = []
    if isinstance(node, Node):
        keys = [k for k, _ in node.pairs]
        for key, cnt in Counter(keys).items():
            if cnt > 1:
                out.append((path, key, cnt))
        # descer a cada par (mesmo que haja chaves repetidas)
        for k, v in node.pairs:
            child_path = f"{path}.{k}" if path != "$" else k
            _find_dupes(v, child_path, out)
    elif isinstance(node, list):
        for i, v in enumerate(node):
            _find_dupes(v, f"{path}[{i}]", out)
    # valores escalares: nada a fazer
    return out

def _iter_json_files(paths):
    for p in paths:
        p = pathlib.Path(p)
        if p.is_file() and p.suffix.lower() == ".json":
            yield p
        elif p.is_dir():
            for f in p.rglob("*.json"):
                if f.is_file():
                    yield f

def main(argv=None):
    ap = argparse.ArgumentParser(description="Verifica chaves duplicadas em JSON.")
    ap.add_argument("paths", nargs="*", default=["locales"], help="Ficheiros ou diretórios a verificar")
    args = ap.parse_args(argv)

    any_dupes = False
    for jf in sorted(_iter_json_files(args.paths)):
        try:
            root = _parse_json_as_nodes(jf)
        except Exception as e:
            print(f"[ERR] {jf}: JSON inválido — {e}")
            any_dupes = True
            continue
        dupes = _find_dupes(root)
        if dupes:
            any_dupes = True
            print(f"[!] {jf}: chaves duplicadas:")
            for at, key, cnt in dupes:
                print(f"    • {at} → '{key}' (repetida {cnt}×)")
        else:
            print(f"[OK] {jf}: sem duplicadas")

    sys.exit(1 if any_dupes else 0)

if __name__ == "__main__":
    main()
