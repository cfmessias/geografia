#!/usr/bin/env python3
# i18n_merge_missing.py — Lê i18n_missing_report.txt e injeta chaves novas no pt.json/en.json.

from __future__ import annotations
import re, json, unicodedata
from pathlib import Path
from typing import Dict, Any
import ast

ROOT = Path(__file__).resolve().parents[1]  # pasta do projeto
LOCALES = ROOT / "locales"
PT = LOCALES / "pt.json"
EN = LOCALES / "en.json"
REPORT = ROOT / "i18n_missing_report.txt"

LINE_RE = re.compile(r"^\[(?P<path>.+?)\]\s+sem mapping pt\.json:\s+(?P<repr>.+?)\s*$")

def load_json(p: Path) -> Dict[str, Any]:
    return json.loads(p.read_text("utf-8"))

def save_json(p: Path, data: Dict[str, Any]) -> None:
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")

def slug(s: str) -> str:
    # slug simples para chaves: sem emojis, sem pontuação, com _ e minúsculas
    s = s.strip()
    # remove emojis/símbolos
    s = "".join(ch for ch in s if not unicodedata.category(ch).startswith(("Cs", "So")))
    # normaliza e remove acentos
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    # troca separadores
    s = s.lower()
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    # evita slugs vazios
    return s or "label"

def ensure_path(d: Dict[str, Any], dotted: str) -> Dict[str, Any]:
    cur = d
    parts = dotted.split(".")
    for part in parts:
        cur = cur.setdefault(part, {})
    return cur

def set_key(d: Dict[str, Any], dotted: str, value: str) -> str:
    parts = dotted.split(".")
    cur = d
    for p in parts[:-1]:
        cur = cur.setdefault(p, {})
    key = parts[-1]
    # colisão: acrescenta sufixo numérico
    base = key
    idx = 2
    while key in cur and cur[key] != value:
        key = f"{base}_{idx}"
        idx += 1
    cur[key] = value
    return ".".join(parts[:-1] + [key])

def detect_namespace(path_str: str) -> str:
    # tenta inferir namespace a partir do ficheiro (forecast.py → "forecast")
    p = Path(path_str)
    stem = p.stem
    if stem == "app":
        return "app"
    if stem:
        return stem
    return "labels"

def main():
    if not REPORT.exists():
        print(f"ERRO: {REPORT} não existe. Corre primeiro apply_i18n.py --dry-run")
        return
    pt = load_json(PT)
    en = load_json(EN)

    lines = REPORT.read_text("utf-8").splitlines()
    added = 0
    for line in lines:
        m = LINE_RE.match(line)
        if not m:
            continue
        path = m.group("path").strip()
        text_repr = m.group("repr").strip()

        # converte o repr (com aspas simples OU duplas) para string real
        try:
            text = ast.literal_eval(text_repr)
        except Exception:
            continue

        # ignorar vazios/whitespace
        if not text or not text.strip():
            continue
       
        import re
        if "<" in text and re.search(r"</?\w+[^>]*>", text):
            continue
        
        ns = detect_namespace(path)  # namespace
        key_slug = slug(text)
        dotted = f"{ns}.{key_slug}" if ns not in ("app", "labels") else f"labels.{key_slug}" if ns == "labels" else f"app.{key_slug}"

        # se já existir em pt.json com outro caminho, não duplica
        # percorre arvore à procura do valor
        def find_value(d: Dict[str, Any], needle: str, prefix=""):
            hits = []
            if isinstance(d, dict):
                for k, v in d.items():
                    kp = f"{prefix}.{k}" if prefix else k
                    if isinstance(v, dict):
                        hits += find_value(v, needle, kp)
                    elif v == needle:
                        hits.append(kp)
            return hits

        hits = find_value(pt, text)
        if hits:
            # já existe mapeado — ignora
            continue

        # escreve no PT e copia para EN (para revisão/tradução posterior)
        final_key_pt = set_key(pt, dotted, text)
        _ = set_key(en, dotted, text)
        added += 1
        print(f"+ {final_key_pt} -> {text}")

    if added:
        save_json(PT, pt)
        save_json(EN, en)
        print(f"Adicionadas {added} chaves a pt.json/en.json.")
        print("Agora volta a correr:  python tools/apply_i18n.py")
    else:
        print("Nenhuma chave adicionada; possivelmente já estavam todas no pt.json.")

if __name__ == "__main__":
    main()
