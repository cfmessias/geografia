#!/usr/bin/env python3
# apply_i18n.py — Varre o projeto e substitui literais (ou variáveis constantes) por t("key")
# baseado em locales/pt.json. Cobre Streamlit, Markdown, Plotly, Matplotlib, dicts/tuplos e
# agora também variáveis atribuídas a strings (const propagation simples).
#
# Uso:
#   python tools/apply_i18n.py --include demografia.py views/graficos.py --alias tr --dry-run
#   python tools/apply_i18n.py --include demografia.py views/graficos.py --alias tr
#
# Requer: pip install libcst

from __future__ import annotations
import argparse, json, sys, re
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional, Set

try:
    import libcst as cst
    from libcst import matchers as m
except ImportError:
    print("Este script requer 'libcst'. Instala com:  pip install libcst", file=sys.stderr)
    sys.exit(1)

# ---------- Config ----------
DEFAULT_INCLUDE_DIRS = ["app.py", "views", "meteo.py", "paises.py", "demografia.py"]

WIDGET_FUNCS = {
    "title", "header", "subheader", "caption",
    "button", "form_submit_button", "download_button",
    "link_button", "page_link",
    "selectbox", "radio", "multiselect", "slider",
    "text_input", "number_input", "date_input", "time_input",
    "checkbox", "toggle", "textarea", "file_uploader",
    "markdown", "write",
}

TITLE_KWARGS = {
    "title", "title_text",
    "x_title", "y_title", "xaxis_title", "yaxis_title",
    "legend_title_text",
    "label", "name",
    "xlabel", "ylabel",  # Matplotlib .set(xlabel=..., ylabel=...)
}

_SIMPLE_HTML_RE = re.compile(r"</?\w+[^>]*>")

# ---------- Utilidades JSON ----------
def _flatten(d: Dict[str, Any], prefix: str = "") -> List[Tuple[str, Any]]:
    out = []
    for k, v in d.items():
        kk = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            out.extend(_flatten(v, kk))
        else:
            out.append((kk, v))
    return out

def load_locales(locales_dir: Path) -> Tuple[Dict[str, str], Dict[str, str]]:
    pt_path = locales_dir / "pt.json"
    en_path = locales_dir / "en.json"
    if not pt_path.exists() or not en_path.exists():
        print(f"ERRO: Faltam pt.json/en.json em {locales_dir}", file=sys.stderr)
        sys.exit(2)
    pt = json.loads(pt_path.read_text("utf-8"))
    pt_map = {k: v for k, v in _flatten(pt) if isinstance(v, str)}

    pref_order = ("labels.", "controls.", "app.", "country.", "forecast.", "precip.", "temperature.", "seismicity.", "paises.", "meteo.", "comparison.", "climate_")
    def prio(key: str) -> Tuple[int, int]:
        for i, p in enumerate(pref_order):
            if key.startswith(p):
                return (i, len(key))
        return (len(pref_order), len(key))

    grouped: Dict[str, List[str]] = {}
    for k, v in pt_map.items():
        grouped.setdefault(v, []).append(k)

    reverse: Dict[str, str] = {}
    for txt, keys in grouped.items():
        reverse[txt] = keys[0] if len(keys) == 1 else sorted(keys, key=prio)[0]
    return reverse, pt_map

# ---------- Helpers ----------
def is_simple_text(s: str) -> bool:
    # evita f-strings/HTML explícito
    if "{" in s or "}" in s:
        return False
    if _SIMPLE_HTML_RE.search(s):
        return False
    return True

def attr_chain(expr: cst.BaseExpression) -> List[str]:
    chain: List[str] = []
    cur: Optional[cst.BaseExpression] = expr
    while isinstance(cur, cst.Attribute):
        if isinstance(cur.attr, cst.Name):
            chain.insert(0, cur.attr.value)
        cur = cur.value
    if isinstance(cur, cst.Name):
        chain.insert(0, cur.value)
    return chain

def replace_in_dict_of_strings(d: cst.Dict, replace_fn) -> cst.Dict:
    new_elements = []
    for el in d.elements:
        if el is None:
            new_elements.append(el); continue
        val = el.value
        if isinstance(val, cst.SimpleString):
            val = replace_fn(val)
        elif isinstance(val, cst.Name):  # pode ser var constante
            val = replace_fn(val)  # delegate
        new_elements.append(el.with_changes(value=val))
    return d.with_changes(elements=new_elements)

def replace_title_text_in_dict(d: cst.Dict, replace_fn) -> cst.Dict:
    new_elements = []
    for el in d.elements:
        if el is None or not isinstance(el.key, (cst.SimpleString, cst.Name)):
            new_elements.append(el); continue
        keyname = el.key.evaluated_value if isinstance(el.key, cst.SimpleString) else el.key.value
        val = el.value
        if keyname == "text":
            if isinstance(val, (cst.SimpleString, cst.Name)):
                val = replace_fn(val)
        new_elements.append(el.with_changes(value=val))
    return d.with_changes(elements=new_elements)

def deep_replace(value: cst.BaseExpression, replace_fn):
    # percorre dicts/listas/tuplos/calls e troca strings simples ou nomes que remetam a strings
    if isinstance(value, (cst.SimpleString, cst.Name)):
        return replace_fn(value)
    if isinstance(value, cst.Dict):
        new_elems = []
        for el in value.elements:
            if el is None:
                new_elems.append(el); continue
            keyname = None
            if isinstance(el.key, cst.Name):
                keyname = el.key.value
            elif isinstance(el.key, cst.SimpleString):
                keyname = el.key.evaluated_value
            val = el.value
            if keyname in {"text", "title", "name", "label"} and isinstance(val, (cst.SimpleString, cst.Name)):
                val = replace_fn(val)
            else:
                val = deep_replace(val, replace_fn)
            new_elems.append(el.with_changes(value=val))
        return value.with_changes(elements=new_elems)
    if isinstance(value, (cst.List, cst.Tuple)):
        new_elems = []
        for el in value.elements:
            if el is None:
                new_elems.append(el); continue
            v = el.value
            v2 = deep_replace(v, replace_fn)
            new_elems.append(el.with_changes(value=v2))
        return value.with_changes(elements=new_elems)
    if isinstance(value, cst.Call):
        new_args = []
        for a in value.args:
            if a.keyword and a.keyword.value in {"title", "annotations", "colorbar", "marker"}:
                val = a.value
                val = deep_replace(val, replace_fn)
                new_args.append(a.with_changes(value=val))
            else:
                new_args.append(a)
        return value.with_changes(args=new_args)
    return value

# ---------- Rewriter ----------
class I18nTransformer(cst.CSTTransformer):
    def __init__(self, reverse_pt_to_key: Dict[str, str], pt_map: Dict[str, str],
                 report_missing: Set[str], inject_import: bool, alias_name: str):
        self.rev = reverse_pt_to_key
        self.pt_map = pt_map
        self.report_missing = report_missing
        self.inject_import = inject_import
        self.alias = alias_name  # "t" ou "tr"
        self.module: Optional[cst.Module] = None
        self.seen_replacement = False

        # pilha de scopes: [global, func1, func2, ...] com var->string
        self.scopes: List[Dict[str, str]] = [dict()]

    # ------- gestão de scopes e constantes -------
    def _set_const(self, name: str, value: str):
        if name and value is not None:
            self.scopes[-1][name] = value

    def _get_const(self, name: str) -> Optional[str]:
        for scope in reversed(self.scopes):
            if name in scope:
                return scope[name]
        return None

    def visit_FunctionDef(self, node: cst.FunctionDef) -> Optional[bool]:
        self.scopes.append({})
        return True

    def leave_FunctionDef(self, original_node: cst.FunctionDef, updated_node: cst.FunctionDef) -> cst.FunctionDef:
        self.scopes.pop()
        return updated_node

    def visit_Assign(self, node: cst.Assign) -> Optional[bool]:
        # a = "texto"
        val = node.value
        if isinstance(val, cst.SimpleString):
            sval = val.evaluated_value
            for tgt in node.targets:
                if isinstance(tgt.target, cst.Name):
                    self._set_const(tgt.target.value, sval)
                elif isinstance(tgt.target, cst.Tuple) and isinstance(val, cst.Tuple):
                    # (a,b)=("x","y")
                    t_el = [e.value for e in tgt.target.elements if e is not None]
                    v_el = [e.value for e in val.elements if e is not None]
                    if len(t_el) == len(v_el):
                        for te, ve in zip(t_el, v_el):
                            if isinstance(te, cst.Name) and isinstance(ve, cst.SimpleString):
                                self._set_const(te.value, ve.evaluated_value)
        return True

    def visit_AnnAssign(self, node: cst.AnnAssign) -> Optional[bool]:
        # a: str = "texto"
        if isinstance(node.value, cst.SimpleString) and isinstance(node.target, cst.Name):
            self._set_const(node.target.value, node.value.evaluated_value)
        return True

    # ------- tradução -------
    def wrap_t(self, key: str) -> cst.Call:
        return cst.Call(func=cst.Name(self.alias), args=[cst.Arg(value=cst.SimpleString(f'"{key}"'))])

    def _translate_string(self, s: str) -> Optional[cst.Call]:
        if s.strip() == "":
            return None
        if "<" in s and _SIMPLE_HTML_RE.search(s):
            return None  # ignora HTML/CSS/JS
        if not is_simple_text(s):
            self.report_missing.add(s)
            return None
        key = self.rev.get(s)
        if not key:
            self.report_missing.add(s)
            return None
        self.seen_replacement = True
        return self.wrap_t(key)

    def translate_expr(self, expr: cst.BaseExpression) -> cst.BaseExpression:
        # SimpleString direto
        if isinstance(expr, cst.SimpleString):
            res = self._translate_string(expr.evaluated_value)
            return res if res is not None else expr

        # Nome que pode remeter para constante de string
        if isinstance(expr, cst.Name):
            val = self._get_const(expr.value)
            if val is not None:
                res = self._translate_string(val)
                return res if res is not None else expr

        return expr

    # ---------- Call processing ----------
    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.Call:
        chain = attr_chain(original_node.func)

        # st.tabs([...])  → traduz cada label literal da lista/tuplo
        if len(chain) >= 2 and chain[0] == "st" and chain[-1] == "tabs":
            if updated_node.args:
                first = updated_node.args[0].value
                if isinstance(first, (cst.List, cst.Tuple)):
                    new_elements = []
                    for el in first.elements:
                        if el is None or el.value is None:
                            new_elements.append(el); continue
                        new_val = self.translate_expr(el.value)
                        new_elements.append(el.with_changes(value=new_val))
                    new_first = first.with_changes(elements=new_elements)
                    new_args = [updated_node.args[0].with_changes(value=new_first)] + list(updated_node.args[1:])
                    updated_node = updated_node.with_changes(args=new_args)
            return updated_node

        # st.<widget> / st.sidebar.<widget> → 1º argumento
        if chain and chain[0] == "st":
            func_name = chain[-1]
            if func_name in WIDGET_FUNCS and updated_node.args:
                first_arg = updated_node.args[0]
                if func_name == "write":
                    if isinstance(first_arg.value, (cst.SimpleString, cst.Name)):
                        new_val = self.translate_expr(first_arg.value)
                        updated_node = updated_node.with_changes(args=[first_arg.with_changes(value=new_val)] + list(updated_node.args[1:]))
                else:
                    new_val = self.translate_expr(first_arg.value)
                    updated_node = updated_node.with_changes(args=[first_arg.with_changes(value=new_val)] + list(updated_node.args[1:]))

        # kwargs (title=, x_title=, labels={...}, title={"text": ...}, annotations, layout, colorbar, marker)
        if updated_node.args:
            new_args = []
            for arg in updated_node.args:
                if arg.keyword:
                    k = arg.keyword.value

                    if k == "labels" and isinstance(arg.value, cst.Dict):
                        new_val = replace_in_dict_of_strings(arg.value, self.translate_expr)
                        new_args.append(arg.with_changes(value=new_val)); continue

                    if k == "title" and isinstance(arg.value, cst.Dict):
                        new_val = replace_title_text_in_dict(arg.value, self.translate_expr)
                        new_args.append(arg.with_changes(value=new_val)); continue

                    if k in {"annotations", "layout", "colorbar", "marker"} and isinstance(arg.value, (cst.List, cst.Tuple, cst.Dict, cst.Call)):
                        new_val = deep_replace(arg.value, self.translate_expr)
                        new_args.append(arg.with_changes(value=new_val)); continue

                    if k in TITLE_KWARGS:
                        new_val = self.translate_expr(arg.value)
                        new_args.append(arg.with_changes(value=new_val)); continue

                new_args.append(arg)
            updated_node = updated_node.with_changes(args=new_args)

        # Matplotlib: plt.title("…"), plt.xlabel("…"), plt.ylabel("…")
        # OO API: ax.set_title("…"), ax.set_xlabel("…"), ax.set_ylabel("…")
        if chain:
            last = chain[-1]
            if last in {"title", "xlabel", "ylabel", "set_title", "set_xlabel", "set_ylabel"} and updated_node.args:
                first_arg = updated_node.args[0]
                new_val = self.translate_expr(first_arg.value)
                updated_node = updated_node.with_changes(args=[first_arg.with_changes(value=new_val)] + list(updated_node.args[1:]))

        return updated_node

    def leave_Module(self, original_node: cst.Module, updated_node: cst.Module) -> cst.Module:
        self.module = updated_node
        if not self.inject_import or not self.seen_replacement:
            return updated_node

        code = updated_node.code
        if self.alias == "t":
            if re.search(r"\bfrom\s+services\.i18n\s+import\s+t\b", code):
                return updated_node
            import_stmt = "from services.i18n import t\n"
        else:
            pattern = rf"\bfrom\s+services\.i18n\s+import\s+t\s+as\s+{re.escape(self.alias)}\b"
            if re.search(pattern, code):
                return updated_node
            import_stmt = f"from services.i18n import t as {self.alias}\n"

        body = list(updated_node.body)
        insert_idx = 0
        for i, stmt in enumerate(body):
            if m.matches(stmt, m.SimpleStatementLine(body=[m.ImportFrom() | m.Import()])) or \
               m.matches(stmt, m.SimpleStatementLine(body=[m.Expr(value=m.SimpleString())])):
                insert_idx = i + 1
            else:
                break
        new_import = cst.parse_statement(import_stmt)
        body.insert(insert_idx, new_import)
        return updated_node.with_changes(body=body)

# ---------- Execução ----------
def process_file(path: Path, reverse_map: Dict[str, str], pt_map: Dict[str, str],
                 write: bool, backup_ext: str, inject_import: bool, alias_name: str) -> Tuple[int, List[str]]:
    code = path.read_text("utf-8")
    try:
        mod = cst.parse_module(code)
    except Exception as e:
        return 0, [f"[{path}] Falhou parse: {e}"]

    missing: Set[str] = set()
    tr = I18nTransformer(reverse_map, pt_map, missing, inject_import, alias_name)
    new_mod = mod.visit(tr)
    replaced = 1 if tr.seen_replacement else 0

    if write and tr.seen_replacement:
        if backup_ext:
            path.with_suffix(path.suffix + backup_ext).write_text(code, "utf-8")
        path.write_text(new_mod.code, "utf-8")

    msgs = []
    if missing:
        for s in sorted(missing):
            msgs.append(f"[{path}] sem mapping pt.json: {repr(s)}")
    return replaced, msgs

def iter_py_files(root: Path, include: List[str]) -> List[Path]:
    files: List[Path] = []
    for inc in include:
        p = root / inc
        if p.is_file() and p.suffix == ".py":
            files.append(p)
        elif p.is_dir():
            files += [x for x in p.rglob("*.py") if x.is_file()]
    files = [f for f in files if not f.name.endswith("apply_i18n.py")]
    return sorted(files)

def main():
    ap = argparse.ArgumentParser(description="Aplicar i18n (t('key')) usando locales/pt.json como fonte de chaves.")
    ap.add_argument("--project-root", default=".", help="raiz do projeto (onde estão app.py, views/)")
    ap.add_argument("--locales-dir", default="locales", help="pasta dos JSON (pt.json, en.json)")
    ap.add_argument("--include", nargs="*", default=DEFAULT_INCLUDE_DIRS, help="ficheiros/pastas a varrer")
    ap.add_argument("--dry-run", action="store_true", help="não escreve ficheiros; apenas relatório")
    ap.add_argument("--no-backup", action="store_true", help="não criar .bak")
    ap.add_argument("--no-inject", action="store_true", help="não injetar 'from services.i18n import t'")
    ap.add_argument("--alias", default="t", help="alias para o import (ex.: 'tr' evita colisões com variáveis locais 't')")
    args = ap.parse_args()

    root = Path(args.project_root).resolve()
    locales = (root / args.locales_dir).resolve()

    reverse_map, pt_map = load_locales(locales)

    files = iter_py_files(root, args.include)
    if not files:
        print("Nenhum .py encontrado nos caminhos indicados.", file=sys.stderr)
        sys.exit(3)

    total_changed = 0
    all_msgs: List[str] = []
    for f in files:
        changed, msgs = process_file(
            f, reverse_map, pt_map,
            write=(not args.dry_run),
            backup_ext="" if args.no_backup else ".bak",
            inject_import=(not args.no_inject),
            alias_name=args.alias,
        )
        total_changed += changed
        all_msgs.extend(msgs)

    if all_msgs:
        report_path = root / "i18n_missing_report.txt"
        report_path.write_text("\n".join(all_msgs), "utf-8")
        print(f"[i18n] Relatório de textos NÃO mapeados → {report_path}")
        print("      Acrescenta estes textos ao pt.json (e en.json) e volta a correr o script.")
    print(f"[i18n] Ficheiros com substituições aplicadas: {total_changed} / {len(files)}")
    if args.dry_run:
        print("[i18n] Modo dry-run: nada foi escrito.")

if __name__ == "__main__":
    main()
