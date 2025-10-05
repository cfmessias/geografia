#!/usr/bin/env python3
# Garante que todas as views/app chamam _ensure_lang_state() antes de renderizar
# e que existe um import compatível de i18n_boot.

from __future__ import annotations
from pathlib import Path
from typing import List
import libcst as cst
from libcst import matchers as m

# Que ficheiros/pastas processar
INCLUDE = ["app.py", "views", "ind_demograficos.py", "meteo.py", "paises.py"]


# --- Transformer --------------------------------------------------------------

class EnsureI18nInit(cst.CSTTransformer):
    def __init__(self, is_app_file: bool):
        self.is_app_file = is_app_file
        self.has_boot_import = False       # já existe import direto/compat?
        self.has_top_call = False          # já chama _ensure_lang_state() no topo?
        self.render_funcs = []             # nomes de funções render* encontradas
        self.changed = False

    # Detecta imports existentes
    def leave_ImportFrom(self, orig: cst.ImportFrom, upd: cst.ImportFrom) -> cst.ImportFrom:
        mod = upd.module
        if isinstance(mod, cst.Name) and mod.value in {"services.i18n_boot", "services"}:
            # "from services.i18n_boot import ..." OU "from services import i18n_boot"
            names = [n.name.value if isinstance(n, cst.ImportAlias) and isinstance(n.name, cst.Name) else ""
                     for n in upd.names] if isinstance(upd.names, list) else []
            if "i18n_boot" in names or any(v in {"_ensure_lang_state", "init_i18n_state"} for v in names):
                self.has_boot_import = True
        elif isinstance(mod, cst.Attribute) and isinstance(mod.value, cst.Name) and mod.value.value == "services":
            if isinstance(mod.attr, cst.Name) and mod.attr.value == "i18n_boot":
                self.has_boot_import = True
        return upd

    def leave_Import(self, orig: cst.Import, upd: cst.Import) -> cst.Import:
        # "import services.i18n_boot as ..." também conta
        for alias in upd.names:
            name = alias.name
            if isinstance(name, cst.Attribute) and isinstance(name.value, cst.Name):
                if name.value.value == "services" and isinstance(name.attr, cst.Name) and name.attr.value == "i18n_boot":
                    self.has_boot_import = True
        return upd

    def leave_FunctionDef(self, orig: cst.FunctionDef, upd: cst.FunctionDef) -> cst.FunctionDef:
        name = orig.name.value
        if name.startswith("render"):
            self.render_funcs.append(name)
            body = upd.body
            if m.matches(body, m.IndentedBlock()):
                stmts = list(body.body)
                # já tem chamada nas primeiras 3 linhas?
                already = any(
                    m.matches(s, m.SimpleStatementLine(
                        body=[m.Expr(value=m.Call(func=m.Name("_ensure_lang_state")))]
                    )) or
                    m.matches(s, m.SimpleStatementLine(
                        body=[m.Expr(value=m.Call(func=m.Name("init_i18n_state")))]
                    ))
                    for s in stmts[:3]
                )
                if not already:
                    call = cst.parse_statement("_ensure_lang_state()\n")
                    stmts.insert(0, call)
                    upd = upd.with_changes(body=body.with_changes(body=stmts))
                    self.changed = True
        return upd

    def leave_Module(self, orig: cst.Module, upd: cst.Module) -> cst.Module:
        body = list(upd.body)

        # 1) Inserir import compatível se faltar
        #    try:
        #        from services.i18n_boot import _ensure_lang_state
        #    except ImportError:
        #        from services.i18n_boot import init_i18n_state as _ensure_lang_state
        if not self.has_boot_import:
            insert_idx = 0
            for i, stmt in enumerate(body):
                if m.matches(stmt, m.SimpleStatementLine(body=[m.Expr(value=m.SimpleString())])) or \
                   m.matches(stmt, m.SimpleStatementLine(body=[m.ImportFrom() | m.Import()])):
                    insert_idx = i + 1
                else:
                    break
            imp_block = (
                "try:\n"
                "    from services.i18n_boot import _ensure_lang_state\n"
                "except ImportError:\n"
                "    from services.i18n_boot import init_i18n_state as _ensure_lang_state\n"
            )
            body.insert(insert_idx, cst.parse_statement(imp_block))
            self.changed = True

        # 2) Se não há funções render*, garantir chamada de topo (útil p/ app.py e views com código top-level)
        if not self.render_funcs:
            # já existe chamada nas 6 primeiras linhas reais?
            first_six = body[:6]
            has_call = any(
                m.matches(s, m.SimpleStatementLine(body=[m.Expr(value=m.Call(func=m.Name("_ensure_lang_state")))])) or
                m.matches(s, m.SimpleStatementLine(body=[m.Expr(value=m.Call(func=m.Name("init_i18n_state")))]))
                for s in first_six
            )
            if not has_call:
                # Inserir de preferência após st.set_page_config se existir, senão depois de imports/docstring
                insert_idx = 0
                after_page_config_idx = None
                for i, stmt in enumerate(body[:20]):
                    if m.matches(stmt, m.SimpleStatementLine(body=[m.Expr(value=m.SimpleString())])) or \
                       m.matches(stmt, m.SimpleStatementLine(body=[m.ImportFrom() | m.Import()])):
                        insert_idx = i + 1
                    # detectar st.set_page_config(...)
                    if m.matches(stmt, m.SimpleStatementLine(
                        body=[m.Expr(value=m.Call(func=m.Attribute(value=m.Name("st"), attr=m.Name("set_page_config"))))]
                    )):
                        after_page_config_idx = i + 1
                insert_at = after_page_config_idx if after_page_config_idx is not None else insert_idx
                body.insert(insert_at, cst.parse_statement("_ensure_lang_state()\n"))
                self.changed = True

        return upd.with_changes(body=body)

# --- File iteration -----------------------------------------------------------

def iter_files(root: Path) -> List[Path]:
    files: List[Path] = []
    for inc in INCLUDE:
        p = root / inc
        if p.is_file() and p.suffix == ".py":
            files.append(p)
        elif p.is_dir():
            for f in p.rglob("*.py"):
                # não tocar nestas pastas
                if any(seg in {"services", "tools", "__pycache__"} for seg in f.parts):
                    continue
                files.append(f)
    return sorted(files)

def main():
    root = Path(".").resolve()
    files = iter_files(root)
    changed = 0
    for f in files:
        code = f.read_text("utf-8")
        is_app = f.name == "app.py"
        try:
            mod = cst.parse_module(code)
        except Exception:
            # ignora ficheiros que não parseiam (rare)
            continue
        tr = EnsureI18nInit(is_app_file=is_app)
        new = mod.visit(tr)
        if tr.changed and new.code != code:
            # backup e grava
            (f.with_suffix(f.suffix + ".bak")).write_text(code, "utf-8")
            f.write_text(new.code, "utf-8")
            print(f"[PATCHED] {f}")
            changed += 1
    print(f"Arquivos atualizados: {changed}/{len(files)}")

if __name__ == "__main__":
    main()
