# services/i18n.py
# i18n leve com JSON, plurais simples e fallback para EN.
# Funciona em runtime; ideal para Streamlit na Cloud.

from __future__ import annotations
from pathlib import Path
import json
from typing import Any, Dict

_LOCALES_DIR = Path(__file__).resolve().parent.parent / "locales"
_DEFAULT_LANG = "pt"
_FALLBACK_LANG = "en"

class I18n:
    def __init__(self, default_lang: str = _DEFAULT_LANG):
        self.lang = default_lang
        self._cache: Dict[str, Dict[str, Any]] = {}
        # Precarrega default + fallback para evitar I/O múltiplo
        for code in {default_lang, _FALLBACK_LANG}:
            self._load_lang(code)

    def _load_lang(self, lang: str) -> None:
        if lang in self._cache:
            return
        path = _LOCALES_DIR / f"{lang}.json"
        if path.exists():
            self._cache[lang] = json.loads(path.read_text(encoding="utf-8"))
        else:
            self._cache[lang] = {}

    def set_language(self, lang: str) -> None:
        self._load_lang(lang)
        self.lang = lang

    # lookup por chave "a.b.c"
    def _get(self, lang: str, key: str) -> Any:
        data = self._cache.get(lang, {})
        node: Any = data
        for part in key.split("."):
            if not isinstance(node, dict):
                return None
            node = node.get(part)
            if node is None:
                return None
        return node

    def t(self, key: str, **params) -> str:
        # 1) idioma corrente, 2) fallback en, 3) devolve a própria chave
        s = self._get(self.lang, key)
        if s is None:
            s = self._get(_FALLBACK_LANG, key)
        if s is None:
            # ajuda a localizar chaves em falta durante dev
            return f"[{key}]"
        return s.format(**params) if params else s

    def tn(self, key_base: str, n: int, **params) -> str:
        # pluralização simples (zero/one/other)
        form = "zero" if n == 0 else "one" if n == 1 else "other"
        key = f"{key_base}.{form}"
        params = {"n": n, **params}
        return self.t(key, **params)

# Singleton simples para uso em todo o app
_i18n = I18n()

def set_language(lang: str) -> None:
    _i18n.set_language(lang)

def t(key: str, **params) -> str:
    return _i18n.t(key, **params)

def tn(key_base: str, n: int, **params) -> str:
    return _i18n.tn(key_base, n, **params)
