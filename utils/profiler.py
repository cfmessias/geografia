# utils/profiler.py
from __future__ import annotations
from contextlib import contextmanager
from cProfile import Profile
import pstats, io
import streamlit as st
from typing import Iterable

@contextmanager
def cprofile_block(label: str, sort: str = "cumtime", top: int = 30,
                   include: Iterable[str] = ()):
    """
    Perfila o bloco e mostra um top 'top' por 'sort'.
    'include' filtra linhas pelo path (qualquer substring).
    """
    pr = Profile()
    pr.enable()
    try:
        yield
    finally:
        pr.disable()
        s = io.StringIO()
        stats = pstats.Stats(pr, stream=s)
        stats.strip_dirs().sort_stats(sort)

        if include:
            # filtra por ficheiros/módulos de interesse
            def _match(func):
                filename = func[0]
                return any(sub.lower() in filename.lower() for sub in include)
            stats = stats.filter(_match)

        stats.print_stats(top)
        st.markdown(f"#### 🔬 Perfil — {label}")
        st.code(s.getvalue(), language="text")
