# utils/timing.py
from __future__ import annotations

import os
import time
from contextlib import contextmanager
from typing import Callable, Any, List

import pandas as pd
import streamlit as st

# onde guardamos o log dos tempos
_PERF_KEY = "_perf_log"


# ───────────────────────── helpers de ativação ─────────────────────────

def _perf_enabled() -> bool:
    """
    Perf UI/medidas visíveis ficam ATIVAS se:
      - query param ?perf=1/true/yes/on, OU
      - env var SHOW_TIMERS=1/true/yes/on.
    Caso contrário, ficam silenciosas (sem widgets).
    """
    # 1) query param (prioritário)
    try:
        q = st.query_params.get("perf")  # Streamlit >= 1.29
    except Exception:
        # compat: versões antigas sem .query_params
        try:
            q = st.experimental_get_query_params().get("perf", [None])[0]
        except Exception:
            q = None
    if q is not None:
        return str(q).lower() in ("1", "true", "t", "yes", "y", "on")

    # 2) env var
    return os.getenv("SHOW_TIMERS", "").lower() in ("1", "true", "t", "yes", "y", "on")


# ───────────────────────── API pública ─────────────────────────

def clear_perf() -> None:
    """Limpa o log de tempos da sessão atual."""
    st.session_state[_PERF_KEY] = []


def _log(label: str, ms: float, state: str) -> None:
    st.session_state.setdefault(_PERF_KEY, []).append(
        {"label": label, "ms": float(ms), "state": state}
    )


@contextmanager
def timed(label: str, show_status: bool | None = None, expanded: bool = False):
    """
    Cronometra um bloco de código.

    Uso:
        with timed("🌍 Países"):
            ...

    Comportamento:
      - Por omissão NÃO mostra status (silencioso).
      - Ativa a UI com ?perf=1 (ou SHOW_TIMERS=1) ou force com show_status=True.
      - O tempo fica sempre registado em session_state para show_perf_panel().
    """
    if show_status is None:
        show_status = _perf_enabled()

    status = None
    placeholder = None
    t0 = time.perf_counter()
    try:
        if show_status and callable(getattr(st, "status", None)):
            status = st.status(f"⏳ {label}", state="running", expanded=expanded)
        elif show_status:
            placeholder = st.empty()
            placeholder.info(f"⏳ {label}…")
        yield
    except Exception:
        ms = (time.perf_counter() - t0) * 1000.0
        _log(label, ms, "error")
        if show_status:
            if status:
                status.update(label=f"❌ {label} — {ms:.0f} ms", state="error")
            elif placeholder:
                placeholder.error(f"❌ {label} — {ms:.0f} ms")
        raise
    else:
        ms = (time.perf_counter() - t0) * 1000.0
        _log(label, ms, "ok")
        if show_status:
            if status:
                status.update(label=f"✅ {label} — {ms:.0f} ms", state="complete")
            elif placeholder:
                placeholder.success(f"✅ {label} — {ms:.0f} ms")


def timed_call(label: str, fn: Callable[..., Any], *args, **kwargs) -> Any:
    """Atalho: mede uma função e devolve o resultado."""
    with timed(label):
        return fn(*args, **kwargs)


def show_perf_panel(where: str = "sidebar", title: str = "⏱️ Desempenho", enabled: bool | None = None):
    """
    Mostra um painel com o log de tempos (somatório + tabela).
    Só aparece se:
      - enabled=True, OU
      - ?perf=1 / SHOW_TIMERS=1.
    """
    if enabled is None:
        enabled = _perf_enabled()
    if not enabled:
        return

    log: List[dict] = st.session_state.get(_PERF_KEY, [])
    if not log:
        return

    df = pd.DataFrame(log)
    df["ms"] = df["ms"].round(0).astype(int)
    total = int(df["ms"].sum())

    target = st.sidebar if where == "sidebar" else st
    with target:
        st.markdown(f"### {title}")
        st.caption(f"Total do rerun: **{total} ms**")
        st.dataframe(
            df[["label", "ms", "state"]],
            hide_index=True,
            use_container_width=True,
        )
