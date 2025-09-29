# utils/timing.py
from __future__ import annotations
import time
from contextlib import contextmanager
from typing import Callable, Any, List
import streamlit as st
import pandas as pd

_PERF_KEY = "_perf_log"

def clear_perf() -> None:
    st.session_state[_PERF_KEY] = []

def _log(label: str, ms: float, state: str) -> None:
    st.session_state.setdefault(_PERF_KEY, []).append(
        {"label": label, "ms": float(ms), "state": state}
    )

@contextmanager
def timed(label: str, show_status: bool = True, expanded: bool = False):
    """Usa com 'with timed("Nome do bloco"):' para cronometrares e ver no UI."""
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
        ms = (time.perf_counter() - t0) * 1000
        _log(label, ms, "error")
        if status:
            status.update(label=f"❌ {label} — {ms:.0f} ms", state="error")
        elif placeholder:
            placeholder.error(f"❌ {label} — {ms:.0f} ms")
        raise
    else:
        ms = (time.perf_counter() - t0) * 1000
        _log(label, ms, "ok")
        if status:
            status.update(label=f"✅ {label} — {ms:.0f} ms", state="complete")
        elif placeholder:
            placeholder.success(f"✅ {label} — {ms:.0f} ms")

def timed_call(label: str, fn: Callable[..., Any], *args, **kwargs) -> Any:
    with timed(label):
        return fn(*args, **kwargs)

def show_perf_panel(where: str = "sidebar", title: str = "⏱️ Desempenho", enabled: bool | None = None):
    """
    Mostra o painel de desempenho **apenas** se:
      - enabled=True, OU
      - query param ?perf=1/true/yes estiver presente.
    """
    # decidir visibilidade (default: escondido)
    if enabled is None:
        try:
            q = st.query_params.get("perf", "0")
        except Exception:
            q = "0"
        enabled = str(q).lower() in ("1", "true", "t", "yes", "y")

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
        st.dataframe(df[["label","ms","state"]], hide_index=True, use_container_width=True)
