# utils/header.py
from __future__ import annotations
import streamlit as st
from services.i18n import t as tr

_CSS = """
<style>
.g2k-hero{
  margin:.2rem 0 .6rem;
}
.g2k-row{                 /* linha só com emoji + título */
  display:flex;
  align-items:center;     /* emoji alinhado ao título */
  gap:.6rem;
}
.g2k-ico{
  font-size:1.6rem;       /* ajusta se quiseres maior/menor */
  line-height:1;
}
.g2k-title{
  margin:0;
  font-size:1.45rem;      /* H2 compacto */
  line-height:1.25;
  font-weight:800;
}
.g2k-tag, .g2k-sub{
  color:var(--secondary-text-color);
  font-size:.96rem;
  margin:.25rem 0 0 2.2rem; /* recuo igual à largura do emoji para alinhar visualmente */
}
</style>
"""

def render_brand_header(icon: str | None = "🌍", show_divider: bool = False) -> None:
    """Cabeçalho compacto: (emoji + título) na 1ª linha; textos abaixo sem emoji."""
    st.markdown(_CSS, unsafe_allow_html=True)
    ico_html = f"<div class='g2k-ico'>{icon}</div>" if icon else ""
    st.markdown(
        f"""
<div class="g2k-hero">
  <div class="g2k-row">
    {ico_html}
    <h2 class="g2k-title">{tr("app.name")}</h2>
  </div>
  <div class="g2k-tag">{tr("app.tagline")}</div>
  <div class="g2k-sub">{tr("app.subtitle")}</div>
</div>
""",
        unsafe_allow_html=True,
    )
    if show_divider:
        st.divider()
