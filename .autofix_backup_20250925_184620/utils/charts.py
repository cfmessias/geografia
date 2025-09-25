# -*- coding: utf-8 -*-
from typing import Iterable, Optional
import plotly.express as px
from plotly.graph_objs import Figure

GRID_COLOR = "rgba(180, 180, 180, 0.35)"
ZEROLINE_COLOR = "rgba(180, 180, 180, 0.6)"
GRID_WIDTH = 1.2
MARGIN = dict(l=10, r=10, t=60, b=10)
TITLE_X = 0.02
# utils/charts.py
import pandas as pd


# utils/charts.py
def _apply_grid(fig, strong: bool = True):
    # Y: grelha principal (mais visível)
    fig.update_yaxes(
        showgrid=True,
        gridcolor="rgba(160,160,160,0.35)" if strong else "rgba(160,160,160,0.20)",
        gridwidth=1.2 if strong else 0.8,
        zeroline=False,
    )
    # X: grelha mais suave, só para ajudar leitura
    fig.update_xaxes(
        showgrid=True,
        gridcolor="rgba(160,160,160,0.18)" if strong else "rgba(160,160,160,0.12)",
        gridwidth=0.8 if strong else 0.6,
        zeroline=False,
    )
    return fig

def line_with_tail_labels(
    df: pd.DataFrame,
    x: str,
    y: str,
    color: str = "source",
    title: str = "",
    x_title: str = "",
    y_title: str = "",
    height: int = 280,
    label_font_size: int = 12,
    markers: bool = True,           # <- podes ligar/desligar marcadores
):
    fig = px.line(df, x=x, y=y, color=color, title=title, markers=markers)
    fig.update_layout(
        showlegend=False,
        height=height,
        margin=dict(l=6, r=6, t=40, b=0),
        xaxis_title=x_title,
        yaxis_title=y_title,
    )

    # ordem temporal + folga à direita para rótulos no fim das linhas
    dff = df.sort_values(x)
    try:
        xmin = pd.to_datetime(dff[x]).min()
        xmax = pd.to_datetime(dff[x]).max()
        if pd.notna(xmax) and pd.notna(xmin):
            fig.update_xaxes(range=[xmin, xmax + pd.Timedelta(hours=36)])
    except Exception:
        pass

    # rótulo no último ponto de cada série
    for i, (serie, g) in enumerate(dff.groupby(color)):
        g = g.dropna(subset=[y])
        if g.empty:
            continue
        last = g.iloc[-1]
        fig.add_annotation(
            x=last[x], y=last[y], text=str(serie),
            xanchor="left", yanchor="middle",
            xshift=8, yshift=(i - 1) * 8,
            font=dict(size=label_font_size),
            showarrow=False,
        )

    _apply_grid(fig, strong=True)  # <- grelha visível
    return fig

def _apply_base_layout(fig: Figure, x_title: str = "", y_title: str = "", title: Optional[str] = None) -> Figure:
    if title is not None:
        fig.update_layout(title=title, title_x=TITLE_X)
    fig.update_layout(margin=MARGIN, hovermode="x unified", legend_title_text="")
    fig.update_xaxes(showgrid=True, gridcolor=GRID_COLOR, gridwidth=GRID_WIDTH,
                     zeroline=True, zerolinewidth=1.2, zerolinecolor=ZEROLINE_COLOR,
                     title=x_title)
    fig.update_yaxes(showgrid=True, gridcolor=GRID_COLOR, gridwidth=GRID_WIDTH,
                     zeroline=True, zerolinewidth=1.2, zerolinecolor=ZEROLINE_COLOR,
                     title=y_title)
    return fig

def line(df, x, y, *, color=None, markers=True, title=None, x_title="", y_title="") -> Figure:
    fig = px.line(df, x=x, y=y, color=color, markers=markers)
    return _apply_base_layout(fig, x_title, y_title, title)

def bar(df, x, y, *, color=None, title=None, x_title="", y_title="") -> Figure:
    fig = px.bar(df, x=x, y=y, color=color)
    return _apply_base_layout(fig, x_title, y_title, title)

def hist(df, x, *, nbins=None, title=None, x_title="", y_title="Contagem") -> Figure:
    fig = px.histogram(df, x=x, nbins=nbins)
    return _apply_base_layout(fig, x_title, y_title, title)

def scatter_geo(df, lat, lon, *, size=None, color=None, title=None, hover_data=None) -> Figure:
    fig = px.scatter_geo(df, lat=lat, lon=lon, size=size, color=color, hover_data=hover_data)
    fig.update_layout(margin=dict(l=0, r=0, t=40, b=0), title=title, title_x=TITLE_X)
    return fig

def set_y_range(fig: Figure, min_val: float, max_val: float) -> Figure:
    fig.update_yaxes(range=[min_val, max_val]); return fig

def add_trend_line(fig: Figure, x: Iterable, y_fit: Iterable, name: str) -> Figure:
    fig.add_scatter(x=list(x), y=list(y_fit), mode="lines", name=name); return fig
