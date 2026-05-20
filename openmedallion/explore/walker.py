"""explore/walker.py — pygwalker interactive HTML explorer generator.

pygwalker has native Polars support — no pandas bridge needed.
"""
from __future__ import annotations

from pathlib import Path


def generate_walker(
    source: Path,
    output: Path,
    title: str = "",  # noqa: ARG001 — reserved for future pygwalker title API
) -> None:
    """Write a pygwalker interactive HTML explorer from a Parquet file.

    The output is a fully self-contained HTML file (all assets inlined) that
    renders a Tableau-like drag-and-drop interface in any modern browser.

    Args:
        source: Input Parquet file (gold layer output).
        output: Destination HTML file path.
        title:  Reserved — pygwalker does not currently expose a title arg.
    """
    import pygwalker as pyg
    import polars as pl

    df = pl.read_parquet(source)
    html: str = pyg.walk(df, return_html=True)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html, encoding="utf-8")
