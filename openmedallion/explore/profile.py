"""explore/profile.py — ydata-profiling HTML report generator."""
from __future__ import annotations

from pathlib import Path


def generate_profile(
    source: Path,
    output: Path,
    title: str = "",
    *,
    minimal: bool = False,
) -> None:
    """Write a ydata-profiling HTML report from a Parquet file.

    Args:
        source:  Input Parquet file (gold layer output).
        output:  Destination HTML file path.
        title:   Report title shown in the HTML header.
        minimal: When True, skips expensive correlation / interaction charts.
                 Useful for wide tables (50+ columns).
    """
    from ydata_profiling import ProfileReport
    import polars as pl

    df = pl.read_parquet(source).to_pandas()
    report = ProfileReport(df, title=title or source.stem, minimal=minimal)
    output.parent.mkdir(parents=True, exist_ok=True)
    report.to_file(output)
