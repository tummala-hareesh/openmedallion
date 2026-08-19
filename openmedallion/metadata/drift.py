"""metadata/drift.py — schema-drift detection for `medallion metadata refresh`.

Closes the flagged gap: metadata.yaml's `status: stale` has existed in the
schema since build order step 1, but nothing ever set a table to `stale`.

Pure, no-LLM: compares a table's stored `TableMeta.schema_hash` (set the last
time metadata was drafted/refreshed) against a fresh DuckDB DESCRIBE of the
live silver/gold Parquet. A table with no stored hash (drafted before this
field existed) is NOT treated as drift — it simply hasn't been fingerprinted
yet; `refresh_metadata()` sets its hash on next write without flagging it.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

import duckdb
import yaml

from openmedallion.config.loader import load_project


def _schema_fingerprint(col_info: list[tuple[str, str]]) -> str:
    """Order-independent hash of (column_name, dtype) pairs."""
    canonical = "|".join(f"{name}:{dtype}" for name, dtype in sorted(col_info))
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def _describe_columns(path: Path) -> list[tuple[str, str]]:
    con = duckdb.connect()
    try:
        return [
            (row[0], row[1])
            for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
        ]
    finally:
        con.close()


def detect_drift(
    project: str,
    projects_root: str | Path = "projects",
) -> tuple[dict[str, str], list[str]]:
    """Compare stored ``schema_hash`` against live Parquet for every tracked table.

    Returns:
        ``(drift, dropped)`` — ``drift`` maps table name -> human-readable reason
        for tables whose live schema hash differs from the stored one (tables
        with no stored hash yet are excluded, not treated as drift). ``dropped``
        lists tables present in ``metadata.yaml`` whose Parquet file no longer
        exists in either layer.
    """
    root      = Path(projects_root) / project
    meta_path = root / "metadata.yaml"
    if not meta_path.exists():
        return {}, []

    with open(meta_path) as f:
        raw = yaml.safe_load(f) or {}
    tables: dict = raw.get("tables") or {}
    if not tables:
        return {}, []

    cfg        = load_project(project, projects_root)
    silver_dir = Path(cfg["paths"]["silver"])
    gold_dir   = Path(cfg["paths"]["gold"]) / cfg["pipeline"]["name"]

    drift: dict[str, str] = {}
    dropped: list[str] = []

    for name, entry in tables.items():
        stored_hash = entry.get("schema_hash")
        layer = entry.get("layer", "silver")
        layer_dir = silver_dir if layer == "silver" else gold_dir
        path = layer_dir / f"{name}.parquet"

        if not path.exists():
            dropped.append(name)
            continue

        if stored_hash is None:
            continue  # not fingerprinted yet — refresh will set it, not flag it

        live_hash = _schema_fingerprint(_describe_columns(path))
        if live_hash != stored_hash:
            drift[name] = f"schema changed since last metadata refresh ({stored_hash} -> {live_hash})"

    return drift, dropped
