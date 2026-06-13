"""pipeline/duckdb_views.py — Register Parquet files as DuckDB views or tables.

Called at the end of the silver and gold pipeline steps when
``duckdb.enabled: true`` is set inside ``bronze_to_silver:`` or
``silver_to_gold:`` in the layer YAML.

Two modes
---------
``views``  (default)
    ``CREATE OR REPLACE VIEW <stem> AS SELECT * FROM read_parquet('<abs_path>')``
    The .duckdb file is tiny — data stays in Parquet.  Not standalone-shareable;
    recipients need both the .duckdb file *and* the Parquet files.

``tables``
    ``CREATE OR REPLACE TABLE <stem> AS SELECT * FROM read_parquet('<abs_path>')``
    Data is copied into DuckDB.  The resulting file is self-contained and can be
    shared on its own (e.g. attached to a report or sent to a colleague).
    File size ≈ sum of Parquet sizes.
"""
from __future__ import annotations

from pathlib import Path

_VALID_MODES = {"views", "tables"}


def register(
    parquet_dir: str | Path,
    db_path: str | Path,
    mode: str = "views",
) -> None:
    """Register all ``*.parquet`` files in *parquet_dir* as DuckDB views or tables.

    Parameters
    ----------
    parquet_dir:
        Directory containing the Parquet files to register.
    db_path:
        Path to the ``.duckdb`` file to create or update.  Parent directories
        are created automatically.
    mode:
        ``"views"`` — lightweight pointer, data stays in Parquet (default).
        ``"tables"`` — data embedded in DuckDB, file is standalone-shareable.
    """
    import duckdb

    if mode not in _VALID_MODES:
        raise ValueError(
            f"[duckdb] mode must be one of {sorted(_VALID_MODES)}, got '{mode}'"
        )

    parquet_dir = Path(parquet_dir)
    db_path     = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    parquets = sorted(parquet_dir.glob("*.parquet"))
    if not parquets:
        print(f"⚠️   [duckdb] no Parquet files found in {parquet_dir} — skipping")
        return

    kind = "VIEW" if mode == "views" else "TABLE"
    con  = duckdb.connect(str(db_path))
    try:
        for path in parquets:
            abs_path = str(path.resolve()).replace("\\", "/")
            con.execute(
                f"CREATE OR REPLACE {kind} {path.stem} AS "
                f"SELECT * FROM read_parquet('{abs_path}')"
            )
        print(
            f"🦆  [duckdb] {len(parquets)} {kind.lower()}s registered → {db_path}"
            f"  (mode: {mode})"
        )
    finally:
        con.close()
