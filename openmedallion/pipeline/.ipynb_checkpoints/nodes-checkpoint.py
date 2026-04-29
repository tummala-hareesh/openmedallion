"""pipeline/nodes.py — Hamilton DAG node definitions.

Each function is a Hamilton node. Hamilton infers execution order from
argument names — a node that takes ``bronze`` as an argument automatically
runs after the ``bronze`` node, unless ``bronze`` is injected directly
via ``inputs=`` in the driver call.

This enables layer-level execution:

    --layer bronze   →  final_vars=["bronze"]
    --layer silver   →  final_vars=["silver"],  inputs includes bronze paths
    --layer gold     →  final_vars=["gold"],    inputs includes silver paths
    --layer export   →  final_vars=["bi_export"]  (full run, default)

DAG shape
---------
.. code-block:: text

    cfg (injected)
        │
        ▼
    config
        │
        ├──▶ bronze({config})
        │         │
        │         ▼
        ├──▶ silver({config, bronze})
        │         │
        │         ▼
        ├──▶ gold({config, silver})
        │         │
        │         ▼
        └──▶ bi_export({config, gold})
"""
from pathlib import Path
from openmedallion.pipeline.bronze import BronzeLoader
from openmedallion.pipeline.silver import SilverTransformer
from openmedallion.pipeline.gold   import GoldAggregator
from openmedallion.pipeline.export import BIExporter


def config(cfg: dict) -> dict:
    """Pass the raw merged config dict into the DAG."""
    return cfg


def bronze(config: dict) -> dict[str, Path]:
    """Ingest source tables via dlt → raw Parquet in the bronze layer."""
    return BronzeLoader(config).load()


def silver(config: dict, bronze: dict[str, Path]) -> dict[str, Path]:
    """Apply structural transforms and UDFs → silver Parquet."""
    return SilverTransformer(config).transform()


def gold(config: dict, silver: dict[str, Path]) -> dict[str, list[Path]]:
    """Aggregate silver tables → per-BI-project gold Parquet."""
    return GoldAggregator(config).aggregate()


def bi_export(config: dict, gold: dict[str, list[Path]]) -> None:
    """Copy gold Parquet to the export directory; write CSV fallbacks."""
    BIExporter(config).export()
