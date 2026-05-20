"""cli/main.py — CLI entry point for openmedallion.

Commands
--------
medallion init <project>
    Scaffold a new project under <project>/.

medallion run <project> [--layer LAYER] [--projects PATH] [--no-explore]
    Run the pipeline for a project.

    --layer     Which layer to run up to and including.
                bronze   — ingest only
                silver   — bronze + silver
                gold     — bronze + silver + gold  (default)
                export   — full pipeline including BI export
                explore  — bronze → silver → gold → HTML reports
                           (requires [profile] and/or [explore] extras)

    --no-explore  Skip all inline explore: report generation in every layer.
                  Useful for fast runs or CI where optional deps are absent.

    --projects  Override the project root directory (default: . — current directory).

Examples
--------
    medallion init      sales_project
    medallion run       sales_project
    medallion run       sales_project --layer bronze
    medallion run       sales_project --layer explore
    medallion run       sales_project --no-explore
"""
import argparse
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

from openmedallion.scaffold.templates import init_project
from openmedallion.config.loader      import load_project
from hamilton                         import driver
from openmedallion.pipeline           import nodes as pipeline_nodes


# Maps --layer value → Hamilton final_vars + human label
LAYERS: dict[str, tuple[list[str], str]] = {
    "bronze":  (["bronze"],    "bronze ingestion"),
    "silver":  (["silver"],    "bronze → silver"),
    "gold":    (["gold"],      "bronze → silver → gold"),
    "export":  (["bi_export"], "full pipeline + BI export"),
    "explore": (["explore"],   "bronze → silver → gold → HTML reports"),
}
DEFAULT_LAYER = "gold"


# ---------------------------------------------------------------------------
# Command implementations
# ---------------------------------------------------------------------------

def cmd_init(args: argparse.Namespace) -> None:
    _W = 58
    print(f"\n{'━' * _W}")
    print(f"  medallion  ·  init  ·  {args.project}")
    print(f"{'━' * _W}\n")
    init_project(
        project=args.project,
        path_project=args.path_project,
        path_data=args.path_data or None,
    )


def cmd_run(args: argparse.Namespace) -> None:
    final_vars, label = LAYERS[args.layer]
    _W = 58

    print(f"\n{'━' * _W}")
    print(f"  medallion  ·  {args.project}  ·  {label}")
    print(f"{'━' * _W}\n")

    cfg = load_project(args.project, args.projects)
    if not args.explore:
        cfg["_explore"] = False

    builder = driver.Builder().with_modules(pipeline_nodes)
    dr = builder.build()

    inputs:    dict = {"cfg": cfg}
    overrides: dict = {}

    if args.layer in ("silver", "gold"):
        bronze_paths = _discover_bronze_paths(cfg)
        if bronze_paths:
            overrides["bronze"] = bronze_paths
            print("\n  ⏭️  bronze  skipped (existing files)")

    if args.layer == "gold":
        silver_paths = _discover_silver_paths(cfg)
        if silver_paths:
            overrides["silver"] = silver_paths
            print("  ⏭️  silver  skipped (existing files)")

    dr.execute(final_vars=final_vars, inputs=inputs, overrides=overrides)

    print(f"\n{'━' * _W}")
    print(f"  ✅  {label} complete.")
    print(f"{'━' * _W}\n")


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _discover_bronze_paths(cfg: dict) -> dict[str, Path]:
    bronze_dir = Path(cfg["paths"]["bronze"])
    if not bronze_dir.exists():
        print(f"⚠️   [bronze] directory not found: {bronze_dir}")
        return {}
    return {p.stem: p for p in bronze_dir.glob("*.parquet")}


def _discover_silver_paths(cfg: dict) -> dict[str, Path]:
    silver_dir = Path(cfg["paths"]["silver"])
    if not silver_dir.exists():
        print(f"⚠️   [silver] directory not found: {silver_dir}")
        return {}
    return {p.name: p for p in silver_dir.glob("*.parquet")}


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="medallion",
        description="Medallion data pipeline — dlt + Polars + Hamilton",
    )
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    sub.required = True

    # init
    p_init = sub.add_parser("init", help="Scaffold a new project directory")
    p_init.add_argument("project", help="Project name (used as the folder name)")
    p_init.add_argument(
        "--path-project", default=".", metavar="PATH",
        help="Directory where <project>/ folder is created (default: . — current directory)",
    )
    p_init.add_argument(
        "--path-data", default="", metavar="PATH",
        help="Base data directory written into main.yaml paths (default: 'data')",
    )

    # run
    p_run = sub.add_parser("run", help="Execute the pipeline for a project")
    p_run.add_argument("project", help="Project name")
    p_run.add_argument(
        "--layer", choices=list(LAYERS), default=DEFAULT_LAYER, metavar="LAYER",
        help=f"Layer to run up to: {', '.join(LAYERS)} (default: {DEFAULT_LAYER})",
    )
    p_run.add_argument(
        "--projects", default=".", metavar="PATH",
        help="Parent directory containing the project folder (default: . — current directory)",
    )
    p_run.add_argument(
        "--no-explore", dest="explore", action="store_false", default=True,
        help="Skip all inline explore: report generation in every layer",
    )

    return parser


_HANDLERS = {
    "init": cmd_init,
    "run":  cmd_run,
}


def main() -> None:
    """Console script entry point — invoked by ``medallion`` command."""
    _parser = _build_parser()
    _args   = _parser.parse_args()
    _HANDLERS[_args.command](_args)


if __name__ == "__main__":
    main()
