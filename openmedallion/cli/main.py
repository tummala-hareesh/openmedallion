"""cli/main.py — CLI entry point for openmedallion.

Commands
--------
medallion init <project>
    Scaffold a new project under <project>/.

medallion run <project> [--layer LAYER] [--projects PATH] [--track]
    Run the pipeline for a project.

    --layer     Which layer to run up to and including.
                bronze  — ingest only
                silver  — bronze + silver
                gold    — bronze + silver + gold  (default)
                export  — full pipeline including BI export

    --projects  Override the project root directory (default: . — current directory).

    --track     Start the live status dashboard at http://localhost:8765 and
                attach the status tracker to the Hamilton DAG.

medallion dag
    Print the Hamilton pipeline DAG as a text tree (no graphviz required).

medallion visualize <project> [--layer LAYER] [--output PATH] [--open]
    Export the Hamilton DAG as a PNG image (requires graphviz).

medallion status [--port PORT]
    Start the live status dashboard server standalone.

Examples
--------
    medallion init      sales_project
    medallion run       sales_project
    medallion run       sales_project --layer bronze
    medallion run       sales_project --track
    medallion dag
    medallion visualize sales_project
    medallion visualize sales_project --layer gold --open
    medallion status
    medallion status    --port 9000
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
    "bronze": (["bronze"],    "bronze ingestion"),
    "silver": (["silver"],    "bronze → silver"),
    "gold":   (["gold"],      "bronze → silver → gold"),
    "export": (["bi_export"], "full pipeline + BI export"),
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

    builder = driver.Builder().with_modules(pipeline_nodes)
    if args.track:
        from openmedallion.viz.tracker import PipelineStatusTracker
        from openmedallion.viz.server  import start_server
        tracker = PipelineStatusTracker()
        start_server(block=False)
        builder = builder.with_adapters(tracker)

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


def cmd_dag(_: argparse.Namespace) -> None:
    from openmedallion.viz.dag import print_dag
    print_dag()


def cmd_visualize(args: argparse.Namespace) -> None:
    from openmedallion.viz.dag import export_dag, export_execution_dag, open_image

    if args.layer is None:
        out = Path(args.output or "dag.png")
        print(f"📊  Exporting full DAG → {out}")
        path = export_dag(out)
    else:
        out = Path(args.output or f"dag_{args.layer}.png")
        final_vars, _ = LAYERS[args.layer]
        cfg = load_project(args.project)
        inputs: dict = {"cfg": cfg}
        if args.layer in ("silver", "gold"):
            inputs["bronze"] = _discover_bronze_paths(cfg)
        if args.layer == "gold":
            inputs["silver"] = _discover_silver_paths(cfg)
        print(f"📊  Exporting execution DAG (layer={args.layer}) → {out}")
        path = export_execution_dag(final_vars, inputs, output_path=out)

    print(f"✅  Saved: {path}")
    if args.open:
        open_image(path)


def cmd_status(args: argparse.Namespace) -> None:
    from openmedallion.viz.server import start_server
    print(f"🖥️   Starting status dashboard on port {args.port} ...")
    start_server(port=args.port, block=True)


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
        "--track", action="store_true",
        help="Start live status dashboard and attach the Hamilton tracker",
    )

    # dag
    sub.add_parser("dag", help="Print the Hamilton pipeline DAG as a text tree")

    # visualize
    p_viz = sub.add_parser("visualize", help="Export the Hamilton DAG as a PNG image")
    p_viz.add_argument("project", help="Project name (used when --layer is set)")
    p_viz.add_argument(
        "--layer", choices=list(LAYERS), default=None, metavar="LAYER",
        help="Export only the execution path for this layer (default: full DAG)",
    )
    p_viz.add_argument("--output", default=None, metavar="PATH",
                       help="Output file path (default: dag.png or dag_<layer>.png)")
    p_viz.add_argument("--open", action="store_true",
                       help="Open the image in the default viewer after export")

    # status
    p_status = sub.add_parser("status", help="Start the live status dashboard server")
    p_status.add_argument(
        "--port", type=int, default=8765, metavar="PORT",
        help="TCP port to listen on (default: 8765)",
    )

    return parser


_HANDLERS = {
    "init":      cmd_init,
    "run":       cmd_run,
    "dag":       cmd_dag,
    "visualize": cmd_visualize,
    "status":    cmd_status,
}


def main() -> None:
    """Console script entry point — invoked by ``medallion`` command."""
    _parser = _build_parser()
    _args   = _parser.parse_args()
    _HANDLERS[_args.command](_args)


if __name__ == "__main__":
    main()
