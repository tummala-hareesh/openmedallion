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

medallion query <project> "<question>" [--projects PATH] [--model MODEL] [--provider PROVIDER]
    Ask a natural-language question directly in the terminal — no server needed.
    Requires: openmedallion[cerebrum]

    --projects  Project root directory (default: . — current directory).
    --model     Model identifier (default: from settings / llama3.2).
    --provider  LLM backend: ollama | openrouter | openai | custom
                (default: from settings / ollama).

medallion ask <project> [--projects PATH] [--port PORT] [--model MODEL] [--provider PROVIDER]
    Start the cerebrum LLM engine + neuron FastAPI server on :8000.
    Requires: openmedallion[cerebrum]

    --projects  Project root directory (default: . — current directory).
    --port      Port for the neuron HTTP server (default: 8000).
    --model     Model identifier (default: from settings / llama3.2).
    --provider  LLM backend: ollama | openrouter | openai | custom
                (default: from settings / ollama).

medallion cortex <project> [--neuron-url URL] [--port PORT] [--debug]
    Start the Dash cortex UI on :8050 (connects to neuron at --neuron-url).
    Requires: openmedallion[cortex]

    --neuron-url  URL of the running neuron server (default: http://localhost:8000).
    --port        Port for the Dash server (default: 8050).
    --debug       Enable Dash hot-reload (development mode).

Examples
--------
    medallion init      sales_project
    medallion run       sales_project
    medallion run       sales_project --layer bronze
    medallion run       sales_project --layer explore
    medallion run       sales_project --no-explore
    medallion query     sales_project "What are the top 5 products by revenue?"
    medallion query     sales_project "Show monthly trends" --model mistral
    medallion query     sales_project "Top revenue" --provider openrouter --model openai/gpt-4o
    medallion ask       sales_project
    medallion ask       sales_project --model mistral --port 8001
    medallion ask       sales_project --provider openrouter --model openai/gpt-4o
    medallion cortex    sales_project
    medallion cortex    sales_project --neuron-url http://localhost:8001 --debug
"""
import argparse
import sys
from pathlib import Path
from openmedallion.scaffold.templates import init_project
from openmedallion.config.loader      import load_project
from hamilton                         import driver
from openmedallion.pipeline           import nodes as pipeline_nodes

sys.stdout.reconfigure(encoding="utf-8")

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


def cmd_query(args: argparse.Namespace) -> None:
    _W = 58
    print(f"\n{'━' * _W}")
    print(f"  medallion  ·  query  ·  {args.project}")
    print(f"{'━' * _W}\n")

    try:
        from openmedallion.cerebrum.pipeline import CerebrumPipeline
    except ImportError:
        print("  ❌  cerebrum not installed — run: pip install 'openmedallion[cerebrum]'")
        sys.exit(1)

    from openmedallion.config import settings

    provider = args.provider or settings.LLM_PROVIDER
    model    = args.model    or settings.LLM_MODEL
    cfg        = load_project(args.project, args.projects)
    silver_dir = Path(cfg["paths"]["silver"])

    if not silver_dir.exists():
        print(f"  ❌  Silver layer not found: {silver_dir}")
        print(f"       Run: medallion run {args.project}")
        sys.exit(1)

    print(f"  🤖  provider →  {provider}")
    print(f"  🤖  model    →  {model}")
    print(f"  🗂️   silver   →  {silver_dir}")
    print(f"  ❓  question  →  {args.question}\n")

    def _on_step(msg: str) -> None:
        print(f"  ·  {msg}...", flush=True)

    try:
        import httpx
        pipeline = CerebrumPipeline(
            silver_dir,
            model=model,
            provider=provider,
            api_key=settings.LLM_API_KEY,
            base_url=settings.LLM_BASE_URL,
        )
        qr = pipeline.ask(args.question, on_step=_on_step)
    except (httpx.ConnectError, httpx.ConnectTimeout):
        if provider == "ollama":
            _url = settings.LLM_BASE_URL or settings.OLLAMA_URL
            print(f"\n  ❌  Ollama is not reachable at {_url}")
            print("       Start it with: ollama serve")
        else:
            print(f"\n  ❌  LLM provider '{provider}' is not reachable.")
            print("       Check your base_url and network connection.")
        sys.exit(1)
    except Exception as exc:
        print(f"\n  ❌  {exc}")
        sys.exit(1)

    rows = qr.result.to_dicts()

    print(f"  SQL\n  {'─' * (_W - 2)}")
    for line in qr.sql.strip().splitlines():
        print(f"    {line}")

    print(f"\n  Results  ({len(rows)} row{'s' if len(rows) != 1 else ''})")
    print(f"  {'─' * (_W - 2)}")
    if rows:
        _print_table(rows)
    else:
        print("  (no rows returned)")

    if qr.recommended_prompt:
        print("\n  Recommended prompt")
        print(f"  {'─' * (_W - 2)}")
        print(f"  {qr.recommended_prompt}")

    print(f"\n{'━' * _W}\n")


def _print_table(rows: list[dict]) -> None:
    """Print a list of dicts as a plain aligned table."""
    if not rows:
        return
    cols    = list(rows[0].keys())
    widths  = {c: max(len(str(c)), *(len(str(r.get(c, ""))) for r in rows)) for c in cols}
    sep     = "  " + "  ".join("─" * widths[c] for c in cols)
    header  = "  " + "  ".join(str(c).ljust(widths[c]) for c in cols)
    print(header)
    print(sep)
    for row in rows:
        print("  " + "  ".join(str(row.get(c, "")).ljust(widths[c]) for c in cols))


def cmd_ask(args: argparse.Namespace) -> None:
    _W = 58
    print(f"\n{'━' * _W}")
    print(f"  medallion  ·  ask  ·  {args.project}")
    print(f"{'━' * _W}\n")

    try:
        import uvicorn
    except ImportError:
        print("  ❌  uvicorn not installed — run: pip install 'openmedallion[cerebrum]'")
        sys.exit(1)

    import os
    from openmedallion.config import settings

    provider = args.provider or settings.LLM_PROVIDER
    model    = args.model    or settings.LLM_MODEL
    os.environ["MEDALLION_PROJECTS_ROOT"] = args.projects
    os.environ["MEDALLION_LLM_PROVIDER"]  = provider
    os.environ["MEDALLION_LLM_MODEL"]     = model

    base = f"http://localhost:{args.port}"
    print(f"  🧠  neuron   →  {base}")
    print(f"  🤖  provider →  {provider}")
    print(f"  🤖  model    →  {model}")
    print(f"  📋  api docs  →  {base}/docs")
    print(f"  ❤️   health   →  {base}/health\n")

    from openmedallion.neuron.server import app as neuron_app
    uvicorn.run(neuron_app, host="0.0.0.0", port=args.port)


def cmd_cortex(args: argparse.Namespace) -> None:
    _W = 58
    print(f"\n{'━' * _W}")
    print(f"  medallion  ·  cortex  ·  {args.project}")
    print(f"{'━' * _W}\n")

    try:
        import dash  # noqa: F401
    except ImportError:
        print("  ❌  dash not installed — run: pip install 'openmedallion[cortex]'")
        sys.exit(1)

    print(f"  🖥️   cortex  →  http://localhost:{args.port}")
    print(f"  🔗  neuron  →  {args.neuron_url}\n")

    from openmedallion.cortex.app import run as cortex_run
    cortex_run(
        args.project,
        neuron_url=args.neuron_url,
        port=args.port,
        debug=args.debug,
    )


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

    # query
    p_query = sub.add_parser(
        "query",
        help="Ask a question directly in the terminal — no server needed (requires [cerebrum] extra)",
    )
    p_query.add_argument("project",  help="Project name")
    p_query.add_argument("question", help="Natural-language question to ask")
    p_query.add_argument(
        "--projects", default=".", metavar="PATH",
        help="Parent directory containing the project folder (default: .)",
    )
    p_query.add_argument(
        "--model", default=None, metavar="MODEL",
        help="Model identifier (default: from settings.yaml / MEDALLION_LLM_MODEL / llama3.2)",
    )
    p_query.add_argument(
        "--provider", default=None, metavar="PROVIDER",
        help="LLM backend: ollama | openrouter | openai | custom (default: from settings / ollama)",
    )

    # ask
    p_ask = sub.add_parser(
        "ask",
        help="Start cerebrum + neuron LLM query server (requires [cerebrum] extra)",
    )
    p_ask.add_argument("project", help="Project name")
    p_ask.add_argument(
        "--projects", default=".", metavar="PATH",
        help="Parent directory containing the project folder (default: .)",
    )
    p_ask.add_argument(
        "--port", type=int, default=8000, metavar="PORT",
        help="Port for the neuron HTTP server (default: 8000)",
    )
    p_ask.add_argument(
        "--model", default=None, metavar="MODEL",
        help="Model identifier (default: from settings.yaml / MEDALLION_LLM_MODEL / llama3.2)",
    )
    p_ask.add_argument(
        "--provider", default=None, metavar="PROVIDER",
        help="LLM backend: ollama | openrouter | openai | custom (default: from settings / ollama)",
    )

    # cortex
    p_cortex = sub.add_parser(
        "cortex",
        help="Start Dash cortex UI on :8050 (requires [cortex] extra)",
    )
    p_cortex.add_argument("project", help="Project name")
    p_cortex.add_argument(
        "--neuron-url", default="http://localhost:8000", metavar="URL",
        help="Base URL of the running neuron server (default: http://localhost:8000)",
    )
    p_cortex.add_argument(
        "--port", type=int, default=8050, metavar="PORT",
        help="Port for the Dash server (default: 8050)",
    )
    p_cortex.add_argument(
        "--debug", action="store_true", default=False,
        help="Enable Dash hot-reload (development mode)",
    )

    return parser


_HANDLERS = {
    "init":   cmd_init,
    "run":    cmd_run,
    "query":  cmd_query,
    "ask":    cmd_ask,
    "cortex": cmd_cortex,
}


def main() -> None:
    """Console script entry point — invoked by ``medallion`` command."""
    _parser = _build_parser()
    _args   = _parser.parse_args()
    _HANDLERS[_args.command](_args)


if __name__ == "__main__":
    main()
