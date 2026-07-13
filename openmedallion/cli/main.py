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

medallion metadata generate <project> [--projects PATH] [--model MODEL] [--provider PROVIDER]
    LLM-draft metadata.yaml for silver/gold tables not yet status: approved.
    Approved tables are left completely untouched. Part of the RAG accuracy
    roadmap (see CLAUDE.md) — `metadata refresh` is not yet built.
    Requires: openmedallion[cerebrum]

    --projects  Project root directory (default: . — current directory).
    --model     Model identifier (default: from settings / llama3.2).
    --provider  LLM backend: ollama | openrouter | openai | custom
                (default: from settings / ollama).

medallion metadata approve <project> [--projects PATH]
    Interactively review draft/stale tables in metadata.yaml one at a time —
    [a]pprove, [s]kip (leave as-is, reviewable later), or [q]uit early.
    Each approval is written to disk immediately, so quitting mid-review
    never loses already-approved tables.

    --projects  Project root directory (default: . — current directory).

medallion relationships generate <project> [--projects PATH]
    Detect silver/gold table relationships via three deterministic rules —
    FK naming convention, source-table lineage, shared grain columns. No LLM
    call. Approved relationships are left completely untouched; hand-added
    relationships (no detection method) are always preserved.

    --projects  Project root directory (default: . — current directory).

medallion relationships approve <project> [--projects PATH]
    Interactively review draft/stale relationships one at a time — [a]pprove,
    [s]kip (leave as-is, reviewable later), or [q]uit early. Each approval is
    written to disk immediately, so quitting mid-review never loses
    already-approved relationships.

    --projects  Project root directory (default: . — current directory).

medallion examples generate <project> [--projects PATH] [--count N] [--model MODEL] [--provider PROVIDER]
    LLM-draft synthetic (question, sql) pairs from status: approved
    silver-layer tables in metadata.yaml + approved relationships between
    them. Each SQL is validated (allowlist + DuckDB EXPLAIN), with one retry
    on failure — examples that never validate are dropped, not written.
    Existing verified: true examples are kept; verified: false ones are
    replaced by the fresh batch. Requires: openmedallion[cerebrum]

    --projects  Project root directory (default: . — current directory).
    --count     Target number of (question, sql) pairs to request (default: 20).
    --model     Model identifier (default: from settings / llama3.2).
    --provider  LLM backend: ollama | openrouter | openai | custom
                (default: from settings / ollama).

medallion examples approve <project> [--projects PATH]
    Interactively review unverified examples in synthetic.jsonl one at a
    time — [a]pprove, [s]kip (leave as-is, reviewable later), or [q]uit
    early. Each approval is written to disk immediately.

    --projects  Project root directory (default: . — current directory).

medallion examples harvest <project> [--projects PATH]
    Promote thumbs-up candidates from examples/harvested.jsonl into
    synthetic.jsonl as verified: false — still needs a human pass via
    `examples approve` before becoming a few-shot example. Promoted
    candidates are marked status: harvested so re-running never
    double-promotes them; duplicates of existing synthetic.jsonl entries
    are skipped.

    --projects  Project root directory (default: . — current directory).

medallion examples review <project> [--projects PATH]
    List every thumbs-down failure recorded in examples/failures.jsonl —
    a plain listing, not grouped by error type (failures.jsonl doesn't
    currently capture a reason to group by).

    --projects  Project root directory (default: . — current directory).

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
    medallion metadata generate sales_project
    medallion metadata approve  sales_project
    medallion relationships generate sales_project
    medallion relationships approve  sales_project
    medallion examples generate sales_project --count 15
    medallion examples approve  sales_project
    medallion examples harvest  sales_project
    medallion examples review   sales_project
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
        from openmedallion.cerebrum.pipeline import (
            AmbiguousQuestionError,
            CerebrumPipeline,
            MultiQueryResult,
        )
    except ImportError:
        print("  ❌  cerebrum not installed — run: pip install 'openmedallion[cerebrum]'")
        sys.exit(1)

    from openmedallion.config import settings
    from openmedallion.metadata.loader import load_metadata

    provider = args.provider or settings.LLM_PROVIDER
    model    = args.model    or settings.LLM_MODEL
    cfg          = load_project(args.project, args.projects)
    silver_dir   = Path(cfg["paths"]["silver"])
    examples_dir = Path(args.projects) / args.project / "examples"

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
        metadata = load_metadata(args.project, args.projects)
        pipeline = CerebrumPipeline(
            silver_dir,
            examples_dir=examples_dir if examples_dir.exists() else None,
            metadata=metadata,
            model=model,
            provider=provider,
            api_key=settings.LLM_API_KEY,
            base_url=settings.LLM_BASE_URL,
            detect_ambiguity=args.detect_ambiguity,
            decompose_queries=args.decompose,
        )
        qr = pipeline.ask(args.question, on_step=_on_step)
    except AmbiguousQuestionError as exc:
        print(f"\n  ❓  Your question is ambiguous: {exc.clarification}")
        print("       Try rephrasing with more specifics.")
        sys.exit(1)
    except (httpx.ConnectError, httpx.ConnectTimeout):
        if provider == "ollama":
            _url = settings.LLM_BASE_URL or settings.OLLAMA_URL
            print(f"\n  ❌  Ollama is not reachable at {_url}")
            print("       Start it with: ollama serve")
        else:
            print(f"\n  ❌  LLM provider '{provider}' is not reachable.")
            print("       Check your base_url and network connection.")
        sys.exit(1)
    except ModuleNotFoundError as exc:
        if exc.name == "chromadb":
            print("\n  ❌  Dynamic few-shot retrieval / schema pruning requires chromadb.")
            print("       Run: pip install 'openmedallion[cerebrum]'")
        else:
            print(f"\n  ❌  {exc}")
        sys.exit(1)
    except Exception as exc:
        print(f"\n  ❌  {exc}")
        sys.exit(1)

    if isinstance(qr, MultiQueryResult):
        print(f"  🧩  Question decomposed into {len(qr.sub_questions)} sub-questions\n")
        for i, sub_result in enumerate(qr.results, start=1):
            print(f"  {'═' * (_W - 2)}")
            print(f"  Sub-question {i}: {sub_result.question}")
            _print_query_result(sub_result, _W)
    else:
        _print_query_result(qr, _W)

    print(f"\n{'━' * _W}\n")


def _print_query_result(qr, _W: int) -> None:
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


def cmd_metadata(args: argparse.Namespace) -> None:
    if args.metadata_command == "generate":
        cmd_metadata_generate(args)
    elif args.metadata_command == "approve":
        cmd_metadata_approve(args)


def cmd_metadata_generate(args: argparse.Namespace) -> None:
    _W = 58
    print(f"\n{'━' * _W}")
    print(f"  medallion  ·  metadata generate  ·  {args.project}")
    print(f"{'━' * _W}\n")

    from openmedallion.config import settings
    from openmedallion.metadata.generator import generate_metadata

    provider = args.provider or settings.LLM_PROVIDER
    model    = args.model    or settings.LLM_MODEL

    print(f"  🤖  provider →  {provider}")
    print(f"  🤖  model    →  {model}\n")

    def _on_step(msg: str) -> None:
        print(f"  ·  {msg}", flush=True)

    try:
        import httpx
        result = generate_metadata(
            args.project, args.projects,
            model=model, provider=provider,
            api_key=settings.LLM_API_KEY, base_url=settings.LLM_BASE_URL,
            on_step=_on_step,
        )
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

    n_approved = sum(1 for t in result.tables.values() if t.status == "approved")
    n_draft    = len(result.tables) - n_approved
    print(f"\n  ✅  {len(result.tables)} table(s) — {n_draft} drafted, {n_approved} already approved (untouched)")
    print(f"  📄  Written to: {Path(args.projects) / args.project / 'metadata.yaml'}")
    print(f"       Review with: medallion metadata approve {args.project}")
    print(f"\n{'━' * _W}\n")


def cmd_metadata_approve(args: argparse.Namespace) -> None:
    from openmedallion.metadata.approve import approve_metadata, list_reviewable_tables

    _W = 58
    print(f"\n{'━' * _W}")
    print(f"  medallion  ·  metadata approve  ·  {args.project}")
    print(f"{'━' * _W}\n")

    try:
        reviewable = list_reviewable_tables(args.project, args.projects)
    except (FileNotFoundError, ValueError) as exc:
        print(f"  ❌  {exc}")
        sys.exit(1)

    if not reviewable:
        print("  ✅  Nothing to review — no draft/stale tables in metadata.yaml.")
        print(f"\n{'━' * _W}\n")
        return

    print(f"  {len(reviewable)} table(s) to review — [a]pprove / [s]kip (default) / [q]uit\n")
    n_approved = 0
    for name, table in reviewable:
        print(f"  {'─' * (_W - 2)}")
        print(f"  {name}  ({table.layer}, status: {table.status})")
        if table.description:
            print(f"    description : {table.description}")
        if table.synonyms:
            print(f"    synonyms    : {', '.join(table.synonyms)}")
        for col_name, col in table.columns.items():
            print(f"    · {col_name}: {col.description or '(no description)'}")

        choice = input("\n  approve this table? [a/s/q] ").strip().lower()
        if choice in ("q", "quit"):
            print("\n  Stopping review — remaining tables left as-is.")
            break
        if choice in ("a", "approve"):
            approve_metadata(args.project, args.projects, {name: {"status": "approved"}})
            n_approved += 1
            print("  ✅  approved\n")
        else:
            print("  ⏭️   skipped (left as-is, reviewable again later)\n")

    print(f"  {'─' * (_W - 2)}")
    print(f"  {n_approved} table(s) approved this session.")
    print(f"\n{'━' * _W}\n")


def cmd_relationships(args: argparse.Namespace) -> None:
    if args.relationships_command == "generate":
        cmd_relationships_generate(args)
    elif args.relationships_command == "approve":
        cmd_relationships_approve(args)


def cmd_relationships_generate(args: argparse.Namespace) -> None:
    _W = 58
    print(f"\n{'━' * _W}")
    print(f"  medallion  ·  relationships generate  ·  {args.project}")
    print(f"{'━' * _W}\n")

    from openmedallion.relationships.generator import generate_relationships

    def _on_step(msg: str) -> None:
        print(f"  ·  {msg}", flush=True)

    try:
        result = generate_relationships(args.project, args.projects, on_step=_on_step)
    except Exception as exc:
        print(f"\n  ❌  {exc}")
        sys.exit(1)

    n_approved = sum(1 for r in result.relationships if r.status == "approved")
    print(f"\n  ✅  {len(result.relationships)} relationship(s) — {n_approved} already approved (untouched)")
    print(f"  📄  Written to: {Path(args.projects) / args.project / 'relationships.yaml'}")
    print(f"       Review with: medallion relationships approve {args.project}")
    print(f"\n{'━' * _W}\n")


def cmd_relationships_approve(args: argparse.Namespace) -> None:
    from openmedallion.relationships.approve import approve_relationships, list_reviewable_relationships

    _W = 58
    print(f"\n{'━' * _W}")
    print(f"  medallion  ·  relationships approve  ·  {args.project}")
    print(f"{'━' * _W}\n")

    try:
        reviewable = list_reviewable_relationships(args.project, args.projects)
    except (FileNotFoundError, ValueError) as exc:
        print(f"  ❌  {exc}")
        sys.exit(1)

    if not reviewable:
        print("  ✅  Nothing to review — no draft/stale relationships in relationships.yaml.")
        print(f"\n{'━' * _W}\n")
        return

    print(f"  {len(reviewable)} relationship(s) to review — [a]pprove / [s]kip (default) / [q]uit\n")
    n_approved = 0
    for rel in reviewable:
        print(f"  {'─' * (_W - 2)}")
        print(f"  {rel.from_table} -> {rel.to_table}  (status: {rel.status})")
        print(f"    join_on     : {', '.join(rel.join_on)}")
        print(f"    confidence  : {rel.confidence}")
        print(f"    method      : {rel.method or '(hand-added, no detection rule)'}")

        choice = input("\n  approve this relationship? [a/s/q] ").strip().lower()
        if choice in ("q", "quit"):
            print("\n  Stopping review — remaining relationships left as-is.")
            break
        key = (rel.from_table, rel.to_table, tuple(rel.join_on))
        if choice in ("a", "approve"):
            approve_relationships(args.project, args.projects, {key: {"status": "approved"}})
            n_approved += 1
            print("  ✅  approved\n")
        else:
            print("  ⏭️   skipped (left as-is, reviewable again later)\n")

    print(f"  {'─' * (_W - 2)}")
    print(f"  {n_approved} relationship(s) approved this session.")
    print(f"\n{'━' * _W}\n")


def cmd_examples(args: argparse.Namespace) -> None:
    if args.examples_command == "generate":
        cmd_examples_generate(args)
    elif args.examples_command == "approve":
        cmd_examples_approve(args)
    elif args.examples_command == "harvest":
        cmd_examples_harvest(args)
    elif args.examples_command == "review":
        cmd_examples_review(args)


def cmd_examples_generate(args: argparse.Namespace) -> None:
    _W = 58
    print(f"\n{'━' * _W}")
    print(f"  medallion  ·  examples generate  ·  {args.project}")
    print(f"{'━' * _W}\n")

    from openmedallion.config import settings
    from openmedallion.examples.generator import generate_examples

    provider = args.provider or settings.LLM_PROVIDER
    model    = args.model    or settings.LLM_MODEL

    print(f"  🤖  provider →  {provider}")
    print(f"  🤖  model    →  {model}")
    print(f"  🎯  target   →  {args.count} examples\n")

    def _on_step(msg: str) -> None:
        print(f"  ·  {msg}", flush=True)

    try:
        import httpx
        result = generate_examples(
            args.project, args.projects,
            count=args.count, model=model, provider=provider,
            api_key=settings.LLM_API_KEY, base_url=settings.LLM_BASE_URL,
            on_step=_on_step,
        )
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

    n_verified = sum(1 for e in result if e.verified)
    print(f"\n  ✅  {len(result)} example(s) — {len(result) - n_verified} new, {n_verified} already verified (untouched)")
    print(f"  📄  Written to: {Path(args.projects) / args.project / 'examples' / 'synthetic.jsonl'}")
    print(f"       Review with: medallion examples approve {args.project}")
    print(f"\n{'━' * _W}\n")


def cmd_examples_approve(args: argparse.Namespace) -> None:
    from openmedallion.examples.approve import approve_examples, content_hash, list_reviewable_examples

    _W = 58
    print(f"\n{'━' * _W}")
    print(f"  medallion  ·  examples approve  ·  {args.project}")
    print(f"{'━' * _W}\n")

    try:
        reviewable = list_reviewable_examples(args.project, args.projects)
    except (FileNotFoundError, ValueError) as exc:
        print(f"  ❌  {exc}")
        sys.exit(1)

    if not reviewable:
        print("  ✅  Nothing to review — no unverified examples in synthetic.jsonl.")
        print(f"\n{'━' * _W}\n")
        return

    print(f"  {len(reviewable)} example(s) to review — [a]pprove / [s]kip (default) / [q]uit\n")
    n_approved = 0
    for example in reviewable:
        print(f"  {'─' * (_W - 2)}")
        print(f"  question : {example.question}")
        print(f"  sql      : {example.sql}")

        choice = input("\n  approve this example? [a/s/q] ").strip().lower()
        if choice in ("q", "quit"):
            print("\n  Stopping review — remaining examples left as-is.")
            break
        key = content_hash(example.question, example.sql)
        if choice in ("a", "approve"):
            approve_examples(args.project, args.projects, {key: {"verified": True}})
            n_approved += 1
            print("  ✅  approved\n")
        else:
            print("  ⏭️   skipped (left as-is, reviewable again later)\n")

    print(f"  {'─' * (_W - 2)}")
    print(f"  {n_approved} example(s) approved this session.")
    print(f"\n{'━' * _W}\n")


def cmd_examples_harvest(args: argparse.Namespace) -> None:
    from openmedallion.examples.harvest import harvest_candidates

    _W = 58
    print(f"\n{'━' * _W}")
    print(f"  medallion  ·  examples harvest  ·  {args.project}")
    print(f"{'━' * _W}\n")

    result = harvest_candidates(args.project, args.projects)

    if not result:
        print("  ✅  Nothing new to harvest — no unharvested candidates in harvested.jsonl.")
        print(f"\n{'━' * _W}\n")
        return

    print(f"  🌾  Promoted {len(result)} candidate(s) into synthetic.jsonl as unverified:\n")
    for example in result:
        print(f"    · {example.question}")

    print(f"\n  📄  Written to: {Path(args.projects) / args.project / 'examples' / 'synthetic.jsonl'}")
    print(f"       Review with: medallion examples approve {args.project}")
    print(f"\n{'━' * _W}\n")


def cmd_examples_review(args: argparse.Namespace) -> None:
    from openmedallion.examples.harvest import list_failures

    _W = 58
    print(f"\n{'━' * _W}")
    print(f"  medallion  ·  examples review  ·  {args.project}")
    print(f"{'━' * _W}\n")

    failures = list_failures(args.project, args.projects)

    if not failures:
        print("  ✅  No failures recorded in failures.jsonl.")
        print(f"\n{'━' * _W}\n")
        return

    print(f"  {len(failures)} thumbs-down failure(s):\n")
    for i, failure in enumerate(failures, start=1):
        print(f"  {'─' * (_W - 2)}")
        print(f"  {i}. question : {failure.question}")
        print(f"     sql      : {failure.sql}")

    print(f"\n{'━' * _W}\n")


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
    p_query.add_argument(
        "--detect-ambiguity", action="store_true", default=False,
        help="One extra LLM call checks if the question is ambiguous before generating SQL (default: off)",
    )
    p_query.add_argument(
        "--decompose", action="store_true", default=False,
        help="One extra LLM call checks if the question decomposes into independent sub-questions (default: off)",
    )

    # metadata
    p_metadata = sub.add_parser(
        "metadata",
        help="Manage a project's metadata.yaml (RAG accuracy roadmap, requires [cerebrum] extra)",
    )
    meta_sub = p_metadata.add_subparsers(dest="metadata_command", metavar="ACTION")
    meta_sub.required = True

    p_meta_generate = meta_sub.add_parser(
        "generate",
        help="LLM-draft metadata.yaml for silver/gold tables not yet approved",
    )
    p_meta_generate.add_argument("project", help="Project name")
    p_meta_generate.add_argument(
        "--projects", default=".", metavar="PATH",
        help="Parent directory containing the project folder (default: .)",
    )
    p_meta_generate.add_argument(
        "--model", default=None, metavar="MODEL",
        help="Model identifier (default: from settings.yaml / MEDALLION_LLM_MODEL / llama3.2)",
    )
    p_meta_generate.add_argument(
        "--provider", default=None, metavar="PROVIDER",
        help="LLM backend: ollama | openrouter | openai | custom (default: from settings / ollama)",
    )

    p_meta_approve = meta_sub.add_parser(
        "approve",
        help="Interactively review draft/stale tables in metadata.yaml",
    )
    p_meta_approve.add_argument("project", help="Project name")
    p_meta_approve.add_argument(
        "--projects", default=".", metavar="PATH",
        help="Parent directory containing the project folder (default: .)",
    )

    # relationships
    p_relationships = sub.add_parser(
        "relationships",
        help="Manage a project's relationships.yaml (RAG accuracy roadmap)",
    )
    rel_sub = p_relationships.add_subparsers(dest="relationships_command", metavar="ACTION")
    rel_sub.required = True

    p_rel_generate = rel_sub.add_parser(
        "generate",
        help="Detect silver/gold table relationships (FK naming, lineage, grain) — no LLM call",
    )
    p_rel_generate.add_argument("project", help="Project name")
    p_rel_generate.add_argument(
        "--projects", default=".", metavar="PATH",
        help="Parent directory containing the project folder (default: .)",
    )

    p_rel_approve = rel_sub.add_parser(
        "approve",
        help="Interactively review draft/stale relationships in relationships.yaml",
    )
    p_rel_approve.add_argument("project", help="Project name")
    p_rel_approve.add_argument(
        "--projects", default=".", metavar="PATH",
        help="Parent directory containing the project folder (default: .)",
    )

    # examples
    p_examples = sub.add_parser(
        "examples",
        help="Manage a project's synthetic Q→SQL examples (RAG accuracy roadmap, requires [cerebrum] extra)",
    )
    ex_sub = p_examples.add_subparsers(dest="examples_command", metavar="ACTION")
    ex_sub.required = True

    p_ex_generate = ex_sub.add_parser(
        "generate",
        help="LLM-draft synthetic (question, sql) pairs from approved metadata + relationships",
    )
    p_ex_generate.add_argument("project", help="Project name")
    p_ex_generate.add_argument(
        "--projects", default=".", metavar="PATH",
        help="Parent directory containing the project folder (default: .)",
    )
    p_ex_generate.add_argument(
        "--count", type=int, default=20, metavar="N",
        help="Target number of (question, sql) pairs to request (default: 20)",
    )
    p_ex_generate.add_argument(
        "--model", default=None, metavar="MODEL",
        help="Model identifier (default: from settings.yaml / MEDALLION_LLM_MODEL / llama3.2)",
    )
    p_ex_generate.add_argument(
        "--provider", default=None, metavar="PROVIDER",
        help="LLM backend: ollama | openrouter | openai | custom (default: from settings / ollama)",
    )

    p_ex_approve = ex_sub.add_parser(
        "approve",
        help="Interactively review unverified examples in synthetic.jsonl",
    )
    p_ex_approve.add_argument("project", help="Project name")
    p_ex_approve.add_argument(
        "--projects", default=".", metavar="PATH",
        help="Parent directory containing the project folder (default: .)",
    )

    p_ex_harvest = ex_sub.add_parser(
        "harvest",
        help="Promote thumbs-up candidates from harvested.jsonl into synthetic.jsonl (unverified)",
    )
    p_ex_harvest.add_argument("project", help="Project name")
    p_ex_harvest.add_argument(
        "--projects", default=".", metavar="PATH",
        help="Parent directory containing the project folder (default: .)",
    )

    p_ex_review = ex_sub.add_parser(
        "review",
        help="List thumbs-down failures recorded in failures.jsonl",
    )
    p_ex_review.add_argument("project", help="Project name")
    p_ex_review.add_argument(
        "--projects", default=".", metavar="PATH",
        help="Parent directory containing the project folder (default: .)",
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
    "init":          cmd_init,
    "run":           cmd_run,
    "query":         cmd_query,
    "metadata":      cmd_metadata,
    "relationships": cmd_relationships,
    "examples":      cmd_examples,
    "ask":           cmd_ask,
    "cortex":        cmd_cortex,
}


def main() -> None:
    """Console script entry point — invoked by ``medallion`` command."""
    _parser = _build_parser()
    _args   = _parser.parse_args()
    _HANDLERS[_args.command](_args)


if __name__ == "__main__":
    main()
