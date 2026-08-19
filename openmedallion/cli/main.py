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
                [--detect-ambiguity] [--decompose] [--use-templates] [--user NAME]
    Ask a natural-language question directly in the terminal — no server needed.
    Every question is logged to <project>/chat_history/<user>.jsonl (audit
    only, never fed back into the LLM prompt).
    Requires: openmedallion[cerebrum]

    --projects           Project root directory (default: . — current directory).
    --model              Model identifier (default: from settings / llama3.2).
    --provider           LLM backend: ollama | openrouter | openai | custom
                         (default: from settings / ollama).
    --detect-ambiguity   One extra LLM call checks if the question is ambiguous
                         before generating SQL (default: off).
    --decompose          One extra LLM call checks if the question decomposes
                         into independent sub-questions (default: off).
    --use-templates      A high-confidence match to a curated templated:true
                         example skips SQL generation entirely, filling params
                         instead (Template-Routed Query Layer roadmap, default: off).
    --user               Display name chat history is recorded under
                         (default: OS username via getpass.getuser()).

medallion metadata generate <project> [--projects PATH] [--model MODEL] [--provider PROVIDER]
    LLM-draft metadata.yaml for silver/gold tables not yet status: approved.
    Approved tables are left completely untouched. Part of the RAG accuracy
    roadmap (see CLAUDE.md).
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

medallion metadata refresh <project> [--projects PATH] [--model MODEL] [--provider PROVIDER] [--check]
    Detect schema drift (live Parquet vs. the schema_hash stored in
    metadata.yaml) and re-draft only affected/unapproved tables. An approved
    table that drifts is flipped to status: stale WITHOUT a new LLM draft —
    review it again with `metadata approve` or re-run `refresh` to redraft it.
    Dropped tables (Parquet file no longer exists) are flagged, never deleted.
    --check runs drift detection only (no LLM call, no write); exits 1 if any
    table is drifted or dropped — CI-friendly.
    Requires: openmedallion[cerebrum]

    --projects  Project root directory (default: . — current directory).
    --model     Model identifier (default: from settings / llama3.2).
    --provider  LLM backend: ollama | openrouter | openai | custom
                (default: from settings / ollama).
    --check     Detect drift only; no LLM call, no write, exit code reflects result.

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

medallion relationships erd <project> [--projects PATH] [--all]
    Render relationships.yaml + real Parquet column dtypes as a Mermaid
    erDiagram, written to <project>/relationships_erd.md. Approved-only by
    default; only tables referenced by an included relationship are drawn.
    No LLM call, no PK inference.

    --projects  Project root directory (default: . — current directory).
    --all       Include draft/stale relationships too (default: approved only).

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
    are skipped. harvested.jsonl/failures.jsonl are now written at
    session-end curation (cortex's End Session / idle timeout / tab-close),
    not at the moment of a thumbs click — see chat_history in CLAUDE.md.

    --projects  Project root directory (default: . — current directory).

medallion examples review <project> [--projects PATH]
    List every thumbs-down failure recorded in examples/failures.jsonl —
    a plain listing, not grouped by error type (failures.jsonl doesn't
    currently capture a reason to group by).

    --projects  Project root directory (default: . — current directory).

medallion examples eval <project> [--projects PATH] [--model MODEL] [--provider PROVIDER]
    Admin regression check: re-run every verified: true example's question
    through the current pipeline and compare the freshly-generated SQL's
    EXECUTED RESULT against the stored golden SQL's result (not raw SQL
    text — syntactically different SQL can be semantically identical).
    Prints a match/mismatch per question. No model retraining happens
    anywhere in this project — this checks whether a curation change
    (new metadata/examples/relationships) helped or hurt.
    Requires: openmedallion[cerebrum]

    --projects  Project root directory (default: . — current directory).
    --model     Model identifier (default: from settings / llama3.2).
    --provider  LLM backend: ollama | openrouter | openai | custom
                (default: from settings / ollama).

medallion ask <project> [--projects PATH] [--port PORT] [--model MODEL] [--provider PROVIDER]
    Start the cerebrum LLM engine + neuron FastAPI server on :8000. Exposes
    /query, /feedback, /history, /session/end, /health, /docs.
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
    medallion query     sales_project "Which rep leads?" --user alice
    medallion metadata generate sales_project
    medallion metadata approve  sales_project
    medallion metadata refresh  sales_project
    medallion relationships generate sales_project
    medallion relationships approve  sales_project
    medallion relationships erd      sales_project
    medallion examples generate sales_project --count 15
    medallion examples approve  sales_project
    medallion examples harvest  sales_project
    medallion examples review   sales_project
    medallion examples eval     sales_project
    medallion ask       sales_project
    medallion ask       sales_project --model mistral --port 8001
    medallion ask       sales_project --provider openrouter --model openai/gpt-4o
    medallion cortex    sales_project
    medallion cortex    sales_project --neuron-url http://localhost:8001 --debug
"""
import argparse
import getpass
import sys
import uuid
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

    provider     = args.provider     or settings.LLM_PROVIDER
    model        = args.model        or settings.LLM_MODEL
    nlg_provider = args.nlg_provider or settings.LLM_NLG_PROVIDER
    nlg_model    = args.nlg_model    or settings.LLM_NLG_MODEL
    cfg          = load_project(args.project, args.projects)
    silver_dir   = Path(cfg["paths"]["silver"])
    examples_dir = Path(args.projects) / args.project / "examples"

    if not silver_dir.exists():
        print(f"  ❌  Silver layer not found: {silver_dir}")
        print(f"       Run: medallion run {args.project}")
        sys.exit(1)

    print(f"  🤖  provider     →  {provider}")
    print(f"  🤖  model        →  {model}")
    if nlg_provider or nlg_model:
        print(f"  🤖  nlg provider →  {nlg_provider or provider}")
        print(f"  🤖  nlg model    →  {nlg_model or model}")
    print(f"  🗂️   silver       →  {silver_dir}")
    print(f"  ❓  question      →  {args.question}\n")

    def _on_step(msg: str) -> None:
        print(f"  ·  {msg}...", flush=True)

    from openmedallion.neuron.chat_history import record_chat_turn

    username   = args.user or getpass.getuser()
    session_id = uuid.uuid4().hex  # one per CLI invocation — no thumbs-up UI here to group multiple

    def _log_failed(error: str) -> None:
        try:
            record_chat_turn(
                args.project, args.projects, username,
                question=args.question, sql="", answer=error,
                row_count=0, columns=[], session_id=session_id,
                response_generated=False,
            )
        except Exception:
            pass  # chat history is best-effort — never mask the real error

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
            nlg_model=nlg_model,
            nlg_provider=nlg_provider,
            nlg_api_key=settings.LLM_NLG_API_KEY,
            nlg_base_url=settings.LLM_NLG_BASE_URL,
            detect_ambiguity=args.detect_ambiguity,
            decompose_queries=args.decompose,
            use_templates=args.use_templates,
        )
        qr = pipeline.ask(args.question, on_step=_on_step)
    except AmbiguousQuestionError as exc:
        print(f"\n  ❓  Your question is ambiguous: {exc.clarification}")
        print("       Try rephrasing with more specifics.")
        _log_failed(f"Ambiguous: {exc.clarification}")
        sys.exit(1)
    except (httpx.ConnectError, httpx.ConnectTimeout):
        if provider == "ollama":
            _url = settings.LLM_BASE_URL or settings.OLLAMA_URL
            print(f"\n  ❌  Ollama is not reachable at {_url}")
            print("       Start it with: ollama serve")
            detail = f"Ollama not reachable at {_url}"
        else:
            print(f"\n  ❌  LLM provider '{provider}' is not reachable.")
            print("       Check your base_url and network connection.")
            detail = f"LLM provider '{provider}' not reachable"
        _log_failed(detail)
        sys.exit(1)
    except ModuleNotFoundError as exc:
        if exc.name == "chromadb":
            print("\n  ❌  Dynamic few-shot retrieval / schema pruning requires chromadb.")
            print("       Run: pip install 'openmedallion[cerebrum]'")
        else:
            print(f"\n  ❌  {exc}")
        _log_failed(str(exc))
        sys.exit(1)
    except Exception as exc:
        print(f"\n  ❌  {exc}")
        _log_failed(str(exc))
        sys.exit(1)

    def _record(single_qr) -> None:
        rows = single_qr.result.to_dicts()
        answer = single_qr.answer or (
            f"Found {len(rows)} row(s) for: {single_qr.question}"
            if rows
            else f"No results found for: {single_qr.question}"
        )
        try:
            record_chat_turn(
                args.project, args.projects, username,
                question=single_qr.question, sql=single_qr.sql, answer=answer,
                row_count=len(rows), columns=single_qr.result.columns,
                session_id=session_id,
            )
        except Exception:
            pass  # chat history is best-effort — never break a successful answer

    if isinstance(qr, MultiQueryResult):
        print(f"  🧩  Question decomposed into {len(qr.sub_questions)} sub-questions\n")
        for i, sub_result in enumerate(qr.results, start=1):
            print(f"  {'═' * (_W - 2)}")
            print(f"  Sub-question {i}: {sub_result.question}")
            _print_query_result(sub_result, _W)
            _record(sub_result)
    else:
        _print_query_result(qr, _W)
        _record(qr)

    print(f"\n{'━' * _W}\n")


def _print_query_result(qr, _W: int) -> None:
    if qr.answer is not None:
        # Schema/meta-question — answered directly by the NLG model, no
        # SQL/table to print.
        print(f"  Answer\n  {'─' * (_W - 2)}")
        print(f"  {qr.answer}")
        return

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
    elif args.metadata_command == "refresh":
        cmd_metadata_refresh(args)


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

    if args.profile:
        print("  📊  ydata-profiling enrichment enabled (dtype / stats / accepted_values)\n")

    try:
        import httpx
        result = generate_metadata(
            args.project, args.projects,
            model=model, provider=provider,
            api_key=settings.LLM_API_KEY, base_url=settings.LLM_BASE_URL,
            use_profiling=args.profile,
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
    except ModuleNotFoundError as exc:
        if exc.name == "ydata_profiling":
            print("\n  ❌  --profile requires ydata-profiling.")
            print("       Run: pip install 'openmedallion[profile]'")
        else:
            print(f"\n  ❌  {exc}")
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


def cmd_metadata_refresh(args: argparse.Namespace) -> None:
    from openmedallion.metadata.drift import detect_drift

    _W = 58
    print(f"\n{'━' * _W}")
    print(f"  medallion  ·  metadata refresh  ·  {args.project}")
    print(f"{'━' * _W}\n")

    try:
        drift, dropped = detect_drift(args.project, args.projects)
    except Exception as exc:
        print(f"  ❌  {exc}")
        sys.exit(1)

    if args.check:
        if not drift and not dropped:
            print("  ✅  No schema drift detected.")
            print(f"\n{'━' * _W}\n")
            return
        for name, reason in drift.items():
            print(f"  ⚠️   {name} — {reason}")
        for name in dropped:
            print(f"  ⚠️   {name} — Parquet file no longer found")
        print(f"\n{'━' * _W}\n")
        sys.exit(1)

    from openmedallion.config import settings
    from openmedallion.metadata.generator import refresh_metadata

    provider = args.provider or settings.LLM_PROVIDER
    model    = args.model    or settings.LLM_MODEL

    print(f"  🤖  provider →  {provider}")
    print(f"  🤖  model    →  {model}\n")

    if args.profile:
        print("  📊  ydata-profiling enrichment enabled (dtype / stats / accepted_values)\n")

    def _on_step(msg: str) -> None:
        print(f"  ·  {msg}", flush=True)

    try:
        import httpx
        result = refresh_metadata(
            args.project, args.projects,
            model=model, provider=provider,
            api_key=settings.LLM_API_KEY, base_url=settings.LLM_BASE_URL,
            use_profiling=args.profile,
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
    except ModuleNotFoundError as exc:
        if exc.name == "ydata_profiling":
            print("\n  ❌  --profile requires ydata-profiling.")
            print("       Run: pip install 'openmedallion[profile]'")
        else:
            print(f"\n  ❌  {exc}")
        sys.exit(1)
    except Exception as exc:
        print(f"\n  ❌  {exc}")
        sys.exit(1)

    n_stale = sum(1 for t in result.tables.values() if t.status == "stale")
    print(f"\n  ✅  {len(drift)} table(s) drifted, {n_stale} now stale, {len(dropped)} dropped table(s) flagged")
    print(f"  📄  Written to: {Path(args.projects) / args.project / 'metadata.yaml'}")
    print(f"       Review with: medallion metadata approve {args.project}")
    print(f"\n{'━' * _W}\n")


def cmd_relationships(args: argparse.Namespace) -> None:
    if args.relationships_command == "generate":
        cmd_relationships_generate(args)
    elif args.relationships_command == "approve":
        cmd_relationships_approve(args)
    elif args.relationships_command == "erd":
        cmd_relationships_erd(args)


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


def cmd_relationships_erd(args: argparse.Namespace) -> None:
    from openmedallion.relationships.erd import generate_erd

    _W = 58
    print(f"\n{'━' * _W}")
    print(f"  medallion  ·  relationships erd  ·  {args.project}")
    print(f"{'━' * _W}\n")

    try:
        generate_erd(args.project, args.projects, include_all=args.all)
    except Exception as exc:
        print(f"  ❌  {exc}")
        sys.exit(1)

    scope = "every relationship (--all)" if args.all else "approved relationships only"
    print(f"  🗺️   Rendered {scope} as a Mermaid erDiagram")
    print(f"  📄  Written to: {Path(args.projects) / args.project / 'relationships_erd.md'}")
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
    elif args.examples_command == "eval":
        cmd_examples_eval(args)


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
    if args.template:
        _cmd_examples_approve_template(args)
        return

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


def _cmd_examples_approve_template(args: argparse.Namespace) -> None:
    """`medallion examples approve --template` — Template-Routed Query Layer
    roadmap (see CLAUDE.md), build order step 2. Reviews already-`verified:
    true` examples for promotion to `templated: true`, a separate and
    stricter pass than verification itself.
    """
    import re

    from openmedallion.examples.approve import approve_examples, content_hash, list_templatable_examples

    _W = 58
    print(f"\n{'━' * _W}")
    print(f"  medallion  ·  examples approve --template  ·  {args.project}")
    print(f"{'━' * _W}\n")

    try:
        templatable = list_templatable_examples(args.project, args.projects)
    except (FileNotFoundError, ValueError) as exc:
        print(f"  ❌  {exc}")
        sys.exit(1)

    if not templatable:
        print("  ✅  Nothing to review — no verified, untemplated examples in synthetic.jsonl.")
        print(f"\n{'━' * _W}\n")
        return

    print(f"  {len(templatable)} example(s) to review — [t]emplate / [s]kip (default) / [q]uit\n")
    n_templated = 0
    for example in templatable:
        print(f"  {'─' * (_W - 2)}")
        print(f"  question : {example.question}")
        print(f"  sql      : {example.sql}")

        choice = input("\n  promote to template? [t/s/q] ").strip().lower()
        if choice in ("q", "quit"):
            print("\n  Stopping review — remaining examples left as-is.")
            break
        if choice in ("t", "template"):
            slot_names = sorted(set(re.findall(r"\{(\w+)\}", example.sql)))
            params: dict[str, str] = {}
            for slot in slot_names:
                desc = input(f"    describe param '{slot}' (e.g. 'ISO date range, e.g. 2026-Q1'): ").strip()
                params[slot] = desc
            key = content_hash(example.question, example.sql)
            approve_examples(args.project, args.projects, {key: {"templated": True, "params": params}})
            n_templated += 1
            print("  ✅  templated\n")
        else:
            print("  ⏭️   skipped (left as-is, reviewable again later)\n")

    print(f"  {'─' * (_W - 2)}")
    print(f"  {n_templated} example(s) templated this session.")
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


def cmd_examples_eval(args: argparse.Namespace) -> None:
    """Admin regression check — maps onto the roadmap's flagged-but-unbuilt
    "RAG eval set" idea, reframed as an admin-triggered command per request:
    re-run every verified synthetic.jsonl question through the current
    pipeline and compare its EXECUTED RESULT against the stored golden SQL's
    result. No model retraining happens anywhere in this project — this
    checks whether a curation change (new metadata/examples/relationships)
    improved or regressed answers."""
    _W = 58
    print(f"\n{'━' * _W}")
    print(f"  medallion  ·  examples eval  ·  {args.project}")
    print(f"{'━' * _W}\n")

    from openmedallion.config import settings
    from openmedallion.config.loader import load_project
    from openmedallion.examples.eval import run_eval

    provider = args.provider or settings.LLM_PROVIDER
    model    = args.model    or settings.LLM_MODEL

    cfg          = load_project(args.project, args.projects)
    silver_dir   = Path(cfg["paths"]["silver"])
    examples_dir = Path(args.projects) / args.project / "examples"

    print(f"  🤖  provider →  {provider}")
    print(f"  🤖  model    →  {model}\n")

    def _on_step(msg: str) -> None:
        print(f"  ·  {msg}", flush=True)

    try:
        import httpx
        results = run_eval(
            silver_dir, examples_dir,
            model=model, provider=provider,
            api_key=settings.LLM_API_KEY, base_url=settings.LLM_BASE_URL,
            on_step=_on_step,
            use_templates=args.use_templates,
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

    if not results:
        print("  ✅  Nothing to evaluate — no verified examples in synthetic.jsonl.")
        print(f"\n{'━' * _W}\n")
        return

    n_match = sum(1 for r in results if r.match)
    for r in results:
        icon = "✅" if r.match else "❌"
        print(f"  {icon}  {r.question}")
        if not r.match:
            print(f"       golden : {r.golden_sql}")
            print(f"       new    : {r.new_sql}")
            if r.error:
                print(f"       error  : {r.error}")
        if args.use_templates:
            t_icon = "✅" if r.templated_match else "❌"
            changed = r.templated_match != r.match
            flag = "  ⚠️  template-routing changed the result" if changed else ""
            print(f"       {t_icon}  templated : {r.templated_sql or '(no template matched)'}{flag}")
            if r.templated_error:
                print(f"           error  : {r.templated_error}")

    print(f"\n  {'─' * (_W - 2)}")
    print(f"  {n_match}/{len(results)} matched")
    if args.use_templates:
        n_templated_match = sum(1 for r in results if r.templated_match)
        n_changed = sum(1 for r in results if r.templated_match != r.match)
        print(f"  {n_templated_match}/{len(results)} matched with use_templates=True")
        print(f"  {n_changed} question(s) where template-routing changed the result")
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
        "--nlg-model", default=None, metavar="MODEL",
        help="Model for recommend() + schema/meta-question answers (default: same as --model)",
    )
    p_query.add_argument(
        "--nlg-provider", default=None, metavar="PROVIDER",
        help="LLM backend for --nlg-model (default: same as --provider)",
    )
    p_query.add_argument(
        "--detect-ambiguity", action="store_true", default=False,
        help="One extra LLM call checks if the question is ambiguous before generating SQL (default: off)",
    )
    p_query.add_argument(
        "--decompose", action="store_true", default=False,
        help="One extra LLM call checks if the question decomposes into independent sub-questions (default: off)",
    )
    p_query.add_argument(
        "--use-templates", action="store_true", default=False,
        help="A high-confidence match to a curated templated:true example skips SQL generation "
             "entirely, filling params instead (Template-Routed Query Layer roadmap, default: off)",
    )
    p_query.add_argument(
        "--user", default=None, metavar="NAME",
        help="Display name chat history is recorded under (default: OS username)",
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
    p_meta_generate.add_argument(
        "--profile", action="store_true", default=False,
        help="Enrich columns with ydata-profiling dtype/stats/accepted_values (requires openmedallion[profile])",
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

    p_meta_refresh = meta_sub.add_parser(
        "refresh",
        help="Detect schema drift and re-draft only affected/unapproved tables",
    )
    p_meta_refresh.add_argument("project", help="Project name")
    p_meta_refresh.add_argument(
        "--projects", default=".", metavar="PATH",
        help="Parent directory containing the project folder (default: .)",
    )
    p_meta_refresh.add_argument(
        "--model", default=None, metavar="MODEL",
        help="Model identifier (default: from settings.yaml / MEDALLION_LLM_MODEL / llama3.2)",
    )
    p_meta_refresh.add_argument(
        "--provider", default=None, metavar="PROVIDER",
        help="LLM backend: ollama | openrouter | openai | custom (default: from settings / ollama)",
    )
    p_meta_refresh.add_argument(
        "--check", action="store_true", default=False,
        help="Only detect drift (no LLM call, no write); exit 1 if any table is drifted/dropped",
    )
    p_meta_refresh.add_argument(
        "--profile", action="store_true", default=False,
        help="Enrich re-drafted columns with ydata-profiling dtype/stats/accepted_values (requires openmedallion[profile])",
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

    p_rel_erd = rel_sub.add_parser(
        "erd",
        help="Render relationships.yaml as a Mermaid ER diagram (relationships_erd.md)",
    )
    p_rel_erd.add_argument("project", help="Project name")
    p_rel_erd.add_argument(
        "--projects", default=".", metavar="PATH",
        help="Parent directory containing the project folder (default: .)",
    )
    p_rel_erd.add_argument(
        "--all", action="store_true", default=False,
        help="Include draft/stale relationships too (default: approved only)",
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
    p_ex_approve.add_argument(
        "--template", action="store_true",
        help="Review verified examples for promotion to templated:true "
             "(Template-Routed Query Layer roadmap) instead of reviewing unverified ones",
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

    p_ex_eval = ex_sub.add_parser(
        "eval",
        help="Admin regression check: re-run verified examples, compare results against golden SQL",
    )
    p_ex_eval.add_argument("project", help="Project name")
    p_ex_eval.add_argument(
        "--projects", default=".", metavar="PATH",
        help="Parent directory containing the project folder (default: .)",
    )
    p_ex_eval.add_argument(
        "--model", default=None, metavar="MODEL",
        help="Model identifier (default: from settings.yaml / MEDALLION_LLM_MODEL / llama3.2)",
    )
    p_ex_eval.add_argument(
        "--provider", default=None, metavar="PROVIDER",
        help="LLM backend: ollama | openrouter | openai | custom (default: from settings / ollama)",
    )
    p_ex_eval.add_argument(
        "--use-templates", action="store_true", default=False,
        help="Additionally re-run each question with use_templates=True and report "
             "where template-routing changed the result (Template-Routed Query Layer roadmap, default: off)",
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
