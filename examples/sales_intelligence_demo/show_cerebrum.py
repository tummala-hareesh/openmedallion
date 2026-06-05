"""show_cerebrum.py — demonstrate cerebrum against the sales_intel silver layer.

Runs without Ollama by exercising the cerebrum components directly:
  1. build_schema_context  — shows the DDL context the LLM would receive
  2. build_prompt          — shows the full prompt for one sample question
  3. executor.execute      — runs three real DuckDB queries against silver Parquet
  4. recommender.recommend — generates a canonical prompt via a mock LLM

Run after the pipeline has been seeded and run up to at least silver:
    python seed.py
    medallion run sales_intel --layer silver
    python show_cerebrum.py

To use a real Ollama LLM instead:
    ollama pull llama3.2          # one-time model download
    medallion ask sales_intel     # starts neuron on :8000
    medallion cortex sales_intel  # starts Dash UI on :8050 (separate terminal)
"""
from __future__ import annotations

import sys
from pathlib import Path

# Ensure the package is importable when run from this directory
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from openmedallion.cerebrum.executor    import execute
from openmedallion.cerebrum.prompt      import build_prompt
from openmedallion.cerebrum.recommender import recommend
from openmedallion.cerebrum.schema      import build_schema_context

SILVER_DIR = Path("data/silver")

W = 66

# ── helpers ───────────────────────────────────────────────────────────────────

def _header(title: str) -> None:
    print(f"\n{'━' * W}")
    print(f"  {title}")
    print(f"{'━' * W}\n")


def _section(n: int, title: str) -> None:
    print(f"\n── {n}. {title} {'─' * (W - len(title) - 6)}")


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    _header("cerebrum demo  ·  sales_intel  ·  offline mode (no Ollama needed)")

    if not SILVER_DIR.exists() or not list(SILVER_DIR.glob("*.parquet")):
        print("  ❌  Silver layer not found.")
        print("  Run the pipeline first:")
        print("      python seed.py")
        print("      medallion run sales_intel --layer silver")
        sys.exit(1)

    # ── 1. Schema context ─────────────────────────────────────────────────────
    _section(1, "Schema context (what the LLM receives)")
    schema_ctx = build_schema_context(SILVER_DIR)
    print(schema_ctx)

    # ── 2. Prompt preview ─────────────────────────────────────────────────────
    _section(2, "Prompt preview (truncated to 400 chars)")
    sample_q = "Which sales reps exceeded their quarterly target?"
    prompt   = build_prompt(schema_ctx, sample_q)
    print(prompt[:400] + "…\n")

    # ── 3. Direct executor queries (no LLM) ───────────────────────────────────
    _section(3, "DuckDB executor — three example queries")

    queries = [
        (
            "Top 5 reps by total revenue",
            "SELECT name, region, team, "
            "ROUND(SUM(amount), 0) AS total_revenue, COUNT(*) AS num_deals "
            "FROM rep_performance GROUP BY name, region, team "
            "ORDER BY total_revenue DESC LIMIT 5",
        ),
        (
            "Revenue by region",
            "SELECT region, "
            "ROUND(SUM(amount), 0) AS total_revenue, COUNT(*) AS num_deals "
            "FROM rep_performance GROUP BY region ORDER BY total_revenue DESC",
        ),
        (
            "Quarterly revenue trend",
            "SELECT quarter, "
            "ROUND(SUM(amount), 0) AS quarterly_revenue, COUNT(*) AS num_deals "
            "FROM rep_performance GROUP BY quarter ORDER BY quarter",
        ),
    ]

    results = []
    for label, sql in queries:
        print(f"\n  Q: {label}")
        print(f"  SQL: {sql}\n")
        df = execute(sql, SILVER_DIR)
        print(df)
        results.append((label, sql, df))

    # ── 4. Recommender (mock LLM) ─────────────────────────────────────────────
    _section(4, "Recommender — canonical reproducible prompt (mock LLM)")

    label, sql, df = results[0]

    def _mock_llm(prompt_text: str) -> str:
        return "Show the top 5 sales reps ranked by total closed revenue, including their region and team"

    canonical = recommend(label, sql, df, llm_fn=_mock_llm)
    print(f"  Original : {label}")
    print(f"  Canonical: {canonical}\n")

    # ── 5. How to go live ─────────────────────────────────────────────────────
    _section(5, "Go live with Ollama")
    print("  Install Ollama:  https://ollama.com")
    print("  Pull a model:    ollama pull llama3.2")
    print()
    print("  Terminal 1 — start the LLM query server:")
    print("    medallion ask sales_intel")
    print()
    print("  Terminal 2 — start the Dash UI:")
    print("    medallion cortex sales_intel")
    print()
    print("  Open http://localhost:8050 and ask questions like:")
    for q in [
        "Which rep had the highest revenue in Q2?",
        "Show me the quarterly trend for the North region",
        "Which team — Enterprise or SMB — closed more deals?",
        "Who is at risk of missing their target this quarter?",
    ]:
        print(f"    • {q}")
    print()
    print(f"{'━' * W}\n")


if __name__ == "__main__":
    main()
