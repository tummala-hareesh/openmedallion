"""show_rag_workflow.py — demonstrate the RAG accuracy add-ons against sales_intel.

Runs entirely offline (no Ollama/ChromaDB needed) by injecting a small
deterministic bag-of-words embedding function in place of a real one — the
same `_embed_fn=` injection point `CerebrumPipeline` exposes for tests. This
lets the demo show real ranking/confidence-gating behavior without a network
call or a heavy embedding-model download.

Covers the four pieces of the RAG accuracy roadmap (see CLAUDE.md):
  1. metadata.yaml / relationships.yaml — curated schema knowledge
  2. Dynamic few-shot retrieval          — examples/synthetic.jsonl
  3. Schema pruning + confidence gate    — approved-only ranking, with a
     raw-schema ChromaDB fallback when confidence is low
  4. How to go live with the LLM-backed commands (metadata/relationships/
     examples generate+approve, and `medallion query`)

Run after the pipeline has been seeded and run at least through silver:
    python seed.py
    medallion run sales_intel --layer silver
    python show_rag_workflow.py
"""
from __future__ import annotations

import sys
from pathlib import Path

# Ensure the package is importable when run from this directory
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from openmedallion.cerebrum.pipeline     import CerebrumPipeline
from openmedallion.cerebrum.retrieval    import CONFIDENCE_THRESHOLD
from openmedallion.cerebrum.schema       import describe_all_tables, rank_relevant_tables_scored
from openmedallion.metadata.loader       import load_metadata
from openmedallion.relationships.loader  import load_relationships

PROJECT      = "sales_intel"
PROJECTS_ROOT = "."
SILVER_DIR   = Path("data/silver")

W = 66

# ── deterministic offline "embedding" ────────────────────────────────────────
# A real embed_fn (ChromaDB) maps text -> a dense vector from a language
# model. This one maps text -> a term-frequency vector over a small fixed
# domain vocabulary — good enough to demonstrate ranking/confidence-gating
# without any model download, and fully deterministic for a demo script.
_VOCAB = [
    "revenue", "region", "rep", "sales", "quarter", "target", "product",
    "deal", "transaction", "attainment", "team", "quota", "leaderboard",
    "weather", "forecast", "rainfall", "temperature",
]


def bag_of_words_embed(texts: list[str]) -> list[list[float]]:
    def vec(text: str) -> list[float]:
        t = text.lower()
        return [float(t.count(word)) for word in _VOCAB]
    return [vec(t) for t in texts]


def _header(title: str) -> None:
    print(f"\n{'━' * W}")
    print(f"  {title}")
    print(f"{'━' * W}\n")


def _section(n: int, title: str) -> None:
    print(f"\n── {n}. {title} {'─' * (W - len(title) - 6)}")


def main() -> None:
    _header("RAG accuracy add-ons demo  ·  sales_intel  ·  offline mode")

    if not SILVER_DIR.exists() or not list(SILVER_DIR.glob("*.parquet")):
        print("  ❌  Silver layer not found.")
        print("  Run the pipeline first:")
        print("      python seed.py")
        print("      medallion run sales_intel --layer silver")
        sys.exit(1)

    # ── 1. Curated knowledge ───────────────────────────────────────────────
    _section(1, "metadata.yaml + relationships.yaml (curated knowledge)")

    metadata = load_metadata(PROJECT, PROJECTS_ROOT)
    approved = [(name, t) for name, t in metadata.tables.items() if t.status == "approved"]
    print(f"  {len(approved)} approved table(s) in metadata.yaml:")
    for name, table in approved:
        print(f"    · {name:<16} {table.description}")

    relationships = load_relationships(PROJECT, PROJECTS_ROOT)
    print(f"\n  {len(relationships.relationships)} approved relationship(s) in relationships.yaml:")
    for rel in relationships.relationships:
        print(f"    · {rel.from_table} → {rel.to_table}  on {rel.join_on}  ({rel.confidence})")

    # ── 2. Dynamic few-shot retrieval ──────────────────────────────────────
    _section(2, "Dynamic few-shot retrieval (examples/synthetic.jsonl)")

    sample_q = "Which rep is leading in revenue this quarter?"
    pipeline = CerebrumPipeline(
        SILVER_DIR,
        examples_dir=Path("sales_intel/examples"),
        metadata=metadata,
        _embed_fn=bag_of_words_embed,
        _client=lambda _p: "SELECT 1",  # unused directly in this section
    )
    few_shot = pipeline._get_few_shot(sample_q)
    print(f"  Q: {sample_q!r}")
    print(f"  Top {len(few_shot or [])} verified example(s) retrieved as few-shot context:")
    for ex in few_shot or []:
        print(f"    · {ex['question']}")

    # ── 3. Schema pruning + confidence gate ────────────────────────────────
    _section(3, "Schema pruning + confidence-gated fallback")

    print("  A business question, ranked against the curated (approved-only) corpus:")
    names, confidence = rank_relevant_tables_scored(
        "What is the attainment percentage by rep this quarter?", metadata, bag_of_words_embed, top_k=2
    )
    verdict = "structured ranking is trusted" if confidence >= CONFIDENCE_THRESHOLD else "below the gate — pipeline would fall back"
    print(f"    confidence = {confidence:.2f}  vs. threshold {CONFIDENCE_THRESHOLD}  → {verdict}")
    print(f"    top-ranked tables: {names}")
    print("    (this toy bag-of-words embedder is intentionally crude — a real")
    print("     ChromaDB sentence embedding scores semantic matches far higher;")
    print("     the gating mechanism itself works identically either way)")

    print("\n  An off-topic question — zero vocabulary overlap with any curated table,")
    print("  so confidence is unambiguously 0.0 and the pipeline falls back to a")
    print("  raw-schema search over every silver table (no metadata.yaml required):")

    steps: list[str] = []
    fallback_pipeline = CerebrumPipeline(
        SILVER_DIR,
        metadata=metadata,
        _embed_fn=bag_of_words_embed,
        _client=lambda _p: "SELECT 1",
    )
    fallback_pipeline._get_relevant_tables(
        "What's the weather forecast and rainfall for tomorrow?",
        on_step=steps.append,
    )
    for s in steps:
        print(f"    · {s}")
    print(f"    (compare: describe_all_tables() sees {len(describe_all_tables(SILVER_DIR))} "
          f"raw silver table(s), metadata.yaml or not)")

    # ── 4. Go live ──────────────────────────────────────────────────────────
    _section(4, "Go live with the LLM-backed commands")
    print("  Regenerate/refresh curated knowledge (all require an LLM — Ollama by default):")
    print("    medallion metadata generate      sales_intel   # draft descriptions for new tables")
    print("    medallion metadata approve       sales_intel   # human review, one table at a time")
    print("    medallion relationships generate sales_intel   # no LLM call — pure pattern matching")
    print("    medallion relationships approve  sales_intel")
    print("    medallion relationships erd      sales_intel   # Mermaid ER diagram, no LLM call")
    print("    medallion examples generate      sales_intel --count 15")
    print("    medallion examples approve       sales_intel")
    print()
    print("  After a cortex thumbs-up/down session, promote/inspect feedback:")
    print("    medallion examples harvest sales_intel   # promote thumbs-up into synthetic.jsonl")
    print("    medallion examples review  sales_intel   # list thumbs-down failures")
    print()
    print("  Query with the accuracy features engaged automatically (metadata=/examples_dir=")
    print("  are always passed once metadata.yaml/examples/ exist — no extra flag needed):")
    print('    medallion query sales_intel "Which rep is leading in revenue this quarter?"')
    print()
    print("  Opt-in flags for ambiguity detection / query decomposition (each costs one")
    print("  extra LLM call, off by default):")
    print('    medallion query sales_intel "Show me the good ones" --detect-ambiguity')
    print('    medallion query sales_intel "Headcount by team and revenue by region" --decompose')
    print(f"\n{'━' * W}\n")


if __name__ == "__main__":
    main()
