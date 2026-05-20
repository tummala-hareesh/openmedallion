"""pipeline/explore.py — generate HTML exploration reports from gold Parquet outputs.

Reads ``gold_to_explore`` from explore.yaml and produces one HTML file per
table entry using the configured report_type:

    profile  →  ydata-profiling full statistical report  (requires [profile] extra)
    walker   →  pygwalker interactive drag-and-drop explorer (requires [explore] extra)

Reports are written to ``paths.explore/<project>/<output_file>``.

The shared helper :func:`_dispatch_reports` is also called by
:class:`~openmedallion.pipeline.silver.SilverTransformer` for inline silver explore.
"""
from pathlib import Path

from openmedallion import storage


def _dispatch_reports(
    src: Path,
    out_dir: Path,
    specs: list[dict],
    context: str = "explore",
) -> None:
    """Dispatch one or more report specs for a single resolved Parquet file.

    Args:
        src:     Absolute path to the source Parquet file.
        out_dir: Directory where HTML output files are written (created if absent).
        specs:   List of report spec dicts (report_type, output_file, title, minimal).
        context: Log prefix shown in console output, e.g. ``"explore/silver"``.
    """
    if not specs:
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    for spec in specs:
        report_type = spec.get("report_type", "profile")
        output_file = spec.get("output_file", f"{src.stem}_{report_type}.html")
        title       = spec.get("title", src.stem.replace("_", " ").title())
        out         = out_dir / output_file

        if report_type == "profile":
            from openmedallion.explore.profile import generate_profile
            generate_profile(src, out, title=title, minimal=spec.get("minimal", False))
            print(f"📈  [{context}] profile → {out}")

        elif report_type == "walker":
            from openmedallion.explore.walker import generate_walker
            generate_walker(src, out, title=title)
            print(f"🗺️   [{context}] walker  → {out}")

        else:
            print(f"⚠️   [{context}] unknown report_type '{report_type}' — skipping")


class ExploreGenerator:
    """Generate HTML exploration reports from gold Parquet files.

    Args:
        cfg: Merged project config dict.  Reads ``paths.gold``,
             ``paths.explore``, and ``gold_to_explore``.
    """

    def __init__(self, cfg: dict) -> None:
        self.gold_root = cfg["paths"]["gold"]
        self.projects  = cfg.get("gold_to_explore", {}).get("projects", [])

    def generate(self) -> None:
        """Run all explore report generations defined in explore.yaml."""
        if not self.projects:
            print("  ℹ️   [explore] No explore config found — skipping.")
            return

        print(f"\n── Explore {'─' * 48}")
        for project in self.projects:
            name     = project["name"]
            gold_dir = Path(storage.join(self.gold_root, name))
            out_dir  = Path(self.gold_root) / "add-ons" / name

            for table in project.get("tables", []):
                src = gold_dir / table["source_file"]
                if not src.exists():
                    print(f"⏭️   [explore/{name}] skip missing {src}")
                    continue

                _dispatch_reports(src, out_dir, [table], context=f"explore/{name}")
