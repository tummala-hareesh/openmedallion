"""pipeline/explore.py — generate HTML exploration reports from gold Parquet outputs.

Reads ``gold_to_explore`` from explore.yaml and produces one HTML file per
table entry using the configured report_type:

    profile  →  ydata-profiling full statistical report  (requires [profile] extra)
    walker   →  pygwalker interactive drag-and-drop explorer (requires [explore] extra)

Reports are written to ``paths.explore/<project>/<output_file>``.
"""
from pathlib import Path

from openmedallion import storage


class ExploreGenerator:
    """Generate HTML exploration reports from gold Parquet files.

    Args:
        cfg: Merged project config dict.  Reads ``paths.gold``,
             ``paths.explore``, and ``gold_to_explore``.
    """

    def __init__(self, cfg: dict) -> None:
        self.gold_root    = cfg["paths"]["gold"]
        self.explore_root = cfg["paths"].get("explore", "data/explore")
        self.projects     = cfg.get("gold_to_explore", {}).get("projects", [])

    def generate(self) -> None:
        """Run all explore report generations defined in explore.yaml."""
        if not self.projects:
            print("  ℹ️   [explore] No explore config found — skipping.")
            return

        print(f"\n── Explore {'─' * 48}")
        for project in self.projects:
            name     = project["name"]
            gold_dir = Path(storage.join(self.gold_root, name))
            out_dir  = Path(storage.join(self.explore_root, name))
            out_dir.mkdir(parents=True, exist_ok=True)

            for table in project.get("tables", []):
                src = gold_dir / table["source_file"]
                if not src.exists():
                    print(f"⏭️   [explore/{name}] skip missing {src}")
                    continue

                report_type = table.get("report_type", "profile")
                output_file = table.get("output_file", f"{src.stem}_{report_type}.html")
                title       = table.get("title", src.stem.replace("_", " ").title())
                out         = out_dir / output_file

                if report_type == "profile":
                    from openmedallion.explore.profile import generate_profile
                    generate_profile(
                        src, out, title=title,
                        minimal=table.get("minimal", False),
                    )
                    print(f"📈  [explore/{name}] profile → {out}")

                elif report_type == "walker":
                    from openmedallion.explore.walker import generate_walker
                    generate_walker(src, out, title=title)
                    print(f"🗺️   [explore/{name}] walker  → {out}")

                else:
                    print(
                        f"⚠️   [explore/{name}] unknown report_type "
                        f"'{report_type}' for {table['source_file']} — skipping"
                    )
