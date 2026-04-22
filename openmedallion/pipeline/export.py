"""pipeline/export.py — copy gold Parquet to the export directory; write CSV as BI fallback."""
from pathlib import Path

from openmedallion import storage


class BIExporter:
    """Copy gold outputs to the export directory with CSV fallbacks.

    Args:
        cfg: Merged project config dict.
    """

    def __init__(self, cfg: dict):
        self.gold_root   = cfg["paths"]["gold"]
        self.export_root = cfg["paths"]["export"]
        self.projects    = cfg["bi_export"]["projects"]
        self.enabled     = cfg["bi_export"]["enabled"]

    def export(self) -> None:
        """Run the export for all configured BI projects."""
        if not self.enabled:
            return

        for project in self.projects:
            name    = project["name"]
            src_dir = storage.join(self.gold_root,   name)
            dst_dir = storage.join(self.export_root, name)
            storage.mkdir(dst_dir)

            for table in project["tables"]:
                src = storage.join(src_dir, table)
                if not storage.exists(src):
                    print(f"⏭️   [export] skip missing {src}")
                    continue

                dst = storage.join(dst_dir, table)
                storage.copy(src, dst)
                print(f"📤  [export/{name}] parquet → {dst}")

                csv_name = Path(table).with_suffix(".csv").name
                csv_dst  = storage.join(dst_dir, csv_name)
                storage.write_csv(storage.read_parquet(src), csv_dst)
                print(f"📄  [export/{name}] csv    → {csv_dst}")
