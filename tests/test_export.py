"""tests/test_export.py — tests for BIExporter."""
import polars as pl
from pathlib import Path

from openmedallion.pipeline.export import BIExporter


def make_cfg(tmp_path, projects, enabled=True):
    gold   = tmp_path / "gold"
    export = tmp_path / "export"
    gold.mkdir(exist_ok=True)
    export.mkdir(exist_ok=True)
    return {
        "paths": {"gold": str(gold), "export": str(export)},
        "bi_export": {"enabled": enabled, "projects": projects},
    }


def write_gold(tmp_path, project, filename, df):
    d = tmp_path / "gold" / project
    d.mkdir(parents=True, exist_ok=True)
    df.write_parquet(d / filename)


class TestBIExporter:

    def test_parquet_copied_to_export(self, tmp_path):
        df = pl.DataFrame({"id": [1, 2, 3]})
        write_gold(tmp_path, "dashboard", "summary.parquet", df)
        cfg = make_cfg(tmp_path, [{"name": "dashboard", "tables": ["summary.parquet"]}])
        BIExporter(cfg).export()
        assert (tmp_path / "export" / "dashboard" / "summary.parquet").exists()

    def test_csv_written_alongside_parquet(self, tmp_path):
        df = pl.DataFrame({"id": [1, 2]})
        write_gold(tmp_path, "bi", "t.parquet", df)
        cfg = make_cfg(tmp_path, [{"name": "bi", "tables": ["t.parquet"]}])
        BIExporter(cfg).export()
        csv_path = tmp_path / "export" / "bi" / "t.csv"
        assert csv_path.exists()
        result = pl.read_csv(csv_path)
        assert result["id"].to_list() == [1, 2]

    def test_exported_parquet_content_matches_source(self, tmp_path):
        df = pl.DataFrame({"val": [10, 20, 30]})
        write_gold(tmp_path, "bi", "data.parquet", df)
        cfg = make_cfg(tmp_path, [{"name": "bi", "tables": ["data.parquet"]}])
        BIExporter(cfg).export()
        exported = pl.read_parquet(tmp_path / "export" / "bi" / "data.parquet")
        assert exported.equals(df)

    def test_multiple_tables_exported(self, tmp_path):
        for name in ("a.parquet", "b.parquet"):
            write_gold(tmp_path, "bi", name, pl.DataFrame({"x": [1]}))
        cfg = make_cfg(tmp_path, [{"name": "bi", "tables": ["a.parquet", "b.parquet"]}])
        BIExporter(cfg).export()
        assert (tmp_path / "export" / "bi" / "a.parquet").exists()
        assert (tmp_path / "export" / "bi" / "b.parquet").exists()

    def test_multiple_projects_exported_to_separate_dirs(self, tmp_path):
        write_gold(tmp_path, "proj_a", "t.parquet", pl.DataFrame({"x": [1]}))
        write_gold(tmp_path, "proj_b", "t.parquet", pl.DataFrame({"x": [2]}))
        cfg = make_cfg(tmp_path, [
            {"name": "proj_a", "tables": ["t.parquet"]},
            {"name": "proj_b", "tables": ["t.parquet"]},
        ])
        BIExporter(cfg).export()
        assert (tmp_path / "export" / "proj_a" / "t.parquet").exists()
        assert (tmp_path / "export" / "proj_b" / "t.parquet").exists()

    def test_missing_gold_file_skipped_not_raised(self, tmp_path, capsys):
        cfg = make_cfg(tmp_path, [{"name": "bi", "tables": ["nonexistent.parquet"]}])
        BIExporter(cfg).export()
        assert not (tmp_path / "export" / "bi" / "nonexistent.parquet").exists()
        assert "skip" in capsys.readouterr().out.lower()

    def test_disabled_export_skips_everything(self, tmp_path):
        write_gold(tmp_path, "bi", "t.parquet", pl.DataFrame({"x": [1]}))
        cfg = make_cfg(tmp_path, [{"name": "bi", "tables": ["t.parquet"]}], enabled=False)
        BIExporter(cfg).export()
        assert not (tmp_path / "export" / "bi").exists()

    def test_export_creates_project_dir_if_missing(self, tmp_path):
        write_gold(tmp_path, "newbi", "t.parquet", pl.DataFrame({"x": [1]}))
        cfg = make_cfg(tmp_path, [{"name": "newbi", "tables": ["t.parquet"]}])
        BIExporter(cfg).export()
        assert (tmp_path / "export" / "newbi").is_dir()
