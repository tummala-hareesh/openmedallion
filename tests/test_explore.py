"""Tests for the explore module — no optional deps (ydata-profiling / pygwalker) required.

All tests target public interfaces and mock the deferred library imports so CI
passes without installing the [profile] or [explore] optional extras.
"""
import json
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import polars as pl
import pytest

from openmedallion.explore.profile import generate_profile
from openmedallion.explore.walker  import generate_walker
from openmedallion.pipeline.explore import ExploreGenerator


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def parquet_file(tmp_path) -> Path:
    """Write a minimal Parquet file to tmp_path and return its path."""
    df = pl.DataFrame({"id": [1, 2, 3], "value": [10.0, 20.0, 30.0]})
    path = tmp_path / "sample.parquet"
    df.write_parquet(path)
    return path


def _make_cfg(tmp_path: Path, projects: list | None = None) -> dict:
    """Build a minimal config dict for ExploreGenerator tests."""
    return {
        "paths": {
            "gold":    str(tmp_path / "gold"),
            "explore": str(tmp_path / "explore"),
        },
        "gold_to_explore": {"projects": projects or []},
    }


# ---------------------------------------------------------------------------
# generate_profile — ImportError handling
# ---------------------------------------------------------------------------

def test_generate_profile_raises_import_error_when_missing(tmp_path, parquet_file):
    """generate_profile raises ImportError with install hint when ydata-profiling is absent."""
    with patch.dict("sys.modules", {"ydata_profiling": None}):
        with pytest.raises(ImportError, match="openmedallion\\[profile\\]"):
            generate_profile(parquet_file, tmp_path / "out.html")


# ---------------------------------------------------------------------------
# generate_profile — happy path (mocked ProfileReport)
# ---------------------------------------------------------------------------

def _mock_ydata(pr_cls):
    """Return a sys.modules patch dict that makes ydata_profiling importable."""
    mock_mod = MagicMock()
    mock_mod.ProfileReport = pr_cls
    return {"ydata_profiling": mock_mod}


def test_generate_profile_calls_profile_report(tmp_path, parquet_file):
    """generate_profile creates parent dirs and calls ProfileReport.to_file."""
    mock_report = MagicMock()
    mock_pr_cls = MagicMock(return_value=mock_report)

    with patch.dict("sys.modules", _mock_ydata(mock_pr_cls)):
        output = tmp_path / "sub" / "report.html"
        generate_profile(parquet_file, output, title="Test Report")

    mock_pr_cls.assert_called_once()
    _, kwargs = mock_pr_cls.call_args
    assert kwargs.get("title") == "Test Report"
    assert kwargs.get("minimal") is False
    mock_report.to_file.assert_called_once_with(output)
    assert output.parent.exists()


def test_generate_profile_minimal_flag(tmp_path, parquet_file):
    """minimal=True is forwarded to ProfileReport."""
    mock_report = MagicMock()
    mock_pr_cls = MagicMock(return_value=mock_report)

    with patch.dict("sys.modules", _mock_ydata(mock_pr_cls)):
        generate_profile(parquet_file, tmp_path / "out.html", minimal=True)

    _, kwargs = mock_pr_cls.call_args
    assert kwargs.get("minimal") is True


def test_generate_profile_uses_stem_as_default_title(tmp_path, parquet_file):
    """When no title is given, the Parquet filename stem is used."""
    mock_report = MagicMock()
    mock_pr_cls = MagicMock(return_value=mock_report)

    with patch.dict("sys.modules", _mock_ydata(mock_pr_cls)):
        generate_profile(parquet_file, tmp_path / "out.html")

    _, kwargs = mock_pr_cls.call_args
    assert kwargs.get("title") == parquet_file.stem


# ---------------------------------------------------------------------------
# generate_walker — ImportError handling
# ---------------------------------------------------------------------------

def test_generate_walker_raises_import_error_when_missing(tmp_path, parquet_file):
    """generate_walker raises ImportError with install hint when pygwalker is absent."""
    with patch.dict("sys.modules", {"pygwalker": None}):
        with pytest.raises(ImportError, match="openmedallion\\[explore\\]"):
            generate_walker(parquet_file, tmp_path / "out.html")


# ---------------------------------------------------------------------------
# generate_walker — happy path (mocked pyg.walk)
# ---------------------------------------------------------------------------

def _mock_pygwalker(html: str = "<html>walker</html>"):
    """Return a sys.modules patch dict that makes pygwalker importable."""
    mock_mod = MagicMock()
    mock_mod.walk.return_value = html
    return {"pygwalker": mock_mod}, mock_mod


def test_generate_walker_writes_html(tmp_path, parquet_file):
    """generate_walker calls pyg.walk and writes the HTML string to disk."""
    mods, mock_pyg = _mock_pygwalker()
    with patch.dict("sys.modules", mods):
        output = tmp_path / "sub" / "explorer.html"
        generate_walker(parquet_file, output, title="My Explorer")

    mock_pyg.walk.assert_called_once()
    assert output.exists()
    assert output.read_text() == "<html>walker</html>"
    assert output.parent.exists()


def test_generate_walker_passes_return_html_true(tmp_path, parquet_file):
    """generate_walker always passes return_html=True to pyg.walk."""
    mods, mock_pyg = _mock_pygwalker("<html/>")
    with patch.dict("sys.modules", mods):
        generate_walker(parquet_file, tmp_path / "out.html")

    _, kwargs = mock_pyg.walk.call_args
    assert kwargs.get("return_html") is True


# ---------------------------------------------------------------------------
# ExploreGenerator — no-op when projects list is empty
# ---------------------------------------------------------------------------

def test_explore_generator_skips_when_no_projects(tmp_path, capsys):
    """ExploreGenerator.generate() prints a skip message when no projects configured."""
    cfg = _make_cfg(tmp_path, projects=[])
    ExploreGenerator(cfg).generate()
    out = capsys.readouterr().out
    assert "skipping" in out.lower()


def test_explore_generator_skips_when_gold_to_explore_absent(tmp_path, capsys):
    """ExploreGenerator.generate() is a no-op when gold_to_explore key is absent."""
    cfg = {"paths": {"gold": str(tmp_path), "explore": str(tmp_path)}}
    ExploreGenerator(cfg).generate()
    out = capsys.readouterr().out
    assert "skipping" in out.lower()


# ---------------------------------------------------------------------------
# ExploreGenerator — skips missing gold files
# ---------------------------------------------------------------------------

def test_explore_generator_skips_missing_source(tmp_path, capsys):
    """Missing gold Parquet files are skipped with a printed notice, no exception."""
    (tmp_path / "gold" / "myproject").mkdir(parents=True)
    cfg = _make_cfg(tmp_path, projects=[{
        "name": "myproject",
        "tables": [{"source_file": "nonexistent.parquet", "report_type": "profile"}],
    }])
    ExploreGenerator(cfg).generate()
    out = capsys.readouterr().out
    assert "skip" in out.lower()


# ---------------------------------------------------------------------------
# ExploreGenerator — unknown report_type logged, not raised
# ---------------------------------------------------------------------------

def test_explore_generator_warns_on_unknown_report_type(tmp_path, capsys, parquet_file):
    """An unknown report_type prints a warning and does not raise."""
    gold_dir = tmp_path / "gold" / "myproject"
    gold_dir.mkdir(parents=True)
    (gold_dir / "sample.parquet").write_bytes(parquet_file.read_bytes())

    cfg = _make_cfg(tmp_path, projects=[{
        "name": "myproject",
        "tables": [{"source_file": "sample.parquet", "report_type": "unknown_type"}],
    }])
    ExploreGenerator(cfg).generate()
    out = capsys.readouterr().out
    assert "unknown" in out.lower()


# ---------------------------------------------------------------------------
# ExploreGenerator — profile dispatch
# ---------------------------------------------------------------------------

def test_explore_generator_dispatches_profile(tmp_path, parquet_file):
    """ExploreGenerator calls generate_profile for report_type: profile."""
    gold_dir = tmp_path / "gold" / "proj"
    gold_dir.mkdir(parents=True)
    (gold_dir / "data.parquet").write_bytes(parquet_file.read_bytes())

    cfg = _make_cfg(tmp_path, projects=[{
        "name": "proj",
        "tables": [{
            "source_file":  "data.parquet",
            "report_type":  "profile",
            "output_file":  "data_profile.html",
            "title":        "My Profile",
        }],
    }])

    with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
        ExploreGenerator(cfg).generate()

    mock_gp.assert_called_once()
    args, kwargs = mock_gp.call_args
    assert args[0] == gold_dir / "data.parquet"
    assert args[1] == tmp_path / "gold" / "add-ons" / "proj" / "data_profile.html"
    assert kwargs.get("title") == "My Profile"
    assert kwargs.get("minimal") is False


def test_explore_generator_profile_minimal_forwarded(tmp_path, parquet_file):
    """minimal: true in explore.yaml table config is forwarded to generate_profile."""
    gold_dir = tmp_path / "gold" / "proj"
    gold_dir.mkdir(parents=True)
    (gold_dir / "data.parquet").write_bytes(parquet_file.read_bytes())

    cfg = _make_cfg(tmp_path, projects=[{
        "name": "proj",
        "tables": [{"source_file": "data.parquet", "report_type": "profile", "minimal": True}],
    }])

    with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
        ExploreGenerator(cfg).generate()

    _, kwargs = mock_gp.call_args
    assert kwargs.get("minimal") is True


# ---------------------------------------------------------------------------
# ExploreGenerator — walker dispatch
# ---------------------------------------------------------------------------

def test_explore_generator_dispatches_walker(tmp_path, parquet_file):
    """ExploreGenerator calls generate_walker for report_type: walker."""
    gold_dir = tmp_path / "gold" / "proj"
    gold_dir.mkdir(parents=True)
    (gold_dir / "data.parquet").write_bytes(parquet_file.read_bytes())

    cfg = _make_cfg(tmp_path, projects=[{
        "name": "proj",
        "tables": [{
            "source_file": "data.parquet",
            "report_type": "walker",
            "output_file": "data_explorer.html",
            "title":       "My Explorer",
        }],
    }])

    with patch("openmedallion.explore.walker.generate_walker") as mock_gw:
        ExploreGenerator(cfg).generate()

    mock_gw.assert_called_once()
    args, kwargs = mock_gw.call_args
    assert args[0] == gold_dir / "data.parquet"
    assert args[1] == tmp_path / "gold" / "add-ons" / "proj" / "data_explorer.html"
    assert kwargs.get("title") == "My Explorer"


# ---------------------------------------------------------------------------
# ExploreGenerator — default output filename and title derivation
# ---------------------------------------------------------------------------

def test_explore_generator_default_output_filename(tmp_path, parquet_file):
    """When output_file is omitted, defaults to <stem>_<report_type>.html."""
    gold_dir = tmp_path / "gold" / "proj"
    gold_dir.mkdir(parents=True)
    (gold_dir / "headcount.parquet").write_bytes(parquet_file.read_bytes())

    cfg = _make_cfg(tmp_path, projects=[{
        "name": "proj",
        "tables": [{"source_file": "headcount.parquet", "report_type": "profile"}],
    }])

    with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
        ExploreGenerator(cfg).generate()

    args, _ = mock_gp.call_args
    assert args[1].name == "headcount_profile.html"


def test_explore_generator_default_title_derived_from_stem(tmp_path, parquet_file):
    """When title is omitted, the stem is title-cased with underscores as spaces."""
    gold_dir = tmp_path / "gold" / "proj"
    gold_dir.mkdir(parents=True)
    (gold_dir / "salary_by_job.parquet").write_bytes(parquet_file.read_bytes())

    cfg = _make_cfg(tmp_path, projects=[{
        "name": "proj",
        "tables": [{"source_file": "salary_by_job.parquet", "report_type": "profile"}],
    }])

    with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
        ExploreGenerator(cfg).generate()

    _, kwargs = mock_gp.call_args
    assert kwargs.get("title") == "Salary By Job"


# ---------------------------------------------------------------------------
# ExploreGenerator — reports land under gold/add-ons/<project>/
# ---------------------------------------------------------------------------

def test_explore_generator_output_under_gold_addons(tmp_path, parquet_file):
    """ExploreGenerator writes reports to gold_path/add-ons/<project>/."""
    gold_dir = tmp_path / "gold" / "proj"
    gold_dir.mkdir(parents=True)
    (gold_dir / "data.parquet").write_bytes(parquet_file.read_bytes())

    cfg = _make_cfg(tmp_path, projects=[{
        "name": "proj",
        "tables": [{"source_file": "data.parquet", "report_type": "profile",
                    "output_file": "data_profile.html"}],
    }])

    with patch("openmedallion.explore.profile.generate_profile") as mock_gp:
        ExploreGenerator(cfg).generate()

    args, _ = mock_gp.call_args
    assert args[1].parent == tmp_path / "gold" / "add-ons" / "proj"
