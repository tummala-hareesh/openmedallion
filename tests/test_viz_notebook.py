"""Tests for PipelineDashboard._render_html() — no Panel dependency required."""
import json
import time
from pathlib import Path

import pytest

from openmedallion.viz.notebook import PipelineDashboard


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sf(tmp_path) -> Path:
    return tmp_path / "pipeline_status.json"


def _write(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data))


# ── Mode 2 / no-tracking ──────────────────────────────────────────────────────

def test_missing_file_shows_idle_hint(tmp_path):
    dash = PipelineDashboard(status_file=tmp_path / "nonexistent.json")
    html = dash._render_html()
    assert "IDLE" in html
    assert "--track" in html  # hint is present


def test_corrupt_file_shows_idle(sf):
    sf.write_text("{not valid json")
    html = PipelineDashboard(status_file=sf)._render_html()
    assert "IDLE" in html


def test_read_status_returns_none_for_missing(tmp_path):
    dash = PipelineDashboard(status_file=tmp_path / "x.json")
    assert dash._read_status() is None


def test_read_status_returns_none_for_corrupt(sf):
    sf.write_text("???")
    assert PipelineDashboard(status_file=sf)._read_status() is None


# ── Pipeline state rendering ──────────────────────────────────────────────────

def test_idle_state(sf):
    _write(sf, {"state": "idle", "nodes": {}})
    assert "IDLE" in PipelineDashboard(status_file=sf)._render_html()


def test_running_state_with_run_id(sf):
    _write(sf, {"state": "running", "run_id": "abc123xyz",
                "start_time": time.time(), "nodes": {}})
    html = PipelineDashboard(status_file=sf)._render_html()
    assert "RUNNING" in html
    assert "abc123" in html  # run_id prefix shown


def test_success_state(sf):
    _write(sf, {"state": "success", "nodes": {}})
    assert "SUCCESS" in PipelineDashboard(status_file=sf)._render_html()


def test_failed_state(sf):
    _write(sf, {"state": "failed", "nodes": {}})
    assert "FAILED" in PipelineDashboard(status_file=sf)._render_html()


# ── Node rendering ────────────────────────────────────────────────────────────

def test_node_icons(sf):
    t0 = time.time()
    _write(sf, {"state": "running", "nodes": {
        "bronze": {"state": "success", "start": t0,     "end": t0 + 1, "error": None},
        "silver": {"state": "running", "start": t0 + 1, "end": None,   "error": None},
        "gold":   {"state": "pending", "start": None,   "end": None,   "error": None},
    }})
    html = PipelineDashboard(status_file=sf)._render_html()
    assert "✅" in html and "⏳" in html and "⬜" in html


def test_node_error_shown(sf):
    _write(sf, {"state": "failed", "nodes": {
        "bronze": {"state": "failed", "start": None, "end": None,
                   "error": "Connection refused"},
    }})
    html = PipelineDashboard(status_file=sf)._render_html()
    assert "❌" in html
    assert "Connection refused" in html


def test_elapsed_time_seconds(sf):
    t0 = time.time() - 3.5
    _write(sf, {"state": "success", "nodes": {
        "bronze": {"state": "success", "start": t0, "end": t0 + 3.5, "error": None},
    }})
    assert "3.5s" in PipelineDashboard(status_file=sf)._render_html()


def test_all_nodes_rendered(sf):
    t0 = time.time()
    _write(sf, {"state": "running", "nodes": {
        "bronze": {"state": "success", "start": t0,     "end": t0 + 1, "error": None},
        "silver": {"state": "running", "start": t0 + 1, "end": None,   "error": None},
        "gold":   {"state": "pending", "start": None,   "end": None,   "error": None},
    }})
    html = PipelineDashboard(status_file=sf)._render_html()
    assert all(n in html for n in ["bronze", "silver", "gold"])
