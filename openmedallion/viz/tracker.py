"""viz/tracker.py — Hamilton lifecycle hooks for real-time pipeline status tracking."""
import json
import time
from pathlib import Path
from typing import Any, Collection

from hamilton.lifecycle import NodeExecutionHook, GraphExecutionHook
from hamilton.graph_types import HamiltonGraph

DEFAULT_STATUS_FILE = Path("pipeline_status.json")

_ALL_NODES = ["config", "bronze", "silver", "gold", "bi_export"]


class PipelineStatusTracker(NodeExecutionHook, GraphExecutionHook):
    """Writes node execution state to a JSON file after every state change.

    Attach to a Hamilton driver via ``driver.Builder().with_adapters(tracker)``.

    Args:
        status_file: Path to the JSON file. Defaults to ``pipeline_status.json``
            in the current working directory.
    """

    def __init__(self, status_file: Path = DEFAULT_STATUS_FILE) -> None:
        self.status_file = status_file
        self._reset()

    def run_before_graph_execution(
        self, *, graph: HamiltonGraph, final_vars: list[str], inputs: dict[str, Any],
        overrides: dict[str, Any], execution_path: Collection[str], run_id: str, **kwargs: Any,
    ) -> None:
        node_names = list(execution_path) or _ALL_NODES
        self.status = {
            "state": "running", "run_id": run_id,
            "start_time": time.time(), "end_time": None, "error": None,
            "nodes": {
                name: {"state": "pending", "start": None, "end": None, "error": None}
                for name in node_names
            },
        }
        self._write()

    def run_after_graph_execution(
        self, *, graph: HamiltonGraph, success: bool, error: Exception | None,
        results: dict[str, Any] | None, run_id: str, **kwargs: Any,
    ) -> None:
        self.status["state"] = "success" if success else "failed"
        self.status["end_time"] = time.time()
        self.status["error"] = str(error) if error else None
        self._write()

    def run_before_node_execution(self, *, node_name: str, **kwargs: Any) -> None:
        self.status["nodes"].setdefault(
            node_name, {"state": "pending", "start": None, "end": None, "error": None}
        )
        self.status["nodes"][node_name].update({"state": "running", "start": time.time()})
        self._write()

    def run_after_node_execution(
        self, *, node_name: str, success: bool, error: Exception | None, **kwargs: Any,
    ) -> None:
        node = self.status["nodes"].setdefault(node_name, {})
        node["state"] = "success" if success else "failed"
        node["end"] = time.time()
        node["error"] = str(error) if error else None
        self._write()

    def _reset(self) -> None:
        self.status: dict[str, Any] = {
            "state": "idle", "run_id": None, "start_time": None,
            "end_time": None, "error": None, "nodes": {},
        }
        self._write()

    def _write(self) -> None:
        self.status_file.write_text(json.dumps(self.status, indent=2))
