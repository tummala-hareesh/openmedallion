"""viz/notebook.py — Inline Jupyter pipeline status widget (Panel).

Usage::

    from openmedallion.viz.notebook import PipelineDashboard

    dash = PipelineDashboard()   # reads pipeline_status.json in CWD by default
    dash.show()                  # renders inline in Jupyter; polls every 500 ms

    # … run your pipeline …

    dash.stop()                  # stop polling when done

Without --track (Mode 2):
    Shows IDLE + a hint message. No errors thrown.
    If a previous tracked run left pipeline_status.json on disk, that state is shown.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from openmedallion.viz.tracker import DEFAULT_STATUS_FILE

_ICONS = {
    "pending": "⬜",
    "running": "⏳",
    "success": "✅",
    "failed":  "❌",
}

_BADGE_STYLE = {
    "idle":    "background:#1c2128;color:#6e7681",
    "running": "background:#1f3a5f;color:#79c0ff",
    "success": "background:#1b3a2a;color:#56d364",
    "failed":  "background:#3a1b1b;color:#f85149",
}

_HINT_NO_FILE = (
    "No pipeline_status.json found. "
    "Run <code>medallion run &lt;project&gt; --track</code> to see live status."
)


class PipelineDashboard:
    """Inline Jupyter pipeline status widget powered by Panel.

    Args:
        status_file: Path to the JSON file written by PipelineStatusTracker.
                     Defaults to ``pipeline_status.json`` in CWD — same
                     default as PipelineStatusTracker.
        refresh_ms:  Polling interval in milliseconds. Default 500.
    """

    def __init__(
        self,
        status_file: Path = DEFAULT_STATUS_FILE,
        refresh_ms: int = 500,
    ) -> None:
        self.status_file = Path(status_file)
        self.refresh_ms = refresh_ms
        self._pane = None
        self._cb   = None

    # ------------------------------------------------------------------
    # Rendering — pure Python, no Panel import
    # ------------------------------------------------------------------

    def _read_status(self) -> dict | None:
        """Return parsed status dict, or None if file is absent/corrupt."""
        if not self.status_file.exists():
            return None
        try:
            return json.loads(self.status_file.read_text())
        except (json.JSONDecodeError, OSError):
            return None

    def _render_html(self) -> str:
        """Build the complete HTML string from current pipeline_status.json."""
        status = self._read_status()

        # Mode 2 / no tracking: show idle + hint
        if status is None:
            return (
                '<div style="font-family:monospace;background:#0d1117;color:#c9d1d9;'
                'padding:12px;border-radius:6px">'
                '<div style="background:#1c2128;color:#6e7681;display:inline-block;'
                'padding:4px 14px;border-radius:4px;font-weight:bold;margin-bottom:10px">'
                "IDLE</div>"
                f'<div style="color:#6e7681;font-size:.85em;margin-top:4px">'
                f"{_HINT_NO_FILE}</div>"
                "</div>"
            )

        state     = status.get("state", "idle")
        badge_css = _BADGE_STYLE.get(state, _BADGE_STYLE["idle"])
        run_id    = status.get("run_id")
        t_start   = status.get("start_time")

        meta = ""
        if run_id and t_start:
            meta = (
                f'<div style="color:#6e7681;font-size:.78em;margin-bottom:8px">'
                f'run {run_id[:8]}  ·  started '
                f'{time.strftime("%H:%M:%S", time.localtime(t_start))}'
                "</div>"
            )

        badge = (
            f'<div style="{badge_css};display:inline-block;padding:4px 14px;'
            f'border-radius:4px;font-weight:bold;margin-bottom:10px">'
            f"{state.upper()}</div>"
        )

        rows: list[str] = []
        for name, info in status.get("nodes", {}).items():
            ns      = info.get("state", "pending")
            icon    = _ICONS.get(ns, "?")
            t0, t1  = info.get("start"), info.get("end")
            elapsed = ""
            if t0:
                ms = ((t1 or time.time()) - t0) * 1000
                elapsed = f"{ms / 1000:.1f}s" if ms >= 1000 else f"{ms:.0f}ms"
            row = (
                f'<div style="padding:4px 8px">'
                f'{icon} <strong>{name}</strong> '
                f'<span style="color:#8b949e;font-size:.85em">{elapsed}</span>'
            )
            if info.get("error"):
                row += (
                    f'<br><span style="color:#f85149;font-size:.8em">'
                    f'{info["error"]}</span>'
                )
            row += "</div>"
            rows.append(row)

        return (
            '<div style="font-family:monospace;background:#0d1117;color:#c9d1d9;'
            'padding:12px;border-radius:6px;max-width:520px">'
            + badge + meta + "".join(rows) + "</div>"
        )

    # ------------------------------------------------------------------
    # Panel integration
    # ------------------------------------------------------------------

    def show(self):
        """Render the dashboard inline in Jupyter and start polling.

        Call this as the last expression in a cell, or assign to a variable
        and display it.  Panel must be loaded — ``pn.extension()`` is called
        automatically.
        """
        import panel as pn
        from IPython.display import display

        # extension() must output its JS/CSS into the current cell output;
        # calling it here (rather than inside a nested helper) ensures that.
        pn.extension()

        self._pane = pn.pane.HTML(self._render_html(), width=520)

        def _tick():
            self._pane.object = self._render_html()

        self._cb = pn.state.add_periodic_callback(_tick, period=self.refresh_ms)

        # display() explicitly pushes the widget into the cell output;
        # more reliable than relying on the auto-repr of the return value.
        display(self._pane)
        return self._pane

    def stop(self) -> None:
        """Stop the polling callback. Call this when the pipeline is done."""
        if self._cb is not None:
            self._cb.stop()
            self._cb = None
