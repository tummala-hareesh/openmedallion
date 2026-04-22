"""viz/server.py — Live pipeline status dashboard (FastAPI + uvicorn).

Routes
------
GET /          Browser dashboard (HTML)
GET /status    Current status snapshot (JSON)
GET /stream    SSE stream — emits a ``data:`` event on every file change

Usage
-----
Embedded alongside a pipeline run (non-blocking):
    from openmedallion.viz.server import start_server
    server = start_server(block=False)

Standalone blocking:
    medallion status [--port PORT]
"""
import asyncio
import json
import os
import threading
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse

DEFAULT_PORT = 8765
DEFAULT_STATUS_FILE = Path(os.environ.get("STATUS_FILE", "pipeline_status.json"))

_status_file: Path = DEFAULT_STATUS_FILE

app = FastAPI(title="Pipeline Status", docs_url=None, redoc_url=None)

# ---------------------------------------------------------------------------
# HTML dashboard
# ---------------------------------------------------------------------------

_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Pipeline Status</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Segoe UI', monospace;
      background: #0d1117;
      color: #c9d1d9;
      padding: 2rem;
      min-height: 100vh;
    }
    h1 { color: #58a6ff; font-size: 1.5rem; margin-bottom: 0.5rem; }
    #meta { color: #6e7681; font-size: 0.8rem; margin-bottom: 1.5rem; }
    #pipeline-state {
      font-size: 1.1rem;
      font-weight: 600;
      margin-bottom: 1.5rem;
      padding: 0.5rem 1rem;
      border-radius: 6px;
      display: inline-block;
    }
    .state-idle    { background: #1c2128; color: #6e7681; }
    .state-running { background: #1f3a5f; color: #79c0ff; }
    .state-success { background: #1b3a2a; color: #56d364; }
    .state-failed  { background: #3a1b1b; color: #f85149; }
    #nodes { display: flex; flex-direction: column; gap: 0.5rem; max-width: 640px; }
    .node {
      display: flex; align-items: center; gap: 0.75rem;
      padding: 0.6rem 1rem; border-radius: 6px;
      border-left: 4px solid transparent;
      transition: background 0.2s, border-color 0.2s;
    }
    .node-pending { background: #1c2128; border-color: #30363d; }
    .node-running { background: #1f3a5f; border-color: #388bfd; }
    .node-success { background: #1b3a2a; border-color: #3fb950; }
    .node-failed  { background: #3a1b1b; border-color: #f85149; }
    .node-icon     { font-size: 1.1rem; width: 1.4rem; text-align: center; }
    .node-name     { flex: 1; font-weight: 600; }
    .node-duration { font-size: 0.78rem; color: #8b949e; }
    .node-error    { font-size: 0.75rem; color: #f85149; margin-top: 0.2rem; }
    #conn-indicator {
      position: fixed; top: 1rem; right: 1rem;
      width: 10px; height: 10px; border-radius: 50%;
      background: #f85149; transition: background 0.3s;
    }
    #conn-indicator.connected { background: #3fb950; }
  </style>
</head>
<body>
  <div id="conn-indicator" title="SSE connection"></div>
  <h1>Pipeline Status</h1>
  <div id="meta"></div>
  <div id="pipeline-state" class="state-idle">IDLE</div>
  <div id="nodes"></div>
  <script>
    const ICONS = { pending: '⬜', running: '⏳', success: '✅', failed: '❌' };
    function fmt(ts) { return ts ? new Date(ts * 1000).toLocaleTimeString() : '-'; }
    function dur(start, end) {
      if (!start) return '';
      const ms = ((end || Date.now() / 1000) - start) * 1000;
      return ms >= 1000 ? (ms / 1000).toFixed(1) + 's' : Math.round(ms) + 'ms';
    }
    function render(d) {
      const badge = document.getElementById('pipeline-state');
      badge.textContent = (d.state || 'idle').toUpperCase();
      badge.className = 'state-' + (d.state || 'idle');
      document.getElementById('meta').textContent = d.run_id
        ? 'run ' + d.run_id.slice(0, 8) + '  started ' + fmt(d.start_time) : 'No run yet';
      const cont = document.getElementById('nodes');
      cont.innerHTML = '';
      for (const [name, info] of Object.entries(d.nodes || {})) {
        const state = info.state || 'pending';
        const wrap  = document.createElement('div');
        wrap.className = 'node node-' + state;
        const icon = document.createElement('span');
        icon.className = 'node-icon'; icon.textContent = ICONS[state] || '?';
        const label = document.createElement('span');
        label.className = 'node-name'; label.textContent = name;
        const elapsed = document.createElement('span');
        elapsed.className = 'node-duration'; elapsed.textContent = dur(info.start, info.end);
        wrap.append(icon, label, elapsed);
        if (info.error) {
          const err = document.createElement('div');
          err.className = 'node-error'; err.textContent = info.error;
          wrap.appendChild(err);
        }
        cont.appendChild(wrap);
      }
    }
    function connect() {
      const dot = document.getElementById('conn-indicator');
      const es  = new EventSource('/stream');
      es.onopen    = () => dot.classList.add('connected');
      es.onmessage = e => { try { render(JSON.parse(e.data)); } catch (_) {} };
      es.onerror   = () => { dot.classList.remove('connected'); es.close(); setTimeout(connect, 3000); };
    }
    connect();
  </script>
</body>
</html>
"""

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def dashboard() -> str:
    return _HTML


@app.get("/status")
async def status() -> dict:
    if _status_file.exists():
        return json.loads(_status_file.read_text())
    return {"state": "idle", "nodes": {}}


@app.get("/stream")
async def stream() -> StreamingResponse:
    async def _event_gen():
        last: str | None = None
        while True:
            current = (
                _status_file.read_text()
                if _status_file.exists()
                else json.dumps({"state": "idle", "nodes": {}})
            )
            if current != last:
                yield f"data: {current}\n\n"
                last = current
            await asyncio.sleep(0.4)

    return StreamingResponse(
        _event_gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def start_server(
    port: int = DEFAULT_PORT,
    *,
    block: bool = True,
    status_file: Path = DEFAULT_STATUS_FILE,
) -> uvicorn.Server:
    """Start the FastAPI status dashboard with uvicorn.

    Args:
        port:        TCP port to listen on. Defaults to ``8765``.
        block:       If ``True``, run in the calling thread (blocks until Ctrl-C).
                     If ``False``, run in a daemon thread and return immediately.
        status_file: JSON file written by :class:`PipelineStatusTracker`.

    Returns:
        The ``uvicorn.Server`` instance.
    """
    global _status_file
    _status_file = status_file

    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="warning")
    server = uvicorn.Server(config)
    print(f"  Status dashboard: http://localhost:{port}")

    if block:
        server.run()
    else:
        t = threading.Thread(target=server.run, daemon=True)
        t.start()

    return server
