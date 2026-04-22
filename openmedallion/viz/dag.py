"""viz/dag.py — Hamilton pipeline DAG inspection and export.

Text tree (no dependencies):
    from openmedallion.viz.dag import print_dag
    print_dag()

PNG/SVG export (requires graphviz):
    uv sync --extra viz
    from openmedallion.viz.dag import export_dag, export_execution_dag
    export_dag("dag.png")
"""
import subprocess
import sys
from collections import deque
from pathlib import Path
from typing import Any

from hamilton import driver
from openmedallion.pipeline import nodes as pipeline_nodes


def _build_driver() -> driver.Driver:
    return driver.Builder().with_modules(pipeline_nodes).build()


def _fmt_type(t: Any) -> str:
    """Return a compact, readable type string (no module prefixes)."""
    if t is type(None):
        return "None"
    # Check __origin__ first: GenericAlias (dict[str, Path]) delegates __name__
    # to its origin, so checking __name__ first would silently drop the args.
    origin = getattr(t, "__origin__", None)
    if origin is not None:
        args = getattr(t, "__args__", ()) or ()
        origin_name = getattr(origin, "__name__", str(origin))
        if args:
            return f"{origin_name}[{', '.join(_fmt_type(a) for a in args)}]"
        return origin_name
    if hasattr(t, "__name__"):
        return t.__name__
    return str(t).replace("typing.", "")


def print_dag() -> None:
    """Print the pipeline DAG as an ASCII tree — no graphviz required."""
    dr = _build_driver()
    graph_nodes = dr.graph.nodes

    all_referenced = {k for n in graph_nodes.values() for k in n.input_types}
    external = sorted(all_referenced - set(graph_nodes))

    node_deps: dict[str, list[str]] = {}
    node_ext: dict[str, list[str]] = {}
    for name, node in graph_nodes.items():
        node_deps[name] = sorted(k for k in node.input_types if k in graph_nodes)
        node_ext[name] = sorted(k for k in node.input_types if k in external)

    in_degree = {n: len(deps) for n, deps in node_deps.items()}
    dependents: dict[str, list[str]] = {n: [] for n in graph_nodes}
    for name, deps in node_deps.items():
        for dep in deps:
            dependents[dep].append(name)

    queue: deque[str] = deque(sorted(n for n, d in in_degree.items() if d == 0))
    order: list[str] = []
    while queue:
        n = queue.popleft()
        order.append(n)
        for dep in sorted(dependents[n]):
            in_degree[dep] -= 1
            if in_degree[dep] == 0:
                queue.append(dep)

    bar = "─" * 51
    print(f"\n  {bar}")
    print("  Pipeline DAG  (Hamilton nodes)")
    print(f"  {bar}")
    if external:
        print()
        for inp in external:
            print(f"  [input]  {inp}")
    fn_start = False
    print()
    for name in order:
        node = graph_nodes[name]
        args = node_ext[name] + node_deps[name]
        ret = _fmt_type(node.type)
        if not args:
            print(f"  [input]  {name} : {ret}")
        else:
            if fn_start:
                print("    |")
                print("    v")
            print(f"  {name}({', '.join(args)}) -> {ret}")
            fn_start = True
    print(f"\n  {bar}\n")


def export_dag(output_path: str | Path = "dag.png", orient: str = "LR") -> Path:
    """Export every node in the pipeline DAG to an image file."""
    _require_graphviz()
    output_path = Path(output_path)
    dr = _build_driver()
    dr.display_all_functions(
        output_file_path=str(output_path),
        orient=orient,
        render_kwargs={"cleanup": True},
    )
    return output_path


def export_execution_dag(
    final_vars: list[str],
    inputs: dict[str, Any],
    output_path: str | Path = "dag_execution.png",
    orient: str = "LR",
) -> Path:
    """Export only the nodes that will execute for a given set of final_vars."""
    _require_graphviz()
    output_path = Path(output_path)
    dr = _build_driver()
    dr.visualize_execution(
        final_vars=final_vars,
        output_file_path=str(output_path),
        inputs=inputs,
        orient=orient,
        render_kwargs={"cleanup": True},
    )
    return output_path


def open_image(path: Path) -> None:
    """Open an image file in the default viewer for the current OS."""
    if sys.platform == "win32":
        subprocess.run(["start", str(path)], shell=True, check=False)
    elif sys.platform == "darwin":
        subprocess.run(["open", str(path)], check=False)
    else:
        subprocess.run(["xdg-open", str(path)], check=False)


def _require_graphviz() -> None:
    try:
        import graphviz  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "Graphviz Python package not found.\n"
            "Install it with:  uv sync --extra viz"
        ) from e
