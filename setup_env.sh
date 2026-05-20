#!/usr/bin/env bash
# Manage the local .venv and its Jupyter kernel.
#
# Usage:
#   ./setup_env.sh create          — create venv + install deps + register kernel
#   ./setup_env.sh delete          — delete venv + unregister kernel
#   ./setup_env.sh list            — list venvs + kernels
#   ./setup_env.sh create venv     — create venv + install deps only
#   ./setup_env.sh create kernel   — register kernel from existing venv only
#   ./setup_env.sh delete venv     — delete .venv directory only
#   ./setup_env.sh delete kernel   — unregister Jupyter kernel only
#   ./setup_env.sh list venv       — list venvs in current directory
#   ./setup_env.sh list kernel     — list registered Jupyter kernels
set -euo pipefail

VENV_DIR=".venv_oma"
PROJECT_NAME="oma"
VENV_PROMPT=$PROJECT_NAME   # shown in shell prompt on activate
KERNEL_NAME=$PROJECT_NAME
KERNEL_DISPLAY=$PROJECT_NAME
PYTHON_VERSION=3.12

# ── helpers ──────────────────────────────────────────────────────────────────

create_venv() {
    echo "==> Creating venv with uv (Python $PYTHON_VERSION)..."
    uv venv "$VENV_DIR" --python "$PYTHON_VERSION" --prompt "$VENV_PROMPT" --clear

    echo "==> Installing dependencies..."
    uv pip install -r pyproject.toml --python "$VENV_DIR/bin/python"

    echo "==> Installing openmedallion (editable)..."
    uv pip install -e . --python "$VENV_DIR/bin/python"

    echo "==> Installing ipykernel..."
    uv pip install ipykernel --python "$VENV_DIR/bin/python"

    echo "    venv ready at $VENV_DIR/"
}

create_kernel() {
    if [[ ! -x "$VENV_DIR/bin/python" ]]; then
        echo "ERROR: $VENV_DIR/bin/python not found — create the venv first." >&2
        exit 1
    fi
    echo "==> Registering Jupyter kernel '$KERNEL_NAME'..."
    "$VENV_DIR/bin/python" -m ipykernel install \
        --user \
        --name "$KERNEL_NAME" \
        --display-name "$KERNEL_DISPLAY"
    echo "    Kernel '$KERNEL_DISPLAY' registered."
}

delete_venv() {
    if [[ -d "$VENV_DIR" ]]; then
        echo "==> Removing $VENV_DIR/..."
        rm -rf "$VENV_DIR"
        echo "    Done."
    else
        echo "    $VENV_DIR not found — nothing to remove."
    fi
}

_jupyter() {
    # prefer venv jupyter, fall back to system jupyter
    local j="$VENV_DIR/bin/jupyter"
    [[ -x "$j" ]] || j="$(command -v jupyter 2>/dev/null)" || { echo "ERROR: jupyter not found" >&2; exit 1; }
    "$j" "$@"
}

delete_kernel() {
    if _jupyter kernelspec list 2>/dev/null | grep -q "^$KERNEL_NAME "; then
        echo "==> Unregistering Jupyter kernel '$KERNEL_NAME'..."
        _jupyter kernelspec uninstall -f "$KERNEL_NAME"
        echo "    Done."
    else
        echo "    Kernel '$KERNEL_NAME' not found — nothing to remove."
    fi
}

list_venvs() {
    echo "==> Venvs in $(pwd):"
    local found=0
    for d in */ .*/; do
        [[ -x "${d}bin/python" ]] || continue
        local prompt
        prompt=$(grep -m1 'VIRTUAL_ENV_PROMPT=' "${d}bin/activate" 2>/dev/null \
                 | sed 's/.*VIRTUAL_ENV_PROMPT="\(.*\)"/\1/' || true)
        local pyver
        pyver=$("${d}bin/python" --version 2>&1 | awk '{print $2}')
        printf "  %-20s  Python %-8s  prompt: (%s)\n" "$d" "$pyver" "${prompt:-$(basename "$d")}"
        found=1
    done
    [[ $found -eq 1 ]] || echo "  (none found)"
}

list_kernels() {
    echo "==> Registered Jupyter kernels:"
    _jupyter kernelspec list 2>/dev/null | tail -n +2 | while IFS= read -r line; do
        echo "  $line"
    done
}

print_ref() {
    echo ""
    echo "Quick reference:"
    echo "  Activate :  source $VENV_DIR/bin/activate"
    echo "  Python   :  $VENV_DIR/bin/python"
    echo "  Pytest   :  $VENV_DIR/bin/pytest"
    echo "  Kernel   :  $KERNEL_DISPLAY  (select in Jupyter)"
    echo ""
}

print_usage() {
    echo "Usage: $0 <create|delete|list> [venv|kernel]"
    echo ""
    echo "  $0 create           create venv + install deps + register kernel"
    echo "  $0 delete           delete venv + unregister kernel"
    echo "  $0 list             list venvs + kernels"
    echo "  $0 create venv      create venv + install deps only"
    echo "  $0 create kernel    register kernel from existing venv only"
    echo "  $0 delete venv      delete .venv directory only"
    echo "  $0 delete kernel    unregister Jupyter kernel only"
    echo "  $0 list venv        list venvs in current directory"
    echo "  $0 list kernel      list registered Jupyter kernels"
}

# ── dispatch ──────────────────────────────────────────────────────────────────

ACTION="${1:-}"
TARGET="${2:-all}"

case "$ACTION" in
    create)
        case "$TARGET" in
            all)     create_venv; create_kernel; print_ref ;;
            venv)    create_venv ;;
            kernel)  create_kernel ;;
            *)       echo "Unknown target: $TARGET"; print_usage; exit 1 ;;
        esac
        ;;
    delete)
        case "$TARGET" in
            all)     delete_kernel; delete_venv ;;
            venv)    delete_venv ;;
            kernel)  delete_kernel ;;
            *)       echo "Unknown target: $TARGET"; print_usage; exit 1 ;;
        esac
        ;;
    list)
        case "$TARGET" in
            all)     list_venvs; echo; list_kernels ;;
            venv)    list_venvs ;;
            kernel)  list_kernels ;;
            *)       echo "Unknown target: $TARGET"; print_usage; exit 1 ;;
        esac
        ;;
    *)
        print_usage
        exit 1
        ;;
esac
