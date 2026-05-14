#!/usr/bin/env python3
"""Run all openmedallion examples and report pass / fail.

Each example is executed against a clean data directory so stale state
never masks a failure.  Invoked by `make examples` from the repo root.
"""
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO  = Path(__file__).parent.parent          # openmedallion/
EX    = Path(__file__).parent                 # examples/
PY    = REPO / ".venv/bin/python"
MDL   = REPO / ".venv/bin/medallion"

# ---------------------------------------------------------------------------
# Example definitions
# ---------------------------------------------------------------------------
# Each entry:
#   name    display label
#   dir     path relative to examples/
#   setup   command to seed / create the DB (list of args, run from dir)
#   run     medallion command to execute the full pipeline (list of args)
#   clean   subdirectories inside dir to delete before each run
# ---------------------------------------------------------------------------

EXAMPLES = [
    {
        "name":  "local_parquet_demo",
        "dir":   "local_parquet_demo",
        "setup": [PY, "seed.py"],
        "run":   [MDL, "run", "demo"],
        "clean": ["data"],
    },
    {
        "name":  "ecommerce_analytics_demo",
        "dir":   "ecommerce_analytics_demo",
        "setup": [PY, "seed.py"],
        "run":   [MDL, "run", "ecommerce"],
        "clean": ["data"],
    },
    {
        "name":  "incremental_sql_demo",
        "dir":   "incremental_sql_demo",
        "setup": [PY, "setup_db.py"],
        "run":   [MDL, "run", "retail"],
        "clean": ["data"],
    },
    {
        "name":  "oracle_hr_demo",
        "dir":   "oracle_hr_demo",
        "setup": [PY, "setup_db.py"],
        "run":   [MDL, "run", "oracle_hr", "--projects", "."],
        "clean": ["data", "oracle_hr/data/bronze", "oracle_hr/data/silver",
                  "oracle_hr/data/gold"],
    },
]

# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

GREEN  = "\033[32m"
RED    = "\033[31m"
YELLOW = "\033[33m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

W_NAME = 30   # column width for example name


def _clean(root: Path, dirs: list[str]) -> None:
    for d in dirs:
        target = root / d
        if target.exists():
            shutil.rmtree(target)


def _run(cmd: list, cwd: Path) -> tuple[int, str]:
    result = subprocess.run(
        [str(c) for c in cmd],
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout + result.stderr


def main() -> int:
    print(f"\n{BOLD}── Running examples {'─' * 48}{RESET}")

    results = []

    for ex in EXAMPLES:
        root = EX / ex["dir"]
        name = ex["name"]
        print(f"  {name:<{W_NAME}}", end="", flush=True)

        _clean(root, ex["clean"])

        t0 = time.perf_counter()
        rc_setup, log_setup = _run(ex["setup"], cwd=root)
        rc_run,   log_run   = _run(ex["run"],   cwd=root) if rc_setup == 0 else (1, "")
        elapsed = time.perf_counter() - t0

        passed = (rc_setup == 0 and rc_run == 0)
        status = f"{GREEN}PASS{RESET}" if passed else f"{RED}FAIL{RESET}"
        print(f"{status}  {elapsed:5.1f}s")

        if not passed:
            # Show last 20 lines of output so the error is visible inline.
            lines = (log_setup + log_run).strip().splitlines()
            for line in lines[-20:]:
                print(f"    {YELLOW}{line}{RESET}")

        results.append((name, passed, elapsed, log_setup + log_run))

    # ── Summary ──────────────────────────────────────────────────────────────
    passed_n = sum(1 for _, ok, *_ in results if ok)
    total    = len(results)
    print(f"\n{BOLD}── Summary {'─' * 57}{RESET}")
    for name, ok, elapsed, _ in results:
        icon = f"{GREEN}✅{RESET}" if ok else f"{RED}❌{RESET}"
        print(f"  {icon}  {name:<{W_NAME}} {elapsed:5.1f}s")
    print()

    if passed_n == total:
        print(f"  {GREEN}{BOLD}All {total} examples passed.{RESET}")
    else:
        failed_n = total - passed_n
        print(f"  {RED}{BOLD}{failed_n} of {total} examples failed.{RESET}")

    print()
    return 0 if passed_n == total else 1


if __name__ == "__main__":
    sys.exit(main())
