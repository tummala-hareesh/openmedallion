"""openmedallion.explore — HTML report generation from gold Parquet outputs.

Two report types are supported, each backed by an optional extra:

    report_type: profile   →  ydata-profiling (pip install "openmedallion[profile]")
    report_type: walker    →  pygwalker        (pip install "openmedallion[explore]")

Both are imported lazily inside their respective generator functions so that
the core package imports cleanly without either extra installed.
"""
from openmedallion.explore.profile import generate_profile
from openmedallion.explore.walker  import generate_walker

__all__ = ["generate_profile", "generate_walker"]
