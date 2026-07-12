"""metadata/loader.py — load a project's metadata.yaml + metadata_enhancements.yaml.

``metadata.yaml`` is LLM-generated (``medallion metadata generate``, not yet
built) and safe to regenerate at any time. ``metadata_enhancements.yaml`` is
user-maintained and deep-merges on top of it at load time — using the same
``_deep_merge`` semantics as ``config/loader.py`` — so hand-written
descriptions, synonyms, and glossary entries survive a regeneration of
``metadata.yaml`` untouched.
"""
from pathlib import Path

import yaml
from pydantic import ValidationError

from openmedallion.config.errors import format_validation_error
from openmedallion.config.loader import _deep_merge
from openmedallion.metadata.schema import MetadataConfig


def _validate_metadata(cfg: dict) -> MetadataConfig:
    """Build a :class:`MetadataConfig` from a merged metadata dict.

    Raises:
        ValueError: with a human-readable message identifying the failing
            key path, matching the ``[metadata] path: message`` format used
            by ``config/validator.py``'s ``[config]`` equivalent.
    """
    try:
        return MetadataConfig(**cfg)
    except ValidationError as e:
        raise ValueError(format_validation_error(e, "metadata")) from None


def load_metadata(project: str, projects_root: str | Path = "projects") -> MetadataConfig:
    """Load and merge a project's metadata.yaml + metadata_enhancements.yaml.

    Both files are optional — a project with neither returns an empty
    :class:`MetadataConfig` (no tables, no glossary), so this can be called
    safely before ``medallion metadata generate`` has ever been run.

    Args:
        project: Project name — folder name under ``projects_root``.
        projects_root: Parent directory containing project folders.

    Returns:
        MetadataConfig: The merged, validated metadata model.

    Raises:
        ValueError: If the merged metadata fails schema validation.
    """
    root = Path(projects_root) / project

    data: dict = {}
    meta_path = root / "metadata.yaml"
    if meta_path.exists():
        with open(meta_path) as f:
            data = yaml.safe_load(f) or {}

    enh_path = root / "metadata_enhancements.yaml"
    if enh_path.exists():
        with open(enh_path) as f:
            enhancements = yaml.safe_load(f) or {}
        _deep_merge(data, enhancements)

    return _validate_metadata(data)
