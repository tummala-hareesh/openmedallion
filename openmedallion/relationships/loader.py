"""relationships/loader.py — load a project's relationships.yaml.

No overlay/merge step — unlike ``metadata/loader.py``, there is no
``relationships_enhancements.yaml`` (locked design decision: detection is
deterministic, not LLM-drafted, so there's no regeneration-clobber risk to
guard against). Hand-edits go directly into ``relationships.yaml``.
"""
from pathlib import Path

import yaml
from pydantic import ValidationError

from openmedallion.config.errors import format_validation_error
from openmedallion.relationships.schema import RelationshipsConfig


def _validate_relationships(cfg: dict) -> RelationshipsConfig:
    """Build a :class:`RelationshipsConfig` from a relationships dict.

    Raises:
        ValueError: with a human-readable message identifying the failing
            key path, matching the ``[relationships] path: message`` format
            used by ``config/validator.py``'s ``[config]`` equivalent.
    """
    try:
        return RelationshipsConfig(**cfg)
    except ValidationError as e:
        raise ValueError(format_validation_error(e, "relationships")) from None


def load_relationships(project: str, projects_root: str | Path = "projects") -> RelationshipsConfig:
    """Load a project's relationships.yaml.

    ``relationships.yaml`` is optional — a project without one returns an
    empty :class:`RelationshipsConfig`, so this can be called safely before
    ``medallion relationships generate`` has ever been run.

    Args:
        project: Project name — folder name under ``projects_root``.
        projects_root: Parent directory containing project folders.

    Returns:
        RelationshipsConfig: The validated relationships model.

    Raises:
        ValueError: If the file fails schema validation.
    """
    root = Path(projects_root) / project
    rel_path = root / "relationships.yaml"

    data: dict = {}
    if rel_path.exists():
        with open(rel_path) as f:
            data = yaml.safe_load(f) or {}

    return _validate_relationships(data)
