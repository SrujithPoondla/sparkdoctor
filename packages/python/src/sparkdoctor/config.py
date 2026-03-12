"""Configuration loading from pyproject.toml [tool.sparkdoctor] section."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Try tomllib (3.11+), fall back to tomli, fall back to None
try:
    import tomllib  # type: ignore[import-not-found]
except ModuleNotFoundError:
    try:
        import tomli as tomllib  # type: ignore[no-redef]
    except ModuleNotFoundError:
        tomllib = None  # type: ignore[assignment]


@dataclass(frozen=True)
class SparkDoctorConfig:
    """Configuration parsed from ``[tool.sparkdoctor]`` in ``pyproject.toml``."""

    disable: set[str] = field(default_factory=set)
    severity_overrides: dict[str, str] = field(default_factory=dict)
    exclude: tuple[str, ...] = ()

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SparkDoctorConfig:
        """Build config from the ``[tool.sparkdoctor]`` dict."""
        disable = set(data.get("disable", []))
        severity_overrides = dict(data.get("severity_overrides", {}))
        exclude = tuple(data.get("exclude", []))
        return cls(
            disable=disable,
            severity_overrides=severity_overrides,
            exclude=exclude,
        )


def load_config(start_dir: Path | None = None) -> SparkDoctorConfig:
    """Find and load ``[tool.sparkdoctor]`` from the nearest ``pyproject.toml``.

    Searches *start_dir* and its parents. Returns default config if no
    ``pyproject.toml`` is found or if the section is missing.
    """
    if tomllib is None:
        return SparkDoctorConfig()

    pyproject = _find_pyproject(start_dir or Path.cwd())
    if pyproject is None:
        return SparkDoctorConfig()

    try:
        with open(pyproject, "rb") as f:
            data = tomllib.load(f)
    except Exception as exc:
        logger.warning("could not parse %s: %s", pyproject, exc)
        return SparkDoctorConfig()

    tool_section = data.get("tool", {}).get("sparkdoctor", {})
    if not tool_section:
        return SparkDoctorConfig()

    return SparkDoctorConfig.from_dict(tool_section)


def _find_pyproject(start: Path) -> Path | None:
    """Walk up from *start* looking for ``pyproject.toml``."""
    current = start.resolve()
    for parent in [current, *current.parents]:
        candidate = parent / "pyproject.toml"
        if candidate.is_file():
            return candidate
    return None
