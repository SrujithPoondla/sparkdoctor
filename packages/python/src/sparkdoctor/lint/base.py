"""Base classes for SparkDoctor's lint engine: Rule, Diagnostic, Severity."""
from __future__ import annotations

import ast
import logging
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any

try:
    import yaml as _yaml
except ImportError:
    _yaml = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


class Severity(str, Enum):
    """Diagnostic severity levels, ordered from most to least severe."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"

    def __ge__(self, other: Severity) -> bool:
        return _SEVERITY_ORDER[self] >= _SEVERITY_ORDER[other]

    def __gt__(self, other: Severity) -> bool:
        return _SEVERITY_ORDER[self] > _SEVERITY_ORDER[other]

    def __le__(self, other: Severity) -> bool:
        return not self.__gt__(other)

    def __lt__(self, other: Severity) -> bool:
        return not self.__ge__(other)


# Defined after the class since Enum members aren't available during class body
_SEVERITY_ORDER = {Severity.ERROR: 2, Severity.WARNING: 1, Severity.INFO: 0}

_SEVERITY_MAP = {"error": Severity.ERROR, "warning": Severity.WARNING, "info": Severity.INFO}


class Category(str, Enum):
    """Rule category — what kind of issue the rule detects."""

    PERFORMANCE = "performance"
    CORRECTNESS = "correctness"
    STYLE = "style"


_CATEGORY_MAP = {
    "performance": Category.PERFORMANCE,
    "correctness": Category.CORRECTNESS,
    "style": Category.STYLE,
}


# ---------------------------------------------------------------------------
# Rules YAML loader
# ---------------------------------------------------------------------------

_rules_spec: dict[str, dict] | None = None


def _find_rules_yaml() -> Path | None:
    """Walk up from this file to find core/rules.yaml."""
    current = Path(__file__).resolve().parent
    for _ in range(10):
        candidate = current / "core" / "rules.yaml"
        if candidate.exists():
            return candidate
        current = current.parent
    return None


def _load_rules_spec() -> dict[str, dict]:
    """Load and cache the rules spec from core/rules.yaml."""
    global _rules_spec
    if _rules_spec is not None:
        return _rules_spec

    yaml_path = _find_rules_yaml()
    if yaml_path is None or _yaml is None:
        if _yaml is None:
            logger.debug("PyYAML not installed — rules metadata loaded from class attributes")
        else:
            logger.debug("core/rules.yaml not found — rules metadata loaded from class attributes")
        _rules_spec = {}
        return _rules_spec

    with open(yaml_path) as f:
        data = _yaml.safe_load(f)

    _rules_spec = data.get("rules", {})
    return _rules_spec


def _resolve_text(value: str | dict, language: str) -> str:
    """Resolve a text field that may be a string or a per-language map."""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return value.get(language, value.get("default", ""))
    return ""


@dataclass(frozen=True)
class Diagnostic:
    """A single finding from a lint rule."""

    rule_id: str
    severity: Severity
    message: str
    explanation: str
    suggestion: str
    line: int
    col: int
    filename: str = ""

    def as_dict(self) -> dict:
        """Return a JSON-serializable dictionary."""
        result = asdict(self)
        result["severity"] = self.severity.value
        return result


class Parser(ABC):
    """Base class for language parsers.

    A parser converts source code into a tree that rules can analyze.
    The built-in PythonParser uses ``ast.parse``. Language plugins
    (e.g. ``sparkdoctor-scala``) provide their own parser + tree type.
    """

    language: str  # e.g. "python", "scala"
    file_extensions: tuple[str, ...]  # e.g. (".py",), (".scala",)

    @abstractmethod
    def parse(self, source: str, filename: str = "") -> Any:
        """Parse source code and return a tree object."""
        ...


class PythonParser(Parser):
    """Built-in parser for Python source files using the ``ast`` module."""

    language = "python"
    file_extensions = (".py",)

    def parse(self, source: str, filename: str = "") -> ast.AST:
        return ast.parse(source, filename=filename)


class Rule(ABC):
    """Base class for all SparkDoctor lint rules.

    Subclasses only need to define ``rule_id`` and ``check()``.
    Metadata (severity, title, category, explanation, suggestion) is loaded
    automatically from ``core/rules.yaml``.

    Attributes:
        rule_id: Unique identifier (e.g. "SDK001").
        severity: How severe the finding is.
        title: Short human-readable title.
        category: What kind of issue this rule detects.
        language: Which language this rule analyzes (default: "python").
        api_version: Rule API version for plugin compatibility checking.
    """

    rule_id: str
    severity: Severity
    title: str
    category: Category = Category.PERFORMANCE
    language: str = "python"
    api_version: int = 1

    _EXPLANATION: str = ""
    _SUGGESTION: str = ""

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        # Skip validation for abstract subclasses
        if getattr(cls, "__abstractmethods__", None):
            return

        rule_id = getattr(cls, "rule_id", None)
        if not rule_id:
            raise TypeError(
                f"Rule subclass {cls.__name__} must define class attribute 'rule_id'"
            )

        # Load metadata from rules.yaml if available
        spec = _load_rules_spec().get(rule_id)
        if spec:
            lang = getattr(cls, "language", "python")
            if not hasattr(cls, "severity") or cls.__dict__.get("severity") is None:
                cls.severity = _SEVERITY_MAP[spec["severity"]]
            if not hasattr(cls, "title") or cls.__dict__.get("title") is None:
                cls.title = spec["title"]
            if "category" not in cls.__dict__:
                cls.category = _CATEGORY_MAP[spec["category"]]
            if "_EXPLANATION" not in cls.__dict__:
                cls._EXPLANATION = _resolve_text(spec.get("explanation", ""), lang)
            if "_SUGGESTION" not in cls.__dict__:
                cls._SUGGESTION = _resolve_text(spec.get("suggestion", ""), lang)

        # Validate required attributes are set (either from YAML or class)
        for attr in ("severity", "title"):
            if not hasattr(cls, attr) or getattr(cls, attr) is None:
                raise TypeError(
                    f"Rule subclass {cls.__name__} must define class attribute '{attr}' "
                    f"(or add {rule_id} to core/rules.yaml)"
                )

    @abstractmethod
    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        """Analyze an AST and return diagnostics. Empty list means no findings."""
        ...
