"""Base classes for SparkDoctor's lint engine: Rule, Diagnostic, Severity."""
from __future__ import annotations

import ast
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, List


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


class Category(str, Enum):
    """Rule category — what kind of issue the rule detects."""

    PERFORMANCE = "performance"
    CORRECTNESS = "correctness"
    STYLE = "style"


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

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        # Skip validation for abstract subclasses
        if getattr(cls, "__abstractmethods__", None):
            return
        for attr in ("rule_id", "severity", "title"):
            if not hasattr(cls, attr) or getattr(cls, attr) is None:
                raise TypeError(
                    f"Rule subclass {cls.__name__} must define class attribute '{attr}'"
                )

    @abstractmethod
    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        """Analyze an AST and return diagnostics. Empty list means no findings."""
        ...
