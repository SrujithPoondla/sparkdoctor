"""Base classes for SparkDoctor's lint engine: Rule, Diagnostic, Severity."""
from __future__ import annotations

import ast
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import List


class Severity(str, Enum):
    """Diagnostic severity levels, ordered from most to least severe."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"

    def __ge__(self, other: Severity) -> bool:
        order = {Severity.ERROR: 2, Severity.WARNING: 1, Severity.INFO: 0}
        return order[self] >= order[other]

    def __gt__(self, other: Severity) -> bool:
        order = {Severity.ERROR: 2, Severity.WARNING: 1, Severity.INFO: 0}
        return order[self] > order[other]

    def __le__(self, other: Severity) -> bool:
        return not self.__gt__(other)

    def __lt__(self, other: Severity) -> bool:
        return not self.__ge__(other)


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


class Rule(ABC):
    """Base class for all SparkDoctor lint rules."""

    rule_id: str
    severity: Severity
    title: str

    @abstractmethod
    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        """Analyze an AST and return diagnostics. Empty list means no findings."""
        ...
