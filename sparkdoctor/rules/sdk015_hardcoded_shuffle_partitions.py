"""
SDK015 — Hardcoded spark.sql.shuffle.partitions.

Severity: WARNING
"""
from __future__ import annotations

import ast

from sparkdoctor.lint.base import Diagnostic, Rule, Severity


class HardcodedShufflePartitionsRule(Rule):
    """Detects hardcoded spark.sql.shuffle.partitions configuration."""

    rule_id = "SDK015"
    severity = Severity.WARNING
    title = "Hardcoded spark.sql.shuffle.partitions"

    _EXPLANATION = (
        "Setting spark.sql.shuffle.partitions to a fixed value (commonly 200, the "
        "old default) overrides AQE's ability to dynamically size shuffle partitions. "
        "A count tuned for one data size becomes wrong as data grows or shrinks."
    )

    _SUGGESTION = (
        "Remove the hardcoded setting and let AQE handle it automatically "
        "(spark.sql.adaptive.enabled=true is the default in Spark 3.2+). "
        "AQE will coalesce small partitions and split large ones at runtime.\n"
        "If you must set it, derive from data size rather than hardcoding."
    )

    _CONFIG_KEY = "spark.sql.shuffle.partitions"

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        diagnostics: list[Diagnostic] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr not in ("set", "config"):
                continue
            if self._sets_shuffle_partitions(node):
                diagnostics.append(
                    Diagnostic(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message="Hardcoded spark.sql.shuffle.partitions overrides AQE",
                        explanation=self._EXPLANATION,
                        suggestion=self._SUGGESTION,
                        line=node.lineno,
                        col=node.col_offset,
                    )
                )
        return diagnostics

    def _sets_shuffle_partitions(self, node: ast.Call) -> bool:
        """Check if this call sets spark.sql.shuffle.partitions to a literal value."""
        if len(node.args) < 2:
            return False
        first_arg = node.args[0]
        if not isinstance(first_arg, ast.Constant):
            return False
        if first_arg.value != self._CONFIG_KEY:
            return False
        # Second arg must be a literal (hardcoded value)
        second_arg = node.args[1]
        return isinstance(second_arg, ast.Constant)
