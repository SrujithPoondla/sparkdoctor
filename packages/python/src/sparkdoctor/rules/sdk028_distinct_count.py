"""
SDK028 — distinct().count() is a two-pass operation.

Severity: WARNING
"""

from __future__ import annotations

import ast

from sparkdoctor.lint.base import Diagnostic, Rule
from sparkdoctor.rules._helpers import _has_pyspark_import


class DistinctCountRule(Rule):
    """Detects distinct().count() and dropDuplicates().count() chains."""

    rule_id = "SDK028"

    _DEDUP_METHODS = {"distinct", "dropDuplicates"}

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        if not _has_pyspark_import(tree):
            return []

        diagnostics: list[Diagnostic] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not self._is_count_call(node):
                continue
            # Check if the receiver is a distinct()/dropDuplicates() call
            receiver = node.func.value
            if isinstance(receiver, ast.Call) and self._is_dedup_call(receiver):
                diagnostics.append(
                    Diagnostic(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message=(
                            "distinct().count() forces two passes — use countDistinct() instead"
                        ),
                        explanation=self._EXPLANATION,
                        suggestion=self._SUGGESTION,
                        line=receiver.lineno,
                        col=receiver.col_offset,
                    )
                )
        return diagnostics

    def _is_count_call(self, node: ast.Call) -> bool:
        """Check if this is a zero-argument .count() call."""
        return (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "count"
            and len(node.args) == 0
            and len(node.keywords) == 0
        )

    def _is_dedup_call(self, node: ast.Call) -> bool:
        """Check if this is a .distinct() or .dropDuplicates() call."""
        return isinstance(node.func, ast.Attribute) and node.func.attr in self._DEDUP_METHODS
