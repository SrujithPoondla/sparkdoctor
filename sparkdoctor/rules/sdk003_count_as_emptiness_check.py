"""
SDK003 — count() used as an emptiness check.

Severity: WARNING
"""
from __future__ import annotations

import ast
from typing import List

from sparkdoctor.lint.base import Diagnostic, Rule, Severity


class CountAsEmptinessCheckRule(Rule):
    """Detects df.count() used in comparison expressions."""

    rule_id = "SDK003"
    severity = Severity.WARNING
    title = "count() used as an emptiness check"

    _EXPLANATION = (
        "count() forces a full scan of the entire dataset to return a single number. "
        "Using it to answer a yes/no question (is this DataFrame empty?) wastes all "
        "that compute. On a 10 TB table, you scan 10 TB to learn the answer is \"no\"."
    )

    _SUGGESTION = (
        "Use isEmpty() or limit(1) instead:\n"
        "  if df.isEmpty():          # short-circuits after finding the first row\n"
        "      ...\n"
        "  if df.limit(1).count() == 0:   # scans at most 1 row\n"
        "      ...\n"
        "Note: isEmpty() is available in Spark 2.4+."
    )

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        diagnostics: list[Diagnostic] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Compare):
                continue
            if self._has_count_call(node):
                diagnostics.append(
                    Diagnostic(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message="count() used to check if DataFrame is empty",
                        explanation=self._EXPLANATION,
                        suggestion=self._SUGGESTION,
                        line=node.lineno,
                        col=node.col_offset,
                    )
                )
        return diagnostics

    def _has_count_call(self, node: ast.Compare) -> bool:
        """Return True if the Compare contains a .count() call."""
        if self._is_count_call(node.left):
            return True
        for comparator in node.comparators:
            if self._is_count_call(comparator):
                return True
        return False

    def _is_count_call(self, node: ast.expr) -> bool:
        """Return True if this node is a .count() method call."""
        return (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "count"
        )
