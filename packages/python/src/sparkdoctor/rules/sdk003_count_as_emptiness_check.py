"""
SDK003 — count() used as an emptiness check.

Severity: WARNING
"""

from __future__ import annotations

import ast

from sparkdoctor.lint.base import Diagnostic, Rule


class CountAsEmptinessCheckRule(Rule):
    """Detects df.count() used in comparison expressions."""

    rule_id = "SDK003"

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
        return any(self._is_count_call(comparator) for comparator in node.comparators)

    def _is_count_call(self, node: ast.expr) -> bool:
        """Return True if this node is a zero-argument .count() method call.

        Python's ``list.count(value)`` takes one argument; Spark's ``df.count()``
        takes none.  Requiring zero arguments avoids flagging list.count().
        """
        return (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "count"
            and len(node.args) == 0
            and len(node.keywords) == 0
        )
