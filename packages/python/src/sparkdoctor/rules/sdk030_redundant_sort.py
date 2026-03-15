"""
SDK030 — Multiple orderBy()/sort() in a chain — only the last one matters.

Severity: WARNING
"""

from __future__ import annotations

import ast

from sparkdoctor.lint.base import Diagnostic, Rule
from sparkdoctor.rules._helpers import _has_pyspark_import


class RedundantSortRule(Rule):
    """Detects multiple orderBy()/sort() calls in the same method chain."""

    rule_id = "SDK030"

    _SORT_METHODS = {"orderBy", "sort"}

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        if not _has_pyspark_import(tree):
            return []

        diagnostics: list[Diagnostic] = []
        # Track nodes we've already visited as part of a chain to avoid
        # reporting the same chain multiple times.
        visited: set[int] = set()

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if id(node) in visited:
                continue
            # Walk the chain from this node (outermost) down to the root,
            # collecting all sort/orderBy call nodes.
            sort_calls = self._collect_sort_calls(node, visited)
            if len(sort_calls) < 2:
                continue
            # Flag all but the last (outermost) sort — the earlier ones
            # are redundant. sort_calls is ordered root-to-tip, so flag
            # all except the last element.
            for redundant in sort_calls[:-1]:
                diagnostics.append(
                    Diagnostic(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message=(
                            "Redundant sort — this orderBy()/sort() is overridden "
                            "by a later sort in the same chain"
                        ),
                        explanation=self._EXPLANATION,
                        suggestion=self._SUGGESTION,
                        line=redundant.lineno,
                        col=redundant.col_offset,
                    )
                )
        return diagnostics

    def _collect_sort_calls(self, node: ast.AST, visited: set[int]) -> list[ast.Call]:
        """Walk a method-call chain and return all sort/orderBy Call nodes.

        Returns them in root-to-tip order (earliest call first).
        Does NOT count sortWithinPartitions.
        """
        sort_calls: list[ast.Call] = []
        current = node
        while True:
            if isinstance(current, ast.Call):
                visited.add(id(current))
                if self._is_sort_call(current):
                    sort_calls.append(current)
                # Move to the receiver of this call
                if isinstance(current.func, ast.Attribute):
                    current = current.func.value
                else:
                    break
            elif isinstance(current, ast.Attribute):
                # Attribute access without call (e.g., .write)
                current = current.value
            else:
                break
        # Reverse so root (earliest) is first
        sort_calls.reverse()
        return sort_calls

    def _is_sort_call(self, node: ast.Call) -> bool:
        """Check if this is a .orderBy() or .sort() call (not sortWithinPartitions)."""
        return isinstance(node.func, ast.Attribute) and node.func.attr in self._SORT_METHODS
