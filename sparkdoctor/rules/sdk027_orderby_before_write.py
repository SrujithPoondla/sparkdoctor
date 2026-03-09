"""
SDK027 — orderBy() or sort() immediately before write.

Severity: WARNING
"""
from __future__ import annotations

import ast

from sparkdoctor.lint.base import Category, Diagnostic, Rule, Severity


class OrderByBeforeWriteRule(Rule):
    """Detects orderBy()/sort() immediately before a write operation."""

    rule_id = "SDK027"
    severity = Severity.WARNING
    title = "orderBy()/sort() before write is wasteful"
    category = Category.PERFORMANCE

    _EXPLANATION = (
        "orderBy() and sort() trigger a full global sort (shuffle + sort), which is "
        "one of the most expensive Spark operations. When writing to storage, the "
        "sort order is only preserved within each partition file — readers don't see "
        "a globally sorted dataset. The expensive sort is wasted."
    )

    _SUGGESTION = (
        "If you need sorted output files for read performance, use sortWithinPartitions():\n"
        "  df.sortWithinPartitions('date').write.parquet('path')\n"
        "This sorts within each partition (no global shuffle) and achieves the same "
        "benefit for predicate pushdown and min/max pruning.\n"
        "If you truly need global order, collect the results instead of writing."
    )

    _SORT_METHODS = {"orderBy", "sort"}
    _WRITE_ATTRS = {"write", "writeStream"}

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        diagnostics: list[Diagnostic] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Attribute):
                continue
            if node.attr not in self._WRITE_ATTRS:
                continue
            # Check if the receiver is a sort/orderBy call
            if isinstance(node.value, ast.Call) and self._is_sort_call(node.value):
                diagnostics.append(
                    Diagnostic(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message="orderBy()/sort() before write triggers a wasteful global sort",
                        explanation=self._EXPLANATION,
                        suggestion=self._SUGGESTION,
                        line=node.value.lineno,
                        col=node.value.col_offset,
                    )
                )
        return diagnostics

    def _is_sort_call(self, node: ast.Call) -> bool:
        """Check if this is a .orderBy() or .sort() call."""
        return (
            isinstance(node.func, ast.Attribute)
            and node.func.attr in self._SORT_METHODS
        )
