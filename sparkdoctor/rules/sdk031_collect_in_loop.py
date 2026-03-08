"""
SDK031 — collect() or toPandas() called inside a loop.

Severity: ERROR
"""
from __future__ import annotations

import ast

from sparkdoctor.lint.base import Category, Diagnostic, Rule, Severity
from sparkdoctor.rules._helpers import is_method_call


class CollectInLoopRule(Rule):
    """Detects collect() or toPandas() inside for/while loops."""

    rule_id = "SDK031"
    severity = Severity.ERROR
    title = "collect() or toPandas() inside a loop"
    category = Category.PERFORMANCE

    _EXPLANATION = (
        "Calling collect() or toPandas() inside a loop triggers a full Spark action "
        "on every iteration, pulling data to the driver repeatedly. This causes "
        "O(N) full-dataset scans and can OOM the driver on any iteration."
    )

    _SUGGESTION = (
        "Collect the data once before the loop, or restructure the logic to use "
        "DataFrame operations (groupBy, pivot, window functions) instead of "
        "iterating and collecting per-group."
    )

    _METHODS = {"collect", "toPandas"}

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        diagnostics: list[Diagnostic] = []
        seen: set[int] = set()
        self._walk_loops(tree, diagnostics, seen)
        return diagnostics

    def _walk_loops(
        self, tree: ast.AST, diagnostics: list[Diagnostic], seen: set[int]
    ) -> None:
        for node in ast.walk(tree):
            if isinstance(node, (ast.For, ast.While)):
                self._check_loop_body(node, diagnostics, seen)

    def _check_loop_body(
        self, loop_node: ast.AST, diagnostics: list[Diagnostic], seen: set[int]
    ) -> None:
        for node in ast.walk(loop_node):
            if node is loop_node:
                continue
            if not isinstance(node, ast.Call):
                continue
            if id(node) in seen:
                continue
            if not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr in self._METHODS:
                seen.add(id(node))
                diagnostics.append(
                    Diagnostic(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message=f"{node.func.attr}() called inside a loop",
                        explanation=self._EXPLANATION,
                        suggestion=self._SUGGESTION,
                        line=node.lineno,
                        col=node.col_offset,
                    )
                )
