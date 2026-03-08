"""
SDK017 — select("*") wildcard in production code.

Severity: WARNING
"""
from __future__ import annotations

import ast

from sparkdoctor.lint.base import Diagnostic, Rule, Severity
from sparkdoctor.rules._helpers import is_method_call


class SelectStarRule(Rule):
    """Detects select('*') which reads all columns unnecessarily."""

    rule_id = "SDK017"
    severity = Severity.WARNING
    title = "select(\"*\") reads all columns"

    _EXPLANATION = (
        "select('*') reads every column from the source, defeating column pruning. "
        "On wide tables (hundreds of columns) or columnar formats (Parquet, ORC), "
        "this reads far more data than needed and wastes I/O, memory, and shuffle bytes."
    )

    _SUGGESTION = (
        "List only the columns you need:\n"
        "  df.select('user_id', 'event_type', 'timestamp')\n"
        "If you truly need all columns, use the DataFrame directly without select."
    )

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        diagnostics: list[Diagnostic] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not is_method_call(node, "select"):
                continue
            if self._has_star_arg(node):
                diagnostics.append(
                    Diagnostic(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message='select("*") reads all columns — defeats column pruning',
                        explanation=self._EXPLANATION,
                        suggestion=self._SUGGESTION,
                        line=node.lineno,
                        col=node.col_offset,
                    )
                )
        return diagnostics

    def _has_star_arg(self, node: ast.Call) -> bool:
        """Check if any positional argument is the literal string '*'."""
        for arg in node.args:
            if isinstance(arg, ast.Constant) and arg.value == "*":
                return True
        return False
