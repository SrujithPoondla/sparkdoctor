"""
SDK026 — f-string or .format() used in spark.sql() calls.

Severity: WARNING
"""

from __future__ import annotations

import ast

from sparkdoctor.lint.base import Diagnostic, Rule


class FStringSqlRule(Rule):
    """Detects f-strings or .format() in spark.sql() calls — SQL injection risk."""

    rule_id = "SDK026"

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        diagnostics: list[Diagnostic] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not self._is_spark_sql_call(node):
                continue
            if node.args and self._is_dynamic_string(node.args[0]):
                diagnostics.append(
                    Diagnostic(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message="spark.sql() called with dynamic string — "
                        "SQL injection risk and prevents plan caching",
                        explanation=self._EXPLANATION,
                        suggestion=self._SUGGESTION,
                        line=node.lineno,
                        col=node.col_offset,
                    )
                )
        return diagnostics

    def _is_spark_sql_call(self, node: ast.Call) -> bool:
        """Check if this is a spark.sql(...) call."""
        if not isinstance(node.func, ast.Attribute):
            return False
        return node.func.attr == "sql"

    def _is_dynamic_string(self, node: ast.expr) -> bool:
        """Check if the expression is an f-string or .format() call."""
        # f-string: ast.JoinedStr
        if isinstance(node, ast.JoinedStr):
            return True
        # "...".format(...): ast.Call on str.format
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "format"
        ):
            return True
        # "..." % (...): ast.BinOp with Mod
        return isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod)
