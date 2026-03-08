"""
SDK026 — f-string or .format() used in spark.sql() calls.

Severity: WARNING
"""
from __future__ import annotations

import ast
from typing import List

from sparkdoctor.lint.base import Category, Diagnostic, Rule, Severity


class FStringSqlRule(Rule):
    """Detects f-strings or .format() in spark.sql() calls — SQL injection risk."""

    rule_id = "SDK026"
    severity = Severity.WARNING
    title = "Dynamic string in spark.sql() — SQL injection risk"
    category = Category.CORRECTNESS

    _EXPLANATION = (
        "Using f-strings or .format() to build SQL queries prevents Catalyst from "
        "caching query plans (each interpolated string is a unique query) and creates "
        "a SQL injection risk if any interpolated value comes from user input or "
        "external configuration."
    )

    _SUGGESTION = (
        "Use parameterized queries or DataFrame API instead:\n"
        "  # Instead of: spark.sql(f\"SELECT * FROM {table}\")\n"
        "  spark.table(table_name).select(\"*\")\n"
        "  # For filtering with variables:\n"
        "  df.filter(F.col(\"status\") == status_var)\n"
        "If you must use spark.sql(), use parameter markers (Spark 3.4+):\n"
        "  spark.sql(\"SELECT * FROM t WHERE id = :id\", args={\"id\": my_id})"
    )

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
        if node.func.attr != "sql":
            return False
        # Accept any receiver — spark.sql, session.sql, etc.
        return True

    def _is_dynamic_string(self, node: ast.expr) -> bool:
        """Check if the expression is an f-string or .format() call."""
        # f-string: ast.JoinedStr
        if isinstance(node, ast.JoinedStr):
            return True
        # "...".format(...): ast.Call on str.format
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute) and node.func.attr == "format":
                return True
        # "..." % (...): ast.BinOp with Mod
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod):
            return True
        return False
