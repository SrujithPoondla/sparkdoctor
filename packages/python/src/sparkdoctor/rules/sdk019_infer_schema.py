"""
SDK019 — inferSchema=True in production read (CSV/JSON).

Severity: WARNING
"""

from __future__ import annotations

import ast

from sparkdoctor.lint.base import Diagnostic, Rule


class InferSchemaRule(Rule):
    """Detects inferSchema=True in CSV/JSON read calls."""

    rule_id = "SDK019"

    _READ_METHODS = {"csv", "json", "load", "text", "orc"}

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        diagnostics: list[Diagnostic] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not self._is_read_call(node):
                continue
            if self._has_infer_schema_true(node):
                diagnostics.append(
                    Diagnostic(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message="inferSchema=True causes an extra data scan and unstable types",
                        explanation=self._EXPLANATION,
                        suggestion=self._SUGGESTION,
                        line=node.lineno,
                        col=node.col_offset,
                    )
                )
        return diagnostics

    def _is_read_call(self, node: ast.Call) -> bool:
        """Check if this is a Spark-style read call (.csv(), .json(), .load(), etc)."""
        if not isinstance(node.func, ast.Attribute):
            return False
        return node.func.attr in self._READ_METHODS

    def _has_infer_schema_true(self, node: ast.Call) -> bool:
        """Check if any keyword argument is inferSchema=True."""
        for kw in node.keywords:
            if (
                kw.arg == "inferSchema"
                and isinstance(kw.value, ast.Constant)
                and kw.value.value is True
            ):
                return True
        return False
