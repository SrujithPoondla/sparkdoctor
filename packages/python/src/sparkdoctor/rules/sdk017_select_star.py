"""
SDK017 — select("*") wildcard in production code.

Severity: WARNING
"""
from __future__ import annotations

import ast

from sparkdoctor.lint.base import Diagnostic, Rule
from sparkdoctor.rules._helpers import is_method_call


class SelectStarRule(Rule):
    """Detects select('*') which reads all columns unnecessarily."""

    rule_id = "SDK017"

    # Sub-accessor attributes that indicate a non-Spark select call
    # (e.g. Optimus: df.cols.select("*"), df.rows.select("*"))
    _NON_DF_ACCESSORS = {"cols", "rows", "ml", "plot", "outliers", "encoding"}

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        diagnostics: list[Diagnostic] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not is_method_call(node, "select"):
                continue
            # Skip sub-accessor patterns: df.cols.select("*") is not Spark's select
            if self._is_sub_accessor_call(node):
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

    def _is_sub_accessor_call(self, node: ast.Call) -> bool:
        """Return True if select is called via a sub-accessor (e.g. df.cols.select)."""
        receiver = node.func.value
        return (
            isinstance(receiver, ast.Attribute)
            and receiver.attr in self._NON_DF_ACCESSORS
        )

    def _has_star_arg(self, node: ast.Call) -> bool:
        """Check if any positional argument is the literal string '*'."""
        return any(isinstance(arg, ast.Constant) and arg.value == "*" for arg in node.args)
