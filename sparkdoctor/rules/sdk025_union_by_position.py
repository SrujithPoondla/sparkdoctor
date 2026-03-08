"""
SDK025 — union() instead of unionByName().

Severity: WARNING
"""
from __future__ import annotations

import ast
from typing import List

from sparkdoctor.lint.base import Diagnostic, Rule, Severity
from sparkdoctor.rules._helpers import is_method_call


class UnionByPositionRule(Rule):
    """Detects .union() calls which match columns by position, not name."""

    rule_id = "SDK025"
    severity = Severity.WARNING
    title = "union() matches columns by position, not by name"

    _EXPLANATION = (
        "union() matches columns by ordinal position, not by name. If the two "
        "DataFrames have the same columns in different order, data silently ends "
        "up in the wrong column. This is a common source of data corruption that "
        "passes all type checks when column types happen to match."
    )

    _SUGGESTION = (
        "Use unionByName() instead — it matches columns by name:\n"
        "  result = df1.unionByName(df2)\n"
        "If schemas may differ, use allowMissingColumns:\n"
        "  result = df1.unionByName(df2, allowMissingColumns=True)"
    )

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        diagnostics: list[Diagnostic] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not is_method_call(node, "union"):
                continue
            # Exclude unionAll — it's the same as union but deprecated name
            # We flag both since both match by position
            diagnostics.append(
                Diagnostic(
                    rule_id=self.rule_id,
                    severity=self.severity,
                    message="union() matches columns by position — "
                    "use unionByName() instead",
                    explanation=self._EXPLANATION,
                    suggestion=self._SUGGESTION,
                    line=node.lineno,
                    col=node.col_offset,
                )
            )
        return diagnostics
