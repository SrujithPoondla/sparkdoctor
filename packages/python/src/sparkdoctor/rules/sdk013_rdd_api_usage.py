"""
SDK013 — RDD API usage on DataFrame.

Severity: ERROR
"""
from __future__ import annotations

import ast

from sparkdoctor.lint.base import Diagnostic, Rule


class RddApiUsageRule(Rule):
    """Detects .rdd access on a DataFrame, disabling Catalyst optimization."""

    rule_id = "SDK013"

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        diagnostics: list[Diagnostic] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Attribute):
                continue
            if node.attr == "rdd":
                diagnostics.append(
                    Diagnostic(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message=".rdd access converts DataFrame to RDD — "
                        "bypasses Catalyst optimizer",
                        explanation=self._EXPLANATION,
                        suggestion=self._SUGGESTION,
                        line=node.lineno,
                        col=node.col_offset,
                    )
                )
        return diagnostics
