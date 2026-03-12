"""
SDK004 — withColumn() called inside a loop.

Severity: ERROR
"""

from __future__ import annotations

import ast

from sparkdoctor.lint.base import Diagnostic, Rule
from sparkdoctor.rules._helpers import is_method_call


class WithColumnInLoopRule(Rule):
    """Detects .withColumn() calls inside for or while loop bodies."""

    rule_id = "SDK004"

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        diagnostics: list[Diagnostic] = []
        for node in ast.walk(tree):
            if not isinstance(node, (ast.For, ast.While)):
                continue
            # Walk only within the loop body (and orelse)
            loop_body = ast.Module(body=node.body + node.orelse, type_ignores=[])
            has_with_column = False
            for child in ast.walk(loop_body):
                if isinstance(child, ast.Call) and is_method_call(child, "withColumn"):
                    has_with_column = True
                    break
            if has_with_column:
                diagnostics.append(
                    Diagnostic(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message="withColumn() called inside a loop",
                        explanation=self._EXPLANATION,
                        suggestion=self._SUGGESTION,
                        line=node.lineno,
                        col=node.col_offset,
                    )
                )
        return diagnostics
