"""
SDK012 — toPandas() without a preceding limit().

Severity: ERROR
"""
from __future__ import annotations

import ast

from sparkdoctor.lint.base import Diagnostic, Rule
from sparkdoctor.rules._helpers import find_method_without_limit


class ToPandasWithoutLimitRule(Rule):
    """Detects .toPandas() calls without a preceding .limit() in the chain."""

    rule_id = "SDK012"

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        return [
            Diagnostic(
                rule_id=self.rule_id,
                severity=self.severity,
                message="toPandas() called without a preceding limit()",
                explanation=self._EXPLANATION,
                suggestion=self._SUGGESTION,
                line=node.lineno,
                col=node.col_offset,
            )
            for node in find_method_without_limit(tree, "toPandas")
        ]
