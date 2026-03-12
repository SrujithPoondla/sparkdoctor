"""
SDK014 — AQE explicitly disabled.

Severity: WARNING
"""

from __future__ import annotations

import ast

from sparkdoctor.lint.base import Diagnostic, Rule
from sparkdoctor.rules._helpers import find_config_set_calls


class AqeDisabledRule(Rule):
    """Detects spark.sql.adaptive.enabled set to false."""

    rule_id = "SDK014"

    _CONFIG_KEY = "spark.sql.adaptive.enabled"
    _FALSE_STRINGS = {"false", "False"}

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        diagnostics: list[Diagnostic] = []
        for node in find_config_set_calls(tree, self._CONFIG_KEY):
            if self._value_is_false(node.args[1]):
                diagnostics.append(
                    Diagnostic(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message="AQE explicitly disabled — removes automatic query optimizations",
                        explanation=self._EXPLANATION,
                        suggestion=self._SUGGESTION,
                        line=node.lineno,
                        col=node.col_offset,
                    )
                )
        return diagnostics

    def _value_is_false(self, node: ast.expr) -> bool:
        """Check if the value node represents a false-like value."""
        if not isinstance(node, ast.Constant):
            return False
        if isinstance(node.value, bool):
            return node.value is False
        return node.value in self._FALSE_STRINGS
