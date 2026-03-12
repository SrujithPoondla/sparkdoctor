"""
SDK006 — repartition(1) or coalesce(1) forces single-partition execution.

Severity: ERROR
"""
from __future__ import annotations

import ast

from sparkdoctor.lint.base import Diagnostic, Rule
from sparkdoctor.rules._helpers import find_repartition_coalesce_calls


class RepartitionToOneRule(Rule):
    """Detects repartition(1) or coalesce(1) — forces all data through one task."""

    rule_id = "SDK006"

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        diagnostics: list[Diagnostic] = []
        for node, n in find_repartition_coalesce_calls(tree):
            if n == 1:
                diagnostics.append(
                    Diagnostic(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message="repartition(1) forces all data through a single task",
                        explanation=self._EXPLANATION,
                        suggestion=self._SUGGESTION,
                        line=node.lineno,
                        col=node.col_offset,
                    )
                )
        return diagnostics
