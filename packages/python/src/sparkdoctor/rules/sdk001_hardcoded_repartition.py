"""
SDK001 — Hardcoded repartition or coalesce count.

Severity: WARNING
"""
from __future__ import annotations

import ast

from sparkdoctor.lint.base import Diagnostic, Rule
from sparkdoctor.rules._helpers import find_repartition_coalesce_calls


class HardcodedRepartitionRule(Rule):
    """Detects repartition(N) or coalesce(N) with a hardcoded integer > 1."""

    rule_id = "SDK001"

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        diagnostics: list[Diagnostic] = []
        for node, n in find_repartition_coalesce_calls(tree):
            if n > 1:
                diagnostics.append(
                    Diagnostic(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message=f"Hardcoded repartition count: {n}",
                        explanation=self._EXPLANATION,
                        suggestion=self._SUGGESTION,
                        line=node.lineno,
                        col=node.col_offset,
                    )
                )
        return diagnostics
