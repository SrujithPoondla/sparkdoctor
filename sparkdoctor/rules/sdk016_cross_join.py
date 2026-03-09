"""
SDK016 — crossJoin() usage.

Severity: WARNING
"""
from __future__ import annotations

import ast

from sparkdoctor.lint.base import Category, Diagnostic, Rule, Severity
from sparkdoctor.rules._helpers import is_method_call


class CrossJoinRule(Rule):
    """Detects .crossJoin() calls which produce N*M rows."""

    rule_id = "SDK016"
    severity = Severity.WARNING
    title = "crossJoin() produces a cartesian product"
    category = Category.PERFORMANCE

    _EXPLANATION = (
        "A cross join of two tables produces N * M rows. Two 1M-row tables produce "
        "1 trillion rows. This is rarely intentional and will exhaust cluster memory "
        "silently — Spark will not warn you. Most accidental cross joins come from "
        "missing join conditions."
    )

    _SUGGESTION = (
        "If the cross join is intentional (e.g. salting), ensure the right side is "
        "small and broadcast it:\n"
        "  salted = left_df.crossJoin(F.broadcast(small_df))\n"
        "If unintentional, add a proper join condition:\n"
        "  result = left.join(right, on=\"key\", how=\"inner\")"
    )

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        diagnostics: list[Diagnostic] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not is_method_call(node, "crossJoin"):
                continue
            diagnostics.append(
                Diagnostic(
                    rule_id=self.rule_id,
                    severity=self.severity,
                    message="crossJoin() produces a cartesian product — "
                    "verify this is intentional",
                    explanation=self._EXPLANATION,
                    suggestion=self._SUGGESTION,
                    line=node.lineno,
                    col=node.col_offset,
                )
            )
        return diagnostics
