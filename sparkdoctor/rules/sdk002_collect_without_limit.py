"""
SDK002 — collect() without a preceding limit().

Severity: WARNING
"""
from __future__ import annotations

import ast
from typing import List

from sparkdoctor.lint.base import Category, Diagnostic, Rule, Severity
from sparkdoctor.rules._helpers import find_method_without_limit


class CollectWithoutLimitRule(Rule):
    """Detects .collect() calls without a preceding .limit() in the chain."""

    rule_id = "SDK002"
    severity = Severity.WARNING
    title = "collect() without a preceding limit()"
    category = Category.PERFORMANCE

    _EXPLANATION = (
        "collect() moves the entire DataFrame to the driver. On a large dataset this "
        "causes driver OOM, long GC pauses, and job failure. There is no automatic "
        "protection — Spark will attempt to transfer all rows regardless of size."
    )

    _SUGGESTION = (
        "Add .limit(N) before .collect() to bound the result:\n"
        "  df.limit(10_000).collect()\n"
        "If you genuinely need all rows on the driver, use .toPandas() on a sampled "
        "subset, or write to storage and read locally."
    )

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        return [
            Diagnostic(
                rule_id=self.rule_id,
                severity=self.severity,
                message="collect() called without a preceding limit()",
                explanation=self._EXPLANATION,
                suggestion=self._SUGGESTION,
                line=node.lineno,
                col=node.col_offset,
            )
            for node in find_method_without_limit(tree, "collect")
        ]
