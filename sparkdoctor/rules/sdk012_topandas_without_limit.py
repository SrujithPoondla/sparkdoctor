"""
SDK012 — toPandas() without a preceding limit().

Severity: ERROR
"""
from __future__ import annotations

import ast
from typing import List

from sparkdoctor.lint.base import Category, Diagnostic, Rule, Severity
from sparkdoctor.rules._helpers import find_method_without_limit


class ToPandasWithoutLimitRule(Rule):
    """Detects .toPandas() calls without a preceding .limit() in the chain."""

    rule_id = "SDK012"
    severity = Severity.ERROR
    title = "toPandas() without a preceding limit()"
    category = Category.PERFORMANCE

    _EXPLANATION = (
        "toPandas() collects ALL rows from ALL executors to the driver in a single "
        "operation. On a large DataFrame this causes driver OOM, interrupts concurrent "
        "readers, and can corrupt downstream state. There is no automatic size guard."
    )

    _SUGGESTION = (
        "Add .limit(N) before .toPandas() to bound the result:\n"
        "  pandas_df = spark_df.limit(10_000).toPandas()\n"
        "If you genuinely need the full result as pandas, write to storage first "
        "and read with pandas locally."
    )

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
