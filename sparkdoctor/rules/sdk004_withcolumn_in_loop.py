"""
SDK004 — withColumn() called inside a loop.

Severity: ERROR
"""
from __future__ import annotations

import ast

from sparkdoctor.lint.base import Category, Diagnostic, Rule, Severity
from sparkdoctor.rules._helpers import is_method_call


class WithColumnInLoopRule(Rule):
    """Detects .withColumn() calls inside for or while loop bodies."""

    rule_id = "SDK004"
    severity = Severity.ERROR
    title = "withColumn() called inside a loop"
    category = Category.PERFORMANCE

    _EXPLANATION = (
        "Every withColumn() call creates a new DataFrame and adds one projection to "
        "the query plan. Calling it in a loop with 50 columns creates 50 nested "
        "projections. Spark's query plan analyzer must process all 50, causing O(N²) "
        "plan compilation time. At 100+ columns this can take minutes before the job "
        "even starts."
    )

    _SUGGESTION = (
        "Collect all column transformations into a list and call select() once:\n"
        "  from pyspark.sql import functions as F\n"
        "  new_cols = [F.col(c).cast(\"string\").alias(c) for c in columns]\n"
        "  df = df.select(\"*\", *new_cols)"
    )

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        diagnostics: list[Diagnostic] = []
        for node in ast.walk(tree):
            if not isinstance(node, (ast.For, ast.While)):
                continue
            # Walk only within the loop body (and orelse)
            loop_body = ast.Module(
                body=node.body + node.orelse, type_ignores=[]
            )
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
