"""
SDK013 — RDD API usage on DataFrame.

Severity: ERROR
"""
from __future__ import annotations

import ast
from typing import List

from sparkdoctor.lint.base import Category, Diagnostic, Rule, Severity


class RddApiUsageRule(Rule):
    """Detects .rdd access on a DataFrame, disabling Catalyst optimization."""

    rule_id = "SDK013"
    severity = Severity.ERROR
    title = "RDD API used on DataFrame"
    category = Category.PERFORMANCE

    _EXPLANATION = (
        "The RDD API forces Spark to serialize every row through Python's pickle "
        "format — 10-100x slower than DataFrame operations that stay in the JVM. "
        "Catalyst cannot optimize through RDD operations, so query plans are less "
        "efficient and Tungsten memory management is bypassed."
    )

    _SUGGESTION = (
        "Replace RDD operations with DataFrame API equivalents:\n"
        "  # Instead of: df.rdd.map(lambda row: ...)\n"
        "  df.withColumn(\"new_col\", F.col(\"value\") * 2)\n"
        "If you need partition-level Python, use mapInPandas (Arrow-backed):\n"
        "  df.mapInPandas(transform_fn, schema=output_schema)"
    )

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
