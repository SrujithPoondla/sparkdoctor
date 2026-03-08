"""
SDK001 — Hardcoded repartition or coalesce count.

Severity: WARNING
"""
from __future__ import annotations

import ast
from typing import List

from sparkdoctor.lint.base import Category, Diagnostic, Rule, Severity
from sparkdoctor.rules._helpers import find_repartition_coalesce_calls


class HardcodedRepartitionRule(Rule):
    """Detects repartition(N) or coalesce(N) with a hardcoded integer > 1."""

    rule_id = "SDK001"
    severity = Severity.WARNING
    title = "Hardcoded repartition count"
    category = Category.PERFORMANCE

    _EXPLANATION = (
        "Partition counts hardcoded at development time become wrong as data grows. "
        "A job tuned for 50 GB creates undersized or oversized partitions when data "
        "changes. The count that was optimal last month may cause spill to disk or "
        "underutilization today."
    )

    _SUGGESTION = (
        "Remove the hardcoded count and let AQE manage partitioning automatically "
        "(requires spark.sql.adaptive.enabled=true, which is the default in Spark 3.2+). "
        "If you need explicit control, derive the count from data size:\n"
        "  num_partitions = max(df.rdd.getNumPartitions(), estimated_rows // 1_000_000)"
    )

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
