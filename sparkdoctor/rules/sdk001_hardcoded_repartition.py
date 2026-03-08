"""
SDK001 — Hardcoded repartition or coalesce count.

Severity: WARNING
"""
from __future__ import annotations

import ast
from typing import List

from sparkdoctor.lint.base import Diagnostic, Rule, Severity
from sparkdoctor.rules._helpers import first_arg_int, is_method_call


class HardcodedRepartitionRule(Rule):
    """Detects repartition(N) or coalesce(N) with a hardcoded integer > 1."""

    rule_id = "SDK001"
    severity = Severity.WARNING
    title = "Hardcoded repartition count"

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

    _METHODS = {"repartition", "coalesce"}

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        diagnostics: list[Diagnostic] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr not in self._METHODS:
                continue
            n = first_arg_int(node)
            if n is not None and n > 1:
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
