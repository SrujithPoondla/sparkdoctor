"""
SDK006 — repartition(1) or coalesce(1) forces single-partition execution.

Severity: ERROR
"""
from __future__ import annotations

import ast
from typing import List

from sparkdoctor.lint.base import Category, Diagnostic, Rule, Severity
from sparkdoctor.rules._helpers import find_repartition_coalesce_calls


class RepartitionToOneRule(Rule):
    """Detects repartition(1) or coalesce(1) — forces all data through one task."""

    rule_id = "SDK006"
    severity = Severity.ERROR
    title = "repartition(1) or coalesce(1) — forces single-partition execution"
    category = Category.PERFORMANCE

    _EXPLANATION = (
        "repartition(1) or coalesce(1) collapses the entire distributed dataset into "
        "a single partition, processed by a single executor core. This eliminates all "
        "parallelism. On a 100 GB dataset, one task processes all 100 GB sequentially. "
        "Memory pressure causes spill to disk. Runtime scales linearly with data size."
    )

    _SUGGESTION = (
        "If you need to write a single output file:\n"
        "  df.write.option(\"maxRecordsPerFile\", 1_000_000).parquet(path)\n"
        "  # or\n"
        "  df.coalesce(1).write.parquet(path)  # acceptable only for small DataFrames "
        "< 1 GB\n"
        "If you genuinely need one partition for downstream logic, document why "
        "explicitly. Most cases that use repartition(1) in production should not."
    )

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
