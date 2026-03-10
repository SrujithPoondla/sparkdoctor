"""
SDK009 — Transformation chain longer than threshold.

Severity: INFO
"""
from __future__ import annotations

import ast

from sparkdoctor.lint.base import Category, Diagnostic, Rule, Severity
from sparkdoctor.rules._helpers import _has_pyspark_import


class ChainLengthRule(Rule):
    """Detects transformation chains longer than 5 method calls."""

    rule_id = "SDK009"
    severity = Severity.INFO
    title = "Long transformation chain without intermediate assignment"
    category = Category.STYLE

    _THRESHOLD = 5

    _EXPLANATION = (
        "Long method chains are hard to debug because there is no intermediate "
        "variable to inspect, hard to extract into reusable functions, and hide "
        "logical groupings. Palantir recommends max 3-5 lines of chaining."
    )

    _SUGGESTION = (
        "Break the chain into named intermediate steps of 4-5 calls each.\n"
        "  # Bad:  df.filter().withColumn().groupBy().agg().filter().orderBy().limit()\n"
        "  # Good: filtered = df.filter().withColumn()\n"
        "  #        result = filtered.groupBy().agg().filter().orderBy().limit()"
    )

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        if not _has_pyspark_import(tree):
            return []

        diagnostics: list[Diagnostic] = []
        seen_chains: set[int] = set()

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Attribute):
                continue

            depth = self._chain_depth(node)
            if depth <= self._THRESHOLD:
                continue

            root_id = self._root_node_id(node)
            if root_id in seen_chains:
                continue
            seen_chains.add(root_id)

            diagnostics.append(
                Diagnostic(
                    rule_id=self.rule_id,
                    severity=self.severity,
                    message=f"Transformation chain has {depth} calls — "
                    f"consider breaking at {self._THRESHOLD}",
                    explanation=self._EXPLANATION,
                    suggestion=self._SUGGESTION,
                    line=node.lineno,
                    col=node.col_offset,
                )
            )
        return diagnostics

    def _chain_depth(self, node: ast.AST) -> int:
        depth = 0
        current = node
        while isinstance(current, ast.Call):
            if isinstance(current.func, ast.Attribute):
                depth += 1
                current = current.func.value
            else:
                break
        return depth

    def _root_node_id(self, node: ast.AST) -> int:
        current = node
        while True:
            if isinstance(current, ast.Call):
                if isinstance(current.func, ast.Attribute):
                    current = current.func.value
                else:
                    return id(current)
            elif isinstance(current, ast.Attribute):
                current = current.value
            else:
                return id(current)
