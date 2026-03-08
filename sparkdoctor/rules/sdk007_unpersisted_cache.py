"""
SDK007 — cache() or persist() without corresponding unpersist().

Severity: INFO
"""
from __future__ import annotations

import ast
from typing import List

from sparkdoctor.lint.base import Diagnostic, Rule, Severity
from sparkdoctor.rules._helpers import is_method_call, receiver_name


class UnpersistedCacheRule(Rule):
    """Detects cache()/persist() without a corresponding unpersist() in the file."""

    rule_id = "SDK007"
    severity = Severity.INFO
    title = "cache()/persist() without unpersist()"

    _EXPLANATION = (
        "Spark holds cached DataFrames in executor memory (and optionally disk) until "
        "explicitly released or the SparkSession ends. In long-running jobs or "
        "notebooks, forgetting unpersist() accumulates cached data, consuming memory "
        "that other operations need and potentially causing executor OOM."
    )

    _SUGGESTION = (
        "Release cached DataFrames when they are no longer needed:\n"
        "  df.cache()\n"
        "  # ... operations that benefit from the cache ...\n"
        "  df.unpersist()\n"
        "In notebooks or long jobs, use a try/finally pattern:\n"
        "  df.cache()\n"
        "  try:\n"
        "      process(df)\n"
        "  finally:\n"
        "      df.unpersist()"
    )

    _CACHE_METHODS = {"cache", "persist"}

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        # Track: variable_name -> (cache_method, line, col)
        cached: dict[str, tuple[str, int, int]] = {}
        unpersisted: set[str] = set()

        for node in ast.walk(tree):
            # Handle cache/persist/unpersist method calls
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                method = node.func.attr
                name = receiver_name(node)

                if method in self._CACHE_METHODS:
                    key = name or f"_anon_{node.lineno}_{node.col_offset}"
                    cached[key] = (method, node.lineno, node.col_offset)
                elif method == "unpersist" and name:
                    unpersisted.add(name)

            # Handle assignment: result = df.cache()
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
                if isinstance(node.value.func, ast.Attribute):
                    if node.value.func.attr in self._CACHE_METHODS:
                        for target in node.targets:
                            if isinstance(target, ast.Name):
                                cached[target.id] = (
                                    node.value.func.attr,
                                    node.value.lineno,
                                    node.value.col_offset,
                                )

        diagnostics: list[Diagnostic] = []
        for var_name, (method, line, col) in cached.items():
            if var_name not in unpersisted:
                diagnostics.append(
                    Diagnostic(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message=(
                            "cache()/persist() without unpersist() — "
                            "cached data may leak memory"
                        ),
                        explanation=self._EXPLANATION,
                        suggestion=self._SUGGESTION,
                        line=line,
                        col=col,
                    )
                )
        return diagnostics
