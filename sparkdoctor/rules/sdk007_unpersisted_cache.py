"""
SDK007 — cache() or persist() without corresponding unpersist().

Severity: INFO
"""
from __future__ import annotations

import ast

from sparkdoctor.lint.base import Category, Diagnostic, Rule, Severity
from sparkdoctor.rules._helpers import (
    _has_pyspark_import,
    chain_contains_method,
    chain_root_name,
    receiver_name,
)


class UnpersistedCacheRule(Rule):
    """Detects cache()/persist() without a corresponding unpersist() in the file."""

    rule_id = "SDK007"
    severity = Severity.INFO
    title = "cache()/persist() without unpersist()"
    category = Category.PERFORMANCE

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

    # Chain methods from RDD API (RDD.cache() is fine, no unpersist needed)
    _RDD_CHAIN_METHODS = {
        "parallelize", "textFile", "wholeTextFiles",
        "map", "flatMap", "reduceByKey", "groupByKey",
        "mapPartitions", "mapValues", "sortByKey",
    }

    # Chain methods from TF dataset API
    _TF_CHAIN_METHODS = {
        "from_tensor_slices", "from_generator", "from_tensors",
        "batch", "prefetch", "repeat", "padded_batch",
    }

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        # If the file doesn't import pyspark, skip entirely — cache/persist
        # is from another library (dask, tensorflow, etc.)
        if not _has_pyspark_import(tree):
            return []

        # First pass: build a map of variable -> assigned-from-chain-root
        # Skip cache/persist assignments since those just wrap the original object.
        var_origins: dict[str, str | None] = {}
        # Track which (line, col) come from assignment RHS — avoid double-counting
        assigned_cache_locations: set[tuple[int, int]] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
                if isinstance(node.value.func, ast.Attribute):
                    if node.value.func.attr in self._CACHE_METHODS:
                        assigned_cache_locations.add(
                            (node.value.lineno, node.value.col_offset)
                        )
                        continue
                root = chain_root_name(node.value)
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id not in var_origins:
                        var_origins[target.id] = root

        # Track: variable_name -> (cache_method, line, col)
        cached: dict[str, tuple[str, int, int]] = {}
        unpersisted: set[str] = set()

        for node in ast.walk(tree):
            # Handle cache/persist/unpersist method calls
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                method = node.func.attr
                name = receiver_name(node)

                if method in self._CACHE_METHODS:
                    # Skip if this call is the RHS of an assignment (handled below)
                    loc = (node.lineno, node.col_offset)
                    if loc in assigned_cache_locations:
                        continue
                    if self._is_non_spark(node):
                        continue
                    key = name or f"_anon_{node.lineno}_{node.col_offset}"
                    cached[key] = (method, node.lineno, node.col_offset)
                elif method == "unpersist" and name:
                    unpersisted.add(name)

            # Handle assignment: result = df.cache()
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
                if isinstance(node.value.func, ast.Attribute):
                    if node.value.func.attr in self._CACHE_METHODS:
                        assign_recv = receiver_name(node.value)
                        if self._is_non_spark(node.value):
                            continue
                        # Track both the assignment target and the receiver
                        for target in node.targets:
                            if isinstance(target, ast.Name):
                                cached[target.id] = (
                                    node.value.func.attr,
                                    node.value.lineno,
                                    node.value.col_offset,
                                )
                        if assign_recv:
                            cached[assign_recv] = (
                                node.value.func.attr,
                                node.value.lineno,
                                node.value.col_offset,
                            )

        diagnostics: list[Diagnostic] = []
        for var_name, (_method, line, col) in cached.items():
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

    def _is_non_spark(self, node: ast.Call) -> bool:
        """Return True if this cache/persist call is on a non-Spark object.

        Checks structural patterns (chain methods) only — no name guessing.
        """
        if chain_contains_method(node, self._TF_CHAIN_METHODS):
            return True
        if chain_contains_method(node, self._RDD_CHAIN_METHODS):
            return True
        return False

