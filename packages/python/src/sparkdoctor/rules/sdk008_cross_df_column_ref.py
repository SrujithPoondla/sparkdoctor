"""
SDK008 — Cross-DataFrame column reference.

Severity: WARNING
"""

from __future__ import annotations

import ast

from sparkdoctor.lint.base import Diagnostic, Rule
from sparkdoctor.rules._helpers import _has_pyspark_import


class CrossDataFrameColumnRefRule(Rule):
    """Detects df1.colA used inside df2.select() or df2.filter()."""

    rule_id = "SDK008"

    # Methods where cross-DF column refs are problematic
    _TARGET_METHODS = {"select", "filter", "where", "withColumn", "drop", "groupBy", "orderBy"}

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        if not _has_pyspark_import(tree):
            return []

        # Step 1: Find all DataFrame variable names from assignments
        df_vars = self._find_df_variables(tree)
        if len(df_vars) < 2:
            return []

        diagnostics: list[Diagnostic] = []

        # Step 2: Walk for target method calls and check for cross-DF refs
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr not in self._TARGET_METHODS:
                continue

            # Get the receiver DF name — only handles simple Name receivers.
            # Chained calls like df2.filter(...).select(df1.col) are skipped
            # because the receiver is a Call node, not a Name. This is
            # conservative to avoid false positives.
            receiver = node.func.value
            if not isinstance(receiver, ast.Name):
                continue
            receiver_df = receiver.id
            if receiver_df not in df_vars:
                continue

            # Check arguments for attribute access on a different DF
            for arg in self._walk_args(node):
                if isinstance(arg, ast.Attribute) and isinstance(arg.value, ast.Name):
                    ref_df = arg.value.id
                    if ref_df in df_vars and ref_df != receiver_df:
                        diagnostics.append(
                            Diagnostic(
                                rule_id=self.rule_id,
                                severity=self.severity,
                                message=f"{ref_df}.{arg.attr} referenced inside "
                                f"{receiver_df}.{node.func.attr}()",
                                explanation=self._EXPLANATION,
                                suggestion=self._SUGGESTION,
                                line=arg.lineno,
                                col=arg.col_offset,
                            )
                        )
        return diagnostics

    # Methods that produce Column objects, not DataFrames.
    _COLUMN_METHODS = {
        "getItem",
        "getField",
        "cast",
        "astype",
        "alias",
        "name",
        "substr",
        "startswith",
        "endswith",
        "contains",
        "like",
        "rlike",
        "isin",
        "between",
        "isNull",
        "isNotNull",
        "asc",
        "desc",
        "over",
        "otherwise",
        "when",
    }

    # Root names that are type constructors, not DataFrame sources.
    _NON_DF_ROOTS = {
        "StructType",
        "StructField",
        "ArrayType",
        "MapType",
        "StringType",
        "IntegerType",
        "LongType",
        "DoubleType",
        "FloatType",
        "BooleanType",
        "DateType",
        "TimestampType",
        "DecimalType",
        "BinaryType",
        "ShortType",
        "ByteType",
        "broadcast",
    }

    def _find_df_variables(self, tree: ast.AST) -> set[str]:
        """Find variable names likely holding DataFrames.

        Detects assignments of the form ``var = obj.method(...)``, which covers
        patterns like ``spark.read.parquet()`` and ``df.filter()``.  Excludes
        assignments from Column operations (getItem, cast, alias, etc.),
        broadcast variables, and type constructors to reduce false positives.
        """
        df_vars: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                value = node.value
                # df = spark.read... / df = other_df.filter(...)
                if isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute):
                    method_name = value.func.attr
                    # Skip Column operations — these produce Column, not DataFrame
                    if method_name in self._COLUMN_METHODS:
                        continue
                    # Skip type constructors and broadcast
                    root = self._chain_root(value)
                    if root in self._NON_DF_ROOTS:
                        continue
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            df_vars.add(target.id)
        return df_vars

    @staticmethod
    def _chain_root(node: ast.AST) -> str | None:
        """Return the root Name.id of a method chain."""
        current = node
        while True:
            if isinstance(current, ast.Call):
                current = current.func
            elif isinstance(current, ast.Attribute):
                current = current.value
            elif isinstance(current, ast.Name):
                return current.id
            else:
                return None

    def _walk_args(self, call: ast.Call):
        """Yield all expression nodes within a call's arguments (recursively)."""
        for arg in call.args:
            yield from ast.walk(arg)
        for kw in call.keywords:
            yield from ast.walk(kw.value)
