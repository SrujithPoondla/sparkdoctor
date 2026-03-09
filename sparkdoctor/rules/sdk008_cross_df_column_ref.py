"""
SDK008 — Cross-DataFrame column reference.

Severity: WARNING
"""
from __future__ import annotations

import ast

from sparkdoctor.lint.base import Category, Diagnostic, Rule, Severity
from sparkdoctor.rules._helpers import _has_pyspark_import


class CrossDataFrameColumnRefRule(Rule):
    """Detects df1.colA used inside df2.select() or df2.filter()."""

    rule_id = "SDK008"
    severity = Severity.WARNING
    title = "Cross-DataFrame column reference"
    category = Category.CORRECTNESS

    _EXPLANATION = (
        "Referencing a column as df1.colA inside df2.select() can cause "
        "AnalysisException at runtime when the DataFrames come from different "
        "lineages. Spark resolves the column against df1's plan, not df2's."
    )

    _SUGGESTION = (
        "Use string column names or F.col() instead of DataFrame attribute access.\n"
        '  # Bad:  df2.select(df1.colA)\n'
        '  # Good: df2.select("colA")  or  df2.select(F.col("colA"))'
    )

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

    def _find_df_variables(self, tree: ast.AST) -> set[str]:
        """Find variable names likely holding DataFrames.

        Detects assignments of the form ``var = obj.method(...)``, which covers
        patterns like ``spark.read.parquet()`` and ``df.filter()``.  Does not
        detect plain function calls (``df = load()``), tuple unpacking, or
        function parameters — intentionally conservative to reduce false positives.
        """
        df_vars: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                value = node.value
                # df = spark.read... / df = other_df.filter(...)
                if isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute):
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            df_vars.add(target.id)
        return df_vars

    def _walk_args(self, call: ast.Call):
        """Yield all expression nodes within a call's arguments (recursively)."""
        for arg in call.args:
            yield from ast.walk(arg)
        for kw in call.keywords:
            yield from ast.walk(kw.value)
