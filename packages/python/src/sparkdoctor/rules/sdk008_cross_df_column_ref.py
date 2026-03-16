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

    def _find_df_variables(self, tree: ast.AST) -> set[str]:
        """Find variable names likely holding DataFrames.

        Uses purely structural AST analysis — no hardcoded method whitelists.

        Pass 1: Collect all variables assigned from method-call chains,
        excluding non-DF imports (pyspark.sql, pyspark.sql.types,
        pyspark.sql.functions).

        Pass 2: Remove variables whose assignment has the "column access"
        pattern — where the method's receiver is ``df.col_name`` (an
        ``ast.Attribute`` whose ``.value`` is an ``ast.Name`` already tracked
        as a DF variable). This catches ``df.name.getItem(0)``,
        ``df.age.cast('int')``, etc.
        """
        non_df_names = self._collect_non_df_imports(tree)

        # Pass 1 — gather all method-chain assignment candidates
        # Maps variable name → the ast.Call value node for later inspection
        candidates: dict[str, ast.Call] = {}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            value = node.value
            if not isinstance(value, ast.Call) or not isinstance(value.func, ast.Attribute):
                continue
            # Exclude if root is a non-DF import (types, functions)
            root = self._chain_root(value)
            if root in non_df_names:
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    candidates[target.id] = value

        # Pass 2 — remove Column-pattern assignments
        # Pattern: Call(Attr(Attr(Name(df_var), col_name), method))
        # i.e. the method's receiver is `df_var.col_name` where df_var is
        # already a DF candidate. This means the result is a Column, not a DF.
        df_vars = set(candidates.keys())
        to_remove: set[str] = set()
        for var_name, call_node in candidates.items():
            receiver = call_node.func.value  # already known to be ast.Attribute
            if (
                isinstance(receiver, ast.Attribute)
                and isinstance(receiver.value, ast.Name)
                and receiver.value.id in df_vars
            ):
                to_remove.add(var_name)

        return df_vars - to_remove

    @staticmethod
    def _collect_non_df_imports(tree: ast.AST) -> set[str]:
        """Collect imported names that are not DataFrames.

        Handles both ``from pyspark.sql import X`` (ImportFrom) and
        ``import pyspark.sql.types as T`` (Import).  Includes names from
        ``pyspark.sql`` (SparkSession, etc.), ``pyspark.sql.types``, and
        ``pyspark.sql.functions``.
        """
        non_df: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                mod = node.module
                if (
                    mod == "pyspark.sql"
                    or mod.startswith("pyspark.sql.types")
                    or mod.startswith("pyspark.sql.functions")
                ):
                    for alias in node.names:
                        name = alias.asname if alias.asname else alias.name
                        non_df.add(name)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name and (
                        alias.name == "pyspark.sql"
                        or alias.name.startswith("pyspark.sql.types")
                        or alias.name.startswith("pyspark.sql.functions")
                    ):
                        name = alias.asname if alias.asname else alias.name
                        non_df.add(name)
        return non_df

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
