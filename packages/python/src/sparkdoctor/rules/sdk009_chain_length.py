"""
SDK009 — Transformation chain longer than threshold.

Severity: INFO
"""

from __future__ import annotations

import ast

from sparkdoctor.lint.base import Diagnostic, Rule
from sparkdoctor.rules._helpers import _has_pyspark_import


class ChainLengthRule(Rule):
    """Detects transformation chains longer than 5 method calls."""

    rule_id = "SDK009"

    _THRESHOLD = 5

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        if not _has_pyspark_import(tree):
            return []

        # Collect names imported from pyspark.sql.types — these are schema/type
        # builders, not DataFrame chains. Derived from imports, not hardcoded.
        type_names = self._collect_type_imports(tree)

        # Collect all candidates, then keep max depth per root to handle
        # ast.walk's unspecified traversal order.
        candidates: dict[int, tuple[int, ast.AST]] = {}  # root_id -> (depth, node)

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Attribute):
                continue

            # Skip schema builder chains (StructType().add().add()...)
            root = self._chain_root_name(node, type_names)
            if root is not None and root in type_names:
                continue

            depth = self._chain_depth(node)
            if depth <= self._THRESHOLD:
                continue

            root_id = self._root_node_id(node)
            prev = candidates.get(root_id)
            if prev is None or depth > prev[0]:
                candidates[root_id] = (depth, node)

        diagnostics: list[Diagnostic] = []
        for depth, node in candidates.values():
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

    @staticmethod
    def _collect_type_imports(tree: ast.AST) -> set[str]:
        """Collect names imported from pyspark.sql.types.

        Handles both ``from pyspark.sql.types import X`` (ImportFrom) and
        ``import pyspark.sql.types as T`` (Import).  These are schema/type
        builders whose chained calls should not be flagged.
        """
        type_names: set[str] = set()
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.ImportFrom)
                and node.module
                and node.module.startswith("pyspark.sql.types")
            ):
                for alias in node.names:
                    name = alias.asname if alias.asname else alias.name
                    type_names.add(name)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name and alias.name.startswith("pyspark.sql.types"):
                        name = alias.asname if alias.asname else alias.name
                        type_names.add(name)
        return type_names

    @staticmethod
    def _chain_root_name(node: ast.AST, type_names: set[str]) -> str | None:
        """Return the root Name.id of a method chain, or None.

        Also recognises module-qualified constructors like ``T.StructType()``
        by returning the attribute name when it matches a known type import.

        For unaliased ``import pyspark.sql.types``, reconstructs the dotted
        module path (e.g. ``pyspark.sql.types``) and checks against
        ``type_names``.  This avoids storing the bare root ``"pyspark"`` which
        would falsely match non-type chains like SparkSession builders.
        """
        current = node
        # Track attributes seen since the last Call, so we can reconstruct
        # the dotted module path when we reach a Name node.
        attrs_since_last_call: list[str] = []
        while True:
            if isinstance(current, ast.Call):
                current = current.func
                attrs_since_last_call = []
            elif isinstance(current, ast.Attribute):
                # Handle module-qualified type builders, e.g. T.StructType()
                if current.attr in type_names:
                    return current.attr
                attrs_since_last_call.append(current.attr)
                current = current.value
            elif isinstance(current, ast.Name):
                # For unaliased `import pyspark.sql.types`, check if any
                # dotted prefix matches type_names.
                # e.g. attrs = ['StructType', 'types', 'sql'], name = 'pyspark'
                # → check "pyspark.sql", "pyspark.sql.types", etc.
                if attrs_since_last_call:
                    parts = [current.id] + list(reversed(attrs_since_last_call))
                    for i in range(2, len(parts) + 1):
                        prefix = ".".join(parts[:i])
                        if prefix in type_names:
                            return prefix
                return current.id
            else:
                return None
