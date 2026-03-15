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

    # Root names that are schema/type builders, not DataFrame chains.
    _SCHEMA_BUILDERS = {
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
    }

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        if not _has_pyspark_import(tree):
            return []

        # Collect all candidates, then keep max depth per root to handle
        # ast.walk's unspecified traversal order.
        candidates: dict[int, tuple[int, ast.AST]] = {}  # root_id -> (depth, node)

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Attribute):
                continue

            # Skip schema builder chains (StructType().add().add()...)
            root = self._chain_root_name(node)
            if root in self._SCHEMA_BUILDERS:
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
    def _chain_root_name(node: ast.AST) -> str | None:
        """Return the root Name.id of a method chain, or None."""
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
