"""
SDK024 — Streaming readStream without explicit schema.

Severity: INFO
"""
from __future__ import annotations

import ast

from sparkdoctor.lint.base import Category, Diagnostic, Rule, Severity
from sparkdoctor.rules._helpers import _has_pyspark_import


class StreamingWithoutSchemaRule(Rule):
    """Detects readStream chains that do not include a .schema() call."""

    rule_id = "SDK024"
    severity = Severity.INFO
    title = "Streaming read without explicit schema"
    category = Category.CORRECTNESS

    _EXPLANATION = (
        "Streaming jobs run indefinitely. A schema inferred at startup will break "
        "when the source schema changes, causing the stream to fail silently or "
        "corrupt data. Explicit schemas also prevent Spark from doing a schema "
        "inference scan at startup which adds latency."
    )

    _SUGGESTION = (
        "Define an explicit schema for streaming reads.\n"
        "  # Bad:  spark.readStream.format(\"json\").load(path)\n"
        "  # Good: spark.readStream.schema(my_schema).format(\"json\").load(path)"
    )

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        if not _has_pyspark_import(tree):
            return []

        diagnostics: list[Diagnostic] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Attribute):
                continue
            # Look for .load() or .table() at the end of a readStream chain
            if node.func.attr not in ("load", "table", "start"):
                continue
            if not self._chain_has_read_stream(node):
                continue
            if self._chain_has_schema(node):
                continue

            diagnostics.append(
                Diagnostic(
                    rule_id=self.rule_id,
                    severity=self.severity,
                    message="readStream without explicit .schema() — "
                    "inferred schema will break on source changes",
                    explanation=self._EXPLANATION,
                    suggestion=self._SUGGESTION,
                    line=node.lineno,
                    col=node.col_offset,
                )
            )
        return diagnostics

    def _chain_has_read_stream(self, node: ast.AST) -> bool:
        """Walk the receiver chain to check if readStream is present."""
        current = node
        while True:
            if isinstance(current, ast.Call):
                if isinstance(current.func, ast.Attribute):
                    current = current.func.value
                else:
                    return False
            elif isinstance(current, ast.Attribute):
                if current.attr == "readStream":
                    return True
                current = current.value
            else:
                return False

    def _chain_has_schema(self, node: ast.AST) -> bool:
        """Walk the receiver chain to check if .schema() is called."""
        current = node
        while True:
            if isinstance(current, ast.Call):
                if isinstance(current.func, ast.Attribute):
                    if current.func.attr == "schema":
                        return True
                    current = current.func.value
                else:
                    return False
            elif isinstance(current, ast.Attribute):
                current = current.value
            else:
                return False
