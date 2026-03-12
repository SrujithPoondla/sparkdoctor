"""
SDK029 — DataFrame write without explicit .mode().

Severity: WARNING
"""

from __future__ import annotations

import ast

from sparkdoctor.lint.base import Diagnostic, Rule
from sparkdoctor.rules._helpers import _has_pyspark_import, chain_contains_method


class WriteWithoutModeRule(Rule):
    """Detects DataFrame writes that don't specify an explicit .mode()."""

    rule_id = "SDK029"

    # Terminal write methods that should have .mode() in the chain.
    _WRITE_TERMINALS = {"parquet", "csv", "json", "orc", "save", "saveAsTable"}

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        if not _has_pyspark_import(tree):
            return []

        diagnostics: list[Diagnostic] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr not in self._WRITE_TERMINALS:
                continue
            # Walk the chain to verify it contains .write (not .writeStream)
            if not self._chain_has_write(node):
                continue
            # Check if .mode() appears anywhere in the chain
            if chain_contains_method(node, {"mode"}):
                continue
            diagnostics.append(
                Diagnostic(
                    rule_id=self.rule_id,
                    severity=self.severity,
                    message="DataFrame write without explicit .mode()",
                    explanation=self._EXPLANATION,
                    suggestion=self._SUGGESTION,
                    line=node.lineno,
                    col=node.col_offset,
                )
            )
        return diagnostics

    @staticmethod
    def _chain_has_write(node: ast.AST) -> bool:
        """Return True if the method chain contains `.write` (not `.writeStream`)."""
        current = node
        while True:
            if isinstance(current, ast.Call):
                current = current.func
            elif isinstance(current, ast.Attribute):
                if current.attr == "write":
                    return True
                if current.attr == "writeStream":
                    return False
                current = current.value
            else:
                return False
