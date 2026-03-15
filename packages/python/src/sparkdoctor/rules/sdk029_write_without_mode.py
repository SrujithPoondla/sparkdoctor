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

        # First pass: collect DataFrameWriter aliases.
        # Maps variable name -> whether .mode() was in the assignment chain.
        writer_aliases = self._collect_writer_aliases(tree)

        diagnostics: list[Diagnostic] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr not in self._WRITE_TERMINALS:
                continue

            # Check for mode= keyword arg in the terminal write call
            if self._has_mode_kwarg(node):
                continue

            # Check inline chains (e.g. df.write.parquet(...))
            if self._chain_has_write(node):
                if chain_contains_method(node, {"mode"}):
                    continue
                diagnostics.append(self._make_diagnostic(node))
                continue

            # Check alias chains (e.g. writer = df.write...; writer.parquet(...))
            alias_name = self._chain_root_name(node)
            if alias_name is not None and alias_name in writer_aliases:
                # .mode() may appear in the assignment chain OR the call chain
                if writer_aliases[alias_name] or chain_contains_method(node, {"mode"}):
                    continue
                diagnostics.append(self._make_diagnostic(node))

        return diagnostics

    def _make_diagnostic(self, node: ast.AST) -> Diagnostic:
        return Diagnostic(
            rule_id=self.rule_id,
            severity=self.severity,
            message="DataFrame write without explicit .mode()",
            explanation=self._EXPLANATION,
            suggestion=self._SUGGESTION,
            line=node.lineno,
            col=node.col_offset,
        )

    @classmethod
    def _collect_writer_aliases(cls, tree: ast.AST) -> dict[str, bool]:
        """Find simple assignments like ``writer = df.write...``.

        Returns a mapping of variable name to whether ``.mode()`` appears in
        the assignment chain.
        """
        aliases: dict[str, bool] = {}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            if len(node.targets) != 1:
                continue
            target = node.targets[0]
            if not isinstance(target, ast.Name):
                continue
            if not cls._chain_has_write(node.value):
                continue
            has_mode = chain_contains_method(node.value, {"mode"})
            aliases[target.id] = has_mode
        return aliases

    @staticmethod
    def _chain_root_name(node: ast.AST) -> str | None:
        """Walk down the method chain and return the root ``Name.id``, if any."""
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

    @staticmethod
    def _has_mode_kwarg(node: ast.Call) -> bool:
        """Return True if the call has a ``mode=`` keyword argument."""
        return any(kw.arg == "mode" for kw in node.keywords)

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
