"""
SDK028 — distinct().count() is a two-pass operation.

Severity: WARNING
"""

from __future__ import annotations

import ast

from sparkdoctor.lint.base import Diagnostic, Rule
from sparkdoctor.rules._helpers import _has_pyspark_import


class DistinctCountRule(Rule):
    """Detects distinct().count() and dropDuplicates().count() chains."""

    rule_id = "SDK028"

    _DEDUP_METHODS = {"distinct", "dropDuplicates"}

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        if not _has_pyspark_import(tree):
            return []

        diagnostics: list[Diagnostic] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not self._is_count_call(node):
                continue
            # Check if the receiver is a distinct()/dropDuplicates() call
            dedup_call = node.func.value
            if not isinstance(dedup_call, ast.Call) or not self._is_dedup_call(dedup_call):
                continue
            # Only flag when preceded by column selection (.select() or [[]])
            # so countDistinct() is a valid replacement.
            # Bare df.distinct().count() (whole-row) has no better alternative.
            if not self._has_column_selection(dedup_call):
                continue
            dedup_name = dedup_call.func.attr
            diagnostics.append(
                Diagnostic(
                    rule_id=self.rule_id,
                    severity=self.severity,
                    message=(
                        f"{dedup_name}().count() forces two passes — use countDistinct() instead"
                    ),
                    explanation=self._EXPLANATION,
                    suggestion=self._SUGGESTION,
                    line=dedup_call.lineno,
                    col=dedup_call.col_offset,
                )
            )
        return diagnostics

    def _is_count_call(self, node: ast.Call) -> bool:
        """Check if this is a zero-argument .count() call."""
        return (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "count"
            and len(node.args) == 0
            and len(node.keywords) == 0
        )

    def _is_dedup_call(self, node: ast.Call) -> bool:
        """Check if this is a .distinct() or .dropDuplicates() call."""
        return isinstance(node.func, ast.Attribute) and node.func.attr in self._DEDUP_METHODS

    @staticmethod
    def _has_column_selection(dedup_call: ast.Call) -> bool:
        """Check if the dedup call's receiver is a column selection.

        Returns True when the chain includes ``.select(...)`` or ``[[...]]``
        before ``.distinct()``/``.dropDuplicates()``, meaning
        ``countDistinct()`` is a valid single-pass replacement.

        For ``dropDuplicates(["col"])``, the column selection is implicit
        in the method arguments, so it always qualifies.
        """
        # dropDuplicates(["col"]) or dropDuplicates(subset=["col"])
        # has implicit column selection
        if isinstance(dedup_call.func, ast.Attribute) and dedup_call.func.attr == "dropDuplicates":
            if dedup_call.args:
                return True
            if any(kw.arg == "subset" for kw in dedup_call.keywords):
                return True

        # Walk down the chain looking for .select() or [[ ]] subscript
        current = dedup_call.func.value if isinstance(dedup_call.func, ast.Attribute) else None
        while current is not None:
            # .select(...) call — but not select("*") which is whole-row
            if (
                isinstance(current, ast.Call)
                and isinstance(current.func, ast.Attribute)
                and current.func.attr == "select"
                and not DistinctCountRule._is_select_star(current)
            ):
                return True
            # df[["col1", "col2"]] subscript with a list
            if isinstance(current, ast.Subscript) and isinstance(current.slice, ast.List):
                return True
            # Continue walking down the chain
            if isinstance(current, ast.Call) and isinstance(current.func, ast.Attribute):
                current = current.func.value
            elif isinstance(current, ast.Attribute):
                current = current.value
            else:
                break
        return False

    @staticmethod
    def _is_select_star(call: ast.Call) -> bool:
        """Check if a .select() call is select('*') — i.e. whole-row.

        Also recognises ``select(col("*"))`` and ``select(F.col("*"))``
        as whole-row equivalents.
        """
        if len(call.args) != 1 or call.keywords:
            return False
        arg = call.args[0]
        # select("*")
        if isinstance(arg, ast.Constant) and arg.value == "*":
            return True
        # select(col("*")) or select(F.col("*"))
        return isinstance(arg, ast.Call) and DistinctCountRule._is_col_star(arg)

    @staticmethod
    def _is_col_star(node: ast.Call) -> bool:
        """Check if a call is col('*') or <module>.col('*')."""
        func = node.func
        # col("*") — bare function call
        is_col = isinstance(func, ast.Name) and func.id == "col"
        # F.col("*") — module-qualified call
        is_col = is_col or (isinstance(func, ast.Attribute) and func.attr == "col")
        return (
            is_col
            and len(node.args) == 1
            and not node.keywords
            and isinstance(node.args[0], ast.Constant)
            and node.args[0].value == "*"
        )
