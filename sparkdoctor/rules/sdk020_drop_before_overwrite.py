"""
SDK020 — DROP TABLE or fs.rm before overwrite write.

Severity: INFO
"""
from __future__ import annotations

import ast
import re

from sparkdoctor.lint.base import Category, Diagnostic, Rule, Severity
from sparkdoctor.rules._helpers import _has_pyspark_import


class DropBeforeOverwriteRule(Rule):
    """Detects DROP TABLE or dbutils.fs.rm before an overwrite write."""

    rule_id = "SDK020"
    severity = Severity.INFO
    title = "DROP TABLE or fs.rm before overwrite write"
    category = Category.CORRECTNESS

    _EXPLANATION = (
        "Manually deleting or dropping a table before writing with overwrite mode "
        "loses Delta time-travel history, interrupts concurrent readers (no atomicity), "
        "and removes table metadata and ACLs. Delta's overwrite mode is atomic — "
        "readers see either the old version or the new version, never partial state."
    )

    _SUGGESTION = (
        "Use overwrite mode directly without manual deletion.\n"
        '  # Bad:  spark.sql("DROP TABLE ..."); df.write.mode("overwrite").save()\n'
        '  # Good: df.write.mode("overwrite").save(path)\n'
        '  # For schema changes: .option("overwriteSchema", "true")'
    )

    _DROP_PATTERN = re.compile(r"DROP\s+TABLE", re.IGNORECASE)

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        if not _has_pyspark_import(tree):
            return []

        delete_lines: list[tuple[int, str]] = []
        overwrite_lines: list[int] = []

        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if self._is_drop_table_sql(node):
                    delete_lines.append((node.lineno, "DROP TABLE"))
                elif self._is_fs_rm(node):
                    delete_lines.append((node.lineno, "dbutils.fs.rm()"))

            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "mode"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and node.args[0].value == "overwrite"
            ):
                overwrite_lines.append(node.lineno)

        if not delete_lines or not overwrite_lines:
            return []

        diagnostics: list[Diagnostic] = []
        for del_line, del_type in delete_lines:
            for ow_line in overwrite_lines:
                if 0 < ow_line - del_line <= 20:
                    diagnostics.append(
                        Diagnostic(
                            rule_id=self.rule_id,
                            severity=self.severity,
                            message=f"{del_type} before overwrite write — "
                            "use overwrite mode directly",
                            explanation=self._EXPLANATION,
                            suggestion=self._SUGGESTION,
                            line=del_line,
                            col=0,
                        )
                    )
                    break
        return diagnostics

    def _is_drop_table_sql(self, node: ast.Call) -> bool:
        if not isinstance(node.func, ast.Attribute):
            return False
        if node.func.attr != "sql":
            return False
        if not node.args:
            return False
        arg = node.args[0]
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            return bool(self._DROP_PATTERN.search(arg.value))
        if isinstance(arg, ast.JoinedStr):
            for value in arg.values:
                if (
                    isinstance(value, ast.Constant)
                    and isinstance(value.value, str)
                    and self._DROP_PATTERN.search(value.value)
                ):
                    return True
        return False

    def _is_fs_rm(self, node: ast.Call) -> bool:
        """Check for dbutils.fs.rm() — verifies full chain."""
        if not isinstance(node.func, ast.Attribute):
            return False
        if node.func.attr != "rm":
            return False
        receiver = node.func.value
        if not (isinstance(receiver, ast.Attribute) and receiver.attr == "fs"):
            return False
        return isinstance(receiver.value, ast.Name) and receiver.value.id == "dbutils"
