"""
SDK019 — inferSchema=True in production read (CSV/JSON).

Severity: WARNING
"""
from __future__ import annotations

import ast

from sparkdoctor.lint.base import Diagnostic, Rule, Severity


class InferSchemaRule(Rule):
    """Detects inferSchema=True in CSV/JSON read calls."""

    rule_id = "SDK019"
    severity = Severity.WARNING
    title = "inferSchema=True in production read"

    _EXPLANATION = (
        "inferSchema requires an extra pass over the entire dataset to determine "
        "column types. On large files this doubles read time. It also produces "
        "unstable schemas — a column that looks like integers today may become "
        "strings tomorrow if one value has a decimal point."
    )

    _SUGGESTION = (
        "Define an explicit StructType schema and pass it to .schema():\n"
        "  schema = StructType([StructField('id', IntegerType()), ...])\n"
        "  df = spark.read.schema(schema).csv('path')\n"
        "This is faster (single pass) and ensures consistent types across runs."
    )

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        diagnostics: list[Diagnostic] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if self._has_infer_schema_true(node):
                diagnostics.append(
                    Diagnostic(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message="inferSchema=True causes an extra data scan and unstable types",
                        explanation=self._EXPLANATION,
                        suggestion=self._SUGGESTION,
                        line=node.lineno,
                        col=node.col_offset,
                    )
                )
        return diagnostics

    def _has_infer_schema_true(self, node: ast.Call) -> bool:
        """Check if any keyword argument is inferSchema=True."""
        for kw in node.keywords:
            if kw.arg == "inferSchema" and isinstance(kw.value, ast.Constant):
                if kw.value.value is True:
                    return True
        return False
