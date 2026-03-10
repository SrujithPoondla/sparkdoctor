"""
SDK018 — Inconsistent column reference style.

Severity: INFO
"""
from __future__ import annotations

import ast

from sparkdoctor.lint.base import Category, Diagnostic, Rule, Severity
from sparkdoctor.rules._helpers import _has_pyspark_import


class InconsistentColumnRefRule(Rule):
    """Detects mixing of F.col(), df["col"], and df.col styles in the same file."""

    rule_id = "SDK018"
    severity = Severity.INFO
    title = "Inconsistent column reference style"
    category = Category.STYLE

    _EXPLANATION = (
        "Mixing F.col(), df[\"col\"], and df.col styles in the same file creates "
        "cognitive overhead during code review. df.col is particularly risky because "
        "it looks like a Python attribute and may silently produce Column objects that "
        "cause AnalysisExceptions."
    )

    _SUGGESTION = (
        "Pick one column reference style and use it consistently.\n"
        "Recommended: F.col() or string literals for Spark 3.0+.\n"
        '  df.select(F.col("name"), F.col("age"))  # consistent F.col()\n'
        '  df.select("name", "age")                 # consistent string'
    )

    _DF_METHODS = {
        "select", "filter", "where", "withColumn", "groupBy",
        "orderBy", "sort", "agg", "drop", "join",
    }

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        if not _has_pyspark_import(tree):
            return []

        styles: dict[str, list[int]] = {}

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Attribute):
                continue

            if node.func.attr == "col":
                receiver = node.func.value
                if isinstance(receiver, ast.Name) and receiver.id in ("F", "functions"):
                    styles.setdefault("F.col()", []).append(node.lineno)
                continue

            if node.func.attr not in self._DF_METHODS:
                continue

            for arg in node.args:
                self._detect_styles_in_expr(arg, styles)
            for kw in node.keywords:
                self._detect_styles_in_expr(kw.value, styles)

        if len(styles) < 2:
            return []

        style_names = sorted(styles.keys())
        min_style = min(style_names, key=lambda s: len(styles[s]))
        first_line = min(styles[min_style])

        return [
            Diagnostic(
                rule_id=self.rule_id,
                severity=self.severity,
                message=f"Mixed column reference styles: {', '.join(style_names)}",
                explanation=self._EXPLANATION,
                suggestion=self._SUGGESTION,
                line=first_line,
                col=0,
            )
        ]

    def _detect_styles_in_expr(self, node: ast.AST, styles: dict[str, list[int]]) -> None:
        for child in ast.walk(node):
            if isinstance(child, ast.Subscript):
                if isinstance(child.slice, ast.Constant) and isinstance(
                    child.slice.value, str
                ):
                    styles.setdefault('df["col"]', []).append(child.lineno)
            elif (
                isinstance(child, ast.Attribute)
                and isinstance(child.value, ast.Name)
                and child.attr not in self._DF_METHODS
                and child.attr not in {
                    "col", "columns", "schema", "dtypes", "rdd", "write",
                    "read", "sql", "conf", "sparkContext", "udf",
                    "cache", "persist", "unpersist", "count", "collect",
                    "show", "head", "first", "take", "toPandas",
                    "limit", "alias", "asc", "desc", "cast",
                    "otherwise", "over", "between", "isin",
                }
            ):
                styles.setdefault("df.col", []).append(child.lineno)
