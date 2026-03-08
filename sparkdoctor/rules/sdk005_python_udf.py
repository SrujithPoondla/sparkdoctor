"""
SDK005 — Python UDF without Arrow optimization.

Severity: WARNING
"""
from __future__ import annotations

import ast

from sparkdoctor.lint.base import Category, Diagnostic, Rule, Severity


class PythonUdfRule(Rule):
    """Detects Python UDFs that could use pandas_udf for better performance."""

    rule_id = "SDK005"
    severity = Severity.WARNING
    title = "Python UDF without Arrow optimization"
    category = Category.PERFORMANCE

    _EXPLANATION = (
        "Python UDFs serialize each row to Python, execute Python, then deserialize "
        "back to JVM. This row-at-a-time serialization overhead can make a UDF "
        "10-100x slower than an equivalent Spark built-in function. Catalyst cannot "
        "optimize through UDFs, so query plans are less efficient."
    )

    _SUGGESTION = (
        "First check if a Spark built-in function covers your use case (functions in "
        "pyspark.sql.functions cover most common transformations).\n"
        "If a custom function is necessary, use pandas_udf for vectorized execution:\n"
        "  from pyspark.sql.functions import pandas_udf\n"
        "  @pandas_udf(returnType=StringType())\n"
        "  def my_func(x: pd.Series) -> pd.Series:\n"
        "      return x.str.upper()"
    )

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        diagnostics: list[Diagnostic] = []
        decorator_ids = self._check_decorators(tree, diagnostics)
        self._check_udf_calls(tree, diagnostics, decorator_ids)
        return diagnostics

    def _check_decorators(
        self, tree: ast.AST, diagnostics: list[Diagnostic]
    ) -> set[int]:
        """Check for @udf decorators on function definitions.

        Returns set of ast node ids already reported, to avoid duplicates.
        """
        seen: set[int] = set()
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef):
                continue
            for decorator in node.decorator_list:
                if self._is_plain_udf_decorator(decorator):
                    diagnostics.append(self._make_diagnostic(decorator))
                    seen.add(id(decorator))
                    # Also mark the inner Call if decorator is a Call
                    if isinstance(decorator, ast.Call):
                        seen.add(id(decorator))
        return seen

    def _check_udf_calls(
        self, tree: ast.AST, diagnostics: list[Diagnostic],
        decorator_ids: set[int],
    ) -> None:
        """Check for udf() calls outside decorators (e.g. udf(lambda ...))."""
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if id(node) in decorator_ids:
                continue
            if self._is_udf_call(node):
                diagnostics.append(self._make_diagnostic(node))

    def _is_plain_udf_decorator(self, node: ast.expr) -> bool:
        """Return True if the decorator is @udf or @udf(...) but NOT @pandas_udf.

        Excludes UDFs with useArrow=True since Arrow-enabled UDFs avoid
        the per-row serialization overhead.
        """
        # @udf (bare, no args — can't have useArrow)
        if isinstance(node, ast.Name) and node.id == "udf":
            return True
        # @udf(returnType=...) or @F.udf(...)
        if isinstance(node, ast.Call):
            func = node.func
            is_udf = (
                (isinstance(func, ast.Name) and func.id == "udf")
                or (
                    isinstance(func, ast.Attribute)
                    and func.attr == "udf"
                    and isinstance(func.value, ast.Name)
                )
            )
            if is_udf:
                return not self._has_use_arrow(node)
        return False

    def _is_udf_call(self, node: ast.Call) -> bool:
        """Return True if this is a udf() or F.udf() call (not as a decorator)."""
        # Direct: udf(lambda ...)
        if isinstance(node.func, ast.Name) and node.func.id == "udf":
            return not self._has_use_arrow(node)
        # Qualified: F.udf(...) or spark.udf.register(...)
        if isinstance(node.func, ast.Attribute):
            if node.func.attr == "udf" and isinstance(node.func.value, ast.Name):
                return not self._has_use_arrow(node)
            if (
                node.func.attr == "register"
                and isinstance(node.func.value, ast.Attribute)
                and node.func.value.attr == "udf"
            ):
                return not self._has_use_arrow(node)
        return False

    @staticmethod
    def _has_use_arrow(node: ast.Call) -> bool:
        """Return True if the call has useArrow=True."""
        for kw in node.keywords:
            if kw.arg == "useArrow" and isinstance(kw.value, ast.Constant) and kw.value.value is True:
                return True
        return False

    def _make_diagnostic(self, node: ast.AST) -> Diagnostic:
        return Diagnostic(
            rule_id=self.rule_id,
            severity=self.severity,
            message="Python UDF detected — consider pandas_udf for better performance",
            explanation=self._EXPLANATION,
            suggestion=self._SUGGESTION,
            line=node.lineno,
            col=node.col_offset,
        )
