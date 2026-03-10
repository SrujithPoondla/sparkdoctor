"""
SDK011 — Magic literal in filter/when condition.

Severity: WARNING
"""
from __future__ import annotations

import ast

from sparkdoctor.lint.base import Category, Diagnostic, Rule, Severity
from sparkdoctor.rules._helpers import _has_pyspark_import


class MagicLiteralRule(Rule):
    """Detects bare string/int literals in .filter(), .where(), and .when() conditions."""

    rule_id = "SDK011"
    severity = Severity.WARNING
    title = "Magic literal in filter/when condition"
    category = Category.STYLE

    _EXPLANATION = (
        "Magic literals make code brittle and opaque. When a threshold or status "
        "value changes, the developer must find every occurrence. Named constants "
        "centralize business logic and make code self-documenting."
    )

    _SUGGESTION = (
        "Extract the literal to a named constant.\n"
        '  # Bad:  df.filter(F.col("status") == "active")\n'
        '  # Good: ACTIVE_STATUS = "active"\n'
        '  #        df.filter(F.col("status") == ACTIVE_STATUS)'
    )

    _TARGET_METHODS = {"filter", "where", "when"}

    _ALLOWED_VALUES = {0, 1, -1, None, "", "*"}

    # Functions whose string args are column references, not magic literals.
    _COL_REF_FUNCS = {"col", "column"}

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        if not _has_pyspark_import(tree):
            return []

        diagnostics: list[Diagnostic] = []
        seen: set[tuple[int, int]] = set()

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr not in self._TARGET_METHODS:
                continue

            col_ref_ids = self._collect_col_ref_args(node)

            all_args = list(node.args) + [kw.value for kw in node.keywords]
            for arg in all_args:
                for const in self._find_magic_constants(arg, col_ref_ids):
                    key = (const.lineno, const.col_offset)
                    if key in seen:
                        continue
                    seen.add(key)
                    diagnostics.append(
                        Diagnostic(
                            rule_id=self.rule_id,
                            severity=self.severity,
                            message=f"Magic literal {const.value!r} in "
                            f".{node.func.attr}() — extract to a named constant",
                            explanation=self._EXPLANATION,
                            suggestion=self._SUGGESTION,
                            line=const.lineno,
                            col=const.col_offset,
                        )
                    )
        return diagnostics

    @classmethod
    def _collect_col_ref_args(cls, node: ast.AST) -> set[int]:
        """Return ``id()`` of every Constant that is a column-reference arg."""
        excluded: set[int] = set()
        for child in ast.walk(node):
            if not isinstance(child, ast.Call):
                continue
            func = child.func
            name = None
            if isinstance(func, ast.Attribute):
                name = func.attr
            elif isinstance(func, ast.Name):
                name = func.id
            if name in cls._COL_REF_FUNCS:
                for a in child.args:
                    if isinstance(a, ast.Constant):
                        excluded.add(id(a))
        return excluded

    def _find_magic_constants(
        self, node: ast.AST, col_ref_ids: set[int]
    ) -> list[ast.Constant]:
        results = []
        for child in ast.walk(node):
            if not isinstance(child, ast.Constant):
                continue
            if id(child) in col_ref_ids:
                continue
            value = child.value
            if not isinstance(value, (str, int)):
                continue
            if isinstance(value, bool):
                continue
            if value in self._ALLOWED_VALUES:
                continue
            results.append(child)
        return results
