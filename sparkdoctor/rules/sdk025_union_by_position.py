"""
SDK025 — union() instead of unionByName().

Severity: WARNING
"""
from __future__ import annotations

import ast
from typing import List

from sparkdoctor.lint.base import Category, Diagnostic, Rule, Severity
from sparkdoctor.rules._helpers import is_method_call, receiver_name


class UnionByPositionRule(Rule):
    """Detects .union() calls which match columns by position, not name."""

    rule_id = "SDK025"
    severity = Severity.WARNING
    title = "union() matches columns by position, not by name"
    category = Category.CORRECTNESS

    _EXPLANATION = (
        "union() matches columns by ordinal position, not by name. If the two "
        "DataFrames have the same columns in different order, data silently ends "
        "up in the wrong column. This is a common source of data corruption that "
        "passes all type checks when column types happen to match."
    )

    _SUGGESTION = (
        "Use unionByName() instead — it matches columns by name:\n"
        "  result = df1.unionByName(df2)\n"
        "If schemas may differ, use allowMissingColumns:\n"
        "  result = df1.unionByName(df2, allowMissingColumns=True)"
    )

    _SET_CONSTRUCTORS = {"set", "frozenset"}

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        set_vars = self._find_set_variables(tree)
        diagnostics: list[Diagnostic] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not is_method_call(node, "union"):
                continue
            if self._is_set_union(node, set_vars):
                continue
            diagnostics.append(
                Diagnostic(
                    rule_id=self.rule_id,
                    severity=self.severity,
                    message="union() matches columns by position — "
                    "use unionByName() instead",
                    explanation=self._EXPLANATION,
                    suggestion=self._SUGGESTION,
                    line=node.lineno,
                    col=node.col_offset,
                )
            )
        return diagnostics

    def _find_set_variables(self, tree: ast.AST) -> set[str]:
        """Find variable names assigned from set/frozenset constructors or literals."""
        set_vars: set[str] = set()
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            if self._is_set_expression(node.value):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        set_vars.add(target.id)
            # Also track augmented: for x in ...: s.add(x) patterns
            # by checking if the value is a set method call (.union, .add, etc.)
            if isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute):
                if node.value.func.attr in ("union", "intersection", "difference", "copy"):
                    recv = receiver_name(node.value)
                    if recv and recv in set_vars:
                        for target in node.targets:
                            if isinstance(target, ast.Name):
                                set_vars.add(target.id)
        # Track for-loop targets iterating over lists/tuples of sets
        # e.g. for values1 in [{'a', 'b'}, {'c'}]:
        for node in ast.walk(tree):
            if not isinstance(node, ast.For):
                continue
            if not isinstance(node.target, ast.Name):
                continue
            if isinstance(node.iter, (ast.List, ast.Tuple)):
                if node.iter.elts and all(
                    self._is_set_expression(elt) for elt in node.iter.elts
                ):
                    set_vars.add(node.target.id)
        return set_vars

    def _is_set_expression(self, node: ast.expr) -> bool:
        """Return True if the expression produces a Python set."""
        # {1, 2, 3} or {x for x in ...}
        if isinstance(node, (ast.Set, ast.SetComp)):
            return True
        # set(...) or frozenset(...)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id in self._SET_CONSTRUCTORS:
                return True
        return False

    def _is_set_union(self, node: ast.Call, set_vars: set[str]) -> bool:
        """Return True if this is a Python set/frozenset .union() call."""
        recv = node.func.value
        # {1, 2}.union({3}) or set comprehension
        if isinstance(recv, (ast.Set, ast.SetComp)):
            return True
        # set(items).union(other)
        if isinstance(recv, ast.Call) and isinstance(recv.func, ast.Name):
            if recv.func.id in self._SET_CONSTRUCTORS:
                return True
        # Receiver is a tracked set variable
        name = receiver_name(node)
        if name and name in set_vars:
            return True
        # Check if any argument is a set literal, set constructor, or tracked set var
        for arg in node.args:
            if isinstance(arg, (ast.Set, ast.SetComp)):
                return True
            if isinstance(arg, ast.Call) and isinstance(arg.func, ast.Name):
                if arg.func.id in self._SET_CONSTRUCTORS:
                    return True
            if isinstance(arg, ast.Name) and arg.id in set_vars:
                return True
        return False
