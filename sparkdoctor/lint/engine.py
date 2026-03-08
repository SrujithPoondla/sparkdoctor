"""Lint engine: loads rules and runs them against an AST."""
from __future__ import annotations

import ast
from typing import List

from sparkdoctor.lint.base import Diagnostic, Rule
from sparkdoctor.rules.registry import get_all_rules


class LintEngine:
    """Runs all registered rules against a parsed AST."""

    def __init__(self, rules: list[Rule] | None = None) -> None:
        self.rules = rules if rules is not None else get_all_rules()

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        """Run all rules and return aggregated diagnostics."""
        diagnostics: list[Diagnostic] = []
        for rule in self.rules:
            diagnostics.extend(rule.check(tree, source_lines))
        return diagnostics
