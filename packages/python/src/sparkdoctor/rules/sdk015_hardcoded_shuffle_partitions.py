"""
SDK015 — Hardcoded spark.sql.shuffle.partitions.

Severity: WARNING
"""

from __future__ import annotations

import ast

from sparkdoctor.lint.base import Diagnostic, Rule
from sparkdoctor.rules._helpers import find_config_set_calls


class HardcodedShufflePartitionsRule(Rule):
    """Detects hardcoded spark.sql.shuffle.partitions configuration."""

    rule_id = "SDK015"

    _CONFIG_KEY = "spark.sql.shuffle.partitions"

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        return [
            Diagnostic(
                rule_id=self.rule_id,
                severity=self.severity,
                message="Hardcoded spark.sql.shuffle.partitions overrides AQE",
                explanation=self._EXPLANATION,
                suggestion=self._SUGGESTION,
                line=node.lineno,
                col=node.col_offset,
            )
            for node in find_config_set_calls(tree, self._CONFIG_KEY)
            if isinstance(node.args[1], ast.Constant)
        ]
