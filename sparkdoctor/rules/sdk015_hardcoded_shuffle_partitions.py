"""
SDK015 — Hardcoded spark.sql.shuffle.partitions.

Severity: WARNING
"""
from __future__ import annotations

import ast

from sparkdoctor.lint.base import Diagnostic, Rule, Severity
from sparkdoctor.rules._helpers import find_config_set_calls


class HardcodedShufflePartitionsRule(Rule):
    """Detects hardcoded spark.sql.shuffle.partitions configuration."""

    rule_id = "SDK015"
    severity = Severity.WARNING
    title = "Hardcoded spark.sql.shuffle.partitions"

    _EXPLANATION = (
        "Setting spark.sql.shuffle.partitions to a fixed value (commonly 200, the "
        "old default) overrides AQE's ability to dynamically size shuffle partitions. "
        "A count tuned for one data size becomes wrong as data grows or shrinks."
    )

    _SUGGESTION = (
        "Remove the hardcoded setting and let AQE handle it automatically "
        "(spark.sql.adaptive.enabled=true is the default in Spark 3.2+). "
        "AQE will coalesce small partitions and split large ones at runtime.\n"
        "If you must set it, derive from data size rather than hardcoding."
    )

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
