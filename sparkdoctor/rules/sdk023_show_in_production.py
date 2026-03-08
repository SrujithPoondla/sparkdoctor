"""
SDK023 — show() left in production code.

Severity: INFO
"""
from __future__ import annotations

import ast
from typing import List

from sparkdoctor.lint.base import Diagnostic, Rule, Severity
from sparkdoctor.rules._helpers import is_method_call


class ShowInProductionRule(Rule):
    """Detects .show() calls left in production code."""

    rule_id = "SDK023"
    severity = Severity.INFO
    title = "show() left in production code"

    _EXPLANATION = (
        "Each .show() call triggers a Spark action — a full job execution that "
        "materializes the DataFrame and pulls rows to the driver. A pipeline with "
        "multiple show() calls left in production runs extra jobs before the actual "
        "work begins, wasting compute and adding latency."
    )

    _SUGGESTION = (
        "Remove .show() calls from production code, or suppress with "
        "# noqa: SDK023 if intentional.\n"
        "For logging, use:\n"
        "  import logging\n"
        "  logger = logging.getLogger(__name__)\n"
        "  logger.info(f\"Row count: {df.count()}\")"
    )

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        diagnostics: list[Diagnostic] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not is_method_call(node, "show"):
                continue
            diagnostics.append(
                Diagnostic(
                    rule_id=self.rule_id,
                    severity=self.severity,
                    message=".show() triggers a Spark action — "
                    "remove or guard in production code",
                    explanation=self._EXPLANATION,
                    suggestion=self._SUGGESTION,
                    line=node.lineno,
                    col=node.col_offset,
                )
            )
        return diagnostics
