"""Lint engine: loads rules and runs them against an AST."""

from __future__ import annotations

import ast
import re

from sparkdoctor.lint.base import Diagnostic, Rule
from sparkdoctor.rules.registry import get_all_rules

# Matches: # noqa   or   # noqa: SDK001   or   # noqa: SDK001, SDK004
_NOQA_RE = re.compile(r"#\s*noqa(?:\s*:\s*([\w,\s]+))?", re.IGNORECASE)


class LintEngine:
    """Runs all registered rules against a parsed AST."""

    def __init__(
        self,
        rules: list[Rule] | None = None,
        disable: set[str] | None = None,
    ) -> None:
        all_rules = rules if rules is not None else get_all_rules()
        if disable:
            self.rules = [r for r in all_rules if r.rule_id not in disable]
        else:
            self.rules = all_rules

    def check(
        self,
        tree: ast.AST,
        source_lines: list[str],
        language: str = "python",
    ) -> list[Diagnostic]:
        """Run all rules for the given language and return aggregated diagnostics."""
        noqa_map = _parse_noqa_comments(source_lines)
        diagnostics: list[Diagnostic] = []
        for rule in self.rules:
            if rule.language != language:
                continue
            for diag in rule.check(tree, source_lines):
                if not _is_suppressed(diag, noqa_map):
                    diagnostics.append(diag)
        return diagnostics


def _parse_noqa_comments(source_lines: list[str]) -> dict[int, set[str] | None]:
    """Parse noqa comments from source lines.

    Returns a dict mapping 1-indexed line numbers to either:
    - None: bare ``# noqa`` suppresses all rules on that line
    - set of rule IDs: ``# noqa: SDK001, SDK004`` suppresses only those
    """
    result: dict[int, set[str] | None] = {}
    for i, line in enumerate(source_lines, start=1):
        match = _NOQA_RE.search(line)
        if match:
            codes_str = match.group(1)
            if codes_str:
                codes = {c.strip().upper() for c in codes_str.split(",")}
                result[i] = codes
            else:
                result[i] = None  # bare noqa — suppress all
    return result


def _is_suppressed(diag: Diagnostic, noqa_map: dict[int, set[str] | None]) -> bool:
    """Return True if the diagnostic is suppressed by a noqa comment."""
    if diag.line not in noqa_map:
        return False
    codes = noqa_map[diag.line]
    # None = bare noqa, suppresses everything on this line
    if codes is None:
        return True
    return diag.rule_id in codes
