"""Corpus-based testing framework for SparkDoctor rules.

Collects all .py files in tests/corpus/ (excluding conftest.py, __init__.py, and
test_corpus.py), runs all rules against each file, and compares fired rule IDs
per line against ``# expect: SDK0XX`` annotations.

Annotation format:
    result = df.collect()          # expect: SDK002
    result = df.limit(10).collect()  # expect: none
    x = df.count() == 0            # expect: SDK003, SDK031

- ``# expect: SDK0XX`` — assert that exactly this rule fires on this line
- ``# expect: SDK0XX, SDK0YY`` — assert multiple rules fire on this line
- ``# expect: none`` — assert no rule fires on this line (explicit negative)
- Lines without ``# expect:`` annotations are not checked
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

from sparkdoctor.lint.engine import LintEngine

_CORPUS_DIR = Path(__file__).parent
_EXPECT_RE = re.compile(r"#\s*expect:\s*(.+?)(?:\s*$)")

# Shared engine instance — all rules, no disabling
_ENGINE = LintEngine()

_SKIP = {"conftest.py", "__init__.py", "test_corpus.py"}


def collect_corpus_files() -> list[Path]:
    """Find all .py corpus files (excluding test infrastructure)."""
    return sorted(p for p in _CORPUS_DIR.glob("*.py") if p.name not in _SKIP)


def parse_expectations(source_lines: list[str]) -> dict[int, set[str]]:
    """Parse ``# expect:`` annotations from source lines.

    Returns a dict mapping 1-indexed line numbers to:
    - set of rule IDs (e.g. {"SDK002", "SDK003"})
    - empty set for ``# expect: none`` (meaning zero diagnostics expected)
    """
    expectations: dict[int, set[str]] = {}
    for i, line in enumerate(source_lines, start=1):
        match = _EXPECT_RE.search(line)
        if not match:
            continue
        value = match.group(1).strip().lower()
        if value == "none":
            expectations[i] = set()
        else:
            codes = {c.strip().upper() for c in match.group(1).split(",")}
            expectations[i] = codes
    return expectations
