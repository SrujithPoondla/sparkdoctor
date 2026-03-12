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

    When an ``# expect:`` annotation appears on a closing-paren line (e.g.
    ``)``, the expectation is applied to the preceding non-empty code line.
    This handles ruff format wrapping multi-line expressions.
    """
    expectations: dict[int, set[str]] = {}
    for i, line in enumerate(source_lines, start=1):
        match = _EXPECT_RE.search(line)
        if not match:
            continue
        value = match.group(1).strip().lower()
        if value == "none":
            codes: set[str] = set()
        else:
            codes = {c.strip().upper() for c in match.group(1).split(",")}

        # If the code portion of this line is just a closing paren/bracket,
        # apply the expectation to the preceding code line instead.
        # Walk past nested closing delimiters to find the actual code line.
        closing_only = {")", "]", "}", "):", "),", "],", "},"}
        code_part = line.split("#")[0].strip()
        target_line = i
        if code_part in closing_only:
            for j in range(i - 1, 0, -1):
                prev_code = source_lines[j - 1].split("#")[0].strip()
                if prev_code and not prev_code.startswith("#") and prev_code not in closing_only:
                    target_line = j
                    break

        expectations[target_line] = codes
    return expectations
