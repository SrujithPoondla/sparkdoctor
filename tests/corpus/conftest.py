"""Corpus-based testing framework for SparkDoctor rules.

Collects all .py files in tests/corpus/ (excluding conftest.py and __init__.py),
runs all rules against each file, and compares fired rule IDs per line against
``# expect: SDK0XX`` annotations.

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

import pytest

from sparkdoctor.lint.engine import LintEngine

_CORPUS_DIR = Path(__file__).parent
_EXPECT_RE = re.compile(r"#\s*expect:\s*(.+?)(?:\s*$)")

# Shared engine instance — all rules, no disabling
_ENGINE = LintEngine()


def _collect_corpus_files() -> list[Path]:
    """Find all .py corpus files (excluding test infrastructure)."""
    skip = {"conftest.py", "__init__.py"}
    return sorted(p for p in _CORPUS_DIR.glob("*.py") if p.name not in skip)


def _parse_expectations(source_lines: list[str]) -> dict[int, set[str] | None]:
    """Parse ``# expect:`` annotations from source lines.

    Returns a dict mapping 1-indexed line numbers to:
    - set of rule IDs (e.g. {"SDK002", "SDK003"})
    - empty set for ``# expect: none`` (meaning zero diagnostics expected)
    """
    expectations: dict[int, set[str] | None] = {}
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


def _run_corpus_file(path: Path) -> tuple[dict[int, set[str]], dict[int, set[str] | None]]:
    """Run all rules against a corpus file and return (actual, expected).

    Returns:
        actual: dict mapping line number -> set of rule IDs that fired
        expected: dict mapping line number -> set of expected rule IDs (from annotations)
    """
    source = path.read_text()
    source_lines = source.splitlines()
    tree = ast.parse(source, filename=str(path))
    diagnostics = _ENGINE.check(tree, source_lines)

    actual: dict[int, set[str]] = {}
    for diag in diagnostics:
        actual.setdefault(diag.line, set()).add(diag.rule_id)

    expected = _parse_expectations(source_lines)
    return actual, expected


# ── Parametrized test collection ──────────────────────────────────────────

corpus_files = _collect_corpus_files()


@pytest.fixture(params=corpus_files, ids=[f.stem for f in corpus_files])
def corpus_result(request: pytest.FixtureRequest):
    """Run rules against a corpus file and return (path, actual, expected)."""
    path = request.param
    actual, expected = _run_corpus_file(path)
    return path, actual, expected


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Generate one test per corpus file."""
    if "corpus_file" not in metafunc.fixturenames:
        return
    files = _collect_corpus_files()
    metafunc.parametrize("corpus_file", files, ids=[f.stem for f in files])
