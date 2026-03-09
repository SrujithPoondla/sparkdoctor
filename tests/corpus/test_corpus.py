"""Corpus tests — validate rules against annotated PySpark snippets.

Each .py file in tests/corpus/ is a self-contained PySpark snippet with
``# expect: SDK0XX`` annotations. This test ensures:
- Every annotated line gets exactly the expected diagnostics (no more, no less)
- Lines annotated ``# expect: none`` produce zero diagnostics
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from tests.corpus.conftest import _ENGINE, collect_corpus_files, parse_expectations


def _run_and_check(path: Path) -> list[str]:
    """Run rules against a corpus file. Return list of failure messages."""
    source = path.read_text()
    source_lines = source.splitlines()
    tree = ast.parse(source, filename=str(path))
    diagnostics = _ENGINE.check(tree, source_lines)

    # Build actual: line -> set of rule IDs
    actual: dict[int, set[str]] = {}
    for diag in diagnostics:
        actual.setdefault(diag.line, set()).add(diag.rule_id)

    expected = parse_expectations(source_lines)
    failures: list[str] = []

    for line_no, expected_ids in expected.items():
        actual_ids = actual.get(line_no, set())
        line_text = source_lines[line_no - 1].rstrip()

        # Check for missing expected diagnostics (false negatives)
        missing = expected_ids - actual_ids
        if missing:
            failures.append(
                f"  line {line_no}: expected {missing} but not fired\n"
                f"    {line_text}"
            )

        # Check for unexpected diagnostics (false positives)
        unexpected = actual_ids - expected_ids
        if unexpected:
            failures.append(
                f"  line {line_no}: unexpected {unexpected} fired\n"
                f"    {line_text}"
            )

    # Also check for diagnostics on lines with NO annotation —
    # these are potential false positives we haven't accounted for.
    annotated_lines = set(expected.keys())
    for line_no, rule_ids in actual.items():
        if line_no not in annotated_lines:
            line_text = source_lines[line_no - 1].rstrip()
            ids_str = ", ".join(sorted(rule_ids))
            failures.append(
                f"  line {line_no}: unannotated diagnostic {rule_ids} — "
                f"add '# expect: {ids_str}' or fix the rule\n"
                f"    {line_text}"
            )

    return failures


def test_corpus_files():
    """Run all corpus files and report all failures together."""
    corpus_files = collect_corpus_files()

    if not corpus_files:
        pytest.skip("No corpus files found in tests/corpus/")

    file_failures: dict[str, list[str]] = {}
    for path in corpus_files:
        failures = _run_and_check(path)
        if failures:
            file_failures[path.name] = failures

    if file_failures:
        issue_count = sum(len(f) for f in file_failures.values())
        msg = f"Corpus test failures ({issue_count} issues):\n"
        for filename, failures in file_failures.items():
            msg += f"\n{filename}:\n" + "\n".join(failures)
        raise AssertionError(msg)
