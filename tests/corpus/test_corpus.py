"""Corpus tests — validate rules against annotated PySpark snippets.

Each .py file in tests/corpus/ is a self-contained PySpark snippet with
``# expect: SDK0XX`` annotations. This test ensures:
- Every annotated line gets exactly the expected diagnostics (no more, no less)
- Lines annotated ``# expect: none`` produce zero diagnostics
"""
from __future__ import annotations

import ast
from collections import defaultdict
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


def _count_corpus_coverage(
    corpus_files: list[Path],
) -> tuple[dict[str, int], dict[str, int]]:
    """Count positive and negative corpus annotations per rule.

    Returns:
        positives: rule_id -> count of ``# expect: SDKXXX`` lines
        negatives: rule_id -> count of ``# expect: none`` lines in files
                   that also have positive annotations for that rule
    """
    positives: dict[str, int] = defaultdict(int)
    negatives: dict[str, int] = defaultdict(int)

    for path in corpus_files:
        source_lines = path.read_text().splitlines()
        expectations = parse_expectations(source_lines)

        # First pass: collect which rules have positives in this file
        file_positive_rules: set[str] = set()
        has_none = False
        for ids in expectations.values():
            if ids:
                file_positive_rules |= ids
                for rule_id in ids:
                    positives[rule_id] += 1
            else:
                has_none = True

        # A file's ``# expect: none`` lines count as negatives for each
        # rule that also has positive annotations in the same file.
        if has_none:
            none_count = sum(1 for ids in expectations.values() if not ids)
            for rule_id in file_positive_rules:
                negatives[rule_id] += none_count

    return positives, negatives


# Minimum corpus annotations required per rule
_MIN_POSITIVES = 1
_MIN_NEGATIVES = 1


def _get_builtin_rule_ids() -> set[str]:
    """Return rule IDs for built-in rules only (excludes entry-point plugins)."""
    return {
        r.rule_id for r in _ENGINE.rules
        if r.__class__.__module__.startswith("sparkdoctor.rules.")
    }


def test_all_rules_have_corpus_coverage():
    """Every built-in rule must have both positive and negative corpus annotations."""
    corpus_files = collect_corpus_files()
    all_rule_ids = _get_builtin_rule_ids()
    positives, negatives = _count_corpus_coverage(corpus_files)

    failures: list[str] = []
    for rule_id in sorted(all_rule_ids):
        p = positives.get(rule_id, 0)
        n = negatives.get(rule_id, 0)
        issues = []
        if p < _MIN_POSITIVES:
            issues.append(f"{p} positives (need {_MIN_POSITIVES}+)")
        if n < _MIN_NEGATIVES:
            issues.append(f"{n} negatives (need {_MIN_NEGATIVES}+)")
        if issues:
            failures.append(f"  {rule_id}: {', '.join(issues)}")

    if failures:
        raise AssertionError(
            "Rules with insufficient corpus coverage:\n"
            + "\n".join(failures)
            + "\n\nAdd a corpus file in tests/corpus/ with both "
            "'# expect: SDKXXX' and '# expect: none' annotations.\n"
            "For accurate negative counting, use single-rule corpus files."
        )
