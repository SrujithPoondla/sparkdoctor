"""Tests for SDK003 — count() used as an emptiness check."""

import ast

from sparkdoctor.rules.sdk003_count_as_emptiness_check import CountAsEmptinessCheckRule

RULE = CountAsEmptinessCheckRule()


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_detects_count_equals_zero():
    source = "df.count() == 0"
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK003"


def test_detects_count_greater_than_zero():
    source = "df.count() > 0"
    results = check(source)
    assert len(results) == 1


# ── True negative ───────────────────────────────────────────────────────────


def test_allows_count_assignment():
    source = "row_count = df.count()"
    results = check(source)
    assert results == []


def test_allows_print_count():
    source = "print(df.count())"
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_detects_reversed_comparison():
    """0 == df.count() — left side is the literal."""
    source = "0 == df.count()"
    results = check(source)
    assert len(results) == 1


def test_detects_count_in_complex_comparison():
    source = "df.filter(condition).count() >= 1"
    results = check(source)
    assert len(results) == 1


# ── False positive regression ──────────────────────────────────────────────


def test_allows_list_count_with_argument():
    """Python list.count(value) takes an argument — should not fire."""
    source = "dtypes.count(dtypes[0]) == len(dtypes)"
    results = check(source)
    assert results == []


def test_allows_string_count():
    """str.count(sub) should not fire."""
    source = 'text.count(",") > 3'
    results = check(source)
    assert results == []
