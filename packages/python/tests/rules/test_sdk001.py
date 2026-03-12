"""Tests for SDK001 — Hardcoded repartition count."""

import ast

from sparkdoctor.rules.sdk001_hardcoded_repartition import HardcodedRepartitionRule

RULE = HardcodedRepartitionRule()


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_detects_hardcoded_repartition():
    source = "df.repartition(200)"
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK001"
    assert results[0].line == 1
    assert "200" in results[0].message


def test_detects_hardcoded_coalesce():
    source = "df.coalesce(50)"
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK001"


# ── True negative ───────────────────────────────────────────────────────────


def test_allows_dynamic_repartition():
    source = "df.repartition(num_partitions)"
    results = check(source)
    assert results == []


def test_ignores_repartition_one():
    """repartition(1) is handled by SDK006, not SDK001."""
    source = "df.repartition(1)"
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_detects_repartition_with_column_arg():
    """repartition(200, 'col') — literal count even with column arg."""
    source = 'df.repartition(200, "country")'
    results = check(source)
    assert len(results) == 1
    assert "200" in results[0].message


def test_allows_function_call_arg():
    source = "df.repartition(calculate_partitions())"
    results = check(source)
    assert results == []
