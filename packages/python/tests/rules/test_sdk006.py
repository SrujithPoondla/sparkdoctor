"""Tests for SDK006 — repartition(1) or coalesce(1)."""

import ast

from sparkdoctor.rules.sdk006_repartition_to_one import RepartitionToOneRule

RULE = RepartitionToOneRule()


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_detects_repartition_one():
    source = "df.repartition(1)"
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK006"


def test_detects_coalesce_one():
    source = "df.coalesce(1)"
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK006"


# ── True negative ───────────────────────────────────────────────────────────


def test_allows_repartition_many():
    """repartition(200) is SDK001's territory, not SDK006."""
    source = "df.repartition(200)"
    results = check(source)
    assert results == []


def test_allows_dynamic_repartition():
    source = "df.repartition(n)"
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_detects_repartition_one_with_column():
    """repartition(1, 'date') — count=1 is wrong even with column arg."""
    source = 'df.repartition(1, "date")'
    results = check(source)
    assert len(results) == 1
