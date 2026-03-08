"""Tests for SDK002 — collect() without a preceding limit()."""
import ast

from sparkdoctor.rules.sdk002_collect_without_limit import CollectWithoutLimitRule

RULE = CollectWithoutLimitRule()


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_detects_bare_collect():
    source = "df.collect()"
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK002"


def test_detects_chained_collect_without_limit():
    source = "df.filter(condition).collect()"
    results = check(source)
    assert len(results) == 1


# ── True negative ───────────────────────────────────────────────────────────


def test_allows_limit_before_collect():
    source = "df.limit(100).collect()"
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_allows_chained_limit_before_collect():
    source = "df.filter(condition).limit(1000).collect()"
    results = check(source)
    assert results == []


def test_detects_collect_after_non_limit_chain():
    source = "df.filter(condition).select('col').collect()"
    results = check(source)
    assert len(results) == 1
