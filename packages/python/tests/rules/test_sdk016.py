"""Tests for SDK016 — crossJoin() usage."""

import ast

from sparkdoctor.rules.sdk016_cross_join import CrossJoinRule

RULE = CrossJoinRule()


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_detects_cross_join():
    source = "result = orders.crossJoin(products)"
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK016"


def test_detects_chained_cross_join():
    source = 'result = df1.filter(condition).crossJoin(df2.select("id"))'
    results = check(source)
    assert len(results) == 1


# ── True negative ───────────────────────────────────────────────────────────


def test_allows_regular_join():
    source = 'result = orders.join(products, on="product_id", how="inner")'
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_detects_cross_join_with_broadcast():
    """crossJoin with broadcast is still flagged — user must acknowledge."""
    source = "result = left.crossJoin(F.broadcast(small_df))"
    results = check(source)
    assert len(results) == 1
