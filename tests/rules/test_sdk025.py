"""Tests for SDK025 — union() instead of unionByName()."""
import ast

from sparkdoctor.rules.sdk025_union_by_position import UnionByPositionRule

RULE = UnionByPositionRule()


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_detects_union():
    source = "result = df1.union(df2)"
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK025"


def test_detects_chained_union():
    source = 'result = df1.filter(condition).union(df2.select("col"))'
    results = check(source)
    assert len(results) == 1


# ── True negative ───────────────────────────────────────────────────────────


def test_allows_union_by_name():
    source = "result = df1.unionByName(df2)"
    results = check(source)
    assert results == []


def test_allows_union_by_name_with_missing_cols():
    source = "result = df1.unionByName(df2, allowMissingColumns=True)"
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_detects_multiple_unions():
    source = """
combined = df1.union(df2).union(df3)
""".strip()
    results = check(source)
    assert len(results) == 2
