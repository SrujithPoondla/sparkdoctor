"""Tests for SDK027 — orderBy()/sort() before write."""
import ast

from sparkdoctor.rules.sdk027_orderby_before_write import OrderByBeforeWriteRule

RULE = OrderByBeforeWriteRule()


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_orderby_before_write():
    source = 'df.orderBy("date").write.parquet("path")'
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK027"


def test_sort_before_write():
    source = 'df.sort("date").write.parquet("path")'
    results = check(source)
    assert len(results) == 1


def test_orderby_before_write_stream():
    source = 'df.orderBy("ts").writeStream.start()'
    results = check(source)
    assert len(results) == 1


# ── True negative ───────────────────────────────────────────────────────────


def test_write_without_sort():
    source = 'df.write.parquet("path")'
    results = check(source)
    assert results == []


def test_orderby_without_write():
    source = 'sorted_df = df.orderBy("date")'
    results = check(source)
    assert results == []


def test_sort_within_partitions_before_write():
    source = 'df.sortWithinPartitions("date").write.parquet("path")'
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_orderby_then_other_then_write():
    """orderBy not immediately before write — no flag."""
    source = 'df.orderBy("date").select("a", "b").write.parquet("path")'
    results = check(source)
    assert results == []
