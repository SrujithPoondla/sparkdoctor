"""Tests for SDK030 — multiple orderBy()/sort() in a chain."""

import ast

from sparkdoctor.rules.sdk030_redundant_sort import RedundantSortRule

PYSPARK_IMPORT = "from pyspark.sql import SparkSession\n"
RULE = RedundantSortRule()


def check(source: str):
    full_source = PYSPARK_IMPORT + source
    tree = ast.parse(full_source)
    return RULE.check(tree, full_source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_double_orderby():
    source = 'df.orderBy("a").orderBy("b")'
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK030"


def test_double_sort():
    source = 'df.sort("a").sort("b")'
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK030"


def test_mixed_sort_orderby():
    source = 'df.orderBy("a").sort("b")'
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK030"


def test_sort_filter_sort():
    """Filter between sorts doesn't preserve order — still redundant."""
    source = 'df.sort("a").filter(df.x > 0).sort("b")'
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK030"


def test_flags_first_sort_not_last():
    """The diagnostic should point at the earlier (redundant) sort."""
    source = 'df.orderBy("date").orderBy("user_id").show()'
    results = check(source)
    assert len(results) == 1
    # The flagged line should be the first orderBy (line 2 after prepended import)
    assert results[0].rule_id == "SDK030"
    assert results[0].line == 2


def test_triple_sort():
    """Three sorts — first two are redundant."""
    source = 'df.sort("a").sort("b").sort("c")'
    results = check(source)
    assert len(results) == 2
    assert all(r.rule_id == "SDK030" for r in results)


# ── True negative ───────────────────────────────────────────────────────────


def test_single_orderby():
    source = 'df.orderBy("a", "b")'
    results = check(source)
    assert results == []


def test_single_sort():
    source = 'df.sort("a")'
    results = check(source)
    assert results == []


def test_sort_within_partitions_then_orderby():
    """sortWithinPartitions is a different operation — should not be flagged."""
    source = 'df.sortWithinPartitions("a").orderBy("b")'
    results = check(source)
    assert results == []


def test_separate_statements():
    """Two separate sort statements — not detected (by design)."""
    source = 'df2 = df.sort("a")\ndf3 = df2.sort("b")'
    results = check(source)
    assert results == []


def test_no_pyspark_import():
    """Without pyspark import, nothing should fire."""
    source = 'df.sort("a").sort("b")'
    tree = ast.parse(source)
    results = RULE.check(tree, source.splitlines())
    assert results == []
