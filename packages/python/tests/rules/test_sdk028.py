"""Tests for SDK028 — distinct().count() two-pass operation."""

import ast

from sparkdoctor.rules.sdk028_distinct_count import DistinctCountRule

RULE = DistinctCountRule()


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_distinct_count():
    source = """\
from pyspark.sql import SparkSession
n = df.select("col").distinct().count()
"""
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK028"
    assert "distinct().count()" in results[0].message


def test_drop_duplicates_count():
    source = """\
from pyspark.sql import SparkSession
n = df.dropDuplicates(["col"]).count()
"""
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK028"
    assert "dropDuplicates().count()" in results[0].message


def test_distinct_count_multiple_columns():
    source = """\
from pyspark.sql import SparkSession
n = df.select("a", "b").distinct().count()
"""
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK028"


# ── True negative ───────────────────────────────────────────────────────────


def test_distinct_alone():
    source = """\
from pyspark.sql import SparkSession
deduped = df.distinct()
"""
    results = check(source)
    assert results == []


def test_count_alone():
    source = """\
from pyspark.sql import SparkSession
n = df.count()
"""
    results = check(source)
    assert results == []


def test_distinct_show():
    source = """\
from pyspark.sql import SparkSession
df.distinct().show()
"""
    results = check(source)
    assert results == []


def test_no_pyspark_import():
    source = """\
n = df.distinct().count()
"""
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_chained_distinct_count():
    source = """\
from pyspark.sql import SparkSession
n = df.filter(df.active).select("user_id").distinct().count()
"""
    results = check(source)
    assert len(results) == 1
