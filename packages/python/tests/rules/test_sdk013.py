"""Tests for SDK013 — RDD API usage on DataFrame."""
import ast

from sparkdoctor.rules.sdk013_rdd_api_usage import RddApiUsageRule

RULE = RddApiUsageRule()


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_detects_rdd_access():
    source = "rdd = df.rdd.map(lambda row: row.name)"
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK013"


def test_detects_rdd_mappartitions():
    source = "df.rdd.mapPartitions(lambda it: process(it))"
    results = check(source)
    assert len(results) == 1


# ── True negative ───────────────────────────────────────────────────────────


def test_allows_dataframe_api():
    source = 'result = df.withColumn("new", F.col("value") * 2)'
    results = check(source)
    assert results == []


def test_allows_rdd_variable_name():
    """A variable named 'rdd' should not trigger the rule."""
    source = "rdd = some_function()"
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_detects_rdd_in_chain():
    source = 'result = df.rdd.flatMap(lambda x: x).toDF(["col1"])'
    results = check(source)
    assert len(results) == 1
