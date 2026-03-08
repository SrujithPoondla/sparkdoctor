"""Tests for SDK012 — toPandas() without a preceding limit()."""
import ast

from sparkdoctor.rules.sdk012_topandas_without_limit import ToPandasWithoutLimitRule

RULE = ToPandasWithoutLimitRule()

_PYSPARK_IMPORT = "from pyspark.sql import SparkSession\n"


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_detects_bare_topandas():
    source = _PYSPARK_IMPORT + "pandas_df = spark_df.toPandas()"
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK012"


def test_detects_chained_topandas_without_limit():
    source = _PYSPARK_IMPORT + 'pandas_df = spark_df.filter(F.col("status") == "active").toPandas()'
    results = check(source)
    assert len(results) == 1


# ── True negative ───────────────────────────────────────────────────────────


def test_allows_limit_before_topandas():
    source = _PYSPARK_IMPORT + "pandas_df = spark_df.limit(10_000).toPandas()"
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_allows_chained_limit_before_topandas():
    source = _PYSPARK_IMPORT + 'pandas_df = spark_df.filter(condition).limit(100).toPandas()'
    results = check(source)
    assert results == []


def test_detects_topandas_after_non_limit_chain():
    source = _PYSPARK_IMPORT + 'pandas_df = spark_df.filter(condition).select("col").toPandas()'
    results = check(source)
    assert len(results) == 1


def test_skips_file_without_pyspark_import():
    """Files without pyspark imports should produce zero findings."""
    source = "pandas_df = spark_df.toPandas()"
    results = check(source)
    assert results == []
