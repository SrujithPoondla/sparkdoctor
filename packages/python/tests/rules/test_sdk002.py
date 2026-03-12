"""Tests for SDK002 — collect() without a preceding limit()."""

import ast

from sparkdoctor.rules.sdk002_collect_without_limit import CollectWithoutLimitRule

RULE = CollectWithoutLimitRule()

_PYSPARK_IMPORT = "from pyspark.sql import SparkSession\n"


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_detects_bare_collect():
    source = _PYSPARK_IMPORT + "df.collect()"
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK002"


def test_detects_chained_collect_without_limit():
    source = _PYSPARK_IMPORT + "df.filter(condition).collect()"
    results = check(source)
    assert len(results) == 1


# ── True negative ───────────────────────────────────────────────────────────


def test_allows_limit_before_collect():
    source = _PYSPARK_IMPORT + "df.limit(100).collect()"
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_allows_chained_limit_before_collect():
    source = _PYSPARK_IMPORT + "df.filter(condition).limit(1000).collect()"
    results = check(source)
    assert results == []


def test_detects_collect_after_non_limit_chain():
    source = _PYSPARK_IMPORT + "df.filter(condition).select('col').collect()"
    results = check(source)
    assert len(results) == 1


# ── False positive regression ──────────────────────────────────────────────


def test_allows_rdd_collect():
    """RDD .collect() via sparkContext.parallelize() should not fire."""
    source = _PYSPARK_IMPORT + "sc.parallelize([1, 2, 3]).map(lambda x: x * 2).collect()"
    results = check(source)
    assert results == []


def test_allows_rdd_textfile_collect():
    """RDD .collect() via textFile should not fire."""
    source = (
        _PYSPARK_IMPORT
        + 'rdd = sc.textFile("data.txt")'
        + '.flatMap(lambda x: x.split(" ")).collect()'
    )
    results = check(source)
    assert results == []


def test_allows_polars_collect():
    """Polars lazy frame .collect() should not fire — no pyspark import."""
    source = """\
import polars as pl
pl.scan_parquet("data.parquet").filter(pl.col("x") > 0).collect()
""".strip()
    results = check(source)
    assert results == []


def test_allows_polars_self_collect():
    """self.data.collect() in a file that imports polars should not fire."""
    source = """\
import polars as pl

class MyFrame:
    def to_list(self):
        return self.root.data.collect()
""".strip()
    results = check(source)
    assert results == []


def test_skips_file_without_pyspark_import():
    """Files without pyspark imports should produce zero findings."""
    source = "df.collect()"
    results = check(source)
    assert results == []


def test_still_detects_spark_df_collect():
    """Spark DataFrame .collect() should still fire with pyspark import."""
    source = _PYSPARK_IMPORT + "df.filter(condition).collect()"
    results = check(source)
    assert len(results) == 1
