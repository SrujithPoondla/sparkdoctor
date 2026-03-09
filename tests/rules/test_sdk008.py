"""Tests for SDK008 — Cross-DataFrame column reference."""
import ast

from sparkdoctor.rules.sdk008_cross_df_column_ref import CrossDataFrameColumnRefRule

RULE = CrossDataFrameColumnRefRule()

_PYSPARK = "from pyspark.sql import SparkSession\n"


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_cross_df_in_select():
    source = _PYSPARK + (
        "df1 = spark.read.parquet('a')\n"
        "df2 = spark.read.parquet('b')\n"
        "result = df2.select(df1.name, df2.age)\n"
    )
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK008"
    assert "df1.name" in results[0].message


def test_cross_df_in_filter():
    source = _PYSPARK + (
        "orders = spark.read.parquet('orders')\n"
        "users = spark.read.parquet('users')\n"
        "result = users.filter(orders.status == 'active')\n"
    )
    results = check(source)
    assert len(results) == 1
    assert "orders.status" in results[0].message


# ── True negative ───────────────────────────────────────────────────────────


def test_same_df_ref():
    source = _PYSPARK + (
        "df = spark.read.parquet('data')\n"
        "result = df.select(df.name, df.age)\n"
    )
    results = check(source)
    assert results == []


def test_string_column_refs():
    source = _PYSPARK + (
        "df1 = spark.read.parquet('a')\n"
        "df2 = spark.read.parquet('b')\n"
        'result = df2.select("name", "age")\n'
    )
    results = check(source)
    assert results == []


def test_no_pyspark_import():
    source = (
        "df1 = some_lib.read('a')\n"
        "df2 = some_lib.read('b')\n"
        "result = df2.select(df1.name)\n"
    )
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_only_one_df_variable():
    source = _PYSPARK + (
        "df = spark.read.parquet('data')\n"
        "result = df.select(df.name)\n"
    )
    results = check(source)
    assert results == []


def test_cross_df_in_withcolumn():
    source = _PYSPARK + (
        "df1 = spark.read.parquet('a')\n"
        "df2 = spark.read.parquet('b')\n"
        "result = df2.withColumn('x', df1.value * 2)\n"
    )
    results = check(source)
    assert len(results) == 1
