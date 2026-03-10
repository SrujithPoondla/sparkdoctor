"""Tests for SDK018 — Inconsistent column reference style."""
import ast

from sparkdoctor.rules.sdk018_inconsistent_column_ref import InconsistentColumnRefRule

RULE = InconsistentColumnRefRule()

_PYSPARK = "from pyspark.sql import SparkSession\nimport pyspark.sql.functions as F\n"


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


def test_mixed_fcol_and_subscript():
    source = _PYSPARK + (
        'df.select(F.col("name"), df["age"])\n'
    )
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK018"
    assert "F.col()" in results[0].message
    assert 'df["col"]' in results[0].message


def test_mixed_fcol_and_dot():
    source = _PYSPARK + (
        'x = F.col("name")\n'
        'df.select(F.col("name"), df.custom_field)\n'
    )
    results = check(source)
    assert len(results) == 1
    assert "F.col()" in results[0].message


def test_consistent_fcol():
    source = _PYSPARK + (
        'df.select(F.col("name"), F.col("age"))\n'
    )
    results = check(source)
    assert results == []


def test_consistent_string():
    source = _PYSPARK + (
        'df.select("name", "age")\n'
    )
    results = check(source)
    assert results == []


def test_no_pyspark_import():
    source = 'df.select(F.col("name"), df["age"])\n'
    results = check(source)
    assert results == []


def test_single_style_no_diagnostic():
    source = _PYSPARK + (
        'df.select(df["name"], df["age"], df["city"])\n'
    )
    results = check(source)
    assert results == []
