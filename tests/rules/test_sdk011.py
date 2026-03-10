"""Tests for SDK011 — Magic literal in filter/when condition."""
import ast

from sparkdoctor.rules.sdk011_magic_literal import MagicLiteralRule

RULE = MagicLiteralRule()

_PYSPARK = "from pyspark.sql import SparkSession\nimport pyspark.sql.functions as F\n"


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


def test_string_literal_in_filter():
    source = _PYSPARK + 'df.filter(F.col("status") == "active")\n'
    results = check(source)
    assert len(results) >= 1
    assert any(r.rule_id == "SDK011" for r in results)
    assert any("active" in r.message for r in results)


def test_int_literal_in_filter():
    source = _PYSPARK + 'df.filter(F.col("tier") > 3)\n'
    results = check(source)
    assert any(r.rule_id == "SDK011" and "3" in r.message for r in results)


def test_literal_in_when():
    source = _PYSPARK + 'df.withColumn("label", F.when(F.col("score") < 75, "fail"))\n'
    results = check(source)
    assert any("75" in r.message for r in results)
    assert any("fail" in r.message for r in results)


def test_literal_in_where():
    source = _PYSPARK + 'df.where(F.col("count") == 42)\n'
    results = check(source)
    assert len(results) >= 1


def test_named_constant():
    source = _PYSPARK + (
        'ACTIVE_STATUS = "active"\n'
        'df.filter(F.col("status") == ACTIVE_STATUS)\n'
    )
    results = check(source)
    assert not any("active" in r.message for r in results)


def test_allowed_literals():
    source = _PYSPARK + (
        'df.filter(F.col("flag") == True)\n'
        'df.filter(F.col("count") > 0)\n'
        'df.filter(F.col("count") > 1)\n'
    )
    results = check(source)
    assert results == []


def test_no_pyspark_import():
    source = 'df.filter(col("status") == "active")\n'
    results = check(source)
    assert results == []


def test_multiple_magic_literals_same_filter():
    source = _PYSPARK + (
        'df.filter((F.col("status") == "active") & (F.col("tier") > 5))\n'
    )
    results = check(source)
    assert any("active" in r.message for r in results)
    assert any("5" in r.message for r in results)
