"""Tests for SDK009 — Long transformation chain."""

import ast

from sparkdoctor.rules.sdk009_chain_length import ChainLengthRule

RULE = ChainLengthRule()

_PYSPARK = "from pyspark.sql import SparkSession\n"


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


def test_chain_of_6():
    source = _PYSPARK + (
        "result = df.filter(x).withColumn('a', b).groupBy('c').agg(d).filter(e).orderBy('f')\n"
    )
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK009"
    assert "6" in results[0].message


def test_chain_of_8():
    source = _PYSPARK + (
        "result = (df\n"
        "    .filter(x)\n"
        "    .withColumn('a', b)\n"
        "    .withColumn('c', d)\n"
        "    .groupBy('e')\n"
        "    .agg(f)\n"
        "    .filter(g)\n"
        "    .orderBy('h')\n"
        "    .limit(100))\n"
    )
    results = check(source)
    assert len(results) == 1
    assert "8" in results[0].message


def test_chain_of_3():
    source = _PYSPARK + ("result = df.filter(x).withColumn('a', b).select('c')\n")
    results = check(source)
    assert results == []


def test_chain_of_5_at_threshold():
    source = _PYSPARK + (
        "result = df.filter(x).withColumn('a', b).groupBy('c').agg(d).orderBy('e')\n"
    )
    results = check(source)
    assert results == []


def test_no_pyspark_import():
    source = "result = df.filter(x).withColumn('a', b).groupBy('c').agg(d).filter(e).orderBy('f')\n"
    results = check(source)
    assert results == []


def test_no_duplicate_for_same_chain():
    source = _PYSPARK + ("result = df.a().b().c().d().e().f()\n")
    results = check(source)
    assert len(results) == 1


def test_nested_chains_detected_independently():
    """Two independent long chains — one as argument to the other."""
    source = _PYSPARK + ("result = df.a().b().c().d().e().f(other.x().y().z().w().v().u())\n")
    results = check(source)
    assert len(results) == 2


def test_struct_type_chain_not_flagged():
    """StructType().add().add()... schema builders should not trigger."""
    source = _PYSPARK + (
        "schema = (\n"
        "    StructType()\n"
        "    .add('name', 'string')\n"
        "    .add('age', 'int')\n"
        "    .add('email', 'string')\n"
        "    .add('city', 'string')\n"
        "    .add('state', 'string')\n"
        "    .add('zip', 'string')\n"
        ")\n"
    )
    results = check(source)
    assert results == []


def test_array_type_chain_not_flagged():
    """ArrayType and other type builders should not trigger."""
    source = _PYSPARK + (
        "schema = (\n"
        "    StructType()\n"
        "    .add('a', ArrayType(StringType()).add('x'))\n"
        "    .add('b', 'string')\n"
        "    .add('c', 'string')\n"
        "    .add('d', 'string')\n"
        "    .add('e', 'string')\n"
        "    .add('f', 'string')\n"
        ")\n"
    )
    results = check(source)
    assert results == []
