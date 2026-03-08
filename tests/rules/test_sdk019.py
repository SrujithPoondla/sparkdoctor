"""Tests for SDK019 — inferSchema=True in production read."""
import ast

from sparkdoctor.rules.sdk019_infer_schema import InferSchemaRule

RULE = InferSchemaRule()


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_infer_schema_csv():
    source = 'df = spark.read.csv("data.csv", inferSchema=True)'
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK019"


def test_infer_schema_json():
    source = 'df = spark.read.json("data.json", inferSchema=True)'
    results = check(source)
    assert len(results) == 1


def test_infer_schema_option_style():
    source = 'df = spark.read.option("inferSchema", "true").csv("data.csv")'
    # This uses option() not keyword — rule only catches keyword form
    results = check(source)
    assert results == []


# ── True negative ───────────────────────────────────────────────────────────


def test_explicit_schema():
    source = 'df = spark.read.schema(my_schema).csv("data.csv")'
    results = check(source)
    assert results == []


def test_infer_schema_false():
    source = 'df = spark.read.csv("data.csv", inferSchema=False)'
    results = check(source)
    assert results == []


def test_parquet_no_infer():
    source = 'df = spark.read.parquet("data.parquet")'
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_infer_schema_with_other_options():
    source = 'df = spark.read.csv("data.csv", header=True, inferSchema=True, sep=",")'
    results = check(source)
    assert len(results) == 1
