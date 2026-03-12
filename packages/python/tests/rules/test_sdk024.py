"""Tests for SDK024 — Streaming read without explicit schema."""

import ast

from sparkdoctor.rules.sdk024_streaming_without_schema import StreamingWithoutSchemaRule

RULE = StreamingWithoutSchemaRule()

_PYSPARK = "from pyspark.sql import SparkSession\n"


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_readstream_without_schema():
    source = _PYSPARK + ('df = spark.readStream.format("json").load("s3://bucket/incoming/")\n')
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK024"


def test_kafka_readstream_without_schema():
    source = _PYSPARK + (
        "df = (spark.readStream\n"
        '    .format("kafka")\n'
        '    .option("kafka.bootstrap.servers", "broker:9092")\n'
        '    .option("subscribe", "events")\n'
        "    .load())\n"
    )
    results = check(source)
    assert len(results) == 1


# ── True negative ───────────────────────────────────────────────────────────


def test_readstream_with_schema():
    source = _PYSPARK + ('df = spark.readStream.schema(my_schema).format("json").load(path)\n')
    results = check(source)
    assert results == []


def test_batch_read_without_schema():
    """Batch reads are not streaming — should not trigger."""
    source = _PYSPARK + ('df = spark.read.format("json").load("s3://bucket/data/")\n')
    results = check(source)
    assert results == []


def test_no_pyspark_import():
    source = 'df = spark.readStream.format("json").load("path")\n'
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_readstream_with_schema_in_middle():
    source = _PYSPARK + (
        "df = (spark.readStream\n"
        "    .schema(event_schema)\n"
        '    .format("json")\n'
        '    .option("maxFilesPerTrigger", 100)\n'
        '    .load("s3://bucket/incoming/"))\n'
    )
    results = check(source)
    assert results == []


def test_readstream_table_without_schema():
    source = _PYSPARK + ('df = spark.readStream.table("my_catalog.events")\n')
    results = check(source)
    assert len(results) == 1
