"""Tests for SDK029 — DataFrame write without explicit .mode()."""

import ast

from sparkdoctor.rules.sdk029_write_without_mode import WriteWithoutModeRule

RULE = WriteWithoutModeRule()


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_write_parquet_without_mode():
    source = """\
from pyspark.sql import SparkSession
df.write.parquet("s3://bucket/output/")
"""
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK029"


def test_write_format_save_without_mode():
    source = """\
from pyspark.sql import SparkSession
df.write.format("delta").save("path")
"""
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK029"


def test_write_partition_by_without_mode():
    source = """\
from pyspark.sql import SparkSession
df.write.partitionBy("date").csv("path")
"""
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK029"


def test_write_save_as_table_without_mode():
    source = """\
from pyspark.sql import SparkSession
df.write.saveAsTable("my_table")
"""
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK029"


def test_writer_alias_without_mode():
    """DataFrameWriter alias pattern — no .mode() anywhere."""
    source = """\
from pyspark.sql import SparkSession
writer = df.write.option("mergeSchema", "true")
writer.parquet("path")
"""
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK029"
    assert results[0].line == 3


def test_writer_alias_with_options_without_mode():
    """Alias with chained options but no .mode()."""
    source = """\
from pyspark.sql import SparkSession
writer = df.write.format("delta").option("key", "val")
writer.save("path")
"""
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK029"


# ── True negative ───────────────────────────────────────────────────────────


def test_write_parquet_with_mode():
    source = """\
from pyspark.sql import SparkSession
df.write.mode("overwrite").parquet("s3://bucket/output/")
"""
    results = check(source)
    assert results == []


def test_write_format_save_with_mode():
    source = """\
from pyspark.sql import SparkSession
df.write.format("delta").mode("append").save("path")
"""
    results = check(source)
    assert results == []


def test_write_option_then_mode():
    source = """\
from pyspark.sql import SparkSession
df.write.option("mergeSchema", "true").mode("overwrite").parquet("path")
"""
    results = check(source)
    assert results == []


def test_write_stream_not_flagged():
    source = """\
from pyspark.sql import SparkSession
df.writeStream.format("kafka").start()
"""
    results = check(source)
    assert results == []


def test_insert_into_not_flagged():
    source = """\
from pyspark.sql import SparkSession
df.write.insertInto("table")
"""
    results = check(source)
    assert results == []


def test_writer_alias_with_mode_in_assignment():
    """mode() set during alias assignment — OK."""
    source = """\
from pyspark.sql import SparkSession
writer = df.write.mode("overwrite").option("mergeSchema", "true")
writer.parquet("path")
"""
    results = check(source)
    assert results == []


def test_writer_alias_with_mode_in_call_chain():
    """mode() set on the alias call chain — OK."""
    source = """\
from pyspark.sql import SparkSession
writer = df.write.option("mergeSchema", "true")
writer.mode("overwrite").parquet("path")
"""
    results = check(source)
    assert results == []


# ── Edge cases ──────────────────────────────────────────────────────────────


def test_no_pyspark_import_not_flagged():
    source = 'df.write.parquet("path")'
    results = check(source)
    assert results == []


def test_mode_before_format():
    """mode() before format() — still OK."""
    source = """\
from pyspark.sql import SparkSession
df.write.mode("overwrite").format("parquet").save("path")
"""
    results = check(source)
    assert results == []


def test_multiple_writes_some_with_mode():
    source = """\
from pyspark.sql import SparkSession
df.write.mode("overwrite").parquet("path1")
df.write.parquet("path2")
"""
    results = check(source)
    assert len(results) == 1
    assert results[0].line == 3
