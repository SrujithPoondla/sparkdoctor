"""Tests for SDK020 — DROP TABLE or fs.rm before overwrite write."""
import ast

from sparkdoctor.rules.sdk020_drop_before_overwrite import DropBeforeOverwriteRule

RULE = DropBeforeOverwriteRule()

_PYSPARK = "from pyspark.sql import SparkSession\n"


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


def test_drop_table_before_overwrite():
    source = _PYSPARK + (
        'spark.sql("DROP TABLE IF EXISTS my_table")\n'
        'df.write.format("delta").mode("overwrite").saveAsTable("my_table")\n'
    )
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK020"
    assert "DROP TABLE" in results[0].message


def test_fs_rm_before_overwrite():
    source = _PYSPARK + (
        'dbutils.fs.rm(table_path, recursive=True)\n'
        'df.write.format("delta").mode("overwrite").save(table_path)\n'
    )
    results = check(source)
    assert len(results) == 1
    assert "fs.rm" in results[0].message


def test_fstring_drop_table():
    source = _PYSPARK + (
        'spark.sql(f"DROP TABLE IF EXISTS {table_name}")\n'
        'df.write.mode("overwrite").save(path)\n'
    )
    results = check(source)
    assert len(results) == 1


def test_overwrite_without_drop():
    source = _PYSPARK + (
        'df.write.format("delta").mode("overwrite").save(path)\n'
    )
    results = check(source)
    assert results == []


def test_drop_without_overwrite():
    source = _PYSPARK + (
        'spark.sql("DROP TABLE IF EXISTS old_table")\n'
    )
    results = check(source)
    assert results == []


def test_no_pyspark_import():
    source = (
        'spark.sql("DROP TABLE IF EXISTS my_table")\n'
        'df.write.mode("overwrite").save(path)\n'
    )
    results = check(source)
    assert results == []


def test_drop_far_from_overwrite():
    lines = [_PYSPARK]
    lines.append('spark.sql("DROP TABLE IF EXISTS my_table")\n')
    for i in range(25):
        lines.append(f"x_{i} = {i}\n")
    lines.append('df.write.mode("overwrite").save(path)\n')
    source = "".join(lines)
    results = check(source)
    assert results == []


def test_append_mode_not_flagged():
    source = _PYSPARK + (
        'spark.sql("DROP TABLE IF EXISTS my_table")\n'
        'df.write.mode("append").save(path)\n'
    )
    results = check(source)
    assert results == []
