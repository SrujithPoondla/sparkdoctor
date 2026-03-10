"""Tests for SDK018 — Inconsistent column reference style."""
import ast

from sparkdoctor.rules.sdk018_inconsistent_column_ref import InconsistentColumnRefRule

RULE = InconsistentColumnRefRule()

_PYSPARK = "from pyspark.sql import SparkSession\nimport pyspark.sql.functions as F\n"


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_mixed_fcol_and_subscript():
    source = _PYSPARK + (
        "df = spark.read.parquet('/data')\n"
        'df.select(F.col("name"), df["age"])\n'
    )
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK018"
    assert "F.col()" in results[0].message
    assert 'df["col"]' in results[0].message


def test_mixed_fcol_and_dot():
    source = _PYSPARK + (
        "df = spark.read.parquet('/data')\n"
        'x = F.col("name")\n'
        "df.select(F.col(\"name\"), df.custom_field)\n"
    )
    results = check(source)
    assert len(results) == 1
    assert "F.col()" in results[0].message


def test_file_wide_subscript_outside_method():
    """df["col"] outside a DF method call should still be detected."""
    source = _PYSPARK + (
        "df = spark.read.parquet('/data')\n"
        'age = df["age"]\n'
        'name = F.col("name")\n'
    )
    results = check(source)
    assert len(results) == 1
    assert 'df["col"]' in results[0].message
    assert "F.col()" in results[0].message


def test_sf_alias_detected():
    """import pyspark.sql.functions as sf should be detected."""
    source = (
        "from pyspark.sql import SparkSession\n"
        "import pyspark.sql.functions as sf\n"
        "df = spark.read.parquet('/data')\n"
        'df.select(sf.col("name"), df["age"])\n'
    )
    results = check(source)
    assert len(results) == 1


def test_bare_col_import():
    """from pyspark.sql.functions import col should be detected."""
    source = (
        "from pyspark.sql import SparkSession\n"
        "from pyspark.sql.functions import col\n"
        "df = spark.read.parquet('/data')\n"
        'df.select(col("name"), df["age"])\n'
    )
    results = check(source)
    assert len(results) == 1


# ── True negative ───────────────────────────────────────────────────────────


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
        "df = spark.read.parquet('/data')\n"
        'df.select(df["name"], df["age"], df["city"])\n'
    )
    results = check(source)
    assert results == []


def test_config_subscript_not_flagged():
    """config["key"] should NOT be detected as df["col"] style."""
    source = _PYSPARK + (
        'config = {"key": "value"}\n'
        'df.select(F.col("name"))\n'
        'x = config["key"]\n'
    )
    results = check(source)
    assert results == []


def test_f_sum_not_flagged_as_df_col():
    """F.sum() should NOT be detected as df.col style."""
    source = _PYSPARK + (
        "df = spark.read.parquet('/data')\n"
        'df.select(F.col("name"), F.sum("amount"))\n'
    )
    results = check(source)
    assert results == []
