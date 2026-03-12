"""Tests for SDK026 — f-string or .format() in spark.sql()."""

import ast

from sparkdoctor.rules.sdk026_fstring_sql import FStringSqlRule

RULE = FStringSqlRule()


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_detects_fstring_sql():
    source = 'spark.sql(f"SELECT * FROM {table_name}")'
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK026"


def test_detects_format_sql():
    source = 'spark.sql("SELECT * FROM {}".format(table_name))'
    results = check(source)
    assert len(results) == 1


def test_detects_percent_format_sql():
    source = 'spark.sql("SELECT * FROM %s" % table_name)'
    results = check(source)
    assert len(results) == 1


# ── True negative ───────────────────────────────────────────────────────────


def test_allows_static_sql():
    source = 'spark.sql("SELECT * FROM my_table WHERE id = 1")'
    results = check(source)
    assert results == []


def test_allows_non_sql_fstring():
    """f-strings in non-sql calls should not trigger."""
    source = 'print(f"Processing {table_name}")'
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_detects_session_sql_fstring():
    """Any .sql() receiver should be flagged, not just 'spark'."""
    source = 'session.sql(f"DROP TABLE {table}")'
    results = check(source)
    assert len(results) == 1
