"""Tests for SDK017 — select('*') wildcard."""
import ast

from sparkdoctor.rules.sdk017_select_star import SelectStarRule

RULE = SelectStarRule()


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_select_star_string():
    source = 'df.select("*")'
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK017"


def test_select_star_single_quote():
    source = "df.select('*')"
    results = check(source)
    assert len(results) == 1


# ── True negative ───────────────────────────────────────────────────────────


def test_select_specific_columns():
    source = 'df.select("user_id", "name")'
    results = check(source)
    assert results == []


def test_select_col_expression():
    source = 'df.select(F.col("user_id"))'
    results = check(source)
    assert results == []


def test_select_no_args():
    source = "df.select()"
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_select_star_among_others():
    source = 'df.select("*", "extra_col")'
    results = check(source)
    assert len(results) == 1


def test_non_select_method_with_star():
    source = 'df.filter("*")'
    results = check(source)
    assert results == []


# ── False positive regression ──────────────────────────────────────────────


def test_allows_optimus_cols_select_star():
    """Optimus df.cols.select('*') is a column-type filter, not Spark's select."""
    source = 'df.cols.select("*")'
    results = check(source)
    assert results == []


def test_allows_sub_accessor_select():
    """Any sub-accessor .select('*') should not fire."""
    source = 'df.rows.select("*")'
    results = check(source)
    assert results == []


def test_still_detects_chained_select_star():
    """df.filter(cond).select('*') should still fire."""
    source = 'df.filter(condition).select("*")'
    results = check(source)
    assert len(results) == 1
