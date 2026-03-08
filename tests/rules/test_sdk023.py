"""Tests for SDK023 — show() left in production code."""
import ast

from sparkdoctor.rules.sdk023_show_in_production import ShowInProductionRule

RULE = ShowInProductionRule()


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_detects_bare_show():
    source = "df.show()"
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK023"


def test_detects_show_with_args():
    source = "df.show(20, truncate=False)"
    results = check(source)
    assert len(results) == 1


# ── True negative ───────────────────────────────────────────────────────────


def test_allows_non_df_show():
    """A regular function called 'show' should not trigger if not a method call."""
    source = "result = compute_result()"
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_detects_chained_show():
    source = 'df.filter(F.col("status") == "active").show(5)'
    results = check(source)
    assert len(results) == 1


def test_detects_multiple_shows():
    source = """
df1.show()
df2.show(10)
""".strip()
    results = check(source)
    assert len(results) == 2


def test_allows_guarded_show():
    """show() inside an if block should not be flagged."""
    source = """
if DEBUG_MODE:
    df.show(5)
""".strip()
    results = check(source)
    assert results == []


def test_allows_show_in_if_else():
    source = """
if verbose:
    df.show()
else:
    pass
""".strip()
    results = check(source)
    assert results == []
