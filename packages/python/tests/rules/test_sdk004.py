"""Tests for SDK004 — withColumn() called inside a loop."""
import ast

from sparkdoctor.rules.sdk004_withcolumn_in_loop import WithColumnInLoopRule

RULE = WithColumnInLoopRule()


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_detects_withcolumn_in_for_loop():
    source = """
for col in columns:
    df = df.withColumn(col, F.col(col).cast("string"))
""".strip()
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK004"
    assert results[0].line == 1


def test_detects_withcolumn_in_while_loop():
    source = """
while condition:
    df = df.withColumn("col", expr)
""".strip()
    results = check(source)
    assert len(results) == 1


# ── True negative ───────────────────────────────────────────────────────────


def test_allows_withcolumn_outside_loop():
    source = 'df = df.withColumn("new_col", F.lit(1))'
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_nested_loops_one_diagnostic_per_loop():
    source = """
for col in outer:
    for c in inner:
        df = df.withColumn(c, F.lit(1))
""".strip()
    results = check(source)
    # Both loops contain withColumn (outer contains the inner which has it)
    assert len(results) == 2


def test_loop_without_withcolumn_no_diagnostic():
    source = """
for col in columns:
    print(col)
""".strip()
    results = check(source)
    assert results == []
