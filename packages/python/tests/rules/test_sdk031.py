"""Tests for SDK031 — collect() or toPandas() inside a loop."""

import ast

from sparkdoctor.rules.sdk031_collect_in_loop import CollectInLoopRule

RULE = CollectInLoopRule()


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_collect_in_for_loop():
    source = """
for group in groups:
    result = df.filter(col("g") == group).collect()
""".strip()
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK031"
    assert "collect()" in results[0].message


def test_topandas_in_for_loop():
    source = """
for g in groups:
    pdf = df.filter(col("g") == g).toPandas()
""".strip()
    results = check(source)
    assert len(results) == 1
    assert "toPandas()" in results[0].message


def test_collect_in_while_loop():
    source = """
while True:
    batch = df.limit(100).collect()
    break
""".strip()
    results = check(source)
    assert len(results) == 1


# ── True negative ───────────────────────────────────────────────────────────


def test_collect_outside_loop():
    source = "result = df.limit(100).collect()"
    results = check(source)
    assert results == []


def test_topandas_outside_loop():
    source = "pdf = df.limit(1000).toPandas()"
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_multiple_collects_in_loop():
    source = """
for g in groups:
    a = df1.collect()
    b = df2.toPandas()
""".strip()
    results = check(source)
    assert len(results) == 2
    methods = {r.message.split("()")[0] for r in results}
    assert "collect" in methods
    assert "toPandas" in methods


def test_nested_loops_no_duplicates():
    """collect() in inner loop should only be reported once, not per enclosing loop."""
    source = """
for col in columns:
    for row in df.filter(col).collect():
        print(row)
""".strip()
    results = check(source)
    assert len(results) == 1
