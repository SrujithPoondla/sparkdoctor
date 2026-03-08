"""Tests for SDK025 — union() instead of unionByName()."""
import ast

from sparkdoctor.rules.sdk025_union_by_position import UnionByPositionRule

RULE = UnionByPositionRule()


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_detects_union():
    source = "result = df1.union(df2)"
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK025"


def test_detects_chained_union():
    source = 'result = df1.filter(condition).union(df2.select("col"))'
    results = check(source)
    assert len(results) == 1


# ── True negative ───────────────────────────────────────────────────────────


def test_allows_union_by_name():
    source = "result = df1.unionByName(df2)"
    results = check(source)
    assert results == []


def test_allows_union_by_name_with_missing_cols():
    source = "result = df1.unionByName(df2, allowMissingColumns=True)"
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_detects_multiple_unions():
    source = """
combined = df1.union(df2).union(df3)
""".strip()
    results = check(source)
    assert len(results) == 2


# ── False positive regression: set literals and constructors ────────────────


def test_allows_set_literal_union():
    """{1, 2}.union({3}) should not fire."""
    source = "result = {1, 2}.union({3})"
    results = check(source)
    assert results == []


def test_allows_set_constructor_union():
    """set(items).union(other) should not fire."""
    source = "result = set(items).union(set(other))"
    results = check(source)
    assert results == []


def test_allows_frozenset_union():
    """frozenset(items).union(other) should not fire."""
    source = "result = frozenset(items).union(other)"
    results = check(source)
    assert results == []


def test_allows_set_variable_union():
    """Variable assigned from set() should not fire."""
    source = """
my_set = set(items)
result = my_set.union(other)
""".strip()
    results = check(source)
    assert results == []


def test_allows_set_literal_variable_union():
    """Variable assigned from {1, 2, 3} should not fire."""
    source = """
values = {1, 2, 3}
result = values.union({4, 5})
""".strip()
    results = check(source)
    assert results == []


def test_allows_set_comprehension_variable_union():
    """Variable assigned from set comprehension should not fire."""
    source = """
included = {x for x in items if x > 0}
result = included.union(other_set)
""".strip()
    results = check(source)
    assert results == []


def test_allows_union_with_set_arg():
    """Union called with a set literal as argument should not fire."""
    source = "result = x.union({1, 2, 3})"
    results = check(source)
    assert results == []


def test_allows_union_with_set_constructor_arg():
    """Union called with set() as argument should not fire."""
    source = "result = x.union(set(items))"
    results = check(source)
    assert results == []


def test_allows_union_with_tracked_set_arg():
    """Union called with a tracked set variable as argument should not fire."""
    source = """
other = set(items)
result = x.union(other)
""".strip()
    results = check(source)
    assert results == []


# ── True positives: should NOT be suppressed ────────────────────────────────


def test_still_detects_df_union():
    """DataFrame .union() should still fire."""
    source = "result = df1.union(df2)"
    results = check(source)
    assert len(results) == 1


def test_still_detects_dataset_union():
    """A variable named 'dataset' should NOT be suppressed — could be a DF."""
    source = "result = dataset.union(other_dataset)"
    results = check(source)
    assert len(results) == 1


def test_still_detects_untracked_variable_union():
    """Variables not assigned from set expressions should still fire."""
    source = "result = unknown_var.union(other_var)"
    results = check(source)
    assert len(results) == 1


def test_allows_for_loop_set_variable_union():
    """for values1 in [{...}, {...}]: values1.union(values2) should not fire."""
    source = """
for values1 in [{'a', 'b'}, {'c'}]:
    for values2 in [{'d', 'e'}, {'f'}]:
        result = values1.union(values2)
""".strip()
    results = check(source)
    assert results == []
