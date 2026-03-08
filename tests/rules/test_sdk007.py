"""Tests for SDK007 — cache()/persist() without unpersist()."""
import ast

from sparkdoctor.rules.sdk007_unpersisted_cache import UnpersistedCacheRule

RULE = UnpersistedCacheRule()


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_detects_cache_without_unpersist():
    source = """
df.cache()
result = df.count()
""".strip()
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK007"


def test_detects_persist_without_unpersist():
    source = """
df.persist()
result = df.count()
""".strip()
    results = check(source)
    assert len(results) == 1


# ── True negative ───────────────────────────────────────────────────────────


def test_allows_cache_with_unpersist():
    source = """
df.cache()
result = df.count()
df.unpersist()
""".strip()
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_allows_persist_with_unpersist():
    source = """
df.persist()
process(df)
df.unpersist()
""".strip()
    results = check(source)
    assert results == []


def test_multiple_cached_one_unpersisted():
    source = """
df1.cache()
df2.cache()
df1.unpersist()
""".strip()
    results = check(source)
    assert len(results) == 1
    # df2 is the unpersisted one


def test_assigned_cache_with_both_unpersisted():
    """cached_df = df.cache(), both names unpersisted — clean."""
    source = """
cached_df = df.cache()
process(cached_df)
cached_df.unpersist()
df.unpersist()
""".strip()
    results = check(source)
    assert results == []


def test_assigned_cache_with_target_unpersisted():
    """cached_df = df.cache(), only target unpersisted — df still flagged."""
    source = """
cached_df = df.cache()
process(cached_df)
cached_df.unpersist()
""".strip()
    results = check(source)
    # df is still tracked as cached but not unpersisted
    assert len(results) == 1


def test_assigned_cache_without_unpersist():
    """cached_df = df.cache() without any unpersist — both flagged."""
    source = """
cached_df = df.cache()
process(cached_df)
""".strip()
    results = check(source)
    assert len(results) == 2


def test_anonymous_chained_cache():
    """df.cache().count() — anonymous cache with no variable to unpersist."""
    source = "df.cache().count()"
    results = check(source)
    assert len(results) == 1
