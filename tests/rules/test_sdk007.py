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


# ── False positive regression ──────────────────────────────────────────────


def test_allows_dask_persist():
    """Dask ddf.persist() should not fire."""
    source = """
ddf = dd.read_parquet("data")
ddf = ddf.persist()
""".strip()
    results = check(source)
    assert results == []


def test_allows_tf_dataset_cache():
    """TensorFlow dataset.cache() should not fire."""
    source = """
dataset = tf.data.Dataset.from_tensor_slices(data)
dataset = dataset.cache()
""".strip()
    results = check(source)
    assert results == []


def test_allows_rdd_cache():
    """RDD cache via sparkContext.parallelize() chain should not fire."""
    source = """
rdd = sc.parallelize([1, 2, 3]).map(lambda x: x * 2).cache()
""".strip()
    results = check(source)
    assert results == []


def test_allows_dataset_cache_with_tf_import():
    """dataset.cache() should not fire when file imports tensorflow."""
    source = """
import tensorflow as tf
dataset = make_petastorm_dataset(reader)
dataset = dataset.cache()
""".strip()
    results = check(source)
    assert results == []


def test_detects_dataset_cache_without_tf_import():
    """dataset.cache() SHOULD fire when no non-Spark imports are present."""
    source = """
dataset = create_dataset()
dataset = dataset.cache()
""".strip()
    results = check(source)
    assert len(results) >= 1


def test_detects_spark_cache_in_mixed_import_file():
    """Spark cache should fire even if file imports TF, when origin is Spark."""
    source = """
import tensorflow as tf
df = spark.read.parquet("data")
df.cache()
""".strip()
    results = check(source)
    assert len(results) == 1


def test_still_detects_spark_cache():
    """Spark DataFrame .cache() without unpersist should still fire."""
    source = """
df.cache()
result = df.count()
""".strip()
    results = check(source)
    assert len(results) == 1
