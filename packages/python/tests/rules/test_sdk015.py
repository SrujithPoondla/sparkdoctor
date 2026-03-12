"""Tests for SDK015 — Hardcoded spark.sql.shuffle.partitions."""

import ast

from sparkdoctor.rules.sdk015_hardcoded_shuffle_partitions import HardcodedShufflePartitionsRule

RULE = HardcodedShufflePartitionsRule()


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_conf_set_shuffle_partitions():
    source = 'spark.conf.set("spark.sql.shuffle.partitions", "200")'
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK015"


def test_conf_set_shuffle_partitions_int():
    source = 'spark.conf.set("spark.sql.shuffle.partitions", 200)'
    results = check(source)
    assert len(results) == 1


def test_config_method():
    source = 'spark.config("spark.sql.shuffle.partitions", "100")'
    results = check(source)
    assert len(results) == 1


# ── True negative ───────────────────────────────────────────────────────────


def test_dynamic_value():
    source = 'spark.conf.set("spark.sql.shuffle.partitions", num_partitions)'
    results = check(source)
    assert results == []


def test_other_config():
    source = 'spark.conf.set("spark.executor.memory", "4g")'
    results = check(source)
    assert results == []


def test_get_config():
    source = 'val = spark.conf.get("spark.sql.shuffle.partitions")'
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_only_one_arg():
    source = 'spark.conf.set("spark.sql.shuffle.partitions")'
    results = check(source)
    assert results == []
