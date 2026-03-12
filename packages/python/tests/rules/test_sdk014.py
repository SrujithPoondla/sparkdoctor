"""Tests for SDK014 — AQE explicitly disabled."""

import ast

from sparkdoctor.rules.sdk014_aqe_disabled import AqeDisabledRule

RULE = AqeDisabledRule()


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_aqe_disabled_string():
    source = 'spark.conf.set("spark.sql.adaptive.enabled", "false")'
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK014"


def test_aqe_disabled_bool():
    source = 'spark.conf.set("spark.sql.adaptive.enabled", False)'
    results = check(source)
    assert len(results) == 1


def test_aqe_disabled_capitalized():
    source = 'spark.conf.set("spark.sql.adaptive.enabled", "False")'
    results = check(source)
    assert len(results) == 1


# ── True negative ───────────────────────────────────────────────────────────


def test_aqe_enabled():
    source = 'spark.conf.set("spark.sql.adaptive.enabled", "true")'
    results = check(source)
    assert results == []


def test_aqe_enabled_bool():
    source = 'spark.conf.set("spark.sql.adaptive.enabled", True)'
    results = check(source)
    assert results == []


def test_other_config():
    source = 'spark.conf.set("spark.executor.memory", "false")'
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_dynamic_value():
    source = 'spark.conf.set("spark.sql.adaptive.enabled", aqe_flag)'
    results = check(source)
    assert results == []
