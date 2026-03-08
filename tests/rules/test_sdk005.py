"""Tests for SDK005 — Python UDF without Arrow optimization."""
import ast

from sparkdoctor.rules.sdk005_python_udf import PythonUdfRule

RULE = PythonUdfRule()


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_detects_udf_decorator():
    source = """
@udf(returnType=StringType())
def my_func(x):
    return x.upper()
""".strip()
    results = check(source)
    assert len(results) >= 1
    assert any(r.rule_id == "SDK005" for r in results)


def test_detects_udf_lambda_call():
    source = "transform = udf(lambda x: x * 2, IntegerType())"
    results = check(source)
    assert len(results) >= 1
    assert any(r.rule_id == "SDK005" for r in results)


# ── True negative ───────────────────────────────────────────────────────────


def test_allows_pandas_udf():
    source = """
@pandas_udf(returnType=StringType())
def my_func(x):
    return x.str.upper()
""".strip()
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_detects_f_udf_call():
    source = "transform = F.udf(lambda x: x * 2, IntegerType())"
    results = check(source)
    assert len(results) >= 1


def test_allows_non_udf_decorator():
    source = """
@staticmethod
def my_func(x):
    return x
""".strip()
    results = check(source)
    assert results == []
