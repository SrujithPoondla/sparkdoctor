"""Shared AST utility functions for rule implementations."""
from __future__ import annotations

import ast
from collections.abc import Iterator


def is_method_call(node: ast.Call, method_name: str) -> bool:
    """Check if a Call node is `something.method_name(...)`."""
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == method_name
    )


def first_arg_int(node: ast.Call) -> int | None:
    """Return the integer value of the first argument if it's a literal int."""
    if node.args and isinstance(node.args[0], ast.Constant):
        v = node.args[0].value
        if isinstance(v, int):
            return v
    return None


def receiver_name(node: ast.Call) -> str | None:
    """Get the receiver variable name of a method call (e.g. df.method() -> 'df')."""
    if isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
        return node.func.value.id
    return None


def chain_root_name(node: ast.AST) -> str | None:
    """Return the root variable name of a method call chain.

    For ``a.b().c().d()``, returns ``'a'``.
    """
    current = node
    while True:
        if isinstance(current, ast.Call):
            current = current.func if isinstance(current.func, ast.Attribute) else current.func
        if isinstance(current, ast.Attribute):
            current = current.value
        elif isinstance(current, ast.Name):
            return current.id
        else:
            return None


def chain_contains_method(node: ast.AST, method_names: set[str]) -> bool:
    """Return True if any method in the receiver chain has a name in *method_names*."""
    current = node
    while True:
        if isinstance(current, ast.Call) and isinstance(current.func, ast.Attribute):
            if current.func.attr in method_names:
                return True
            current = current.func.value
        elif isinstance(current, ast.Attribute):
            if current.attr in method_names:
                return True
            current = current.value
        else:
            return False


# Methods that indicate the receiver chain is an RDD, not a DataFrame.
_RDD_CHAIN_METHODS = {
    "parallelize", "textFile", "wholeTextFiles",
    "map", "flatMap", "reduceByKey", "groupByKey",
    "mapPartitions", "mapValues", "sortByKey",
}


def _has_pyspark_import(tree: ast.AST) -> bool:
    """Return True if the file imports pyspark."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] == "pyspark":
                    return True
        elif (
            isinstance(node, ast.ImportFrom)
            and node.module
            and node.module.split(".")[0] == "pyspark"
        ):
            return True
    return False


def _find_non_spark_variables(tree: ast.AST) -> set[str]:
    """Find variable names assigned from non-Spark chains (structural detection).

    Detects patterns like ``rdd = sc.parallelize(...).map(...)`` and marks
    ``rdd`` as non-Spark so that ``rdd.collect()`` is not flagged.
    """
    non_df_methods = _RDD_CHAIN_METHODS
    non_spark_vars: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not isinstance(node.value, ast.Call):
            continue
        if chain_contains_method(node.value, non_df_methods):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    non_spark_vars.add(target.id)
    return non_spark_vars


def find_method_without_limit(
    tree: ast.AST, method_name: str
) -> Iterator[ast.Call]:
    """Yield Call nodes for `something.method_name()` not preceded by `.limit()`.

    Skips files without pyspark imports entirely.
    Skips calls on RDD chains and variables assigned from RDD operations.
    Used by SDK002 (collect) and SDK012 (toPandas).
    """
    if not _has_pyspark_import(tree):
        return

    non_spark_vars = _find_non_spark_variables(tree)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not is_method_call(node, method_name):
            continue
        receiver = node.func.value
        if isinstance(receiver, ast.Call) and is_method_call(receiver, "limit"):
            continue
        # Skip RDD chains (structural)
        if chain_contains_method(node, _RDD_CHAIN_METHODS):
            continue
        # Skip variables assigned from RDD operations
        name = receiver_name(node)
        if name and name in non_spark_vars:
            continue
        yield node


def find_repartition_coalesce_calls(
    tree: ast.AST,
) -> Iterator[tuple[ast.Call, int]]:
    """Yield (Call node, literal int N) for repartition(N)/coalesce(N).

    Used by SDK001 (N > 1) and SDK006 (N == 1).
    """
    methods = {"repartition", "coalesce"}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr not in methods:
            continue
        n = first_arg_int(node)
        if n is not None:
            yield node, n


def find_config_set_calls(
    tree: ast.AST, config_key: str
) -> Iterator[ast.Call]:
    """Yield Call nodes for `.set(config_key, ...)` or `.config(config_key, ...)`.

    Used by SDK014 (AQE disabled) and SDK015 (shuffle partitions).
    """
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr not in ("set", "config"):
            continue
        if len(node.args) < 2:
            continue
        first_arg = node.args[0]
        if not isinstance(first_arg, ast.Constant):
            continue
        if first_arg.value != config_key:
            continue
        yield node
