"""Shared AST utility functions for rule implementations."""
from __future__ import annotations

import ast
from typing import Iterator


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
    if isinstance(node.func, ast.Attribute):
        if isinstance(node.func.value, ast.Name):
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

# Polars lazy-frame chain methods.
_POLARS_CHAIN_METHODS = {
    "scan_parquet", "scan_csv", "scan_ipc", "scan_ndjson", "lazy",
}

# Root variable names that indicate a non-Spark origin.
_NON_SPARK_COLLECT_ROOTS = {"pl", "polars"}


def _find_rdd_variables(tree: ast.AST) -> set[str]:
    """Find variable names assigned from RDD or Polars chains.

    Detects patterns like ``rdd = sc.parallelize(...).map(...)`` and marks
    ``rdd`` as an RDD variable so that ``rdd.collect()`` is not flagged.
    """
    non_df_methods = _RDD_CHAIN_METHODS | _POLARS_CHAIN_METHODS
    rdd_vars: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not isinstance(node.value, ast.Call):
            continue
        # Check if the RHS chain contains RDD/Polars methods
        if chain_contains_method(node.value, non_df_methods):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    rdd_vars.add(target.id)
        # Check if the RHS root is a Polars name
        root = chain_root_name(node.value)
        if root in _NON_SPARK_COLLECT_ROOTS:
            for target in node.targets:
                if isinstance(target, ast.Name):
                    rdd_vars.add(target.id)
    return rdd_vars


def _has_polars_import(tree: ast.AST) -> bool:
    """Return True if the file imports polars or polars-related modules."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                parts = alias.name.split(".")
                if "polars" in parts:
                    return True
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                parts = node.module.split(".")
                if "polars" in parts:
                    return True
    return False


def find_method_without_limit(
    tree: ast.AST, method_name: str
) -> Iterator[ast.Call]:
    """Yield Call nodes for `something.method_name()` not preceded by `.limit()`.

    Skips calls on RDD chains, Polars lazy frames, and variables assigned
    from RDD/Polars operations.
    Used by SDK002 (collect) and SDK012 (toPandas).
    """
    rdd_vars = _find_rdd_variables(tree)
    has_polars = _has_polars_import(tree)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not is_method_call(node, method_name):
            continue
        receiver = node.func.value
        if isinstance(receiver, ast.Call) and is_method_call(receiver, "limit"):
            continue
        # Skip RDD and Polars chains (inline)
        if chain_contains_method(node, _RDD_CHAIN_METHODS | _POLARS_CHAIN_METHODS):
            continue
        if chain_root_name(node) in _NON_SPARK_COLLECT_ROOTS:
            continue
        # Skip variables assigned from RDD/Polars operations
        name = receiver_name(node)
        if name and name in rdd_vars:
            continue
        # In files that import polars, skip self.*.collect() calls
        if has_polars and chain_root_name(node) == "self":
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
