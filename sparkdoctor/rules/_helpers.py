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


def find_method_without_limit(
    tree: ast.AST, method_name: str
) -> Iterator[ast.Call]:
    """Yield Call nodes for `something.method_name()` not preceded by `.limit()`.

    Used by SDK002 (collect) and SDK012 (toPandas).
    """
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not is_method_call(node, method_name):
            continue
        receiver = node.func.value
        if isinstance(receiver, ast.Call) and is_method_call(receiver, "limit"):
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
