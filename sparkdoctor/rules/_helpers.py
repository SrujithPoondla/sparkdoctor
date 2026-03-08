"""Shared AST utility functions for rule implementations."""
from __future__ import annotations

import ast


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
