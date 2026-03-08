"""Auto-discovers and registers all Rule subclasses from local modules and entry points."""
from __future__ import annotations

import importlib
import logging
import pkgutil
import sys

from sparkdoctor.lint.base import Parser, PythonParser, Rule

_EXCLUDE = {"__init__", "registry", "_helpers"}
logger = logging.getLogger(__name__)


def get_all_rules() -> list[Rule]:
    """Import all rule modules and return instantiated Rule subclasses.

    Discovery sources (in order):
    1. Local modules in ``sparkdoctor/rules/``
    2. External packages registered under the ``sparkdoctor.rules`` entry point group
    """
    import sparkdoctor.rules as rules_pkg

    rules: list[Rule] = []
    seen_ids: dict[str, str] = {}  # rule_id -> class_name

    # 1. Local rule modules
    for module_info in pkgutil.iter_modules(rules_pkg.__path__):
        if module_info.name in _EXCLUDE or module_info.name.startswith("_"):
            continue
        module = importlib.import_module(f"sparkdoctor.rules.{module_info.name}")
        _collect_rules_from_module(module, rules, seen_ids)

    # 2. Entry point plugins
    for ep in _iter_entry_points("sparkdoctor.rules"):
        try:
            module = ep.load()
            _collect_rules_from_module(module, rules, seen_ids)
        except Exception as exc:
            logger.warning("failed to load rule plugin %s: %s", ep.name, exc)

    return rules


def get_all_parsers() -> dict[str, Parser]:
    """Return a mapping of language name to Parser instance.

    Discovery sources:
    1. Built-in PythonParser
    2. External packages registered under the ``sparkdoctor.parsers`` entry point group
    """
    parsers: dict[str, Parser] = {}

    # Built-in
    py_parser = PythonParser()
    parsers[py_parser.language] = py_parser

    # Entry point plugins
    for ep in _iter_entry_points("sparkdoctor.parsers"):
        try:
            parser_cls = ep.load()
            if isinstance(parser_cls, type) and issubclass(parser_cls, Parser):
                instance = parser_cls()
                parsers[instance.language] = instance
            else:
                logger.warning(
                    "parser plugin %s is not a Parser subclass, skipping", ep.name
                )
        except Exception as exc:
            logger.warning("failed to load parser plugin %s: %s", ep.name, exc)

    return parsers


def _collect_rules_from_module(
    module: object,
    rules: list[Rule],
    seen_ids: dict[str, str],
) -> None:
    """Find Rule subclasses in a module, instantiate them, and append to rules list."""
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if (
            isinstance(attr, type)
            and issubclass(attr, Rule)
            and attr is not Rule
            and not getattr(attr, "__abstractmethods__", None)
        ):
            instance = attr()
            if instance.rule_id in seen_ids:
                raise ValueError(
                    f"Duplicate rule ID '{instance.rule_id}': "
                    f"{attr.__name__} conflicts with {seen_ids[instance.rule_id]}"
                )
            seen_ids[instance.rule_id] = attr.__name__
            rules.append(instance)


def _iter_entry_points(group: str):
    """Yield entry points for a group, compatible with Python 3.9+."""
    if sys.version_info >= (3, 12):
        from importlib.metadata import entry_points
        return entry_points(group=group)
    elif sys.version_info >= (3, 9):
        from importlib.metadata import entry_points
        # Python 3.9-3.11: entry_points() returns a dict
        eps = entry_points()
        if isinstance(eps, dict):
            return eps.get(group, [])
        # Python 3.10+ may support the keyword arg
        return entry_points(group=group)
    return []
