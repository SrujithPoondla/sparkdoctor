"""Auto-discovers and registers all Rule subclasses in the rules package."""
from __future__ import annotations

import importlib
import pkgutil
from typing import List

from sparkdoctor.lint.base import Rule

_EXCLUDE = {"__init__", "registry", "_helpers"}


def get_all_rules() -> list[Rule]:
    """Import all rule modules and return instantiated Rule subclasses."""
    import sparkdoctor.rules as rules_pkg

    rules: list[Rule] = []
    seen_ids: dict[str, str] = {}  # rule_id -> class_name
    for module_info in pkgutil.iter_modules(rules_pkg.__path__):
        if module_info.name in _EXCLUDE or module_info.name.startswith("_"):
            continue
        module = importlib.import_module(f"sparkdoctor.rules.{module_info.name}")
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                isinstance(attr, type)
                and issubclass(attr, Rule)
                and attr is not Rule
            ):
                instance = attr()
                if instance.rule_id in seen_ids:
                    raise ValueError(
                        f"Duplicate rule ID '{instance.rule_id}': "
                        f"{attr.__name__} conflicts with {seen_ids[instance.rule_id]}"
                    )
                seen_ids[instance.rule_id] = attr.__name__
                rules.append(instance)
    return rules
