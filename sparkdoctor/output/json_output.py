"""JSON output renderer for SparkDoctor diagnostics."""
from __future__ import annotations

import json

from sparkdoctor.lint.base import Diagnostic


def render(diagnostics: list[Diagnostic]) -> str:
    """Return diagnostics as a JSON string."""
    return json.dumps([d.as_dict() for d in diagnostics], indent=2)
