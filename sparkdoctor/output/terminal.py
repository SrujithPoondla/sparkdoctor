"""Rich-based terminal renderer for SparkDoctor diagnostics."""
from __future__ import annotations

from collections import defaultdict

from rich.console import Console
from rich.panel import Panel

from sparkdoctor import __version__
from sparkdoctor.lint.base import Diagnostic, Severity

_SEVERITY_ICONS = {
    Severity.ERROR: "\u2716",    # ✖
    Severity.WARNING: "\u26a0",  # ⚠
    Severity.INFO: "\u2139",     # ℹ
}

_SEVERITY_STYLES = {
    Severity.ERROR: "bold red",
    Severity.WARNING: "bold yellow",
    Severity.INFO: "bold blue",
}


def render(
    diagnostics: list[Diagnostic],
    file_count: int,
    console: Console | None = None,
) -> None:
    """Render diagnostics to the terminal using Rich."""
    if console is None:
        console = Console()

    # Group by filename
    by_file: dict[str, list[Diagnostic]] = defaultdict(list)
    for d in diagnostics:
        by_file[d.filename].append(d)

    # Header
    filenames = ", ".join(by_file.keys()) if by_file else "no files"
    header = f" SparkDoctor v{__version__} "
    console.print(Panel(f"Scanning: {filenames}", title=header, expand=True))
    console.print()

    # Diagnostics per file
    for filename, file_diags in by_file.items():
        console.print(f"  [bold]{filename}[/bold]")
        console.print()
        for d in sorted(file_diags, key=lambda x: x.line):
            icon = _SEVERITY_ICONS[d.severity]
            style = _SEVERITY_STYLES[d.severity]
            console.print(
                f"  [{style}]{icon}[/{style}]  "
                f"line {d.line}  "
                f"[dim]{d.rule_id}[/dim]  "
                f"{d.message}"
            )
            console.print(f"     {d.explanation}")
            console.print(f"     [green]Fix:[/green] {d.suggestion}")
            console.print()

    # Summary
    error_count = sum(1 for d in diagnostics if d.severity == Severity.ERROR)
    warning_count = sum(1 for d in diagnostics if d.severity == Severity.WARNING)
    info_count = sum(1 for d in diagnostics if d.severity == Severity.INFO)

    parts = []
    if error_count:
        parts.append(f"{error_count} error{'s' if error_count != 1 else ''}")
    if warning_count:
        parts.append(f"{warning_count} warning{'s' if warning_count != 1 else ''}")
    if info_count:
        parts.append(f"{info_count} info")

    summary_detail = f"  ({', '.join(parts)})" if parts else ""
    total = len(diagnostics)
    file_label = "file" if file_count == 1 else "files"

    console.rule()
    console.print(
        f"  {total} finding{'s' if total != 1 else ''}{summary_detail}"
        f"  in {file_count} {file_label}"
    )
    console.rule()
