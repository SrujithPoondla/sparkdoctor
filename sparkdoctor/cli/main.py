"""SparkDoctor CLI — entry point for the lint command."""
from __future__ import annotations

import sys
from enum import Enum
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from sparkdoctor.lint.base import Severity

app = typer.Typer(
    name="sparkdoctor",
    help="A Spark performance linter.",
    add_completion=False,
    invoke_without_command=True,
)


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context) -> None:
    """A Spark performance linter."""
    if ctx.invoked_subcommand is None and not ctx.args:
        typer.echo(ctx.get_help())
        raise typer.Exit()


class OutputFormat(str, Enum):
    terminal = "terminal"
    json = "json"


@app.command()
def lint(
    path: str = typer.Argument(..., help="File or directory to lint"),
    format: OutputFormat = typer.Option(
        OutputFormat.terminal, "--format", "-f", help="Output format"
    ),
    severity: Optional[str] = typer.Option(
        None, "--severity", "-s", help="Minimum severity to show (error|warning|info)"
    ),
    exit_code: bool = typer.Option(
        False, "--exit-code", help="Exit with code 1 if diagnostics are found"
    ),
    no_color: bool = typer.Option(
        False, "--no-color", help="Disable colored output"
    ),
    exclude: Optional[list[str]] = typer.Option(
        None, "--exclude", "-e",
        help="Glob patterns to exclude (e.g. 'tests' or 'test_*')"
    ),
    disable: Optional[list[str]] = typer.Option(
        None, "--disable", "-d",
        help="Rule IDs to disable (e.g. 'SDK023' or 'SDK002')"
    ),
    no_config: bool = typer.Option(
        False, "--no-config",
        help="Ignore pyproject.toml [tool.sparkdoctor] configuration"
    ),
) -> None:
    """Lint PySpark files for performance anti-patterns."""
    from sparkdoctor.config import load_config
    from sparkdoctor.lint.runner import run

    target = Path(path)
    if not target.exists():
        typer.echo(f"sparkdoctor: error: path not found: {path}", err=True)
        raise typer.Exit(code=2)

    # Load config from pyproject.toml (unless --no-config)
    config = load_config(target.parent if target.is_file() else target) if not no_config else None

    # Merge CLI args with config — CLI takes precedence
    disabled = set(r.upper() for r in disable) if disable else set()
    excludes = list(exclude) if exclude else []

    if config:
        disabled |= config.disable
        excludes.extend(e for e in config.exclude if e not in excludes)

    diagnostics, file_count = run(
        target,
        exclude=tuple(excludes),
        disable=disabled or None,
    )

    # Severity filtering
    if severity is not None:
        try:
            min_severity = Severity(severity.lower())
        except ValueError:
            typer.echo(
                f"sparkdoctor: error: invalid severity '{severity}'. "
                "Use error, warning, or info.",
                err=True,
            )
            raise typer.Exit(code=2) from None
        diagnostics = [d for d in diagnostics if d.severity >= min_severity]

    # Render output — check entry point plugins for non-built-in formats
    format_name = format.value
    if format_name == "json":
        from sparkdoctor.output.json_output import render

        typer.echo(render(diagnostics))
    elif format_name == "terminal":
        from sparkdoctor.output.terminal import render

        console = Console(no_color=no_color)
        render(diagnostics, file_count, console=console)
    else:
        # Try entry point output plugins
        renderer = _load_output_plugin(format_name)
        if renderer is None:
            typer.echo(
                f"sparkdoctor: error: unknown format '{format_name}'. "
                "Install a plugin or use 'terminal' or 'json'.",
                err=True,
            )
            raise typer.Exit(code=2)
        typer.echo(renderer(diagnostics))

    # Exit code
    if exit_code and diagnostics:
        raise typer.Exit(code=1)


def _load_output_plugin(format_name: str):
    """Try to load an output renderer from the ``sparkdoctor.outputs`` entry point group."""

    if sys.version_info >= (3, 9):
        from importlib.metadata import entry_points

        eps = entry_points()
        if isinstance(eps, dict):
            candidates = eps.get("sparkdoctor.outputs", [])
        else:
            candidates = entry_points(group="sparkdoctor.outputs")

        for ep in candidates:
            if ep.name == format_name:
                return ep.load()
    return None
