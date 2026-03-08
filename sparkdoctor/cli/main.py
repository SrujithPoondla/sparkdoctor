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
    help="A PySpark performance linter — ESLint for PySpark.",
    add_completion=False,
    invoke_without_command=True,
)


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context) -> None:
    """A PySpark performance linter — ESLint for PySpark."""
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
) -> None:
    """Lint PySpark files for performance anti-patterns."""
    from sparkdoctor.lint.runner import run

    target = Path(path)
    if not target.exists():
        typer.echo(f"sparkdoctor: error: path not found: {path}", err=True)
        raise typer.Exit(code=2)

    disabled = set(r.upper() for r in disable) if disable else None
    diagnostics, file_count = run(
        target, exclude=exclude or (), disable=disabled
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
            raise typer.Exit(code=2)
        diagnostics = [d for d in diagnostics if d.severity >= min_severity]

    # Render output
    if format == OutputFormat.json:
        from sparkdoctor.output.json_output import render

        typer.echo(render(diagnostics))
    else:
        from sparkdoctor.output.terminal import render

        console = Console(no_color=no_color)
        render(diagnostics, file_count, console=console)

    # Exit code
    if exit_code and diagnostics:
        raise typer.Exit(code=1)
