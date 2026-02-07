"""CLI app that is exposed to the project root."""

import typer
from performance.src.mock_data.cli import app as mock_app

app = typer.Typer(rich_markup_mode="rich")

app.add_typer(mock_app, name="mock", help="Generates mock datasets that can be used for performance testing")
