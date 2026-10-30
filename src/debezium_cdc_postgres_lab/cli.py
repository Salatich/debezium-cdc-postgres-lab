import typer
from rich.console import Console

app = typer.Typer(
    name="cdc",
    help="CLI runner for Debezium PostgreSQL CDC lab scenarios and operational experiments.",
    no_args_is_help=True,
)
console = Console()

@app.command()
def info() -> None:
    """Display environment status and lab scenarios."""
    console.print("[bold green]Debezium CDC PostgreSQL Lab[/bold green]")
    console.print("Python 3.13 + uv + Debezium + Kafka + Schema Registry")

@app.command()
def status() -> None:
    """Check connectivity to Postgres, Kafka Connect, and Schema Registry."""
    console.print("[yellow]Connectivity check placeholder[/yellow]")

if __name__ == "__main__":
    app()
