"""CLI runner for Debezium PostgreSQL CDC lab scenarios and operational experiments."""

from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from debezium_cdc_postgres_lab.connector import ConnectorClient
from debezium_cdc_postgres_lab.consumer import consume_cdc_events
from debezium_cdc_postgres_lab.db import (
    alter_schema_add_column,
    alter_schema_drop_column,
    delete_customer,
    get_replication_slot_metrics,
    seed_records,
    send_signaling_snapshot,
    stream_traffic_generator,
    update_customer_status,
)

app = typer.Typer(
    name="cdc",
    help="CLI runner for Debezium PostgreSQL CDC lab scenarios and operational experiments.",
    no_args_is_help=True,
)
connector_app = typer.Typer(help="Manage Kafka Connect Debezium connector.")
schema_app = typer.Typer(help="Trigger PostgreSQL schema evolution experiments.")
app.add_typer(connector_app, name="connector")
app.add_typer(schema_app, name="schema")

console = Console()


@app.command()
def info() -> None:
    """Display environment status and lab scenarios."""
    console.print("[bold green]Debezium CDC PostgreSQL Lab[/bold green]")
    console.print("Python 3.13 + uv + Debezium 2.7 + Kafka KRaft + Schema Registry")
    console.print("Run [bold cyan]uv run python scripts/verify_infra.py[/bold cyan] to verify services.")


@connector_app.command("deploy")
def connector_deploy(config_path: str = "connectors/postgres-debezium-connector.json") -> None:
    """Deploy or update the Debezium PostgreSQL connector via Kafka Connect REST API."""
    client = ConnectorClient()
    try:
        res = client.deploy(config_path)
        console.print(f"[bold green]Connector deployed/updated successfully![/bold green] Response: {res}")
    except Exception as e:
        console.print(f"[bold red]Deployment failed:[/bold red] {e}")
        raise typer.Exit(code=1)


@connector_app.command("status")
def connector_status() -> None:
    """Check the status of the Debezium connector and its tasks."""
    client = ConnectorClient()
    try:
        status = client.get_status()
        state = status.get("connector", {}).get("state", status.get("state", "UNKNOWN"))
        color = "green" if state == "RUNNING" else "yellow" if state == "PAUSED" else "red"
        console.print(f"Connector Status: [{color}]{state}[/{color}]")

        tasks = status.get("tasks", [])
        if tasks:
            table = Table(title="Connector Tasks", border_style="cyan")
            table.add_column("Task ID", style="bold")
            table.add_column("State", style="cyan")
            table.add_column("Worker ID", style="dim")
            for t in tasks:
                t_state = t.get("state", "UNKNOWN")
                t_color = "green" if t_state == "RUNNING" else "red"
                table.add_row(str(t.get("id")), f"[{t_color}]{t_state}[/{t_color}]", str(t.get("worker_id")))
            console.print(table)
    except Exception as e:
        console.print(f"[bold red]Failed to get connector status:[/bold red] {e}")


@connector_app.command("delete")
def connector_delete() -> None:
    """Delete the Debezium connector."""
    client = ConnectorClient()
    try:
        deleted = client.delete()
        if deleted:
            console.print("[bold green]Connector deleted successfully.[/bold green]")
        else:
            console.print("[yellow]Connector not found.[/yellow]")
    except Exception as e:
        console.print(f"[bold red]Delete failed:[/bold red] {e}")


@app.command("seed")
def seed(customers: int = 5, orders: int = 10) -> None:
    """Insert randomized customers and orders into PostgreSQL."""
    res = seed_records(num_customers=customers, num_orders=orders)
    console.print(f"[bold green]Seeded successfully:[/bold green] {res}")


@app.command("update")
def update(customer_id: Optional[int] = None) -> None:
    """Update a customer to trigger an UPDATE ('u') event."""
    res = update_customer_status(customer_id)
    if res:
        console.print(f"[bold yellow]Customer updated:[/bold yellow] ID={res['id']}, Name={res['name']}, Status={res['old_status']} -> {res['new_status']}")
    else:
        console.print("[red]No customers found to update.[/red]")


@app.command("delete")
def delete(customer_id: Optional[int] = None) -> None:
    """Delete a customer to trigger a DELETE ('d') event and Tombstone."""
    res = delete_customer(customer_id)
    if res:
        console.print(f"[bold red]Customer deleted:[/bold red] ID={res['deleted_id']} ({res['name']})")
    else:
        console.print("[red]No customers found to delete.[/red]")


@app.command("stream")
def stream(rate: float = 2.0, max_events: Optional[int] = None) -> None:
    """Continuously stream randomized transactions into PostgreSQL to view live CDC throughput in Kafka UI."""
    console.print(f"[bold green]Starting live traffic stream at {rate} tx/sec...[/bold green]")
    console.print("[dim]Press Ctrl+C to stop. Watch http://localhost:8080 for real-time messages![/dim]\n")
    try:
        for event in stream_traffic_generator(rate_per_sec=rate, max_events=max_events):
            console.print(f"  [cyan]➔[/cyan] {event}")
    except KeyboardInterrupt:
        console.print("\n[yellow]Traffic generator stopped.[/yellow]")


@app.command("consume")
def consume(
    topic: str = "inventory.public.customers",
    max_messages: int = 10,
    timeout: float = 10.0,
) -> None:
    """Consume and inspect Debezium CDC Avro records with Schema Registry."""
    consume_cdc_events(topic=topic, max_messages=max_messages, timeout=timeout)


@app.command("slots")
def slots() -> None:
    """Inspect PostgreSQL logical replication slots and WAL retention lag."""
    metrics = get_replication_slot_metrics()
    if not metrics:
        console.print("[yellow]No replication slots found in PostgreSQL.[/yellow]")
        return

    table = Table(title="PostgreSQL Logical Replication Slots (pg_replication_slots)", border_style="cyan")
    table.add_column("Slot Name", style="bold")
    table.add_column("Plugin", style="cyan")
    table.add_column("Active", style="white")
    table.add_column("Restart LSN", style="dim")
    table.add_column("Confirmed Flush LSN", style="dim")
    table.add_column("Lag (Bytes)", style="bold yellow")
    table.add_column("WAL Status", style="green")

    for m in metrics:
        active_color = "green" if m.get("active") else "red"
        lag = m.get("lag_bytes") or 0
        lag_str = f"{lag:,} bytes"
        table.add_row(
            str(m.get("slot_name")),
            str(m.get("plugin")),
            f"[{active_color}]{m.get('active')}[/{active_color}]",
            str(m.get("restart_lsn")),
            str(m.get("confirmed_flush_lsn")),
            lag_str,
            str(m.get("wal_status")),
        )
    console.print(table)


@app.command("snapshot")
def snapshot(table: str = "public.customers") -> None:
    """Trigger an ad-hoc incremental snapshot via Debezium signaling table."""
    sig_id = send_signaling_snapshot(table)
    console.print(f"[bold green]Incremental snapshot signal sent![/bold green] Signal ID: [cyan]{sig_id}[/cyan] for table [bold]{table}[/bold]")


@schema_app.command("add-col")
def schema_add_col() -> None:
    """Add column 'loyalty_tier' with DEFAULT 'STANDARD' to demonstrate backward-compatible schema evolution."""
    msg = alter_schema_add_column()
    console.print(f"[bold green]{msg}[/bold green]")


@schema_app.command("drop-col")
def schema_drop_col() -> None:
    """Drop column 'loyalty_tier' from customers."""
    msg = alter_schema_drop_column()
    console.print(f"[bold yellow]{msg}[/bold yellow]")


if __name__ == "__main__":
    app()
