"""Interactive end-to-end demo script for Debezium CDC Lab."""

import subprocess
import time

from rich.console import Console
from rich.panel import Panel

from debezium_cdc_postgres_lab.connector import ConnectorClient
from debezium_cdc_postgres_lab.consumer import consume_cdc_events
from debezium_cdc_postgres_lab.db import (
    alter_schema_add_column,
    delete_customer,
    get_replication_slot_metrics,
    seed_records,
    send_signaling_snapshot,
    update_customer_status,
)

console = Console()


def run_command(cmd: str) -> None:
    console.print(f"[bold cyan]> {cmd}[/bold cyan]")
    subprocess.run(cmd, shell=True, check=True)


def step(title: str, description: str) -> None:
    console.print()
    console.print(
        Panel(
            f"[bold white]{description}[/bold white]",
            title=f"[bold green]{title}[/bold green]",
            border_style="green",
        )
    )


def main():
    console.print(
        Panel.fit(
            "[bold cyan]Debezium PostgreSQL CDC Interactive Demo[/bold cyan]\n"
            "Lead Assessment Reference Architecture\n"
            "[dim]PostgreSQL 16 (Logical WAL) ➔ Debezium ➔ Kafka (KRaft) ➔ Schema Registry (Avro)[/dim]",
            border_style="cyan",
        )
    )

    # Step 1: Health check
    step(
        "Step 1: Infrastructure Verification",
        "Verifying PostgreSQL, Kafka, Schema Registry, and Kafka Connect...",
    )
    from scripts.verify_infra import main as verify_main

    try:
        verify_main()
    except SystemExit:
        console.print(
            "[bold red]Please ensure all Docker containers are running before starting the demo![/bold red]"
        )
        return

    # Step 2: Deploy Debezium Connector
    step(
        "Step 2: Deploy Debezium Connector",
        "Registering PostgreSQL Debezium connector with Avro serialization...",
    )
    client = ConnectorClient()
    client.deploy()
    console.print("[yellow]Waiting 5 seconds for connector task initialization...[/yellow]")
    time.sleep(5)
    status = client.get_status()
    console.print(
        f"Connector Status: [bold green]{status.get('connector', {}).get('state')}[/bold green]"
    )

    # Step 3: Inspect Initial Snapshot
    step(
        "Step 3: Initial Snapshot Observation",
        "Consuming baseline records captured from the initial snapshot ('r' events)...",
    )
    consume_cdc_events(topic="inventory.public.customers", max_messages=3, timeout=5.0)

    # Step 4: Live Streaming DML (INSERT, UPDATE, DELETE)
    step(
        "Step 4: Live Streaming DML Events",
        "Generating INSERT ('c'), UPDATE ('u'), and DELETE ('d') transactions...",
    )
    seed_res = seed_records(num_customers=2, num_orders=2)
    console.print(f"  [+] Inserted: {seed_res}")
    up_res = update_customer_status()
    console.print(f"  [+] Updated customer: {up_res}")
    del_res = delete_customer()
    console.print(f"  [+] Deleted customer: {del_res}")

    console.print("\n[cyan]Streaming events captured in WAL and emitted to Kafka:[/cyan]")
    consume_cdc_events(topic="inventory.public.customers", max_messages=4, timeout=5.0)

    # Step 5: Replication Slot & WAL Lag Inspection
    step(
        "Step 5: PostgreSQL Replication Slot & WAL Metrics",
        "Inspecting pg_replication_slots for active status and byte lag...",
    )
    slots = get_replication_slot_metrics()
    for s in slots:
        console.print(
            f"  • Slot: [bold]{s['slot_name']}[/bold] | Active: [green]{s['active']}[/green] | Lag: [yellow]{s['lag_bytes']} bytes[/yellow] | Flush LSN: {s['confirmed_flush_lsn']}"
        )

    # Step 6: Schema Evolution (Additive)
    step(
        "Step 6: Schema Evolution in Action",
        "Adding 'loyalty_tier' column to 'customers' without stopping connector...",
    )
    col_msg = alter_schema_add_column()
    console.print(f"  [+] DDL executed: {col_msg}")
    seed_records(num_customers=1, num_orders=1)
    console.print("[cyan]Consuming event with new schema version from Schema Registry:[/cyan]")
    consume_cdc_events(topic="inventory.public.customers", max_messages=1, timeout=5.0)

    # Step 7: Zero-Downtime Ad-Hoc Snapshot
    step(
        "Step 7: Signaling Table Ad-Hoc Snapshot",
        "Triggering incremental table snapshot via debezium_signal table...",
    )
    sig_id = send_signaling_snapshot("public.customers")
    console.print(f"  [+] Signal emitted with ID: [cyan]{sig_id}[/cyan]")

    console.print(
        Panel(
            "[bold green]Demo walkthrough complete! Open Kafka UI at http://localhost:8080 to inspect topics & schemas.[/bold green]"
        )
    )


if __name__ == "__main__":
    main()
