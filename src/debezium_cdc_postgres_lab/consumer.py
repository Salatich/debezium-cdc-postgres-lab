"""Avro CDC consumer with Confluent Schema Registry deserialization."""

import json

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from debezium_cdc_postgres_lab.config import settings

console = Console()

OP_NAMES = {
    "r": "[bold blue]READ (SNAPSHOT)[/bold blue]",
    "c": "[bold green]CREATE (INSERT)[/bold green]",
    "u": "[bold yellow]UPDATE[/bold yellow]",
    "d": "[bold red]DELETE[/bold red]",
    "t": "[bold magenta]TRUNCATE[/bold magenta]",
}


def consume_cdc_events(
    topic: str = "inventory.public.customers", max_messages: int = 10, timeout: float = 10.0
):
    """Consume and pretty-print Debezium Avro CDC envelopes."""
    sr_conf = {"url": settings.schema_registry_url}
    schema_registry_client = SchemaRegistryClient(sr_conf)

    key_deserializer = AvroDeserializer(schema_registry_client)
    value_deserializer = AvroDeserializer(schema_registry_client)

    consumer_conf = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": "cdc-visualizer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    console.print(
        Panel(
            f"Listening on [cyan]{topic}[/cyan] (Max: {max_messages} events, Timeout: {timeout}s)",
            title="CDC Event Stream",
        )
    )

    count = 0
    try:
        while count < max_messages:
            msg = consumer.poll(timeout=timeout)
            if msg is None:
                console.print(f"[dim]No messages received in last {timeout}s timeout.[/dim]")
                break

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            count += 1
            key_ctx = SerializationContext(msg.topic(), MessageField.KEY)
            val_ctx = SerializationContext(msg.topic(), MessageField.VALUE)

            # Deserialize Key
            key_data = key_deserializer(msg.key(), key_ctx) if msg.key() else None

            # Check for Tombstone (Value is None)
            if msg.value() is None:
                console.print(
                    f"💀 [bold red]TOMBSTONE EVENT[/bold red] | Topic: {msg.topic()} | Partition: {msg.partition()} | Key: {key_data}"
                )
                continue

            # Deserialize Value
            envelope = value_deserializer(msg.value(), val_ctx)
            if not envelope:
                console.print(f"[yellow]Empty payload received for key {key_data}[/yellow]")
                continue

            op = envelope.get("op", "?")
            op_str = OP_NAMES.get(op, f"UNKNOWN ({op})")
            source = envelope.get("source", {})
            before = envelope.get("before")
            after = envelope.get("after")

            table = Table(title=f"CDC Event #{count} — {op_str}", border_style="cyan")
            table.add_column("Property", style="bold white", width=20)
            table.add_column("Value", style="cyan")

            table.add_row(
                "Topic / Partition", f"{msg.topic()} [p={msg.partition()} @ offset={msg.offset()}]"
            )
            table.add_row("Key", json.dumps(key_data, default=str))
            table.add_row("Operation", f"{op} ({op_str})")
            table.add_row("Source LSN", str(source.get("lsn", "N/A")))
            table.add_row("Transaction ID (txId)", str(source.get("txId", "N/A")))
            table.add_row(
                "Database / Schema / Table",
                f"{source.get('db')}.{source.get('schema')}.{source.get('table')}",
            )
            table.add_row(
                "Snapshot Phase?",
                "Yes" if source.get("snapshot") in (True, "true", "last") else "No (Streaming)",
            )
            table.add_row("Timestamp (ts_ms)", str(envelope.get("ts_ms")))
            table.add_row(
                "Before State",
                json.dumps(before, indent=2, default=str)
                if before
                else "[dim]null (Insert/Snapshot)[/dim]",
            )
            table.add_row(
                "After State",
                json.dumps(after, indent=2, default=str) if after else "[dim]null (Delete)[/dim]",
            )

            console.print(table)
            console.print()

    finally:
        consumer.close()
