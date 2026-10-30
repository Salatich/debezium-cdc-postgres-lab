"""Database helper operations for DML generation, schema evolution, and WAL slot inspection."""

import json
import uuid
from typing import Any, Dict, List, Optional

import psycopg
from faker import Faker

from debezium_cdc_postgres_lab.config import settings

fake = Faker()


def get_connection():
    return psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        autocommit=True,
    )


def seed_records(num_customers: int = 5, num_orders: int = 10) -> Dict[str, int]:
    """Insert randomized customer and order records."""
    customer_ids: List[int] = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            for _ in range(num_customers):
                first_name = fake.first_name()
                last_name = fake.last_name()
                email = f"{first_name.lower()}.{last_name.lower()}.{fake.unique.random_int(1000, 9999)}@{fake.free_email_domain()}"
                cur.execute(
                    """
                    INSERT INTO customers (first_name, last_name, email, status)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id;
                    """,
                    (
                        first_name,
                        last_name,
                        email,
                        fake.random_element(["ACTIVE", "PENDING", "PREMIUM"]),
                    ),
                )
                customer_ids.append(cur.fetchone()[0])

            order_count = 0
            if customer_ids:
                for _ in range(num_orders):
                    c_id = fake.random_element(customer_ids)
                    status = fake.random_element(["PROCESSING", "SHIPPED", "DELIVERED"])
                    amount = round(
                        fake.pyfloat(left_digits=3, right_digits=2, positive=True, min_value=10.0),
                        2,
                    )
                    cur.execute(
                        """
                        INSERT INTO orders (customer_id, order_status, total_amount)
                        VALUES (%s, %s, %s);
                        """,
                        (c_id, status, amount),
                    )
                    order_count += 1

    return {"customers_created": len(customer_ids), "orders_created": order_count}


def update_customer_status(customer_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """Update a customer to trigger an UPDATE CDC event ('u')."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            if customer_id is None:
                cur.execute("SELECT id, status FROM customers ORDER BY RANDOM() LIMIT 1;")
                row = cur.fetchone()
                if not row:
                    return None
                customer_id, old_status = row
            else:
                cur.execute("SELECT status FROM customers WHERE id = %s;", (customer_id,))
                row = cur.fetchone()
                if not row:
                    return None
                old_status = row[0]

            new_status = "VIP" if old_status != "VIP" else "ACTIVE"
            cur.execute(
                """
                UPDATE customers
                SET status = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                RETURNING id, first_name, last_name, email, status;
                """,
                (new_status, customer_id),
            )
            updated = cur.fetchone()
            return {
                "id": updated[0],
                "name": f"{updated[1]} {updated[2]}",
                "email": updated[3],
                "old_status": old_status,
                "new_status": updated[4],
            }


def delete_customer(customer_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """Delete a customer to trigger a DELETE CDC event ('d') and subsequent Kafka Tombstone."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            if customer_id is None:
                cur.execute(
                    "SELECT id, first_name, last_name FROM customers ORDER BY id DESC LIMIT 1;"
                )
                row = cur.fetchone()
                if not row:
                    return None
                customer_id = row[0]
                name = f"{row[1]} {row[2]}"
            else:
                cur.execute(
                    "SELECT first_name, last_name FROM customers WHERE id = %s;", (customer_id,)
                )
                row = cur.fetchone()
                name = f"{row[0]} {row[1]}" if row else "Unknown"

            cur.execute("DELETE FROM customers WHERE id = %s;", (customer_id,))
            return {"deleted_id": customer_id, "name": name}


def get_replication_slot_metrics() -> List[Dict[str, Any]]:
    """Inspect PostgreSQL pg_replication_slots and calculate WAL retention lag in bytes."""
    query = """
        SELECT
            slot_name,
            plugin,
            slot_type,
            active,
            restart_lsn,
            confirmed_flush_lsn,
            pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes,
            wal_status
        FROM pg_replication_slots;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            return [dict(zip(cols, row)) for row in rows]


def send_signaling_snapshot(table: str = "public.customers") -> str:
    """Trigger an incremental / ad-hoc snapshot via Debezium signal table."""
    signal_id = f"snap-{uuid.uuid4().hex[:8]}"
    payload = json.dumps({"data-collections": [table], "type": "INCREMENTAL"})
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO debezium_signal (id, type, data)
                VALUES (%s, 'execute-snapshot', %s);
                """,
                (signal_id, payload),
            )
    return signal_id


def alter_schema_add_column() -> str:
    """Simulate schema evolution: Add optional column with default value."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                ALTER TABLE customers
                ADD COLUMN IF NOT EXISTS loyalty_tier VARCHAR(20) DEFAULT 'STANDARD';
                """
            )
    return "Added column 'loyalty_tier' VARCHAR(20) DEFAULT 'STANDARD' to customers."


def alter_schema_drop_column() -> str:
    """Simulate schema evolution: Drop column."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                ALTER TABLE customers
                DROP COLUMN IF EXISTS loyalty_tier;
                """
            )
    return "Dropped column 'loyalty_tier' from customers."
