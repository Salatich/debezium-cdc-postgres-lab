"""Infrastructure connectivity and sanity check script for Debezium CDC Lab."""

import json
import os
import sys
import urllib.request

from dotenv import load_dotenv

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "inventory_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
KAFKA_CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083")
KAFKA_UI_URL = os.getenv("KAFKA_UI_URL", "http://localhost:8080")


def check_postgres() -> bool:
    print(
        f"[*] Checking PostgreSQL connectivity on {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}..."
    )
    try:
        import psycopg

        conn_str = f"host={POSTGRES_HOST} port={POSTGRES_PORT} dbname={POSTGRES_DB} user={POSTGRES_USER} password={POSTGRES_PASSWORD} connect_timeout=3"
        with psycopg.connect(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute("SHOW wal_level;")
                wal_level = cur.fetchone()[0]
                print(f"    [+] PostgreSQL online! wal_level = '{wal_level}'")
                if wal_level != "logical":
                    print("    [!] WARNING: wal_level is not 'logical'. CDC replication will fail!")
                    return False
                cur.execute(
                    "SELECT count(*) FROM pg_publication WHERE pubname = 'dbz_publication';"
                )
                has_pub = cur.fetchone()[0] > 0
                print(f"    [+] Publication 'dbz_publication' configured: {has_pub}")
        return True
    except Exception as e:
        print(f"    [x] PostgreSQL check failed: {e}")
        return False


def check_kafka() -> bool:
    print(f"[*] Checking Kafka broker on {KAFKA_BOOTSTRAP}...")
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP, "socket.timeout.ms": 3000})
        metadata = admin.list_topics(timeout=3.0)
        print(f"    [+] Kafka is accessible! Existing topics: {len(metadata.topics)}")
        return True
    except Exception as e:
        print(f"    [x] Kafka check failed: {e}")
        return False


def check_schema_registry() -> bool:
    print(f"[*] Checking Schema Registry on {SCHEMA_REGISTRY_URL}...")
    try:
        req = urllib.request.Request(f"{SCHEMA_REGISTRY_URL}/subjects")
        with urllib.request.urlopen(req, timeout=3) as resp:
            if resp.status == 200:
                subjects = json.loads(resp.read().decode())
                print(f"    [+] Schema Registry is healthy! Registered subjects: {len(subjects)}")
                return True
    except Exception as e:
        print(f"    [x] Schema Registry check failed: {e}")
    return False


def check_kafka_connect() -> bool:
    print(f"[*] Checking Kafka Connect on {KAFKA_CONNECT_URL}...")
    try:
        req = urllib.request.Request(f"{KAFKA_CONNECT_URL}/connector-plugins")
        with urllib.request.urlopen(req, timeout=3) as resp:
            if resp.status == 200:
                plugins = json.loads(resp.read().decode())
                has_pg = any(
                    p.get("class") == "io.debezium.connector.postgresql.PostgresConnector"
                    for p in plugins
                )
                print(f"    [+] Kafka Connect online! Debezium Postgres plugin detected: {has_pg}")
                return True
    except Exception as e:
        print(f"    [x] Kafka Connect check failed: {e}")
    return False


def check_kafka_ui() -> bool:
    print(f"[*] Checking Kafka UI on {KAFKA_UI_URL}...")
    try:
        req = urllib.request.Request(f"{KAFKA_UI_URL}/actuator/health")
        with urllib.request.urlopen(req, timeout=3) as resp:
            if resp.status == 200:
                print("    [+] Kafka UI is healthy!")
                return True
    except Exception as e:
        print(f"    [!] Kafka UI not ready yet or unreachable: {e}")
    return False


def main():
    print("=" * 60)
    print("🔍 DEBEZIUM CDC INFRASTRUCTURE HEALTH CHECK")
    print("=" * 60)

    results = {
        "PostgreSQL (wal_level=logical)": check_postgres(),
        "Kafka (KRaft)": check_kafka(),
        "Confluent Schema Registry": check_schema_registry(),
        "Kafka Connect (Debezium)": check_kafka_connect(),
        "Kafka UI": check_kafka_ui(),
    }

    print("-" * 60)
    all_ok = all(results.values())
    for svc, ok in results.items():
        status_icon = "✅ ONLINE" if ok else "❌ OFFLINE"
        print(f"{svc:40} {status_icon}")
    print("=" * 60)
    if not all_ok:
        print("💡 Hint: Ensure Docker Desktop is running and execute: docker compose up -d")
        sys.exit(1)
    else:
        print("🚀 All infrastructure components are healthy and ready!")


if __name__ == "__main__":
    main()
