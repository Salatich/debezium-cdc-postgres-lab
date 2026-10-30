"""Unit tests for Debezium CDC configuration and settings."""

from debezium_cdc_postgres_lab.config import settings


def test_default_settings():
    assert settings.postgres_db == "inventory_db"
    assert settings.postgres_port == 5432
    assert "inventory_db" in settings.postgres_dsn
    assert "localhost:29092" in settings.kafka_bootstrap_servers
    assert "8081" in settings.schema_registry_url
    assert "8083" in settings.kafka_connect_url
