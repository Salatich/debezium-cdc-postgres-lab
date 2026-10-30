"""Centralized configuration loaded from environment variables."""

import os

from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv()


class Settings(BaseModel):
    # PostgreSQL
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "inventory_db")
    postgres_user: str = os.getenv("POSTGRES_USER", "postgres")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "postgres")
    postgres_repl_user: str = os.getenv("POSTGRES_REPL_USER", "cdc_user")
    postgres_repl_password: str = os.getenv("POSTGRES_REPL_PASSWORD", "cdc_password")

    # Kafka & Schema Registry
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    schema_registry_url: str = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    kafka_connect_url: str = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083")
    kafka_ui_url: str = os.getenv("KAFKA_UI_URL", "http://localhost:8080")

    @property
    def postgres_dsn(self) -> str:
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"


settings = Settings()
