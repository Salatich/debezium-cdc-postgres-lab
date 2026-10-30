"""Kafka Connect REST API client for managing Debezium connectors."""

import json
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from rich.console import Console

from debezium_cdc_postgres_lab.config import settings

console = Console()
CONNECTOR_NAME = "inventory-connector"


class ConnectorClient:
    def __init__(self, base_url: Optional[str] = None):
        self.base_url = (base_url or settings.kafka_connect_url).rstrip("/")

    def get_plugins(self) -> list:
        resp = requests.get(f"{self.base_url}/connector-plugins", timeout=5)
        resp.raise_for_status()
        return resp.json()

    def list_connectors(self) -> list:
        resp = requests.get(f"{self.base_url}/connectors", timeout=5)
        resp.raise_for_status()
        return resp.json()

    def get_status(self, name: str = CONNECTOR_NAME) -> Dict[str, Any]:
        resp = requests.get(f"{self.base_url}/connectors/{name}/status", timeout=5)
        if resp.status_code == 404:
            return {"name": name, "state": "NOT_FOUND", "tasks": []}
        resp.raise_for_status()
        return resp.json()

    def deploy(
        self, config_path: str = "connectors/postgres-debezium-connector.json"
    ) -> Dict[str, Any]:
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)

        name = payload.get("name", CONNECTOR_NAME)
        config = payload.get("config", payload)

        # Use PUT /connectors/{name}/config which creates or updates idempotently
        url = f"{self.base_url}/connectors/{name}/config"
        resp = requests.put(
            url, json=config, headers={"Content-Type": "application/json"}, timeout=10
        )
        resp.raise_for_status()
        return resp.json()

    def pause(self, name: str = CONNECTOR_NAME) -> None:
        resp = requests.put(f"{self.base_url}/connectors/{name}/pause", timeout=5)
        resp.raise_for_status()

    def resume(self, name: str = CONNECTOR_NAME) -> None:
        resp = requests.put(f"{self.base_url}/connectors/{name}/resume", timeout=5)
        resp.raise_for_status()

    def delete(self, name: str = CONNECTOR_NAME) -> bool:
        resp = requests.delete(f"{self.base_url}/connectors/{name}", timeout=5)
        if resp.status_code in (200, 204):
            return True
        elif resp.status_code == 404:
            return False
        resp.raise_for_status()
        return True
