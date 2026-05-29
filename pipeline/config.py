"""Runtime configuration, sourced from environment variables.

Twelve-factor style: no hardcoded hosts, everything overridable. Defaults match
the docker-compose service names so `docker compose up` works out of the box.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "transactions.raw")
    kafka_group: str = os.getenv("KAFKA_GROUP", "warehouse-loader")

    pg_host: str = os.getenv("PG_HOST", "postgres")
    pg_port: int = int(os.getenv("PG_PORT", "5432"))
    pg_db: str = os.getenv("PG_DB", "warehouse")
    pg_user: str = os.getenv("PG_USER", "pipeline")
    pg_password: str = os.getenv("PG_PASSWORD", "pipeline")

    # Producer behaviour
    events_per_second: float = float(os.getenv("EVENTS_PER_SECOND", "20"))
    n_users: int = int(os.getenv("N_USERS", "500"))
    n_merchants: int = int(os.getenv("N_MERCHANTS", "120"))

    @property
    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
            f"user={self.pg_user} password={self.pg_password}"
        )


settings = Settings()
