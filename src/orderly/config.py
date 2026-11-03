"""Where the services find their databases. Each service owns one database and connects to nothing else."""

from __future__ import annotations

import os

# The databases with migrations: the three services, and the monolith they were split from (DESIGN.md "The
# monolith"), which is kept as the baseline.
SERVICES = ("orders", "payments", "notifications", "monolith")
# The monolith publishes and consumes nothing: it doesn't connect to RabbitMQ at all.
WITHOUT_BROKER = frozenset({"monolith"})


def database_url(service: str) -> str:
    """ORDERLY_<SERVICE>_DATABASE_URL if set, else the database scripts/postgres.sh creates on the local server."""
    return os.environ.get(f"ORDERLY_{service.upper()}_DATABASE_URL", f"postgresql://localhost:5432/orderly_{service}")
