"""Where the services find their databases. Each service owns one database and connects to nothing else."""

from __future__ import annotations

import os

# The service databases with migrations so far (DESIGN.md "Data"); the monolith's arrives on day 2.
SERVICES = ("orders", "payments", "notifications")


def database_url(service: str) -> str:
    """ORDERLY_<SERVICE>_DATABASE_URL if set, else the database scripts/postgres.sh creates on the local server."""
    return os.environ.get(f"ORDERLY_{service.upper()}_DATABASE_URL", f"postgresql://localhost:5432/orderly_{service}")
