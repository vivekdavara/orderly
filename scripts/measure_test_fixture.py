"""Why the tests use a schema each, not a database each: time both on the local PostgreSQL.

    .venv/bin/python scripts/measure_test_fixture.py [--url postgresql://localhost:5432/postgres] [--runs 5]

CREATE DATABASE on PostgreSQL 14 copies template1 and forces a checkpoint; CREATE SCHEMA writes a catalog row. The
script creates and drops one of each, `--runs` times, and prints each time in milliseconds.
"""

from __future__ import annotations

import argparse
import time
import uuid

import psycopg
from psycopg import sql


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--url", default="postgresql://localhost:5432/postgres")
    parser.add_argument("--runs", type=int, default=5)
    args = parser.parse_args()
    with psycopg.connect(args.url, autocommit=True) as conn:
        version = conn.execute("SHOW server_version").fetchone()
        print(f"PostgreSQL {version[0] if version else '?'}, {args.runs} runs each")
        for kind in ("DATABASE", "SCHEMA"):
            creates, drops = [], []
            for _ in range(args.runs):
                name = sql.Identifier(f"orderly_measure_{uuid.uuid4().hex[:8]}")
                started = time.perf_counter()
                conn.execute(sql.SQL(f"CREATE {kind} {{}}").format(name))  # kind is one of two literals above
                created = time.perf_counter()
                conn.execute(sql.SQL(f"DROP {kind} {{}}").format(name))
                dropped = time.perf_counter()
                creates.append((created - started) * 1000)
                drops.append((dropped - created) * 1000)
            print(f"CREATE {kind}: " + ", ".join(f"{ms:.1f}" for ms in creates) + " ms")
            print(f"DROP {kind}:   " + ", ".join(f"{ms:.1f}" for ms in drops) + " ms")


if __name__ == "__main__":
    main()
