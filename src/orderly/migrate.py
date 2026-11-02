"""Schema migrations: numbered SQL files per service database, applied in order, edits refused.

Each service's files live in `orderly/migrations/<service>/` as `NNNN_name.sql`, numbered from 0001 with no gaps.
`migrate` applies the ones a database hasn't had, all in one transaction (PostgreSQL's DDL is transactional, so a
failure leaves the database as it was), under an advisory lock (so two services starting at once don't both apply
them), and records each one's SHA-256 in `schema_migrations`. A database whose applied migrations don't match the
files (one edited or renamed after it was applied, or one this code doesn't have) is refused rather than "fixed".

    python -m orderly.migrate orders [--url postgresql://...]
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from importlib import resources
from importlib.resources.abc import Traversable
from typing import Any

import psycopg

from orderly.config import SERVICES, database_url

_FILE = re.compile(r"([0-9]{4})_([a-z0-9_]+)\.sql")
# pg_advisory_xact_lock's key: any fixed bigint, the same in every process that migrates.
_LOCK_KEY = 0x6F72_6465_726C_79  # "orderly"


class MigrationError(Exception):
    """The migrations or the database's record of them are not what they should be. Nothing was changed."""


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.sql.encode()).hexdigest()


def load(folder: Traversable) -> list[Migration]:
    """The migrations in a folder, in order. Anything else in it (but dotfiles and __pycache__) is an error: a file
    named 0002_add_index.sq would otherwise be skipped without a word."""
    found: list[Migration] = []
    for entry in folder.iterdir():
        if entry.name.startswith(".") or entry.name == "__pycache__":
            continue
        match = _FILE.fullmatch(entry.name)
        if match is None or not entry.is_file():
            raise MigrationError(f"{entry.name}: not a migration (expected NNNN_name.sql)")
        found.append(Migration(int(match[1]), match[2], entry.read_text(encoding="utf-8")))
    found.sort(key=lambda migration: migration.version)
    versions = [migration.version for migration in found]
    if versions != list(range(1, len(found) + 1)):
        raise MigrationError(f"migrations must be numbered 1, 2, 3... without gaps or repeats; found {versions}")
    return found


def migrations(service: str) -> list[Migration]:
    if service not in SERVICES:
        raise MigrationError(f"unknown service {service!r} (one of {', '.join(SERVICES)})")
    return load(resources.files("orderly.migrations") / service)


def migrate(conn: psycopg.Connection[Any], service: str, available: Sequence[Migration] | None = None) -> list[int]:
    """Apply what the database lacks; return the versions applied now (empty if it was up to date).

    `conn` should be in autocommit mode: the work runs in its own transaction, and the advisory lock is held until
    that transaction ends.
    """
    files = list(migrations(service) if available is None else available)
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (_LOCK_KEY,))
        conn.execute(
            """CREATE TABLE IF NOT EXISTS schema_migrations (
                   version    integer PRIMARY KEY,
                   name       text NOT NULL,
                   sha256     text NOT NULL,
                   applied_at timestamptz NOT NULL DEFAULT now()
               )"""
        )
        applied = conn.execute("SELECT version, name, sha256 FROM schema_migrations ORDER BY version").fetchall()
        for index, (version, name, sha256) in enumerate(applied):
            if version != index + 1:
                raise MigrationError(f"the database records migrations {[row[0] for row in applied]}: not 1, 2, 3...")
            if version > len(files):
                raise MigrationError(
                    f"the database has migration {version} ({name}); this code has only {len(files)}: is it older?"
                )
            expected = files[index]
            if (name, sha256) != (expected.name, expected.sha256):
                raise MigrationError(
                    f"migration {version} was applied as {name} with SHA-256 {sha256[:12]}..., but the file is now "
                    f"{expected.name} with {expected.sha256[:12]}...: applied migrations must not change; add a new one"
                )
        done = []
        for migration in files[len(applied) :]:
            conn.execute(migration.sql.encode())
            conn.execute(
                "INSERT INTO schema_migrations (version, name, sha256) VALUES (%s, %s, %s)",
                (migration.version, migration.name, migration.sha256),
            )
            done.append(migration.version)
    return done


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m orderly.migrate", description=__doc__.split("\n\n")[0])
    parser.add_argument("service", choices=SERVICES)
    parser.add_argument("--url", help="the database (default: ORDERLY_<SERVICE>_DATABASE_URL or the local one)")
    args = parser.parse_args(argv)
    url = args.url or database_url(args.service)
    try:
        with psycopg.connect(url, autocommit=True) as conn:
            done = migrate(conn, args.service)
    except MigrationError as error:
        print(f"{args.service}: {error}", file=sys.stderr)
        return 1
    latest = len(migrations(args.service))
    print(f"{args.service}: applied {done}, now at {latest}" if done else f"{args.service}: up to date at {latest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
