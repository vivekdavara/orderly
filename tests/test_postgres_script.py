"""scripts/postgres.sh against the real server, on databases under a random prefix (never the real ones)."""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
from psycopg import conninfo, sql

SCRIPT = Path(__file__).parents[1] / "scripts" / "postgres.sh"


def server(url: str) -> dict[str, str]:
    """libpq variables for the server the tests use (CI sets its own PG* variables; they win)."""
    params = conninfo.conninfo_to_dict(url)
    env = {"PGHOST": str(params.get("host", "localhost")), "PGPORT": str(params.get("port", "5432"))}
    for key, var in (("user", "PGUSER"), ("password", "PGPASSWORD")):
        if key in params:
            env[var] = str(params[key])
    return {**env, **{key: value for key, value in os.environ.items() if key.startswith("PG")}}


def databases(url: str, prefix: str) -> set[str]:
    """The databases named <prefix>_<anything> (not LIKE: '_' is a wildcard there)."""
    with psycopg.connect(url) as conn:
        query = "SELECT datname FROM pg_database WHERE left(datname, length(%s) + 1) = %s || '_'"
        rows = conn.execute(query, (prefix, prefix)).fetchall()
    return {row[0] for row in rows}


class Script:
    def __init__(self, url: str, prefix: str) -> None:
        self.url = url
        self.prefix = prefix
        # PYTHON as a bare command name, as CI passes it: the script must resolve it, not test it as a path.
        bin_dir = str(Path(sys.executable).parent)
        self.env = {
            **os.environ,
            **server(url),
            "ORDERLY_PG_PREFIX": prefix,
            "PYTHON": "python",
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        }

    def run(self, *args: str, stdin: str = "", **extra_env: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(SCRIPT), *args],
            env={**self.env, **extra_env},
            input=stdin,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )


@pytest.fixture
def script(test_database: str) -> Iterator[Script]:
    prefix = f"orderly_script_{uuid.uuid4().hex[:8]}"
    yield Script(test_database, prefix)
    with psycopg.connect(test_database, autocommit=True) as conn:
        for name in databases(test_database, prefix):
            conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))


def test_create_makes_and_migrates_the_databases_once(script: Script) -> None:
    first = script.run("create")
    assert first.returncode == 0, first.stderr
    names = {f"{script.prefix}_{suffix}" for suffix in ("orders", "payments", "notifications", "test")}
    assert databases(script.url, script.prefix) == names
    assert f"{script.prefix}_orders created" in first.stdout
    assert "orders: applied [1], now at 1" in first.stdout
    second = script.run("create")
    assert second.returncode == 0, second.stderr
    assert f"{script.prefix}_orders exists" in second.stdout
    assert "orders: up to date at 1" in second.stdout


def test_status_reports_each_database(script: Script) -> None:
    before = script.run("status")
    assert f"{script.prefix}_orders: missing" in before.stdout
    script.run("create")
    after = script.run("status").stdout.splitlines()
    assert after == [
        f"{script.prefix}_orders: migration 1",
        f"{script.prefix}_payments: migration 1",
        f"{script.prefix}_notifications: migration 1",
        f"{script.prefix}_test: present (tests make a schema each)",
    ]


def test_drop_asks_and_drops_only_its_own(script: Script) -> None:
    script.run("create")
    other = Script(script.url, f"{script.prefix}x")  # a prefix that starts the same, but other databases
    other.run("create")
    try:
        refused = script.run("drop", stdin="n\n")
        assert "nothing dropped" in refused.stdout
        assert len(databases(script.url, script.prefix)) == 4
        dropped = script.run("drop", ORDERLY_PG_YES="1")
        assert dropped.returncode == 0, dropped.stderr
        assert databases(script.url, other.prefix) == {
            f"{other.prefix}_{s}" for s in ("orders", "payments", "notifications", "test")
        }
        assert databases(script.url, script.prefix) == set()
    finally:
        other.run("drop", ORDERLY_PG_YES="1")


# An empty ORDERLY_PG_PREFIX means the default, like an unset one.
@pytest.mark.parametrize("prefix", ["Orderly", "orderly; DROP DATABASE x", "1orderly", "a-b", "a" * 42])
def test_a_prefix_that_isnt_a_plain_identifier_is_refused(script: Script, prefix: str) -> None:
    result = script.run("status", ORDERLY_PG_PREFIX=prefix)
    assert result.returncode == 1
    assert "ORDERLY_PG_PREFIX must match" in result.stderr


def test_a_missing_python_is_reported(script: Script) -> None:
    result = script.run("migrate", PYTHON="no-such-python-here")
    assert result.returncode == 1
    assert "no no-such-python-here" in result.stderr


def test_usage(script: Script) -> None:
    result = script.run("reset")
    assert result.returncode == 2 and "usage:" in result.stderr
