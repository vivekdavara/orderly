"""Defect: postgres.sh runs `q "CREATE DATABASE $db" && echo ...` and `q "DROP DATABASE $db ..." && echo ...`; set -e
ignores a failure on the left of &&, so a database that couldn't be created or dropped is reported on stderr but the
script still exits 0. A fake psql (no real server touched) refuses one statement."""

import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).parents[2] / "scripts" / "postgres.sh"


def fake_psql(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "psql"
    path.write_text('#!/usr/bin/env bash\nsql="${@: -1}"\ncase "$sql" in\n' + body + "\n  *) exit 1 ;;\nesac\n")
    path.chmod(0o755)
    return path


def run(tmp_path: Path, psql: Path, command: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "PSQL": str(psql), "PYTHON": "/usr/bin/true", "ORDERLY_PG_YES": "1"}
    return subprocess.run(["bash", str(SCRIPT), command], env=env, capture_output=True, text=True, timeout=30)


def test_create_fails_when_a_database_cannot_be_created(tmp_path: Path) -> None:
    psql = fake_psql(
        tmp_path,
        """  "SELECT 1") echo 1 ;;
  *"datname = 'orderly_test'"*) echo "" ;;
  *"datname = "*) echo 1 ;;
  "CREATE DATABASE"*) echo "ERROR:  permission denied to create database" >&2; exit 1 ;;""",
    )
    result = run(tmp_path, psql, "create")
    assert "permission denied" in result.stderr
    assert result.returncode != 0


def test_drop_fails_when_a_database_cannot_be_dropped(tmp_path: Path) -> None:
    psql = fake_psql(
        tmp_path,
        """  "SELECT 1") echo 1 ;;
  *"datname = "*) echo 1 ;;
  "DROP DATABASE orderly_orders"*) echo "ERROR:  must be owner of database orderly_orders" >&2; exit 1 ;;
  "DROP DATABASE"*) exit 0 ;;""",
    )
    result = run(tmp_path, psql, "drop")
    assert "must be owner" in result.stderr
    assert result.returncode != 0
