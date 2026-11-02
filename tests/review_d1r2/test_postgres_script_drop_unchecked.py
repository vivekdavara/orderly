"""Defect: postgres.sh drop still reports success for a database it didn't drop when the existence check itself fails.
`exists` runs inside `if`, so a psql error there (connection lost after server_up, a timeout, a permission error)
reads as "doesn't exist": the database is skipped without a word and the script exits 0. Round 1's fix ("a database
it can't create or drop is an error, not a success") covers only a failing DROP. A fake psql, no server touched."""

import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).parents[2] / "scripts" / "postgres.sh"


def fake_psql(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "psql"
    path.write_text('#!/usr/bin/env bash\nsql="${@: -1}"\ncase "$sql" in\n' + body + "\n  *) exit 1 ;;\nesac\n")
    path.chmod(0o755)
    return path


def test_drop_fails_when_it_cannot_tell_whether_a_database_exists(tmp_path: Path) -> None:
    psql = fake_psql(
        tmp_path,
        """  "SELECT 1") echo 1 ;;
  *"datname = 'orderly_payments'"*) echo "psql: error: connection to server lost" >&2; exit 2 ;;
  *"datname = "*) echo 1 ;;
  "DROP DATABASE"*) exit 0 ;;""",
    )
    env = {**os.environ, "PSQL": str(psql), "PYTHON": "/usr/bin/true", "ORDERLY_PG_YES": "1"}
    result = subprocess.run(["bash", str(SCRIPT), "drop"], env=env, capture_output=True, text=True, timeout=30)
    assert "orderly_payments dropped" not in result.stdout
    assert result.returncode != 0, f"exit 0 with orderly_payments neither dropped nor reported:\n{result.stdout}"
