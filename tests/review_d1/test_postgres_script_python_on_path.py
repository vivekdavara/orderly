"""Defect: postgres.sh checks `[ -x "$PYTHON" ]`, which only accepts a path, so PYTHON=python (a command on PATH) is
refused with "no python (make venv)". CI's last step runs exactly `PYTHON=python scripts/postgres.sh create`, so that
step can never pass. A fake psql and a fake python on PATH; no server is touched."""

import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).parents[2] / "scripts" / "postgres.sh"


def test_a_python_found_on_path_is_used(tmp_path: Path) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "psql").write_text("#!/usr/bin/env bash\necho 1\n")  # the server is up; every database exists
    (bin_dir / "python").write_text('#!/usr/bin/env bash\necho "migrated $3"\n')
    for tool in ("psql", "python"):
        (bin_dir / tool).chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", "PYTHON": "python", "PSQL": "psql"}
    result = subprocess.run(
        ["bash", str(SCRIPT), "migrate"], env=env, cwd=tmp_path, capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.split("\n")[:3] == ["migrated orders", "migrated payments", "migrated notifications"]
