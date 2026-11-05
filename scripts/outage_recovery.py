"""Recovery from a broker outage, from a load-test run's --json and --samples files (the CSV may be gzipped).

    .venv/bin/python scripts/outage_recovery.py results/load/services-outage-after-1.json \
        results/load/samples/services-outage-after-1.csv.gz

The same `load_test.recovery` the run itself reports since it learnt to; the two runs before the reconnect fix
predate that, so their numbers in the README come from this, on their committed samples.
"""

from __future__ import annotations

import csv
import gzip
import io
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import load_test


def main(run_path: str, samples_path: str) -> None:
    run = json.loads(Path(run_path).read_text())
    raw = Path(samples_path).read_bytes()
    text = gzip.decompress(raw).decode() if samples_path.endswith(".gz") else raw.decode()
    pairs = [
        # A refusal by the awaiting cap is an answer, not an order settling (as in load_test.summarize).
        (float(row["due"]), float(row["outcome_at"]) if row["outcome_at"] and row["outcome"] != "REFUSED" else 0.0)
        for row in csv.DictReader(io.StringIO(text))
    ]
    print(json.dumps(load_test.recovery(pairs, run["outage"])))


if __name__ == "__main__":
    main(*sys.argv[1:3])
