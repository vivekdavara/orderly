"""Print the load test's runs (results/load/*.json, as scripts/load_test.py --json wrote them) as the README's table.

.venv/bin/python scripts/results_table.py results/load/monolith-rate100.json results/load/services-rate100.json ...
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


def latency(summary: dict[str, Any], name: str) -> str:
    values = summary.get(name)
    if not values:
        return "-"
    return " / ".join(f"{values[k]:,.1f}" for k in ("p50", "p95", "p99", "max"))


def row(path: Path) -> str:
    run = json.loads(path.read_text())
    summary = run["summary"]
    loop = f"open, {run['rate']:g}/s" if run["rate"] is not None else f"closed, {run['concurrency']} clients"
    if run.get("outage"):
        loop += f", node stopped at {run['outage']['stopped_at']:.0f} s, back at {run['outage']['started_at']:.0f} s"
    outcomes = summary["outcomes"]
    cancelled = ", ".join(
        f"{n:,} {kind.split(':')[1].replace('payment_', '')}" for kind, n in sorted(outcomes.items()) if ":" in kind
    )
    settled = f"{outcomes.get('CONFIRMED', 0):,} confirmed, {cancelled} cancelled"
    if summary.get("refused"):
        settled += f"; {summary['refused']:,} refused (503, the cap)"
    books = "agree" if run["books"].get("ok") else "**disagree**"
    return (
        f"| {run['target']} | {loop} | {summary['sent_per_s']:,.1f} | {summary['settled_per_s']:,.1f} |"
        f" {latency(summary, 'accept_ms')} | {latency(summary, 'end_to_end_ms')} | {settled} | {books} |"
    )


def main(paths: list[str]) -> None:
    print(
        "| Target | Loop | Sent/s | Settled/s | Accept ms p50 / p95 / p99 / max"
        " | End to end ms p50 / p95 / p99 / max | Outcomes | Books |"
    )
    print("|---|---|---|---|---|---|---|---|")
    for path in paths:
        print(row(Path(path)))


if __name__ == "__main__":
    main(sys.argv[1:])
