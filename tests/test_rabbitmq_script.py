"""scripts/rabbitmq.sh against a real RabbitMQ: each test starts its own node, on its own ports and directory.

Runs where RabbitMQ is installed (Homebrew's, or RABBITMQ_SBIN); skipped elsewhere, including CI, whose broker is a
service container. About 2 s per start.
"""

from __future__ import annotations

import os
import random
import socket
import subprocess
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts" / "rabbitmq.sh"
SBIN = Path(os.environ.get("RABBITMQ_SBIN", "/opt/homebrew/opt/rabbitmq/sbin"))

pytestmark = pytest.mark.skipif(
    not (SBIN / "rabbitmq-server").exists(), reason=f"no RabbitMQ installed at {SBIN} (set RABBITMQ_SBIN)"
)


def free_port() -> int:
    """A port with port + 20000 (the node's distribution port) also free. Not the OS's pick: macOS hands out ephemeral
    ports from 49152, which leaves no room for + 20000."""
    rng = random.Random()
    for _ in range(100):
        port = rng.randrange(20000, 45000)
        if not listening(port) and not listening(port + 20000):
            return port
    raise RuntimeError("no free port pair")


def listening(port: int) -> bool:
    with socket.socket() as probe:
        return probe.connect_ex(("127.0.0.1", port)) == 0


class Node:
    def __init__(self, directory: Path, port: int) -> None:
        self.directory = directory
        self.port = port
        self.env = {
            **os.environ,
            "ORDERLY_RABBITMQ_DIR": str(directory),
            "ORDERLY_RABBITMQ_PORT": str(port),
            "ORDERLY_RABBITMQ_NODE": f"orderly-test-{uuid.uuid4().hex[:8]}@localhost",
        }

    def run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(SCRIPT), *args], env=self.env, capture_output=True, text=True, timeout=120, check=False
        )


@pytest.fixture
def node(tmp_path: Path) -> Iterator[Node]:
    node = Node(tmp_path / "rabbit", free_port())
    yield node
    node.run("stop")


def test_start_status_stop(node: Node) -> None:
    started = node.run("start")
    assert started.returncode == 0, started.stderr
    assert f"on 127.0.0.1:{node.port}" in started.stdout
    assert listening(node.port)
    assert node.run("start").stdout.startswith("rabbitmq already running")
    status = node.run("status")
    assert status.returncode == 0 and status.stdout.startswith("running")
    assert node.run("url").stdout.strip() == f"amqp://guest:guest@127.0.0.1:{node.port}/"
    stopped = node.run("stop")
    assert stopped.returncode == 0 and stopped.stdout.strip() == "rabbitmq stopped"
    assert not listening(node.port)
    assert node.run("status").returncode == 3


def test_it_listens_on_loopback_only_and_without_plugins(node: Node) -> None:
    assert node.run("start").returncode == 0
    config = (node.directory / "rabbitmq.conf").read_text()
    assert f"listeners.tcp.1 = 127.0.0.1:{node.port}" in config
    assert (node.directory / "enabled_plugins").read_text().strip() == "[]."
    pid = (node.directory / "rabbitmq.pid").read_text().strip()
    lsof = subprocess.run(
        ["lsof", "-nP", "-a", "-p", pid, "-iTCP", "-sTCP:LISTEN"], capture_output=True, text=True, check=False
    )
    addresses = {line.split()[8] for line in lsof.stdout.splitlines()[1:]}
    assert addresses == {f"127.0.0.1:{node.port}", f"127.0.0.1:{node.port + 20000}"}


def test_a_busy_port_is_refused(node: Node) -> None:
    with socket.socket() as squatter:
        squatter.bind(("127.0.0.1", node.port))
        squatter.listen()
        result = node.run("start")
    assert result.returncode == 1
    assert f"something else is listening on port {node.port}" in result.stderr


def test_a_directory_it_didnt_make_is_left_alone(node: Node) -> None:
    node.directory.mkdir()
    (node.directory / "precious.txt").write_text("keep me")
    assert "wasn't made by this script" in node.run("start").stderr
    assert "wasn't made by this script" in node.run("reset").stderr
    assert (node.directory / "precious.txt").read_text() == "keep me"


def test_a_live_pid_that_isnt_its_node_is_neither_signalled_nor_forgotten(node: Node) -> None:
    node.directory.mkdir()
    (node.directory / ".orderly-rabbitmq").touch()
    stranger = subprocess.Popen(["sleep", "60"])
    try:
        (node.directory / "rabbitmq.pid").write_text(f"{stranger.pid}\n")
        result = node.run("stop")
        assert result.returncode == 1
        assert "is alive but wasn't started on" in result.stderr
        assert stranger.poll() is None, "the stop signalled a process that isn't its node"
        assert (node.directory / "rabbitmq.pid").exists()
        assert node.run("status").returncode == 3
    finally:
        stranger.kill()
        stranger.wait()


def test_a_stale_pid_file_is_cleared(node: Node) -> None:
    node.directory.mkdir()
    (node.directory / ".orderly-rabbitmq").touch()
    gone = subprocess.Popen(["true"])
    gone.wait()
    (node.directory / "rabbitmq.pid").write_text(f"{gone.pid}\n")
    result = node.run("stop")
    assert result.returncode == 0 and result.stdout.strip() == "rabbitmq not running"
    assert not (node.directory / "rabbitmq.pid").exists()


def test_the_same_directory_through_a_symlink_is_the_same_node(node: Node, tmp_path: Path) -> None:
    assert node.run("start").returncode == 0
    link = tmp_path / "link"
    link.symlink_to(node.directory)
    through_link = Node(link, node.port)
    through_link.env["ORDERLY_RABBITMQ_NODE"] = node.env["ORDERLY_RABBITMQ_NODE"]
    assert through_link.run("status").returncode == 0
    assert through_link.run("stop").returncode == 0
    assert not listening(node.port)


def test_reset_deletes_its_directory(node: Node) -> None:
    assert node.run("start").returncode == 0
    result = node.run("reset")
    assert result.returncode == 0, result.stderr
    assert not node.directory.exists()
    assert node.run("reset").stdout.strip().endswith("nothing to reset")


def test_usage(node: Node) -> None:
    result = node.run("restart")
    assert result.returncode == 2 and "usage:" in result.stderr
