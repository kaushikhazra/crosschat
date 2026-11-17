"""Issue #3 -- the crosschat listener must survive NATS being down.

One test per acceptance criterion, each against a real, throwaway nats-server on
a spare port that the test itself stops and starts. Never touches any other NATS.
Drives the real ``python -m crosschat monitor`` as a subprocess.

The nats-server binary comes from the NATS_SERVER_BIN environment variable, else
PATH; without one the tests skip.
"""

from __future__ import annotations

import os
import queue
import shutil
import socket
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import pytest

NATS_BIN = os.environ.get("NATS_SERVER_BIN") or shutil.which("nats-server")

pytestmark = pytest.mark.skipif(not NATS_BIN, reason="no nats-server binary")

# The listener retries a connect every 2 s and nats.py reconnects every 2 s.
RETRY_INTERVAL = 2.0
# nats.py's own first-connect loop wedged after ~20 s down, so boot tests wait longer.
LONG_OUTAGE = 22.0
WITHIN_ONE_BACKOFF = RETRY_INTERVAL + 8.0
# First-connect retries back off 5/10/20/40/60 s after a 10 s attempt.
BOOT_RECOVERY = 60.0


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Nats:
    """A throwaway nats-server with JetStream, stoppable and restartable."""

    def __init__(self, store_dir: Path) -> None:
        self.port = free_port()
        self.url = f"nats://127.0.0.1:{self.port}"
        self.store_dir = store_dir
        self.proc: subprocess.Popen | None = None

    def start(self) -> None:
        self.proc = subprocess.Popen(
            [
                NATS_BIN,
                "-a",
                "127.0.0.1",
                "-p",
                str(self.port),
                "-js",
                "-sd",
                str(self.store_dir),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.5).close()
                return
            except OSError:
                time.sleep(0.1)
        raise RuntimeError("nats-server did not come up")

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(timeout=10)
        self.proc = None

    def init_infra(self) -> None:
        # JetStream needs a moment after the port opens.
        for _ in range(20):
            out = crosschat("init", self.url)
            if "CROSSCHAT_INIT_OK" in out:
                return
            time.sleep(0.5)
        raise RuntimeError(f"crosschat init failed: {out}")


def crosschat(*args: str) -> str:
    result = subprocess.run(
        [sys.executable, "-m", "crosschat", *args],
        capture_output=True,
        text=True,
        timeout=60,
    )
    return (result.stdout or "") + (result.stderr or "")


class Listener:
    """listen.py as a subprocess, with its stdout lines collected."""

    def __init__(self, nats: Nats, project_dir: Path, project_id: str) -> None:
        env = {
            **os.environ,
            "PYTHONUNBUFFERED": "1",
            "PYTHONIOENCODING": "utf-8",
        }
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "crosschat", "monitor", project_id, nats.url],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        self.lines: list[str] = []
        self._new: queue.Queue[str] = queue.Queue()
        self.stderr = ""
        threading.Thread(target=self._read_out, daemon=True).start()
        threading.Thread(target=self._read_err, daemon=True).start()

    def _read_out(self) -> None:
        for line in self.proc.stdout:
            line = line.rstrip("\n")
            self.lines.append(line)
            self._new.put(line)

    def _read_err(self) -> None:
        self.stderr = self.proc.stderr.read()

    def wait_for(self, prefix: str, timeout: float, containing: str = "") -> str:
        """The next line starting with `prefix` (and containing `containing`)."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                line = self._new.get(timeout=0.2)
            except queue.Empty:
                continue
            if line.startswith(prefix) and containing in line:
                return line
        raise AssertionError(
            f"no {prefix!r} within {timeout}s; saw {self.lines!r}, stderr {self.stderr!r}"
        )

    def state_lines(self) -> list[str]:
        return [l for l in self.lines if not l.startswith("CROSSCHAT_MESSAGE")]

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()


@pytest.fixture
def nats(tmp_path):
    server = Nats(tmp_path / "js")
    yield server
    server.stop()


@pytest.fixture
def project(tmp_path):
    path = tmp_path / "proj"
    path.mkdir()
    return path


@pytest.fixture
def pid():
    return f"t29-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def listeners():
    made: list[Listener] = []
    yield made
    for listener in made:
        listener.stop()


def start_listener(listeners, nats, project, pid) -> Listener:
    listener = Listener(nats, project, pid)
    listeners.append(listener)
    return listener


def send(nats: Nats, pid: str, text: str) -> None:
    # send refuses a destination with no registry entry; register is idempotent.
    out = crosschat("register", f"C:/x/{pid}", nats.url, "--id", pid)
    assert f"CROSSCHAT_REGISTERED {pid}" in out, out
    out = crosschat("send", "tester", pid, text, nats.url)
    assert f"CROSSCHAT_SENT {pid}" in out, out


def test_ac1_listener_does_not_exit_while_nats_is_down(nats, project, pid, listeners):
    """AC 1: NATS unreachable at start -> the listener keeps retrying, never exits."""
    listener = start_listener(listeners, nats, project, pid)  # nats never started

    listener.wait_for("CROSSCHAT_WAITING", timeout=15)
    time.sleep(LONG_OUTAGE)  # several attempts' worth

    assert listener.proc.poll() is None, f"listener exited: {listener.lines!r}"
    assert not any(l.startswith("CROSSCHAT_UNAVAILABLE") for l in listener.lines)


def test_ac2_registers_and_delivers_once_nats_arrives(nats, project, pid, listeners):
    """AC 2: NATS comes up -> registered within one backoff interval; a message arrives."""
    listener = start_listener(listeners, nats, project, pid)
    listener.wait_for("CROSSCHAT_WAITING", timeout=15)
    time.sleep(LONG_OUTAGE)

    nats.start()
    nats.init_infra()
    listener.wait_for("CROSSCHAT_REGISTERED", timeout=BOOT_RECOVERY, containing=pid)

    send(nats, pid, "after-start")
    listener.wait_for("CROSSCHAT_MESSAGE", timeout=15, containing="after-start")


def test_ac3_one_line_per_state_change_not_per_attempt(nats, project, pid, listeners):
    """AC 3: many failed attempts -> a single WAITING line, then a single REGISTERED."""
    listener = start_listener(listeners, nats, project, pid)
    listener.wait_for("CROSSCHAT_WAITING", timeout=15)
    time.sleep(LONG_OUTAGE)  # many attempts

    nats.start()
    nats.init_infra()
    listener.wait_for("CROSSCHAT_REGISTERED", timeout=BOOT_RECOVERY)
    time.sleep(1)

    states = listener.state_lines()
    assert [s.split(" ", 1)[0] for s in states] == [
        "CROSSCHAT_WAITING",
        "CROSSCHAT_REGISTERED",
    ], states


def test_ac4_reconnects_and_reregisters_after_a_drop(nats, project, pid, listeners):
    """AC 4: NATS drops after registration and returns -> registered again, no restart."""
    nats.start()
    nats.init_infra()
    listener = start_listener(listeners, nats, project, pid)
    listener.wait_for("CROSSCHAT_REGISTERED", timeout=20, containing=pid)

    nats.stop()
    listener.wait_for("CROSSCHAT_DISCONNECTED", timeout=15)
    assert listener.proc.poll() is None, "listener exited on the drop"

    nats.start()
    listener.wait_for(
        "CROSSCHAT_REGISTERED", timeout=WITHIN_ONE_BACKOFF, containing=pid
    )


def test_ac4a_resubscribes_one_message_before_and_one_after_the_drop(
    nats, project, pid, listeners
):
    """AC 4a: a message before the drop and one after are both delivered."""
    nats.start()
    nats.init_infra()
    listener = start_listener(listeners, nats, project, pid)
    listener.wait_for("CROSSCHAT_REGISTERED", timeout=20, containing=pid)

    send(nats, pid, "before-drop")
    listener.wait_for("CROSSCHAT_MESSAGE", timeout=15, containing="before-drop")

    nats.stop()
    listener.wait_for("CROSSCHAT_DISCONNECTED", timeout=15)
    nats.start()
    listener.wait_for(
        "CROSSCHAT_REGISTERED", timeout=WITHIN_ONE_BACKOFF, containing=pid
    )

    send(nats, pid, "after-drop")
    listener.wait_for("CROSSCHAT_MESSAGE", timeout=15, containing="after-drop")


def test_ac4b_never_silently_deaf_while_disconnected(nats, project, pid, listeners):
    """AC 4b: a drop is announced with DISCONNECTED, then REGISTERED on recovery, once each."""
    nats.start()
    nats.init_infra()
    listener = start_listener(listeners, nats, project, pid)
    listener.wait_for("CROSSCHAT_REGISTERED", timeout=20, containing=pid)

    nats.stop()
    listener.wait_for("CROSSCHAT_DISCONNECTED", timeout=15)
    time.sleep(3 * RETRY_INTERVAL)  # attempts while down stay quiet
    nats.start()
    listener.wait_for("CROSSCHAT_REGISTERED", timeout=WITHIN_ONE_BACKOFF)
    time.sleep(1)

    assert [s.split(" ", 1)[0] for s in listener.state_lines()] == [
        "CROSSCHAT_REGISTERED",
        "CROSSCHAT_DISCONNECTED",
        "CROSSCHAT_REGISTERED",
    ], listener.state_lines()


def test_ac5_second_listener_is_still_refused(nats, project, pid, listeners):
    """AC 5: a second listener for the same project is refused, and does not retry."""
    nats.start()
    nats.init_infra()
    first = start_listener(listeners, nats, project, pid)
    first.wait_for("CROSSCHAT_REGISTERED", timeout=20, containing=pid)
    time.sleep(4)  # let the first one hold its consumer

    second = start_listener(listeners, nats, project, pid)
    code = second.proc.wait(timeout=30)

    assert code != 0
    assert "already bound" in second.stderr.lower(), second.stderr
    assert first.proc.poll() is None


@pytest.mark.skipif(
    not os.environ.get("CROSSCHAT_LONG_OUTAGE_S"),
    reason="set CROSSCHAT_LONG_OUTAGE_S=<seconds> to run the long mid-session outage",
)
def test_ac4c_survives_a_long_mid_session_outage(nats, project, pid, listeners):
    """AC 4: an outage of minutes, not seconds, is survived the same way."""
    nats.start()
    nats.init_infra()
    listener = start_listener(listeners, nats, project, pid)
    listener.wait_for("CROSSCHAT_REGISTERED", timeout=20, containing=pid)

    nats.stop()
    listener.wait_for("CROSSCHAT_DISCONNECTED", timeout=15)
    time.sleep(float(os.environ["CROSSCHAT_LONG_OUTAGE_S"]))
    assert listener.proc.poll() is None, "listener exited during the outage"

    nats.start()
    listener.wait_for("CROSSCHAT_REGISTERED", timeout=BOOT_RECOVERY, containing=pid)
    send(nats, pid, "after-long-outage")
    listener.wait_for("CROSSCHAT_MESSAGE", timeout=30, containing="after-long-outage")
