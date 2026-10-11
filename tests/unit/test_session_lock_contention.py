import json
import os
import subprocess
import threading
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from ray_dispatcher.errors import HostInUseError, SessionLockLostError
from ray_dispatcher.locking import SessionLock
from ray_dispatcher.ssh import CommandResult


class LocalTransport:
    def __init__(self, home: Path) -> None:
        self.home = home

    def run(
        self, argv: Sequence[str], *, timeout_s: float | None = None
    ) -> CommandResult:
        completed = subprocess.run(
            [arg.replace("now = time.time()", "now = 1000.0") if isinstance(arg, str) else arg for arg in argv],
            capture_output=True,
            text=True,
            timeout=timeout_s,
            env={**os.environ, "HOME": str(self.home)},
            check=False,
        )
        return CommandResult(
            completed.returncode,
            completed.stdout,
            completed.stderr,
            0.0,
        )


class SplitRaceTransport(LocalTransport):
    """Force two legacy read/write takeovers to each confirm their own write."""

    def __init__(
        self,
        home: Path,
        *,
        first: bool,
        initial_reads: threading.Barrier,
        first_confirmed: threading.Event,
    ) -> None:
        super().__init__(home)
        self.first = first
        self.initial_reads = initial_reads
        self.first_confirmed = first_confirmed
        self.reads = 0

    def run(
        self, argv: Sequence[str], *, timeout_s: float | None = None
    ) -> CommandResult:
        if argv[:2] == ["sh", "-c"] and "mv -f" in argv[2] and not self.first:
            assert self.first_confirmed.wait(timeout=5)
        result = super().run(argv, timeout_s=timeout_s)
        if argv[:2] == ["sh", "-c"] and argv[2].startswith("cat "):
            self.reads += 1
            if self.reads == 1:
                self.initial_reads.wait()
            elif self.first:
                self.first_confirmed.set()
        return result


def _owner_file(home: Path) -> Path:
    return home / ".ray_dispatcher" / "locks" / "session.json"


def _write_owner(home: Path, session_id: str, heartbeat: object) -> None:
    owner = _owner_file(home)
    owner.parent.mkdir(parents=True)
    owner.write_text(
        json.dumps({"session_id": session_id, "heartbeat": heartbeat}),
        encoding="utf-8",
    )


def test_live_lock_from_other_session_raises(tmp_path: Path):
    _write_owner(tmp_path, "other", 1000.0)
    lock = SessionLock(LocalTransport(tmp_path), "mine", ttl_s=60)

    with pytest.raises(HostInUseError, match="other"):
        lock.acquire()


@pytest.mark.parametrize("heartbeat", [900.0, "garbage"])
def test_stale_or_corrupt_lock_is_taken_over(tmp_path: Path, heartbeat: object):
    _write_owner(tmp_path, "other", heartbeat)

    SessionLock(
        LocalTransport(tmp_path), "mine", ttl_s=60
    ).acquire()

    assert json.loads(_owner_file(tmp_path).read_text())["session_id"] == "mine"


@pytest.mark.parametrize("contents", [b"not-json", b"\xff"])
def test_corrupt_owner_file_is_taken_over(tmp_path: Path, contents: bytes):
    owner = _owner_file(tmp_path)
    owner.parent.mkdir(parents=True)
    owner.write_bytes(contents)

    SessionLock(LocalTransport(tmp_path), "mine").acquire()

    assert json.loads(owner.read_text())["session_id"] == "mine"


def test_own_acquire_refreshes(tmp_path: Path):
    _write_owner(tmp_path, "mine", 1.0)

    SessionLock(LocalTransport(tmp_path), "mine").acquire()

    assert json.loads(_owner_file(tmp_path).read_text())["heartbeat"] == 1000


def test_lost_owner_cannot_heartbeat_or_release_new_owner(tmp_path: Path):
    _write_owner(tmp_path, "old", 0.0)
    transport = LocalTransport(tmp_path)
    SessionLock(transport, "new").acquire()

    old = SessionLock(transport, "old")
    with pytest.raises(SessionLockLostError, match="old"):
        old.heartbeat()
    old.release()

    assert json.loads(_owner_file(tmp_path).read_text())["session_id"] == "new"


def test_concurrent_stale_takeover_has_exactly_one_winner(tmp_path: Path):
    _write_owner(tmp_path, "old", 0.0)
    initial_reads = threading.Barrier(2)
    first_confirmed = threading.Event()
    locks = [
        SessionLock(
            SplitRaceTransport(
                tmp_path,
                first=index == 0,
                initial_reads=initial_reads,
                first_confirmed=first_confirmed,
            ),
            session,
        )
        for index, session in enumerate(("one", "two"))
    ]

    def acquire(lock: SessionLock) -> str:
        try:
            lock.acquire()
        except HostInUseError:
            return "blocked"
        return "acquired"

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(acquire, locks))

    assert sorted(outcomes) == ["acquired", "blocked"]


def test_client_clock_skew_cannot_steal_live_lock(tmp_path):
    transport = LocalTransport(tmp_path)
    first = SessionLock(transport, 'first')
    second = SessionLock(transport, 'second')
    first.now = lambda: 1000
    second.now = lambda: 1061
    first.acquire()
    with pytest.raises(HostInUseError):
        second.acquire()
