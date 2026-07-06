import time

import pytest

from ray_dispatcher.errors import SessionLockLostError
from ray_dispatcher.locking import HeartbeatThread, SessionLock
from ray_dispatcher.ssh import CommandResult, FakeTransport


def test_heartbeat_thread_beats_then_stops():
    t = FakeTransport()
    hb = HeartbeatThread(SessionLock(t, "mine"), interval_s=0.01)
    hb.start()
    time.sleep(0.05)
    hb.stop()
    refreshes = sum(1 for c in t.calls if c[0] == "run")
    assert refreshes >= 1
    # after stop, no further beats
    settled = refreshes
    time.sleep(0.03)
    assert sum(1 for c in t.calls if c[0] == "run") == settled


def test_heartbeat_thread_records_lost_ownership():
    t = FakeTransport(run_results=lambda _argv: CommandResult(4, "", "", 0.0))
    hb = HeartbeatThread(SessionLock(t, "old"), interval_s=0.01)

    hb.start()
    hb.join(timeout=1)

    assert not hb.is_alive()
    with pytest.raises(SessionLockLostError, match="old"):
        hb.raise_if_failed()


def test_heartbeat_thread_notifies_loss_callback():
    failures = []
    hb = HeartbeatThread(
        SessionLock(FakeTransport(lambda _: CommandResult(4, "", "", 0.0)), "old"),
        interval_s=0.01,
    )
    hb.set_failure_callback(failures.append)

    hb.start()
    hb.join(timeout=1)

    assert len(failures) == 1
    assert isinstance(failures[0], SessionLockLostError)
