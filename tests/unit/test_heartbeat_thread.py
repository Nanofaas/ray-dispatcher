import time

import pytest

from it.unimib.data.nanofaas.ray_dispatcher.errors import SessionLockLostError
from it.unimib.data.nanofaas.ray_dispatcher.locking import HeartbeatThread, SessionLock
from it.unimib.data.nanofaas.ray_dispatcher.ssh import CommandResult, FakeTransport


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


def test_unreachable_session_expires_and_notifies_once():
    from it.unimib.data.nanofaas.ray_dispatcher.ssh import TransportError

    def unavailable(_):
        raise TransportError("host disappeared")

    failures = []
    hb = HeartbeatThread(SessionLock(FakeTransport(unavailable), "evicted", ttl_s=0.05), 0.01)
    hb.set_failure_callback(failures.append)
    hb.start()
    try:
        hb.join(timeout=1)
        assert not hb.is_alive(), "lost host must be reported without waiting for a job timeout"
        assert len(failures) == 1
        with pytest.raises(SessionLockLostError, match="evicted"):
            hb.raise_if_failed()
    finally:
        hb.stop()


def test_temporary_transport_error_does_not_lose_session():
    import threading

    from it.unimib.data.nanofaas.ray_dispatcher.ssh import TransportError

    recovered = threading.Event()
    count = 0

    def heartbeat(_):
        nonlocal count
        count += 1
        if count == 1:
            raise TransportError("temporary outage")
        recovered.set()
        return CommandResult(0, "", "", 0.0)

    failures = []
    hb = HeartbeatThread(SessionLock(FakeTransport(heartbeat), "live", ttl_s=1), 0.01)
    hb.set_failure_callback(failures.append)
    hb.start()
    try:
        assert recovered.wait(timeout=1)
        hb.raise_if_failed()
        assert not failures
    finally:
        hb.stop()


def test_successful_refresh_resets_transport_failure_deadline(monkeypatch):
    from types import SimpleNamespace

    from it.unimib.data.nanofaas.ray_dispatcher import locking
    from it.unimib.data.nanofaas.ray_dispatcher.ssh import TransportError

    now = [0.0]
    steps = iter([20.0, 40.0, 60.0, 80.0, 100.0])

    class Steps:
        def is_set(self):
            return False

        def wait(self, _interval):
            value = next(steps, None)
            if value is None:
                return True
            now[0] = value
            return False

    def heartbeat(_):
        if now[0] == 40:
            return CommandResult(0, "", "", 0)
        raise TransportError("outage")

    monkeypatch.setattr(locking, "time", SimpleNamespace(monotonic=lambda: now[0]))
    hb = HeartbeatThread(SessionLock(FakeTransport(heartbeat), "mine", ttl_s=60), 20)
    hb._stopped = Steps()
    failed_at = []
    hb.set_failure_callback(lambda _: failed_at.append(now[0]))
    hb.run()
    assert failed_at == [100.0]


def test_blocked_transport_expires_even_when_it_ignores_timeout():
    import threading

    release = threading.Event()
    started = threading.Event()

    def blocked(_):
        started.set()
        release.wait()
        return CommandResult(0, "", "", 0)

    failures = []
    hb = HeartbeatThread(SessionLock(FakeTransport(blocked), "blackholed", ttl_s=0.08), 0.01)
    hb.set_failure_callback(failures.append)
    hb.start()
    try:
        assert started.wait(1)
        hb.join(timeout=0.5)
        assert not hb.is_alive(), "SSH channel setup must not block lease expiry"
        assert len(failures) == 1
        with pytest.raises(SessionLockLostError, match="blackholed"):
            hb.raise_if_failed()
    finally:
        release.set()
        hb.stop()
    # A late successful reply must not revive an expired session.
    with pytest.raises(SessionLockLostError):
        hb.raise_if_failed()
