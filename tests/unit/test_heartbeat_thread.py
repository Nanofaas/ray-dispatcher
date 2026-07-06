import time

from ray_dispatcher.locking import HeartbeatThread, SessionLock
from ray_dispatcher.ssh import FakeTransport


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
