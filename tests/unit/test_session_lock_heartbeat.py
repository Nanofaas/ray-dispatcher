from ray_dispatcher.locking import SessionLock
from ray_dispatcher.ssh import FakeTransport


def test_heartbeat_is_one_atomic_remote_operation():
    t = FakeTransport()

    SessionLock(t, "mine").heartbeat()

    runs = [call[1] for call in t.calls if call[0] == "run"]
    assert len(runs) == 1
    assert runs[0][0:2] == ("python3", "-c")
    assert runs[0][-3] == "heartbeat"


def test_release_is_one_atomic_remote_operation():
    t = FakeTransport()

    SessionLock(t, "mine").release()

    runs = [call[1] for call in t.calls if call[0] == "run"]
    assert len(runs) == 1
    assert runs[0][0:2] == ("python3", "-c")
    assert runs[0][-3] == "release"
