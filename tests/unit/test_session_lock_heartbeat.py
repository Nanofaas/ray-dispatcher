from it.unimib.data.nanofaas.ray_dispatcher.locking import SessionLock
from it.unimib.data.nanofaas.ray_dispatcher.ssh import FakeTransport


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


def test_heartbeat_transport_deadline_is_shorter_than_session_ttl():
    from it.unimib.data.nanofaas.ray_dispatcher.ssh import CommandResult

    class DeadlineTransport(FakeTransport):
        def run(self, argv, *, timeout_s=None):
            assert timeout_s is not None and 0 < timeout_s < 60
            return CommandResult(0, "", "", 0)

    SessionLock(DeadlineTransport(), "mine", ttl_s=60).heartbeat()
