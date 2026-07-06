import json

import pytest

from ray_dispatcher.locking import SessionLock
from ray_dispatcher.ssh import CommandResult, FakeTransport, TransportError


def test_acquire_is_one_remote_flocked_python_operation():
    t = FakeTransport()

    SessionLock(t, "sess-1", now=lambda: 1234.5).acquire()

    runs = [call[1] for call in t.calls if call[0] == "run"]
    assert len(runs) == 1
    argv = runs[0]
    assert argv[:2] == ("python3", "-c")
    assert "flock" in argv[2]
    assert "os.replace" in argv[2]
    assert argv[-3:] == ("sess-1", "1234.5", "60.0")


def test_payload_contains_session_and_heartbeat():
    lock = SessionLock(FakeTransport(), "sess-9", now=lambda: 1234.5)
    assert json.loads(lock._payload()) == {
        "session_id": "sess-9",
        "heartbeat": 1234.5,
    }


def test_unexpected_remote_failure_is_not_treated_as_success():
    t = FakeTransport(
        lambda argv: CommandResult(17, "", "python failed", 0.0)
    )

    with pytest.raises(TransportError, match="python failed"):
        SessionLock(t, "mine").acquire()
