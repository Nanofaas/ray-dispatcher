import json

import pytest

from ray_dispatcher.errors import HostInUseError
from ray_dispatcher.locking import SessionLock
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _responder(create_rc, owner, new_owner=None):
    """FakeTransport callback: mkdir ok; create returns create_rc; cat returns owner json.

    Once an ``mv -f`` script is observed (a takeover/refresh write landing), ``cat``
    switches to returning ``new_owner`` (defaults to ``owner`` unchanged), mirroring
    a real remote where a completed write is visible to the next read.
    """
    owner_json = json.dumps(owner) if owner is not None else ""
    new_owner_json = json.dumps(new_owner) if new_owner is not None else owner_json
    state = {"written": False}

    def results(argv):
        script = argv[2]
        if "set -C" in script:
            return CommandResult(create_rc, "", "", 0.0)
        if "mv -f" in script:
            state["written"] = True
            return CommandResult(0, "", "", 0.0)
        if script.startswith("cat "):
            current = new_owner_json if state["written"] else owner_json
            rc = 0 if current else 1
            return CommandResult(rc, current, "", 0.0)
        return CommandResult(0, "", "", 0.0)  # mkdir

    return results


def _scripts(t):
    return [c[1][2] for c in t.calls if c[0] == "run"]


def test_live_lock_from_other_session_raises():
    t = FakeTransport(_responder(create_rc=1,
                                 owner={"session_id": "other", "heartbeat": 1000.0}))
    lock = SessionLock(t, "mine", ttl_s=60.0, now=lambda: 1000.0)  # age 0 < ttl
    with pytest.raises(HostInUseError):
        lock.acquire()
    assert not any("mv -f" in s for s in _scripts(t))  # never took it over


def test_stale_lock_is_taken_over():
    t = FakeTransport(_responder(
        create_rc=1,
        owner={"session_id": "other", "heartbeat": 900.0},
        new_owner={"session_id": "mine", "heartbeat": 1000.0},
    ))
    lock = SessionLock(t, "mine", ttl_s=60.0, now=lambda: 1000.0)  # age 100 > ttl
    lock.acquire()  # no raise
    assert any("mv -f" in s for s in _scripts(t))  # took it over


def test_own_lock_is_refreshed():
    t = FakeTransport(_responder(create_rc=1,
                                 owner={"session_id": "mine", "heartbeat": 1000.0}))
    lock = SessionLock(t, "mine", ttl_s=60.0, now=lambda: 1000.0)
    lock.acquire()  # no raise
    assert any("mv -f" in s for s in _scripts(t))


def test_takeover_race_lost_raises():
    """Stale lock, we take over, but the confirming re-read shows another winner."""
    cats = iter([
        {"session_id": "old", "heartbeat": 0.0},    # first read: stale owner
        {"session_id": "rival", "heartbeat": 999.0}, # post-write read: rival won
    ])

    def results(argv):
        script = argv[2]
        if "set -C" in script:
            return CommandResult(1, "", "", 0.0)  # atomic create failed: lock exists
        if script.startswith("cat "):
            return CommandResult(0, json.dumps(next(cats)), "", 0.0)
        return CommandResult(0, "", "", 0.0)  # mkdir, mv

    t = FakeTransport(results)
    lock = SessionLock(t, "mine", ttl_s=60.0, now=lambda: 1000.0)
    with pytest.raises(HostInUseError, match="rival"):
        lock.acquire()
