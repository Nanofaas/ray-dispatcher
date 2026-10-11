"""Atomic, heartbeat-backed remote session lock (spec §3.2.6, §6.3.1)."""

from __future__ import annotations

import threading
from collections.abc import Callable

from .errors import HostInUseError, SessionLockLostError
from .ssh import Transport, TransportError

_CONFLICT = 3
_LOST = 4
_LOCK_SCRIPT = r"""
import fcntl
import json
import os
import sys
import tempfile
import time

action, session_id, ttl_raw = sys.argv[1:]
ttl = float(ttl_raw)
lock_dir = os.path.join(os.environ["HOME"], ".ray_dispatcher", "locks")
owner_path = os.path.join(lock_dir, "session.json")
guard_path = os.path.join(lock_dir, ".session.guard")
os.makedirs(lock_dir, exist_ok=True)

with open(guard_path, "a", encoding="utf-8") as guard:
    fcntl.flock(guard, fcntl.LOCK_EX)
    now = time.time()
    try:
        with open(owner_path, encoding="utf-8") as owner_file:
            owner = json.load(owner_file)
    except (FileNotFoundError, json.JSONDecodeError, UnicodeError):
        owner = None

    owned = isinstance(owner, dict) and owner.get("session_id") == session_id
    if action == "acquire" and not owned and isinstance(owner, dict):
        try:
            live = now - float(owner.get("heartbeat", 0)) <= ttl
        except (TypeError, ValueError):
            live = False
        if live:
            print(owner.get("session_id", ""))
            raise SystemExit(3)

    if action == "heartbeat" and not owned:
        raise SystemExit(4)

    if action in {"acquire", "heartbeat"}:
        fd, temporary = tempfile.mkstemp(prefix=".session.", dir=lock_dir)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as temporary_file:
                json.dump({"session_id": session_id, "heartbeat": now}, temporary_file)
            os.replace(temporary, owner_path)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
    elif action == "release" and owned:
        try:
            os.unlink(owner_path)
        except FileNotFoundError:
            pass
"""


class SessionLock:
    def __init__(
        self,
        transport: Transport,
        session_id: str,
        *,
        ttl_s: float = 60.0,
    ) -> None:
        self.transport = transport
        self.session_id = session_id
        self.ttl_s = ttl_s

    def _operate(self, action: str) -> None:
        result = self.transport.run(
            [
                "python3",
                "-c",
                _LOCK_SCRIPT,
                action,
                self.session_id,
                str(self.ttl_s),
            ]
        )
        if result.returncode == 0:
            return
        if action == "acquire" and result.returncode == _CONFLICT:
            raise HostInUseError(f"session lock held by {result.stdout.strip()!r}")
        if action == "heartbeat" and result.returncode == _LOST:
            raise SessionLockLostError(
                f"session {self.session_id!r} no longer owns the remote lock"
            )
        detail = result.stderr.strip() or result.stdout.strip()
        raise TransportError(
            f"session-lock {action} failed ({result.returncode}): {detail}",
            returncode=result.returncode,
        )

    def acquire(self) -> None:
        self._operate("acquire")

    def heartbeat(self) -> None:
        self._operate("heartbeat")

    def release(self) -> None:
        self._operate("release")


class HeartbeatThread(threading.Thread):
    """Background daemon that refreshes a SessionLock until stopped."""

    def __init__(self, lock: SessionLock, interval_s: float) -> None:
        super().__init__(daemon=True)
        self._lock = lock
        self._interval = interval_s
        self._stopped = threading.Event()
        self._failed = threading.Event()
        self._failure: SessionLockLostError | None = None
        self._failure_callback: Callable[[SessionLockLostError], None] | None = None
        self._failure_lock = threading.Lock()

    def run(self) -> None:
        while not self._stopped.wait(self._interval):
            try:
                self._lock.heartbeat()
            except SessionLockLostError as exc:
                with self._failure_lock:
                    self._failure = exc
                    self._failed.set()
                    callback = self._failure_callback
                if callback is not None:
                    callback(exc)
                return
            except Exception:  # noqa: BLE001 — heartbeat is best-effort; TTL is the net
                pass

    def raise_if_failed(self) -> None:
        if self._failed.is_set():
            assert self._failure is not None
            raise self._failure

    def set_failure_callback(self, callback: Callable[[SessionLockLostError], None]) -> None:
        with self._failure_lock:
            self._failure_callback = callback
            failure = self._failure
        if failure is not None:
            callback(failure)

    def stop(self) -> None:
        self._stopped.set()
        if self.is_alive():  # safe in a finally-block even if start() never ran
            self.join(timeout=5)
