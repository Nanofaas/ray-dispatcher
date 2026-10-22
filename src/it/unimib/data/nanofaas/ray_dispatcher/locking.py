"""Atomic, heartbeat-backed remote session lock (spec §3.2.6, §6.3.1)."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable

from .errors import HostInUseError, SessionLockLostError
from .ssh import Transport, TransportError

_log = logging.getLogger(__name__)

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
            ],
            **({"timeout_s": self.ttl_s / 3} if action == "heartbeat" else {}),
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

    def _record_failure(self, error: SessionLockLostError) -> None:
        with self._failure_lock:
            self._failure = error
            self._failed.set()
            callback = self._failure_callback
        if callback is not None:
            callback(error)

    def _refresh_before(self, deadline: float) -> None:
        # Fabric's execution timeout excludes SSH channel setup. Supervise the
        # whole operation so an unresponsive connection cannot keep a host alive.
        finished = threading.Event()
        errors: list[Exception] = []

        def refresh() -> None:
            try:
                self._lock.heartbeat()
            except Exception as exc:  # noqa: BLE001 — propagated to the supervisor
                errors.append(exc)
            finally:
                finished.set()

        if time.monotonic() >= deadline:
            raise TimeoutError("session heartbeat deadline expired")
        threading.Thread(target=refresh, daemon=True).start()
        while not self._stopped.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("session heartbeat deadline expired")
            if finished.wait(min(remaining, 0.1)):
                if errors:
                    raise errors[0]
                return

    def run(self) -> None:
        last_confirmed = time.monotonic()
        while not self._stopped.wait(self._interval):
            requested_at = time.monotonic()
            try:
                self._refresh_before(last_confirmed + self._lock.ttl_s)
            except SessionLockLostError as exc:
                self._record_failure(exc)
                return
            except Exception as exc:  # noqa: BLE001 — transient failures are retried within TTL
                if time.monotonic() - last_confirmed >= self._lock.ttl_s:
                    self._record_failure(
                        SessionLockLostError(
                            f"session {self._lock.session_id!r} heartbeat unconfirmed for "
                            f"{self._lock.ttl_s:g}s: {exc}"
                        )
                    )
                    return
                _log.warning("session heartbeat failed; retrying within TTL", exc_info=True)
            else:
                # The remote refresh happened after the request began, not necessarily on reply.
                last_confirmed = requested_at

    def raise_if_failed(self) -> None:
        if self._failed.is_set():
            assert self._failure is not None  # noqa: S101 — internal invariant, not input validation
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
