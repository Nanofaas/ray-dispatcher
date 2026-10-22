"""Host-side SSH/rsync transport and shared SSH options (spec §3.1, §4.1, §5)."""

from __future__ import annotations

import math
import os
import shlex
import subprocess
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

import fabric
import paramiko

from .errors import DispatcherError, ModelValidationError
from .models import RemoteHost

DEFAULT_CONNECT_TIMEOUT_S = 10.0
DEFAULT_CONTROL_TIMEOUT_S = 300.0
DEFAULT_RSYNC_TIMEOUT_S = 300.0
# rsync's OpenSSH connections share one master per (host, user, port): a job's
# burst of transfers opens one connection, not one each, so networks that
# rate-limit SSH connections do not block the host. The master closes itself
# after this idle time.
SSH_CONTROL_PERSIST = "60s"


class _DefaultTimeout:
    pass


_DEFAULT_TIMEOUT = _DefaultTimeout()


def _validate_timeout(value: object, name: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ModelValidationError(f"{name} must be finite and > 0")
    return float(value)


def _resolve_existing(path: str, label: str) -> str:
    resolved = os.path.abspath(os.path.expanduser(path))
    if not os.path.isfile(resolved):
        raise ModelValidationError(f"{label} not found: {path}")
    return resolved


@dataclass(frozen=True)
class SshConfig:
    host: str
    user: str
    port: int
    identity_file: str | None
    known_hosts_file: str

    @classmethod
    def from_host(cls, host: RemoteHost) -> SshConfig:
        known_hosts = _resolve_existing(host.known_hosts_file, "known_hosts file")
        identity = (
            _resolve_existing(host.identity_file, "identity file")
            if host.identity_file is not None
            else None
        )
        return cls(
            host=host.host,
            user=host.user,
            port=host.port,
            identity_file=identity,
            known_hosts_file=known_hosts,
        )


class TransportError(DispatcherError):
    """A transport (ssh/rsync) operation failed. Phase 5 maps this to FailureKind.SSH."""

    def __init__(self, message: str, *, returncode: int | None = None) -> None:
        super().__init__(message)
        self.returncode: int | None = returncode


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str
    duration_s: float

    @property
    def ok(self) -> bool:
        return self.returncode == 0


class Transport(Protocol):
    def run(self, argv: Sequence[str], *, timeout_s: float | None = None) -> CommandResult: ...

    def push(
        self,
        local: str,
        remote: str,
        *,
        delete: bool = False,
        excludes: Sequence[str] = (),
        chmod: str | None = None,
    ) -> None: ...

    def pull(
        self, remote: str, local: str, *, delete: bool = False, excludes: Sequence[str] = ()
    ) -> None: ...


class FakeTransport:
    """In-memory Transport for unit tests. Records calls; programmable run results."""

    def __init__(self, run_results: Callable[[list[str]], CommandResult] | None = None) -> None:
        self.calls: list[tuple[object, ...]] = []
        self._run_results = run_results

    def run(self, argv: Sequence[str], *, timeout_s: float | None = None) -> CommandResult:
        self.calls.append(("run", tuple(argv)))
        if self._run_results is not None:
            return self._run_results(list(argv))
        return CommandResult(0, "", "", 0.0)

    def push(
        self,
        local: str,
        remote: str,
        *,
        delete: bool = False,
        excludes: Sequence[str] = (),
        chmod: str | None = None,
    ) -> None:
        self.calls.append(("push", (local, remote, delete, tuple(excludes), chmod)))

    def pull(
        self, remote: str, local: str, *, delete: bool = False, excludes: Sequence[str] = ()
    ) -> None:
        self.calls.append(("pull", (remote, local, delete, tuple(excludes))))


def _control_path() -> str:
    """The master socket of a (host, user, port): ``%C`` hashes it; kept short
    for the Unix socket path limit (104 bytes on macOS)."""
    return os.path.join(os.path.expanduser("~"), ".ssh", "rd-%C")


def _ssh_e_option(cfg: SshConfig) -> str:
    """The rsync ``-e`` value: an ssh invocation carrying the same SSH settings
    as Fabric, multiplexed over one master connection per host. shlex.join keeps
    paths with spaces safe."""
    parts = [
        "ssh",
        "-p",
        str(cfg.port),
        "-o",
        f"UserKnownHostsFile={cfg.known_hosts_file}",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        "GlobalKnownHostsFile=/dev/null",
        "-o",
        "BatchMode=yes",
        "-o",
        "ControlMaster=auto",
        "-o",
        f"ControlPath={_control_path()}",
        "-o",
        f"ControlPersist={SSH_CONTROL_PERSIST}",
    ]
    if cfg.identity_file:
        parts += ["-i", cfg.identity_file]
    return shlex.join(parts)


def build_rsync_argv(
    cfg: SshConfig,
    src: str,
    dst: str,
    *,
    delete: bool,
    excludes: Sequence[str],
    chmod: str | None = None,
) -> list[str]:
    # --protect-args: stops the remote shell from mangling paths with spaces or special chars
    argv = ["rsync", "-a", "--compress", "--protect-args", "-e", _ssh_e_option(cfg)]
    if delete:
        argv.append("--delete")
    if chmod is not None:
        argv.append(f"--chmod={chmod}")  # final perms set during transfer (no window)
    for ex in excludes:
        argv += ["--exclude", ex]
    argv += [src, dst]
    return argv


class SshTransport:
    """Fabric for run (Task 5), OpenSSH rsync for push/pull. The Fabric
    connection is created lazily so file transfer needs no live connection."""

    def __init__(
        self,
        cfg: SshConfig,
        *,
        connect_timeout_s: float = DEFAULT_CONNECT_TIMEOUT_S,
        control_timeout_s: float = DEFAULT_CONTROL_TIMEOUT_S,
        rsync_timeout_s: float = DEFAULT_RSYNC_TIMEOUT_S,
    ) -> None:
        self.cfg = cfg
        self.connect_timeout_s = _validate_timeout(connect_timeout_s, "connect_timeout_s")
        self.control_timeout_s = _validate_timeout(control_timeout_s, "control_timeout_s")
        self.rsync_timeout_s = _validate_timeout(rsync_timeout_s, "rsync_timeout_s")
        self._conn: Any = None  # lazily-built fabric.Connection (Task 5)
        self._timed_out = False

    def push(
        self,
        local: str,
        remote: str,
        *,
        delete: bool = False,
        excludes: Sequence[str] = (),
        chmod: str | None = None,
    ) -> None:
        dst = f"{self.cfg.user}@{self.cfg.host}:{remote}"
        self._rsync(
            build_rsync_argv(self.cfg, local, dst, delete=delete, excludes=excludes, chmod=chmod)
        )

    def pull(
        self, remote: str, local: str, *, delete: bool = False, excludes: Sequence[str] = ()
    ) -> None:
        src = f"{self.cfg.user}@{self.cfg.host}:{remote}"
        self._rsync(build_rsync_argv(self.cfg, src, local, delete=delete, excludes=excludes))

    def run(
        self,
        argv: Sequence[str],
        *,
        timeout_s: float | None | _DefaultTimeout = _DEFAULT_TIMEOUT,
    ) -> CommandResult:
        if isinstance(timeout_s, _DefaultTimeout):
            effective_timeout = self.control_timeout_s
        elif timeout_s is None:
            effective_timeout = None
        else:
            effective_timeout = _validate_timeout(timeout_s, "timeout_s")
        if self._timed_out:
            raise TransportError("SSH connection disabled after an unconfirmed command timeout")
        if self._conn is None:
            self._conn = build_connection(self.cfg, connect_timeout_s=self.connect_timeout_s)
        command = shlex.join(argv)  # argv is library-controlled; never a user job string
        start = time.monotonic()
        try:
            result = self._run_bounded(command, effective_timeout)
        except Exception as exc:  # noqa: BLE001 — uniform seam failure
            raise TransportError(f"ssh run failed for {command!r}: {exc}") from exc
        return CommandResult(
            returncode=result.exited,
            stdout=result.stdout,
            stderr=result.stderr,
            duration_s=time.monotonic() - start,
        )

    def _run_bounded(self, command: str, timeout: float | None) -> Any:
        connection = self._conn
        finished = threading.Event()
        results: list[Any] = []
        errors: list[BaseException] = []

        def invoke() -> None:
            try:
                results.append(
                    connection.run(command, hide=True, warn=True, timeout=timeout, in_stream=False)
                )
            except BaseException as exc:
                errors.append(exc)
            finally:
                finished.set()

        # Invoke's own timer starts after channel setup and command acknowledgement.
        # An outer deadline covers those phases as well. A timed-out transport is
        # never reused: cleanup must not send later commands on an ambiguous session.
        threading.Thread(target=invoke, daemon=True).start()
        if not finished.wait(timeout):
            self._timed_out = True
            connection.cancelled = True
            threading.Thread(target=connection.close, daemon=True).start()
            raise TimeoutError(f"SSH operation exceeded {timeout}s")
        if errors:
            raise errors[0]
        return results[0]

    def _rsync(self, argv: list[str]) -> None:
        try:
            # ssh does not create the directory of its control socket
            os.makedirs(os.path.dirname(_control_path()), mode=0o700, exist_ok=True)
            subprocess.run(  # noqa: S603 — argv constructed by build_rsync_argv
                argv,
                check=True,
                capture_output=True,
                text=True,
                timeout=self.rsync_timeout_s,
            )
        except subprocess.TimeoutExpired as exc:
            raise TransportError(f"rsync timed out after {exc.timeout}s") from exc
        except subprocess.CalledProcessError as exc:
            raise TransportError(
                f"rsync failed ({exc.returncode}): {exc.stderr}", returncode=exc.returncode
            ) from exc
        except OSError as exc:
            raise TransportError(f"rsync launch failed: {exc}") from exc


def terminate_process_group(
    transport: Transport,
    pgid: int,
    *,
    grace_s: float = 10.0,
    poll_s: float = 0.5,
    now: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    deadline_s: float | None = None,
) -> bool:
    """SIGTERM the remote process group, wait up to grace_s, then SIGKILL.
    Returns True once a probe confirms the group is gone (spec §8.1)."""

    def command(argv: list[str]) -> CommandResult:
        if deadline_s is None:
            return transport.run(argv)
        remaining = deadline_s - now()
        if remaining <= 0:
            raise TransportError("process cleanup deadline expired")
        return transport.run(argv, timeout_s=remaining)

    def gone() -> bool:
        return command(["kill", "-0", f"-{pgid}"]).returncode != 0

    command(["kill", "-TERM", f"-{pgid}"])
    if gone():
        return True
    deadline = now() + grace_s
    if deadline_s is not None:
        deadline = min(deadline, deadline_s - poll_s)
    while now() < deadline:
        if gone():
            return True
        sleep(min(poll_s, max(0, deadline - now())))
    command(["kill", "-KILL", f"-{pgid}"])
    sleep(min(poll_s, max(0, (deadline_s - now()) / 2)) if deadline_s is not None else poll_s)
    return gone()


def write_remote_file(
    transport: Transport, path: str, content: str, *, mode: int | None = None
) -> None:
    """Atomically write ``content`` to absolute remote ``path`` (spec §7 no-shell).

    Data reaches the shell only as a single shlex-quoted ``printf %s`` argument;
    the temp file is renamed into place so a partial write is never observed.
    """
    qtmp = shlex.quote(f"{path}.tmp")
    chmod = f" && chmod {mode:o} {qtmp}" if mode is not None else ""
    cmd = f"printf %s {shlex.quote(content)} > {qtmp}{chmod} && mv -f {qtmp} {shlex.quote(path)}"
    result = transport.run(["sh", "-c", cmd])
    if result.returncode != 0:
        raise TransportError(f"failed to write {path}: {result.stderr}")


class _CancellableConnection(fabric.Connection):  # type: ignore[misc]
    cancelled = False

    def open(self) -> Any:
        # Fabric calls open() for run/create_session, including implicit reconnects.
        # close() alone cannot cancel an in-progress client.connect().
        if self.cancelled:
            raise TransportError("SSH connection cancelled")
        result = super().open()
        if self.cancelled:
            self.client.close()
            raise TransportError("SSH connection cancelled during connect")
        return result


def build_connection(
    cfg: SshConfig, *, connect_timeout_s: float = DEFAULT_CONNECT_TIMEOUT_S
) -> fabric.Connection:
    """A Fabric connection that enforces host-key checking against cfg.known_hosts_file."""
    connect_timeout_s = _validate_timeout(connect_timeout_s, "connect_timeout_s")
    connect_kwargs: dict[str, object] = {}
    if cfg.identity_file:
        connect_kwargs["key_filename"] = cfg.identity_file
    conn = _CancellableConnection(
        host=cfg.host,
        user=cfg.user,
        port=cfg.port,
        connect_timeout=connect_timeout_s,
        connect_kwargs=connect_kwargs,
    )
    client = conn.client  # lazily-created paramiko.SSHClient
    client.load_host_keys(cfg.known_hosts_file)
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    return conn
