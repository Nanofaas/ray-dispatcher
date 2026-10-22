"""Exclusive local-Ray execution backend (spec §3.2, §3.3).

This is the only module that imports Ray. The HostLease actor wraps the Ray-free
LeaseService; SshRayBackend owns one local Ray runtime, started after provisioning
and shut down at teardown.
"""

from __future__ import annotations

import logging
import math
import secrets
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Sequence
from functools import partial
from typing import cast

import ray

from ..digests import runner_digest
from ..errors import (
    DispatcherError,
    ModelValidationError,
    NoHealthyHostsError,
    RayRuntimeConflictError,
    SessionLockLostError,
)
from ..locking import HeartbeatThread
from ..models import (
    HostProvisioningResult,
    Inventory,
    Job,
    JobHandle,
    JobResult,
    JobStatus,
    Project,
    ProvisioningReport,
    RemoteHost,
    RetryPolicy,
)
from ..provisioning import ProvisioningOutcome, RemoteLayout, _default_transport, _label, provision
from ..results import JobLayout, read_result_json, write_result_json
from ..scheduling import (
    HostRuntime,
    Lease,
    LeaseService,
    _LeaseLostError,
    reconcile_host,
    run_job,
    secret_env_map,
)
from ..ssh import Transport
from .base import ExecutionBackend

_log = logging.getLogger(__name__)
_DRAIN_TIMEOUT_S = 30.0

# The async lease state machine, run as a Ray actor that holds no host CPU (§3.2.3).
HostLease = ray.remote(num_cpus=0)(LeaseService)


class _ActorLeaseHandle:
    """Synchronous LeaseHandle with heartbeat daemon (§8.2)."""

    def __init__(
        self,
        actor: ray.actor.ActorHandle[LeaseService],
        *,
        heartbeat_interval_s: float = 5.0,
    ) -> None:
        self._actor = actor
        self._heartbeat_interval_s = heartbeat_interval_s
        self._stop = threading.Event()
        self._lost = threading.Event()
        self._hb_thread: threading.Thread | None = None

    def acquire(self, attempt_id: str, *, exclude: Iterable[str] = ()) -> Lease:
        try:
            lease = ray.get(self._actor.acquire.remote(attempt_id, exclude=tuple(exclude)))
        except ray.exceptions.RayError as exc:
            raise NoHealthyHostsError(f"lease actor unavailable: {exc}") from exc
        self._lost.clear()
        self._stop.clear()
        self._hb_thread = threading.Thread(
            target=self._heartbeat, args=(lease.token,), daemon=True, name="lease-heartbeat"
        )
        self._hb_thread.start()
        return lease  # type: ignore[no-any-return]

    def release(self, token: str) -> None:
        self._stop.set()
        if self._hb_thread is not None:
            self._hb_thread.join(timeout=self._heartbeat_interval_s + 1.0)
            self._hb_thread = None
        try:
            ray.get(self._actor.release.remote(token), timeout=5.0)
        except ray.exceptions.RayError:
            self._lost.set()
            _log.warning("lease release failed; host remains unconfirmed", exc_info=True)

    def register_pid(self, token: str, pid_path: str) -> None:
        ray.get(self._actor.register_pid.remote(token, pid_path))

    def quarantine(self, host: str) -> None:
        try:
            ray.get(self._actor.quarantine.remote(host), timeout=5.0)
        except ray.exceptions.RayError:
            self._lost.set()
            _log.warning("lease quarantine failed; actor unavailable", exc_info=True)

    def raise_if_lost(self) -> None:
        if self._lost.is_set():
            raise _LeaseLostError("lease lost: heartbeat rejected or actor unavailable")

    def _heartbeat(self, token: str) -> None:
        while not self._stop.wait(self._heartbeat_interval_s):
            try:
                alive = ray.get(
                    self._actor.heartbeat.remote(token), timeout=self._heartbeat_interval_s
                )
            except Exception:
                self._lost.set()
                break
            if not alive:
                self._lost.set()
                break


@ray.remote(num_cpus=0, resources={"vm_slot": 1}, max_retries=0)
def _attempt_task(
    job: Job,
    batch_id: str,
    local: JobLayout,
    runtimes: dict[str, HostRuntime],
    inv_hosts: dict[str, RemoteHost],
    actor: ray.actor.ActorHandle,  # type: ignore[type-arg]
    transport_factory: Callable[[RemoteHost], Transport],
    policy: RetryPolicy,
    heartbeat_interval_s: float = 5.0,
    elastic: bool = False,
) -> JobResult:
    """Ray task: run one logical job; terminates as a JobResult (§3.2.3, §6a residual)."""
    lease_handle = _ActorLeaseHandle(actor, heartbeat_interval_s=heartbeat_interval_s)

    def context(host: str) -> tuple[HostRuntime, RemoteHost]:
        return cast(tuple[HostRuntime, RemoteHost], ray.get(actor.host_context.remote(host)))

    try:
        result = run_job(
            job,
            batch_id=batch_id,
            lease=lease_handle,
            runtime_for=(lambda host: context(host)[0]) if elastic else runtimes.__getitem__,
            transport_for=lambda host: transport_factory(
                context(host)[1] if elastic else inv_hosts[host]
            ),
            local=local,
            policy=policy,
        )
    # Ray delivers cancel to a running task as KeyboardInterrupt; TaskCancelledError
    # covers the pre-start window. Both terminate as a CANCELLED JobResult (§8.1).
    except (KeyboardInterrupt, ray.exceptions.TaskCancelledError):
        result = JobResult(
            id=job.id,
            batch_id=batch_id,
            status=JobStatus.CANCELLED,
            returncode=None,
            duration_s=0.0,
            host=None,
            output_dir=None,
            attempts=(),
            error="cancelled",
        )
    except Exception as exc:
        result = JobResult(
            id=job.id,
            batch_id=batch_id,
            status=JobStatus.FAILED,
            returncode=None,
            duration_s=0.0,
            host=None,
            output_dir=None,
            attempts=(),
            error=f"INTERNAL: {type(exc).__name__}: {exc}",
        )
    write_result_json(local.result_json, result)
    return result


class SshRayBackend(ExecutionBackend):
    """Owns one exclusive local Ray runtime and a HostLease actor (spec §3.2)."""

    def __init__(
        self,
        *,
        runner_path: str | None = None,
        transport_factory: Callable[..., Transport] | None = None,
        retry_policy: RetryPolicy = RetryPolicy(),  # noqa: B008
        min_disk_mb: int = 500,
        results_dir: str = "./results",
        require_all_hosts: bool = True,
        lease_ttl_s: float = 60.0,
        sweep_interval_s: float = 10.0,
        elastic: bool = False,
        capacity_wait_s: float = 900.0,
    ) -> None:
        from .. import remote_runner

        if isinstance(min_disk_mb, bool) or not isinstance(min_disk_mb, int) or min_disk_mb < 0:
            raise ModelValidationError("min_disk_mb must be a non-negative integer")
        for name, value in (
            ("lease_ttl_s", lease_ttl_s),
            ("sweep_interval_s", sweep_interval_s),
            ("capacity_wait_s", capacity_wait_s),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ModelValidationError(f"{name} must be finite and > 0")
        self._runner_path = runner_path or remote_runner.__file__
        self._transport_factory = transport_factory or _default_transport
        self._retry_policy = retry_policy
        self._min_disk_mb = min_disk_mb
        self._results_dir = results_dir
        self._require_all_hosts = require_all_hosts
        self._lease_ttl_s = lease_ttl_s
        self._heartbeat_interval_s = min(5.0, lease_ttl_s / 3.0)
        self._sweep_interval_s = sweep_interval_s
        self._sweep_stop = threading.Event()
        self._sweeper: threading.Thread | None = None
        self._owns_runtime = False
        self._actor: ray.actor.ActorHandle | None = None  # type: ignore[type-arg]
        self._outcome: ProvisioningOutcome | None = None
        self._runtimes: dict[str, HostRuntime] = {}
        self._inv_hosts: dict[str, RemoteHost] = {}
        self._refs: dict[str, object] = {}
        self._submitted: set[tuple[str, str]] = set()
        self._cancel_requested: set[str] = set()
        self._draining = False
        self._session_loss: SessionLockLostError | None = None
        self._session_loss_lock = threading.Lock()
        self._elastic = elastic
        self._capacity_wait_s = capacity_wait_s
        self._membership = threading.Condition(threading.RLock())
        self._preparing: set[str] = set()
        self._extra_outcomes: list[ProvisioningOutcome] = []
        self._lost_hosts: set[str] = set()
        self._project: Project | None = None
        self._pending: dict[str, tuple[JobHandle, Job]] = {}
        self._local_results: dict[str, JobResult] = {}
        self._empty_since: float | None = None
        self._handles: dict[str, JobHandle] = {}
        self._confirming: set[str] = set()
        self._reconciling: set[str] = set()
        self._host_heartbeats: dict[str, HeartbeatThread] = {}

    def _on_host_lost(
        self, host: str, error: SessionLockLostError, *, heartbeat: HeartbeatThread | None = None
    ) -> None:
        with self._membership:
            actor = self._actor
            if (
                host in self._lost_hosts
                or actor is None
                or host not in self._runtimes
                or (heartbeat is not None and self._host_heartbeats.get(host) is not heartbeat)
            ):
                return
            ray.get(actor.lose_host.remote(host), timeout=5.0)
            self._lost_hosts.add(host)
        _log.warning("host %s lost its session: %s", host, error)

    def _watch_outcome(self, outcome: ProvisioningOutcome, hosts: Sequence[RemoteHost]) -> None:
        if not self._elastic:
            outcome.set_session_loss_callback(self._on_session_lost)
            return
        names = {_label(host): host.host for host in hosts}
        outcome.sessions = {
            names.get(label, label): pair for label, pair in outcome.sessions.items()
        }
        for host, (_, heartbeat) in outcome.sessions.items():
            self._host_heartbeats[host] = heartbeat
            heartbeat.set_failure_callback(partial(self._on_host_lost, host, heartbeat=heartbeat))

    def add_hosts(self, hosts: Sequence[RemoteHost]) -> ProvisioningReport:
        """Provision concurrently; publish only fully prepared hosts to the lease actor."""
        if not hosts:
            return ProvisioningReport(())
        inventory = Inventory(tuple(hosts))
        names = {host.host for host in hosts}
        with self._membership:
            if not self._elastic or self._actor is None or self._draining:
                raise DispatcherError("adding hosts requires an active elastic backend")
            if names & (self._inv_hosts.keys() | self._preparing | self._confirming):
                raise ModelValidationError("host already registered or preparing")
            self._preparing.update(names)
        outcome: ProvisioningOutcome | None = None
        admitted = False
        try:
            assert self._project is not None  # noqa: S101
            outcome = provision(
                inventory,
                self._project,
                runner_path=self._runner_path,
                transport_factory=self._transport_factory,
                min_disk_mb=self._min_disk_mb,
                allow_empty=True,
            )
            inv_hosts = {host.host: host for host in hosts}
            runtimes = {
                result.host: self._build_runtime(
                    self._project, result, inv_hosts[result.host], runner_digest(self._runner_path)
                )
                for result in outcome.report.hosts
                if result.succeeded
            }
            self._watch_outcome(outcome, inventory.hosts)
            with self._membership:
                if self._draining:
                    raise DispatcherError("backend is draining; host admission stopped")
                outcome.raise_if_session_lost()
                assert self._actor is not None  # noqa: S101
                self._inv_hosts.update({name: inv_hosts[name] for name in runtimes})
                self._runtimes.update(runtimes)
                self._extra_outcomes.append(outcome)
                # Retain sessions even if actor registration fails ambiguously: workers
                # may already have leased this host, so teardown must drain them first.
                admitted = True
                ray.get(
                    self._actor.add_hosts.remote(
                        {name: inv_hosts[name].slots for name in runtimes},
                        {name: (runtime, inv_hosts[name]) for name, runtime in runtimes.items()},
                    )
                )
            return outcome.report
        finally:
            try:
                if outcome is not None and not admitted:
                    outcome.release_all()
            finally:
                with self._membership:
                    self._preparing.difference_update(names)
                    if not admitted:
                        for name in names:
                            self._host_heartbeats.pop(name, None)
                    self._membership.notify_all()

    def remove_hosts(self, hosts: Sequence[str]) -> None:
        with self._membership:
            if not self._elastic or self._actor is None:
                raise DispatcherError("removing hosts requires an active elastic backend")
            ray.get(self._actor.remove_hosts.remote(tuple(hosts)))

    def confirm_hosts_stopped(self, hosts: Sequence[str]) -> None:
        """Caller attests VM deletion/stop; wait for stale local attempts before reuse.

        Safe to run in a preparation thread. An unknown host is a no-op. On timeout,
        metadata is retained and confirmation can be retried after workers finish.
        """
        for host in hosts:
            with self._membership:
                if not self._elastic or self._actor is None or self._draining:
                    raise DispatcherError("confirmation requires an active elastic backend")
                if host in self._preparing or host in self._confirming:
                    raise DispatcherError("host membership operation already in progress")
                if host not in self._inv_hosts:
                    continue
                self._confirming.add(host)
            try:
                self._forget_stopped_host(host)
            finally:
                with self._membership:
                    self._confirming.discard(host)
                    self._membership.notify_all()

    def _finished_job_ids(self) -> tuple[str, ...]:
        with self._membership:
            refs = dict(self._refs)
            handles = dict(self._handles)
        if not refs:
            return ()
        ready = set(ray.wait(list(refs.values()), num_returns=len(refs), timeout=0)[0])
        return tuple(
            f"{handles[token].batch_id}/{handles[token].job_id}"
            for token, ref in refs.items()
            if ref in ready
        )

    def _forget_stopped_host(self, host: str) -> None:
        actor = self._actor
        assert actor is not None  # noqa: S101
        ray.get(actor.lose_host.remote(host), timeout=5.0)
        deadline = time.monotonic() + _DRAIN_TIMEOUT_S
        while True:
            with self._membership:
                reconciling = host in self._reconciling
            if not reconciling and ray.get(
                actor.forget_stopped_host.remote(host, self._finished_job_ids()), timeout=5.0
            ):
                break
            if time.monotonic() >= deadline:
                raise DispatcherError(f"old attempts on {host} have not stopped; identity retained")
            time.sleep(0.05)
        with self._membership:
            self._inv_hosts.pop(host, None)
            self._runtimes.pop(host, None)
            self._lost_hosts.discard(host)
            self._host_heartbeats.pop(host, None)
            outcomes = list(self._extra_outcomes)
            if self._outcome is not None:
                outcomes.append(self._outcome)
            sessions = [
                pair
                for outcome in outcomes
                if (pair := outcome.sessions.pop(host, None)) is not None
            ]
        # VM deletion already removed the remote lock. SSH unlock could target a
        # replacement at the same address, so stop only local heartbeat threads.
        for _, heartbeat in sessions:
            heartbeat.stop()

    def available_hosts(self) -> tuple[str, ...]:
        self._fail_if_session_lost()
        actor = self._actor
        if actor is None:
            return ()
        return cast(tuple[str, ...], ray.get(actor.available_hosts.remote()))

    def _on_session_lost(self, error: SessionLockLostError) -> None:
        with self._session_loss_lock:
            if self._session_loss is not None:
                return
            self._session_loss = error
            refs = list(self._refs.items())
        for token, ref in refs:
            try:
                self._request_cancel(token, ref)
            except BaseException:
                _log.exception("failed to cancel task after session lock loss")

    def _fail_if_session_lost(self) -> None:
        if self._elastic:
            with self._membership:
                outcomes = list(self._extra_outcomes)
                if self._outcome is not None:
                    outcomes.append(self._outcome)
                heartbeats = [
                    (host, heartbeat)
                    for outcome in outcomes
                    for host, (_, heartbeat) in outcome.sessions.items()
                ]
            for host, heartbeat in heartbeats:
                try:
                    heartbeat.raise_if_failed()
                except SessionLockLostError as exc:
                    self._on_host_lost(host, exc, heartbeat=heartbeat)
            return
        with self._session_loss_lock:
            error = self._session_loss
        if error is not None:
            raise error
        if self._outcome is None:
            return
        try:
            self._outcome.raise_if_session_lost()
        except SessionLockLostError as exc:
            self._on_session_lost(exc)
            raise exc

    def setup(
        self, inventory: Inventory, project: Project, *, force: bool = False
    ) -> ProvisioningReport:
        if not inventory.hosts and not self._elastic:
            raise ModelValidationError("empty inventory requires elastic mode")
        if ray.is_initialized():
            raise RayRuntimeConflictError(
                "a Ray runtime is already initialized; ray_dispatcher owns its own "
                "local runtime and will not attach to an external one (§3.2)"
            )
        self._draining = False
        self._project = project
        with self._session_loss_lock:
            self._session_loss = None
        # Provision first; provision() raises NoHealthyHostsError if none succeed,
        # so Ray is never started for an empty healthy set (§3.2.2).
        if inventory.hosts:
            outcome = provision(
                inventory,
                project,
                runner_path=self._runner_path,
                transport_factory=self._transport_factory,
                min_disk_mb=self._min_disk_mb,
                require_all_hosts=self._require_all_hosts,
                force=force,
            )
        else:
            outcome = ProvisioningOutcome(ProvisioningReport(()))
        self._outcome = outcome
        try:
            self._watch_outcome(outcome, inventory.hosts)
            outcome.raise_if_session_lost()
            runner_dig = runner_digest(self._runner_path)
            healthy = {h.host: h for h in outcome.report.hosts if h.succeeded}
            inv_hosts = {h.host: h for h in inventory.hosts}
            slots = {name: inv_hosts[name].slots for name in healthy}
            self._inv_hosts = inv_hosts
            self._runtimes = {
                host_name: self._build_runtime(project, result, inv_hosts[host_name], runner_dig)
                for host_name, result in healthy.items()
            }
            self._fail_if_session_lost()
            ray.init(
                address="local",
                namespace=f"ray-dispatcher-{secrets.token_hex(8)}",
                resources={"vm_slot": float(sum(slots.values()))} if slots else {},
                runtime_env={"excludes": ["pyproject.toml", "uv.lock", "*.lock"]},
            )
            self._owns_runtime = True
            self._actor = HostLease.remote(  # type: ignore[assignment]
                slots,
                lease_ttl_s=self._lease_ttl_s,
                capacity_wait_s=self._capacity_wait_s if self._elastic else 0.0,
                contexts={
                    name: (runtime, inv_hosts[name]) for name, runtime in self._runtimes.items()
                },
            )
            self._sweep_stop.clear()
            self._sweeper = threading.Thread(
                target=self._sweep_loop, daemon=True, name="lease-sweeper"
            )
            self._sweeper.start()
            self._fail_if_session_lost()
            return outcome.report
        except BaseException:
            self._sweep_stop.set()
            if self._actor is not None:
                try:
                    ray.kill(self._actor)
                except BaseException:
                    _log.exception("failed to kill lease actor during setup rollback")
                finally:
                    self._actor = None
            try:
                outcome.release_all()
            except BaseException:
                _log.exception("failed to release sessions during setup rollback")
            finally:
                self._outcome = None
            try:
                if self._owns_runtime or ray.is_initialized():
                    ray.shutdown()
            except BaseException:
                _log.exception("failed to shut down Ray during setup rollback")
            self._owns_runtime = False
            self._runtimes = {}
            self._inv_hosts = {}
            raise

    def _sweep_loop(self) -> None:
        while not self._sweep_stop.wait(self._sweep_interval_s):
            try:
                self._sweep_once()
            except Exception:  # noqa: BLE001
                if self._sweep_stop.is_set():
                    return  # teardown in progress; actor is gone
                # transient actor/SSH hiccup: try again next tick
                _log.warning("lease sweep failed; retrying next tick", exc_info=True)

    def _sweep_once(self, *, deadline_s: float | None = None) -> None:
        """Expire dead leases, reconcile quarantined hosts, readmit clean ones (§8.2)."""
        actor = self._actor
        if actor is None:
            return

        def get(ref: object) -> object:
            if deadline_s is None:
                return ray.get(ref)  # type: ignore[call-overload]
            remaining = deadline_s - time.monotonic()
            if remaining <= 0:
                raise DispatcherError("cleanup deadline expired")
            return ray.get(ref, timeout=remaining)  # type: ignore[call-overload]

        get(actor.sweep.remote())
        for host in cast(list[str], get(actor.quarantined_hosts.remote())):
            with self._membership:
                if host in self._confirming or host not in self._inv_hosts:
                    continue
                self._reconciling.add(host)
            try:
                pid_paths = cast(list[str], get(actor.orphan_paths.remote(host)))
                transport = self._transport_factory(self._inv_hosts[host])
                try:
                    clean = bool(pid_paths) and all(
                        reconcile_host(transport, p, deadline_s=deadline_s)
                        if deadline_s is not None
                        else reconcile_host(transport, p)
                        for p in pid_paths
                    )
                except Exception:  # noqa: BLE001 — unreachable host stays quarantined
                    clean = False
                    _log.warning(
                        "reconcile of host %s failed; host stays quarantined", host, exc_info=True
                    )
                if clean:
                    get(actor.mark_reconciled.remote(host))
            finally:
                with self._membership:
                    self._reconciling.discard(host)
                    self._membership.notify_all()

    def _build_runtime(
        self, project: Project, result: HostProvisioningResult, host: RemoteHost, runner_dig: str
    ) -> HostRuntime:
        host_name = result.host
        transport = self._transport_factory(host)
        home = transport.run(["sh", "-c", 'printf %s "$HOME"']).stdout.strip()
        layout = RemoteLayout(home, project.project_id)
        env_dig = result.environment_digest
        assert env_dig is not None  # noqa: S101 — internal invariant, not input validation
        return HostRuntime(
            host=host_name,
            layout=layout,
            environment_digest=env_dig,
            runner_digest=runner_dig,
            project_path=project.path,
            secret_env=secret_env_map(project, layout),
        )

    def _launch(self, handle: JobHandle, job: Job) -> None:
        task = _attempt_task.options(resources={}) if self._elastic else _attempt_task
        args = (
            job,
            handle.batch_id,
            JobLayout(self._results_dir, handle.batch_id, job.id),
            {} if self._elastic else self._runtimes,
            {} if self._elastic else self._inv_hosts,
            self._actor,
            self._transport_factory,
            self._retry_policy,
            self._heartbeat_interval_s,
        )
        ref = task.remote(*args, elastic=True) if self._elastic else task.remote(*args)
        with self._membership:
            self._refs[handle.token] = ref
            self._handles[handle.token] = handle

    def _finish_pending(self, handle: JobHandle, status: JobStatus, error: str) -> None:
        result = JobResult(handle.job_id, handle.batch_id, status, None, 0.0, None, None, (), error)
        write_result_json(
            JobLayout(self._results_dir, handle.batch_id, handle.job_id).result_json, result
        )
        self._local_results[handle.token] = result
        self._pending.pop(handle.token, None)

    def _pump_pending(self) -> None:
        if not self._pending or self._draining:
            return
        hosts = self.available_hosts()
        if not hosts:
            if self._empty_since is None:
                self._empty_since = time.monotonic()
            if time.monotonic() - self._empty_since >= self._capacity_wait_s:
                for handle, _ in list(self._pending.values()):
                    self._finish_pending(
                        handle, JobStatus.FAILED, "timed out waiting for healthy hosts"
                    )
            return
        self._empty_since = None
        with self._membership:
            capacity = sum(self._inv_hosts[host].slots for host in hosts if host in self._inv_hosts)
        refs = list(self._refs.values())
        pending_refs = ray.wait(refs, num_returns=len(refs), timeout=0)[1] if refs else []
        actor = self._actor
        assert actor is not None  # noqa: S101 — available hosts imply a live actor
        assigned = cast(dict[str, str], ray.get(actor.assigned_hosts.remote()))
        active = sum(
            1
            for token, ref in self._refs.items()
            if ref in pending_refs
            and (
                (
                    host := assigned.get(
                        f"{self._handles[token].batch_id}/{self._handles[token].job_id}"
                    )
                )
                is None
                or host in hosts
            )
        )
        for handle, job in list(self._pending.values())[: max(0, capacity - active)]:
            self._launch(handle, job)
            self._pending.pop(handle.token)

    def submit(self, batch_id: str, job: Job) -> JobHandle:
        self._fail_if_session_lost()
        if self._draining:
            raise DispatcherError("backend is draining; new submissions are rejected")
        token = uuid.uuid4().hex
        handle = JobHandle(batch_id=batch_id, job_id=job.id, token=token)
        if self._elastic:
            self._pending[token] = (handle, job)
            self._pump_pending()
        else:
            self._launch(handle, job)
        self._submitted.add((batch_id, job.id))
        try:
            self._fail_if_session_lost()
        except BaseException:
            self.cancel(handle)
            raise
        return JobHandle(batch_id=batch_id, job_id=job.id, token=token)

    def status(self, handle: JobHandle) -> JobStatus:
        self._fail_if_session_lost()
        if self._elastic:
            self._pump_pending()
            if handle.token in self._local_results:
                return self._local_results[handle.token].status
            if handle.token in self._pending:
                return JobStatus.PENDING
        ref = self._refs.get(handle.token)
        if ref is None:
            raise DispatcherError(
                f"unknown job handle: job {handle.job_id!r} in batch {handle.batch_id!r} "
                "was not submitted through this backend"
            )
        ready, _ = ray.wait([ref], timeout=0)
        if not ready:
            return JobStatus.RUNNING
        try:
            result: JobResult = ray.get(ref)  # type: ignore[call-overload]
        except ray.exceptions.TaskCancelledError:
            return self._cancelled_result(handle).status
        return result.status

    def _request_cancel(self, token: str, ref: object) -> None:
        with self._session_loss_lock:
            if token in self._cancel_requested:
                return
            self._cancel_requested.add(token)
        try:
            ray.cancel(ref)
        except BaseException:
            with self._session_loss_lock:
                self._cancel_requested.discard(token)
            raise

    def cancel(self, handle: JobHandle) -> None:
        if handle.token in self._pending:
            self._finish_pending(handle, JobStatus.CANCELLED, "cancelled")
            return
        ref = self._refs.get(handle.token)
        if ref is not None:
            self._request_cancel(handle.token, ref)

    def resolve(self, handle: JobHandle) -> JobResult:
        self._fail_if_session_lost()
        while handle.token in self._pending:
            self._pump_pending()
            if handle.token in self._pending:
                time.sleep(0.05)
        if handle.token in self._local_results:
            return self._local_results[handle.token]
        ref = self._refs[handle.token]
        try:
            return ray.get(ref)  # type: ignore[call-overload, no-any-return]
        except ray.exceptions.TaskCancelledError:
            return self._cancelled_result(handle)

    def _cancelled_result(self, handle: JobHandle) -> JobResult:
        path = JobLayout(self._results_dir, handle.batch_id, handle.job_id).result_json
        if path.exists():
            return read_result_json(path)
        result = JobResult(
            id=handle.job_id,
            batch_id=handle.batch_id,
            status=JobStatus.CANCELLED,
            returncode=None,
            duration_s=0,
            host=None,
            output_dir=None,
            attempts=(),
            error="cancelled",
        )
        write_result_json(path, result)
        return result

    def running_hosts(self) -> dict[tuple[str, str], str]:
        self._fail_if_session_lost()
        if self._actor is None:
            return {}
        snapshot = cast(dict[str, str], ray.get(self._actor.current_hosts.remote()))
        result = {}
        for attempt_id, host in snapshot.items():
            batch_id, job_id = attempt_id.split("/", 1)
            result[batch_id, job_id] = host
        return result

    def _purge_remote_state(self) -> list[str]:
        errors: list[str] = []
        for host_name, runtime in self._runtimes.items():
            paths = [runtime.layout.project, runtime.layout.secrets]
            paths.extend(
                f"{runtime.layout.runs}/{batch_id}/{job_id}"
                for batch_id, job_id in sorted(self._submitted)
            )
            try:
                result = self._transport_factory(self._inv_hosts[host_name]).run(
                    ["rm", "-rf", *paths]
                )
                if result.returncode != 0:
                    errors.append(f"{host_name}: {result.stderr.strip() or result.stdout.strip()}")
            except Exception as exc:  # noqa: BLE001 — continue cleanup on other hosts
                errors.append(f"{host_name}: {exc}")
        return errors

    def _drain_jobs(self) -> None:
        """Keep lease metadata alive until workers finish and orphans are reconciled."""
        deadline = time.monotonic() + _DRAIN_TIMEOUT_S
        pending = list(self._refs.values())
        for token, ref in list(self._refs.items()):
            self._request_cancel(token, ref)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DispatcherError("remote cleanup not confirmed; session locks retained")
            if pending:
                _, pending = ray.wait(
                    pending, num_returns=len(pending), timeout=min(0.05, remaining)
                )
            actor = self._actor
            if actor is None:
                if pending:
                    raise DispatcherError("cannot drain jobs without lease actor")
                return
            self._sweep_once(deadline_s=deadline)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                continue
            active = ray.get(actor.current_hosts.remote(), timeout=remaining)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                continue
            quarantined = ray.get(actor.quarantined_hosts.remote(), timeout=remaining)
            if not pending and not active and not quarantined:
                return
            time.sleep(min(0.05, max(0, deadline - time.monotonic())))

    def _close_admissions(self) -> None:
        with self._membership:
            self._draining = True
            while self._preparing or self._confirming:
                self._membership.wait()
        for handle, _ in list(self._pending.values()):
            self.cancel(handle)

    def _release_sessions(self) -> BaseException | None:
        outcomes = list(self._extra_outcomes)
        if self._outcome is not None:
            outcomes.append(self._outcome)
        error: BaseException | None = None
        for outcome in outcomes:
            try:
                outcome.release_all()
            except BaseException as exc:
                if error is None:
                    error = exc
        self._extra_outcomes.clear()
        self._outcome = None
        return error

    def _release_clean_sessions(self) -> None:
        """On elastic drain failure, retain only hosts still needing cleanup."""
        actor = self._actor
        if actor is None:
            return
        refs = list(self._refs.values())
        if refs and ray.wait(refs, num_returns=len(refs), timeout=0)[1]:
            return  # a task not yet holding a lease may still be starting
        active = cast(dict[str, str], ray.get(actor.assigned_hosts.remote(), timeout=5.0))
        unsafe = set(active.values()) | set(ray.get(actor.quarantined_hosts.remote(), timeout=5.0))
        outcomes = list(self._extra_outcomes)
        if self._outcome is not None:
            outcomes.append(self._outcome)
        for outcome in outcomes:
            for host, pair in list(outcome.sessions.items()):
                if host in unsafe:
                    continue
                ray.get(actor.remove_hosts.remote((host,)), timeout=5.0)
                try:
                    ProvisioningOutcome(ProvisioningReport(()), {host: pair}).release_all()
                except Exception:
                    _log.exception("failed to release clean host %s", host)
                else:
                    outcome.sessions.pop(host, None)
                    self._host_heartbeats.pop(host, None)

    def _drain_for_teardown(self) -> None:
        try:
            self._drain_jobs()
        except (DispatcherError, ray.exceptions.RayError) as exc:
            if self._elastic:
                try:
                    self._release_clean_sessions()
                except Exception:
                    _log.exception("failed to release clean hosts during partial teardown")
            if isinstance(exc, DispatcherError):
                raise
            raise DispatcherError(
                f"remote cleanup not confirmed: {exc}; session locks retained"
            ) from exc

    def teardown(self, *, purge: bool = False) -> None:
        purge_errors: list[str] = []
        self._close_admissions()
        self._drain_for_teardown()
        if purge:
            self._fail_if_session_lost()
        self._sweep_stop.set()
        if self._sweeper is not None:
            self._sweeper.join(timeout=self._sweep_interval_s + 5.0)
            if self._sweeper.is_alive():
                # ponytail: sweeper stuck in a slow SSH reconcile; we proceed to kill
                # the runtime anyway — its next ray.get fails and the daemon thread
                # exits. Bounded per-host reconcile timeouts if this proves noisy.
                _log.warning("lease-sweeper did not stop within timeout; proceeding with teardown")
            self._sweeper = None
        cleanup_error: BaseException | None = None
        try:
            if self._actor is not None:
                try:
                    ray.kill(self._actor)
                except BaseException as exc:
                    cleanup_error = exc
                finally:
                    self._actor = None
            if purge:
                purge_errors = self._purge_remote_state()
            session_error = self._release_sessions()
            if cleanup_error is None:
                cleanup_error = session_error
        finally:
            if self._owns_runtime:
                try:
                    ray.shutdown()
                except BaseException as exc:
                    if cleanup_error is None:
                        cleanup_error = exc
                finally:
                    self._owns_runtime = False
            self._runtimes = {}
            self._inv_hosts = {}
            self._refs = {}
            self._submitted = set()
            self._cancel_requested.clear()
            self._pending.clear()
            self._local_results.clear()
            self._lost_hosts.clear()
            self._empty_since = None
            self._handles.clear()
            self._host_heartbeats.clear()
        if cleanup_error is not None:
            raise cleanup_error
        if purge_errors:
            raise DispatcherError("remote purge failed: " + "; ".join(purge_errors))
