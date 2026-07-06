"""Exclusive local-Ray execution backend (spec §3.2, §3.3).

This is the only module that imports Ray. The HostLease actor wraps the Ray-free
LeaseService; SshRayBackend owns one local Ray runtime, started after provisioning
and shut down at teardown.
"""

from __future__ import annotations

import logging
import secrets
import threading
import uuid
from collections.abc import Callable, Iterable
from typing import cast

import ray

from ..digests import runner_digest
from ..errors import DispatcherError, RayRuntimeConflictError
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
from ..provisioning import ProvisioningOutcome, RemoteLayout, _default_transport, provision
from ..results import JobLayout, write_result_json
from ..scheduling import (
    HostRuntime,
    Lease,
    LeaseService,
    reconcile_host,
    run_job,
    secret_env_map,
)
from ..ssh import Transport
from .base import ExecutionBackend

_log = logging.getLogger(__name__)

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
        self._hb_thread: threading.Thread | None = None

    def acquire(self, attempt_id: str, *, exclude: Iterable[str] = ()) -> Lease:
        lease = ray.get(self._actor.acquire.remote(attempt_id, exclude=tuple(exclude)))
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
        ray.get(self._actor.release.remote(token))

    def register_pid(self, token: str, pid_path: str) -> None:
        ray.get(self._actor.register_pid.remote(token, pid_path))

    def _heartbeat(self, token: str) -> None:
        while not self._stop.wait(self._heartbeat_interval_s):
            try:
                alive = ray.get(self._actor.heartbeat.remote(token))
            except Exception:
                break
            if not alive:
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
) -> JobResult:
    """Ray task: run one logical job; terminates as a JobResult (§3.2.3, §6a residual)."""
    lease_handle = _ActorLeaseHandle(actor, heartbeat_interval_s=heartbeat_interval_s)
    try:
        result = run_job(
            job,
            batch_id=batch_id,
            lease=lease_handle,
            runtime_for=runtimes.__getitem__,
            transport_for=lambda host: transport_factory(inv_hosts[host]),
            local=local,
            policy=policy,
        )
        write_result_json(local.result_json, result)
        return result
    # Ray delivers cancel to a running task as KeyboardInterrupt; TaskCancelledError
    # covers the pre-start window. Both terminate as a CANCELLED JobResult (§8.1).
    except (KeyboardInterrupt, ray.exceptions.TaskCancelledError):
        cancelled = JobResult(
            id=job.id, batch_id=batch_id, status=JobStatus.CANCELLED,
            returncode=None, duration_s=0.0, host=None, output_dir=None,
            attempts=(), error="cancelled",
        )
        write_result_json(local.result_json, cancelled)
        return cancelled
    except Exception as exc:
        internal = JobResult(
            id=job.id, batch_id=batch_id, status=JobStatus.FAILED,
            returncode=None, duration_s=0.0, host=None, output_dir=None,
            attempts=(), error=f"INTERNAL: {type(exc).__name__}: {exc}",
        )
        write_result_json(local.result_json, internal)
        return internal


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
    ) -> None:
        from .. import remote_runner

        self._runner_path = runner_path or remote_runner.__file__
        self._transport_factory = transport_factory or _default_transport
        self._retry_policy = retry_policy
        self._min_disk_mb = min_disk_mb
        self._results_dir = results_dir
        self._require_all_hosts = require_all_hosts
        self._lease_ttl_s = lease_ttl_s
        self._sweep_interval_s = sweep_interval_s
        self._sweep_stop = threading.Event()
        self._sweeper: threading.Thread | None = None
        self._owns_runtime = False
        self._actor: ray.actor.ActorHandle | None = None  # type: ignore[type-arg]
        self._outcome: ProvisioningOutcome | None = None
        self._runtimes: dict[str, HostRuntime] = {}
        self._inv_hosts: dict[str, RemoteHost] = {}
        self._refs: dict[str, object] = {}

    def setup(
        self, inventory: Inventory, project: Project, *, force: bool = False
    ) -> ProvisioningReport:
        if ray.is_initialized():
            raise RayRuntimeConflictError(
                "a Ray runtime is already initialized; ray_dispatcher owns its own "
                "local runtime and will not attach to an external one (§3.2)"
            )
        # Provision first; provision() raises NoHealthyHostsError if none succeed,
        # so Ray is never started for an empty healthy set (§3.2.2).
        outcome = provision(
            inventory, project,
            runner_path=self._runner_path, transport_factory=self._transport_factory,
            min_disk_mb=self._min_disk_mb,
            require_all_hosts=self._require_all_hosts, force=force,
        )
        self._outcome = outcome
        runner_dig = runner_digest(self._runner_path)
        healthy = {h.host: h for h in outcome.report.hosts if h.succeeded}
        inv_hosts = {h.host: h for h in inventory.hosts}
        slots = {name: inv_hosts[name].slots for name in healthy}
        self._inv_hosts = inv_hosts
        self._runtimes = {
            host_name: self._build_runtime(project, result, inv_hosts[host_name], runner_dig)
            for host_name, result in healthy.items()
        }
        ray.init(
            address="local",
            namespace=f"ray-dispatcher-{secrets.token_hex(8)}",  # unique per session (§3.2.2)
            resources={"vm_slot": float(sum(slots.values()))},
            # Exclude pyproject.toml / lockfiles from the working_dir package Ray auto-uploads
            # so workers don't attempt `uv sync` with relative editable paths that only
            # resolve at the caller's site (e.g. `editable+../../` inside examples/).
            runtime_env={"excludes": ["pyproject.toml", "uv.lock", "*.lock"]},
        )
        self._owns_runtime = True
        self._actor = HostLease.remote(slots, lease_ttl_s=self._lease_ttl_s)  # type: ignore[assignment]
        self._sweep_stop.clear()
        self._sweeper = threading.Thread(
            target=self._sweep_loop, daemon=True, name="lease-sweeper"
        )
        self._sweeper.start()
        return outcome.report

    def _sweep_loop(self) -> None:
        while not self._sweep_stop.wait(self._sweep_interval_s):
            try:
                self._sweep_once()
            except Exception:  # noqa: BLE001
                if self._sweep_stop.is_set():
                    return  # teardown in progress; actor is gone
                # transient actor/SSH hiccup: try again next tick
                _log.warning("lease sweep failed; retrying next tick", exc_info=True)

    def _sweep_once(self) -> None:
        """Expire dead leases, reconcile quarantined hosts, readmit clean ones (§8.2)."""
        actor = self._actor
        if actor is None:
            return
        ray.get(actor.sweep.remote())
        for host in ray.get(actor.quarantined_hosts.remote()):
            pid_paths = ray.get(actor.take_orphans.remote(host))
            transport = self._transport_factory(self._inv_hosts[host])
            try:
                clean = all(reconcile_host(transport, p) for p in pid_paths)
            except Exception:  # noqa: BLE001 — unreachable host stays quarantined
                clean = False
                _log.warning(
                    "reconcile of host %s failed; host stays quarantined", host, exc_info=True
                )
            if clean:
                ray.get(actor.mark_reconciled.remote(host))
            # ponytail: on a failed reconcile the popped pid paths are dropped —
            # the host stays quarantined until a later sweep finds it clean by
            # probe; re-register orphans if partial-kill recovery ever matters.

    def _build_runtime(
        self, project: Project, result: HostProvisioningResult, host: RemoteHost, runner_dig: str
    ) -> HostRuntime:
        host_name = result.host
        transport = self._transport_factory(host)
        home = transport.run(["sh", "-c", 'printf %s "$HOME"']).stdout.strip()
        layout = RemoteLayout(home, project.project_id)
        env_dig = result.environment_digest
        assert env_dig is not None  # healthy hosts always have an environment digest
        return HostRuntime(
            host=host_name,
            layout=layout,
            environment_digest=env_dig,
            runner_digest=runner_dig,
            project_path=project.path,
            secret_env=secret_env_map(project, layout),
        )

    def submit(self, batch_id: str, job: Job) -> JobHandle:
        token = uuid.uuid4().hex
        local = JobLayout(self._results_dir, batch_id, job.id)
        ref = _attempt_task.remote(
            job, batch_id, local, self._runtimes, self._inv_hosts,
            self._actor, self._transport_factory, self._retry_policy,
        )
        self._refs[token] = ref
        return JobHandle(batch_id=batch_id, job_id=job.id, token=token)

    def status(self, handle: JobHandle) -> JobStatus:
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
            return JobStatus.CANCELLED
        return result.status

    def cancel(self, handle: JobHandle) -> None:
        ref = self._refs.get(handle.token)
        if ref is not None:
            ray.cancel(ref)

    def resolve(self, handle: JobHandle) -> JobResult:
        ref = self._refs[handle.token]
        try:
            return ray.get(ref)  # type: ignore[call-overload, no-any-return]
        except ray.exceptions.TaskCancelledError:
            return JobResult(
                id=handle.job_id, batch_id=handle.batch_id, status=JobStatus.CANCELLED,
                returncode=None, duration_s=0.0, host=None, output_dir=None,
                attempts=(), error="cancelled",
            )

    def running_hosts(self) -> dict[str, str]:
        if self._actor is None:
            return {}
        return cast(dict[str, str], ray.get(self._actor.current_hosts.remote()))

    def teardown(self, *, purge: bool = False) -> None:
        # ponytail: purge + cancel/reconcile of outstanding attempts land in 6c/6d;
        # this slice does the runtime + lock release (§10.2,4,5).
        self._sweep_stop.set()
        if self._sweeper is not None:
            self._sweeper.join(timeout=self._sweep_interval_s + 5.0)
            if self._sweeper.is_alive():
                # ponytail: sweeper stuck in a slow SSH reconcile; we proceed to kill
                # the runtime anyway — its next ray.get fails and the daemon thread
                # exits. Bounded per-host reconcile timeouts if this proves noisy.
                _log.warning("lease-sweeper did not stop within timeout; proceeding with teardown")
            self._sweeper = None
        try:
            if self._actor is not None:
                ray.kill(self._actor)
                self._actor = None
            if self._outcome is not None:
                self._outcome.release_all()
        finally:
            if self._owns_runtime:
                try:
                    ray.shutdown()
                finally:
                    self._owns_runtime = False
