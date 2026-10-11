"""Public Dispatcher lifecycle and batch orchestration (spec §4.5)."""

from __future__ import annotations

import logging
import time
import types
import uuid
from collections.abc import Iterator, Sequence
from pathlib import Path

from .backends.base import ExecutionBackend
from .errors import (
    BatchExistsError,
    BatchFailedError,
    DispatcherError,
    ModelValidationError,
    ProvisioningError,
)
from .models import (
    Inventory,
    Job,
    JobHandle,
    JobResult,
    JobStatus,
    Project,
    ProvisioningReport,
    RetryPolicy,
    _is_valid_job_id,
)

_log = logging.getLogger(__name__)

_TERMINAL = frozenset(
    {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.TIMED_OUT}
)


class Dispatcher:
    """Public entry point: lifecycle + batch orchestration over an ExecutionBackend (§4.5)."""

    def __init__(
        self,
        inventory: Inventory,
        project: Project,
        *,
        results_dir: str = "./results",
        retry_policy: RetryPolicy = RetryPolicy(),  # noqa: B008
        raise_on_failure: bool = False,
        require_all_hosts: bool = True,
        backend: ExecutionBackend | None = None,
    ) -> None:
        self._inventory = inventory
        self._project = project
        self._results_dir = results_dir
        self._retry_policy = retry_policy
        self._raise_on_failure = raise_on_failure
        self._require_all_hosts = require_all_hosts
        if backend is None:
            from .backends.ssh_ray import (
                SshRayBackend,  # deferred: avoids `import ray` at module load
            )

            backend = SshRayBackend(
                retry_policy=retry_policy,
                results_dir=results_dir,
                require_all_hosts=require_all_hosts,
            )
        self._backend = backend
        self._setup_done = False
        self._setup_report: ProvisioningReport | None = None
        self._outstanding: list[JobHandle] = []

    def setup(self, *, force: bool = False) -> ProvisioningReport:
        if self._setup_done:
            if force:
                raise DispatcherError(
                    "setup(force=True) rejected after Ray has started; "
                    "teardown and create a new Dispatcher to re-provision (§4.5)"
                )
            return self._setup_report  # type: ignore[return-value]
        report = self._backend.setup(self._inventory, self._project, force=force)
        if self._require_all_hosts:
            failed = [h for h in report.hosts if not h.succeeded]
            if failed:
                raise ProvisioningError(report, f"{len(failed)} host(s) failed provisioning")
        self._setup_done = True
        self._setup_report = report
        return report

    def submit(self, jobs: Sequence[Job], *, batch_id: str | None = None) -> list[JobHandle]:
        ids = [job.id for job in jobs]
        dupes = {i for i in ids if ids.count(i) > 1}
        if dupes:
            raise ModelValidationError(f"duplicate job id(s) in batch: {sorted(dupes)}")
        if batch_id is not None and not _is_valid_job_id(batch_id):
            raise ModelValidationError(f"invalid batch_id: {batch_id!r}")
        if not self._setup_done:
            self.setup()
        if batch_id is None:
            batch_id = uuid.uuid4().hex
        batch_path = Path(self._results_dir) / batch_id
        try:
            batch_path.mkdir(parents=True)  # atomic claim: exists -> another batch owns it
        except FileExistsError:
            raise BatchExistsError(f"batch directory already exists: {batch_path}") from None
        handles: list[JobHandle] = []
        try:
            for job in jobs:
                handle = self._backend.submit(batch_id, job)
                handles.append(handle)
                self._outstanding.append(handle)
        except BaseException:
            for handle in handles:
                try:
                    self._backend.cancel(handle)
                except BaseException:  # noqa: BLE001
                    pass
            raise
        return handles

    def status(self, handle: JobHandle) -> JobStatus:
        return self._backend.status(handle)

    def cancel(self, handle: JobHandle) -> None:
        self._backend.cancel(handle)

    def running_hosts(self) -> dict[tuple[str, str], str]:
        return self._backend.running_hosts()

    def as_completed(self, handles: Sequence[JobHandle]) -> Iterator[JobResult]:
        remaining = list(handles)
        while remaining:
            next_remaining: list[JobHandle] = []
            for h in remaining:
                if self._backend.status(h) in _TERMINAL:
                    yield self._backend.resolve(h)
                else:
                    next_remaining.append(h)
            remaining = next_remaining
            if remaining:
                time.sleep(0.1)  # ponytail: poll; push-based notification is §9.2 progress extra

    def run(self, jobs: Sequence[Job], *, batch_id: str | None = None) -> list[JobResult]:
        handles = self.submit(jobs, batch_id=batch_id)
        results_by_id: dict[str, JobResult] = {}
        for result in self.as_completed(handles):
            results_by_id[result.id] = result
        ordered = [results_by_id[job.id] for job in jobs]
        if self._raise_on_failure and any(r.status != JobStatus.SUCCEEDED for r in ordered):
            raise BatchFailedError(ordered)
        return ordered

    def teardown(self, *, purge: bool = False) -> None:
        if purge and self._outstanding:
            active = [h for h in self._outstanding if self._backend.status(h) not in _TERMINAL]
            if active:
                raise DispatcherError(
                    f"teardown(purge=True) rejected: {len(active)} job(s) still active"
                )
        for h in self._outstanding:
            try:
                self._backend.cancel(h)
            except Exception:  # noqa: BLE001
                pass
        self._backend.teardown(purge=purge)
        self._outstanding.clear()
        self._setup_done = False
        self._setup_report = None

    def __enter__(self) -> Dispatcher:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        try:
            self.teardown()
        except Exception as cleanup_exc:  # noqa: BLE001
            if exc_val is not None:
                # Exception already in flight — log cleanup error, don't replace it.
                _log.exception("teardown error during context exit: %s", cleanup_exc)
            else:
                raise
