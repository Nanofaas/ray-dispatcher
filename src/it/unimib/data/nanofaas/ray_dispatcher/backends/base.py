"""The backend-neutral execution interface (spec §3.3).

A backend maps opaque JobHandles to its own execution mechanism; Ray types never
cross this boundary, so a future non-Ray backend can satisfy the same contract.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence

from ..errors import DispatcherError
from ..models import (
    Inventory,
    Job,
    JobHandle,
    JobResult,
    JobStatus,
    Project,
    ProvisioningReport,
    RemoteHost,
)


class ExecutionBackend(ABC):
    @abstractmethod
    def setup(
        self, inventory: Inventory, project: Project, *, force: bool = False
    ) -> ProvisioningReport: ...

    @abstractmethod
    def submit(self, batch_id: str, job: Job) -> JobHandle: ...

    @abstractmethod
    def status(self, handle: JobHandle) -> JobStatus: ...

    @abstractmethod
    def cancel(self, handle: JobHandle) -> None: ...

    @abstractmethod
    def resolve(self, handle: JobHandle) -> JobResult: ...

    @abstractmethod
    def running_hosts(self) -> dict[tuple[str, str], str]: ...

    @abstractmethod
    def teardown(self, *, purge: bool = False) -> None: ...

    def add_hosts(self, hosts: Sequence[RemoteHost]) -> ProvisioningReport:
        raise DispatcherError("backend does not support elastic hosts")

    def remove_hosts(self, hosts: Sequence[str]) -> None:
        raise DispatcherError("backend does not support elastic hosts")

    def confirm_hosts_stopped(self, hosts: Sequence[str]) -> None:
        """Forget hosts only after the infrastructure confirms their processes cannot run."""
        raise DispatcherError("backend does not support confirmed host removal")

    def available_hosts(self) -> tuple[str, ...]:
        raise DispatcherError("backend does not expose host availability")
