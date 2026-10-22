"""Real local Ray, fake SSH: new capacity runs queued work before old work finishes."""

import time

from it.unimib.data.nanofaas.ray_dispatcher.backends import ssh_ray
from it.unimib.data.nanofaas.ray_dispatcher.models import (
    HostProvisioningResult,
    Inventory,
    Job,
    JobStatus,
    Project,
    ProvisioningReport,
    RemoteHost,
)
from it.unimib.data.nanofaas.ray_dispatcher.provisioning import ProvisioningOutcome
from it.unimib.data.nanofaas.ray_dispatcher.ssh import CommandResult, FakeTransport


def test_new_host_runs_queued_job_while_first_host_is_busy(tmp_path, monkeypatch):
    def provision(inventory, *a, **kw):
        return ProvisioningOutcome(
            ProvisioningReport(
                tuple(HostProvisioningResult(h.host, True, "src", "env") for h in inventory.hosts)
            )
        )

    monkeypatch.setattr(ssh_ray, "provision", provision)
    release = tmp_path / "release"
    started = tmp_path / "started"

    def transport(host):
        def run(argv):
            if 'printf %s "$HOME"' in " ".join(argv):
                return CommandResult(0, "/home/user", "", 0.0)
            if "python3" in " ".join(argv) and host.host == "first":
                started.touch()
                deadline = time.monotonic() + 20
                while not release.exists() and time.monotonic() < deadline:
                    time.sleep(0.02)
            if argv[0] == "cat":
                return CommandResult(0, '{"returncode": 0, "duration_s": 0.1}', "", 0.0)
            return CommandResult(0, "", "", 0.0)

        return FakeTransport(run_results=run)

    backend = ssh_ray.SshRayBackend(
        elastic=True, transport_factory=transport, results_dir=str(tmp_path)
    )
    try:
        backend.setup(Inventory((), allow_empty=True), Project("/proj", "p", "3.10.0", "0.11.25"))
        assert backend.available_hosts() == ()
        backend.add_hosts([RemoteHost("first", "user")])
        first = backend.submit("batch", Job("first", ("true",)))
        second = backend.submit("batch", Job("second", ("true",)))
        deadline = time.monotonic() + 15
        while not started.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert started.exists()
        assert backend.status(second) is JobStatus.PENDING
        backend.add_hosts([RemoteHost("second", "user")])
        assert backend.resolve(second).host == "second"
        assert not release.exists()
        release.touch()
        assert backend.resolve(first).status is JobStatus.SUCCEEDED
    finally:
        release.touch()
        backend.teardown()


def test_confirmed_deletion_allows_same_address_retry_without_stale_quarantine(
    tmp_path, monkeypatch
):
    from it.unimib.data.nanofaas.ray_dispatcher.errors import SessionLockLostError
    from it.unimib.data.nanofaas.ray_dispatcher.models import FailureKind

    def provision(inventory, *a, **kw):
        return ProvisioningOutcome(
            ProvisioningReport(
                tuple(
                    HostProvisioningResult(host.host, True, "src", "env")
                    for host in inventory.hosts
                )
            )
        )

    monkeypatch.setattr(ssh_ray, "provision", provision)
    started, deleted, replacement = (
        tmp_path / name for name in ("started", "deleted", "replacement")
    )

    def transport(host):
        def run(argv):
            if 'printf %s "$HOME"' in " ".join(argv):
                return CommandResult(0, "/home/user", "", 0.0)
            if "python3" in " ".join(argv) and not replacement.exists():
                started.touch()
                deadline = time.monotonic() + 15
                while not replacement.exists() and time.monotonic() < deadline:
                    time.sleep(0.02)
            if argv[0] == "cat" and argv[1].endswith("pid.json") and deleted.exists():
                return CommandResult(1, "", "deleted", 0.0)
            if argv[0] == "cat":
                return CommandResult(0, '{"returncode": 0, "duration_s": 0.1}', "", 0.0)
            return CommandResult(0, "", "", 0.0)

        return FakeTransport(run_results=run)

    backend = ssh_ray.SshRayBackend(
        elastic=True, transport_factory=transport, results_dir=str(tmp_path), lease_ttl_s=0.6
    )
    try:
        backend.setup(
            Inventory((RemoteHost("address", "user"),)), Project("/proj", "p", "3.10.0", "0.11.25")
        )
        handle = backend.submit("batch", Job("job", ("true",)))
        deadline = time.monotonic() + 10
        while not started.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert started.exists()
        deleted.touch()  # fake cloud has confirmed deletion
        backend._on_host_lost("address", SessionLockLostError("evicted"))
        backend.confirm_hosts_stopped(["address"])
        replacement.touch()
        backend.add_hosts([RemoteHost("address", "user")])
        result = backend.resolve(handle)
        assert result.status is JobStatus.SUCCEEDED
        assert len(result.attempts) == 2
        assert result.attempts[0].failure_kind is FailureKind.HOST_LOST
        assert backend.available_hosts() == ("address",)
    finally:
        replacement.touch()
        backend.teardown()
