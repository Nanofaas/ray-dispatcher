"""A dead lease is swept, its orphan reconciled, and the host readmitted (§8.2)."""

import time

import ray

from ray_dispatcher.backends import ssh_ray
from ray_dispatcher.backends.ssh_ray import SshRayBackend
from ray_dispatcher.models import (
    HostProvisioningResult,
    Inventory,
    Project,
    ProvisioningReport,
    RemoteHost,
)
from ray_dispatcher.provisioning import ProvisioningOutcome
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _project():
    return Project(path="/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.25")


def _canned_outcome(*host_names: str) -> ProvisioningOutcome:
    report = ProvisioningReport(tuple(
        HostProvisioningResult(h, True, "src123", "env123") for h in host_names
    ))
    return ProvisioningOutcome(report, sessions={})


def test_expired_lease_is_swept_reconciled_and_host_readmitted(tmp_path, monkeypatch):
    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu", slots=1),))
    monkeypatch.setattr(ssh_ray, "provision", lambda *a, **k: _canned_outcome("10.0.0.1"))

    # ponytail: inline closure so cloudpickle serializes it without a module reference
    def _transport(host: RemoteHost) -> FakeTransport:
        def results(argv: list[str]) -> CommandResult:
            if 'printf %s "$HOME"' in " ".join(argv):
                return CommandResult(0, "/home/ubuntu", "", 0.0)
            if argv[0] == "cat":  # pid file read by reconcile_host
                return CommandResult(0, '{"pgid": 4242}', "", 0.0)
            if argv[:2] == ["kill", "-0"]:  # probe: group already gone
                return CommandResult(1, "", "", 0.0)
            return CommandResult(0, "", "", 0.0)  # kill -TERM/-KILL etc.
        return FakeTransport(run_results=results)

    backend = SshRayBackend(
        transport_factory=_transport, results_dir=str(tmp_path),
        lease_ttl_s=0.5, sweep_interval_s=0.2,
    )
    try:
        backend.setup(inv, _project())
        actor = backend._actor
        # Simulate a worker that died mid-attempt: acquire + register, never heartbeat.
        lease = ray.get(actor.acquire.remote("batch/job-dead"))
        ray.get(actor.register_pid.remote(lease.token, "/runs/b/j/1/pid.json"))
        assert backend.running_hosts() == {("batch", "job-dead"): "10.0.0.1"}
        # Within a few sweep cycles: lease expired -> host quarantined ->
        # orphan reconciled -> host readmitted with its slot free.
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            quarantined = ray.get(actor.quarantined_hosts.remote())
            if backend.running_hosts() == {} and quarantined == []:
                break
            time.sleep(0.1)
        assert backend.running_hosts() == {}
        assert ray.get(actor.quarantined_hosts.remote()) == []
        # capacity is actually back: a fresh acquire succeeds immediately
        again = ray.get(actor.acquire.remote("batch/job-next"))
        assert again.host == "10.0.0.1"
        ray.get(actor.release.remote(again.token))
    finally:
        backend.teardown()
