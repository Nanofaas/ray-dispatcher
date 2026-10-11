import time

from it.unimib.data.nanofaas.ray_dispatcher.backends import ssh_ray
from it.unimib.data.nanofaas.ray_dispatcher.backends.ssh_ray import SshRayBackend
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


def _project():
    return Project(path="/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.25")


def _canned_outcome(*host_names: str) -> ProvisioningOutcome:
    report = ProvisioningReport(
        tuple(HostProvisioningResult(h, True, "src123", "env123") for h in host_names)
    )
    return ProvisioningOutcome(report, sessions={})


def test_running_hosts_empty_before_setup(tmp_path):
    backend = SshRayBackend(results_dir=str(tmp_path))
    assert backend.running_hosts() == {}


def test_running_hosts_reports_host_while_job_in_flight(tmp_path, monkeypatch):
    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu", slots=1),))
    monkeypatch.setattr(ssh_ray, "provision", lambda *a, **k: _canned_outcome("10.0.0.1"))

    started = tmp_path / "started"
    finish = tmp_path / "finish"

    # ponytail: inline closure so cloudpickle serializes it without a module reference
    def _slow_transport(host: RemoteHost) -> FakeTransport:
        def results(argv: list[str]) -> CommandResult:
            if 'printf %s "$HOME"' in " ".join(argv):
                return CommandResult(0, "/home/ubuntu", "", 0.0)
            if argv[0] == "python3":
                started.touch()
                deadline = time.monotonic() + 30
                while not finish.exists() and time.monotonic() < deadline:
                    time.sleep(0.05)
                return CommandResult(0, "", "", 0.0)
            if argv[0] == "cat":
                return CommandResult(0, '{"returncode": 0, "duration_s": 0.05}', "", 0.0)
            return CommandResult(0, "", "", 0.0)

        return FakeTransport(run_results=results)

    backend = SshRayBackend(transport_factory=_slow_transport, results_dir=str(tmp_path))
    try:
        backend.setup(inv, _project())
        handle = backend.submit("batch1", Job(id="j1", command=("echo", "hi")))
        deadline = time.monotonic() + 30
        while not started.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert started.exists(), "runner did not start"
        assert backend.running_hosts() == {("batch1", "j1"): "10.0.0.1"}
        finish.touch()
        result = backend.resolve(handle)
        assert result.status == JobStatus.SUCCEEDED
        assert backend.running_hosts() == {}  # lease released once the attempt finished
    finally:
        finish.touch()
        backend.teardown()
