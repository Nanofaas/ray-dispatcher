import pytest
import ray

from ray_dispatcher.backends import ssh_ray
from ray_dispatcher.backends.ssh_ray import SshRayBackend
from ray_dispatcher.errors import NoHealthyHostsError, ProvisioningError
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


def _home_transport(host):
    def results(argv):
        if 'printf %s "$HOME"' in " ".join(argv):
            return CommandResult(0, "/home/ubuntu", "", 0.0)
        return CommandResult(0, "", "", 0.0)
    return FakeTransport(run_results=results)


def _canned_outcome(*host_names):
    report = ProvisioningReport(tuple(
        HostProvisioningResult(h, True, "src123", "env123") for h in host_names
    ))
    return ProvisioningOutcome(report, sessions={})  # no live locks in this test


def test_setup_starts_runtime_with_vm_slot_sum_and_builds_runtimes(monkeypatch):
    inv = Inventory((
        RemoteHost("10.0.0.1", user="ubuntu", slots=2),
        RemoteHost("10.0.0.2", user="ubuntu", slots=3),
    ))
    monkeypatch.setattr(ssh_ray, "provision",
                        lambda *a, **k: _canned_outcome("10.0.0.1", "10.0.0.2"))
    backend = SshRayBackend(transport_factory=_home_transport)
    try:
        report = backend.setup(inv, _project())
        assert all(h.succeeded for h in report.hosts)
        assert ray.is_initialized()
        assert ray.cluster_resources().get("vm_slot") == 5.0   # 2 + 3 (§3.2.2)
        assert set(backend._runtimes) == {"10.0.0.1", "10.0.0.2"}
        rt = backend._runtimes["10.0.0.1"]
        assert rt.layout.source == "/home/ubuntu/.ray_dispatcher/projects/dfaas/source"
        assert rt.environment_digest == "env123" and rt.runner_digest
    finally:
        backend.teardown()


def test_setup_propagates_no_healthy_hosts_without_starting_ray(monkeypatch):
    def _raise(*a, **k):
        raise NoHealthyHostsError("no host provisioned successfully")

    monkeypatch.setattr(ssh_ray, "provision", _raise)
    backend = SshRayBackend(transport_factory=_home_transport)
    with pytest.raises(NoHealthyHostsError):
        backend.setup(Inventory((RemoteHost("10.0.0.9", user="ubuntu"),)), _project())
    assert not ray.is_initialized()   # Ray never started when provisioning yields no host


def test_setup_with_require_all_hosts_fails_before_ray_starts(monkeypatch):
    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu"),))
    proj = Project(path="/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.25")

    captured: dict[str, object] = {}

    def fake_provision(*args, **kwargs):
        captured.update(kwargs)
        raise ProvisioningError(None, "not every host provisioned")

    monkeypatch.setattr(ssh_ray, "provision", fake_provision)
    backend = SshRayBackend(transport_factory=lambda h: None, require_all_hosts=True)
    with pytest.raises(ProvisioningError):
        backend.setup(inv, proj)
    assert captured.get("require_all_hosts") is True  # flag reached provision()
    assert not ray.is_initialized()  # Ray never started, nothing leaked


class _SessionRecorder:
    def __init__(self):
        self.calls = []

    def stop(self):
        self.calls.append("stop")

    def raise_if_failed(self):
        pass

    def release(self):
        self.calls.append("release")


def test_setup_releases_provisioned_sessions_when_runtime_build_fails(monkeypatch):
    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu"),))
    rec = _SessionRecorder()
    outcome = _canned_outcome("10.0.0.1")
    outcome.sessions = {"host": (rec, rec)}
    monkeypatch.setattr(ssh_ray, "provision", lambda *a, **k: outcome)
    backend = SshRayBackend(transport_factory=lambda h: (_ for _ in ()).throw(RuntimeError("home")))

    with pytest.raises(RuntimeError, match="home"):
        backend.setup(inv, _project())

    assert rec.calls == ["stop", "release"]
    assert backend._outcome is None


def test_setup_shuts_down_runtime_and_releases_sessions_when_actor_creation_fails(monkeypatch):
    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu"),))
    rec = _SessionRecorder()
    outcome = _canned_outcome("10.0.0.1")
    outcome.sessions = {"host": (rec, rec)}
    monkeypatch.setattr(ssh_ray, "provision", lambda *a, **k: outcome)
    initialized = {"value": False}
    monkeypatch.setattr(ssh_ray.ray, "is_initialized", lambda: initialized["value"])
    monkeypatch.setattr(ssh_ray.ray, "init", lambda **k: initialized.update(value=True))
    monkeypatch.setattr(ssh_ray.ray, "shutdown", lambda: initialized.update(value=False))
    monkeypatch.setattr(
        ssh_ray.HostLease, "remote", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("actor"))
    )
    backend = SshRayBackend(transport_factory=_home_transport)

    with pytest.raises(RuntimeError, match="actor"):
        backend.setup(inv, _project())

    assert not initialized["value"]
    assert rec.calls == ["stop", "release"]
    assert backend._outcome is None
    assert backend._actor is None


def test_setup_preserves_primary_error_when_session_release_also_fails(monkeypatch):
    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu"),))
    outcome = _canned_outcome("10.0.0.1")
    monkeypatch.setattr(ssh_ray, "provision", lambda *a, **k: outcome)
    monkeypatch.setattr(
        outcome,
        "release_all",
        lambda: (_ for _ in ()).throw(RuntimeError("release")),
    )
    initialized = {"value": False}
    monkeypatch.setattr(ssh_ray.ray, "is_initialized", lambda: initialized["value"])
    monkeypatch.setattr(ssh_ray.ray, "init", lambda **k: initialized.update(value=True))
    monkeypatch.setattr(ssh_ray.ray, "shutdown", lambda: initialized.update(value=False))
    monkeypatch.setattr(
        ssh_ray.HostLease, "remote", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("actor"))
    )
    backend = SshRayBackend(transport_factory=_home_transport)

    with pytest.raises(RuntimeError, match="actor"):
        backend.setup(inv, _project())

    assert not initialized["value"]
    assert backend._outcome is None
