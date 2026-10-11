import subprocess

import pytest

from it.unimib.data.nanofaas.ray_dispatcher import ssh
from it.unimib.data.nanofaas.ray_dispatcher.errors import ModelValidationError
from it.unimib.data.nanofaas.ray_dispatcher.ssh import SshConfig, SshTransport, TransportError


def _cfg():
    return SshConfig(host="h", user="u", port=22, identity_file=None, known_hosts_file="/kh")


def test_push_builds_remote_dst_and_runs_rsync(monkeypatch):
    seen = {}

    def fake_run(argv, **kw):
        seen["argv"] = argv
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(ssh.subprocess, "run", fake_run)
    SshTransport(_cfg()).push("/local/dir", "/remote/dir", delete=True, excludes=(".git/",))
    assert seen["argv"][0] == "rsync"
    assert seen["argv"][-2:] == ["/local/dir", "u@h:/remote/dir"]
    assert "--delete" in seen["argv"]


def test_pull_builds_remote_src(monkeypatch):
    seen = {}
    monkeypatch.setattr(
        ssh.subprocess,
        "run",
        lambda argv, **kw: seen.update(argv=argv) or subprocess.CompletedProcess(argv, 0, "", ""),
    )
    SshTransport(_cfg()).pull("/remote/out", "/local/out")
    assert seen["argv"][-2:] == ["u@h:/remote/out", "/local/out"]


def test_rsync_failure_raises_transport_error(monkeypatch):
    def boom(argv, **kw):
        raise subprocess.CalledProcessError(23, argv, stderr="rsync: link failed")

    monkeypatch.setattr(ssh.subprocess, "run", boom)
    with pytest.raises(TransportError):
        SshTransport(_cfg()).push("/a", "/b")


@pytest.mark.parametrize("name", ["connect_timeout_s", "control_timeout_s", "rsync_timeout_s"])
@pytest.mark.parametrize("value", [0, -1, float("inf"), float("nan"), None, "10", True])
def test_constructor_rejects_invalid_timeouts(name, value):
    with pytest.raises(ModelValidationError, match=name):
        SshTransport(_cfg(), **{name: value})


def test_run_uses_configured_control_timeout_by_default(monkeypatch):
    class Connection:
        def run(self, command, **kwargs):
            assert kwargs["timeout"] == 17
            return type("Result", (), {"exited": 0, "stdout": "", "stderr": ""})()

    monkeypatch.setattr(ssh, "build_connection", lambda cfg, **kwargs: Connection())
    SshTransport(_cfg(), control_timeout_s=17).run(["true"])


def test_run_default_control_timeout_allows_cold_provisioning(monkeypatch):
    class Connection:
        def run(self, command, **kwargs):
            assert kwargs["timeout"] == 300
            return type("Result", (), {"exited": 0, "stdout": "", "stderr": ""})()

    monkeypatch.setattr(ssh, "build_connection", lambda cfg, **kwargs: Connection())
    SshTransport(_cfg()).run(["true"])


def test_run_allows_explicit_unbounded_timeout(monkeypatch):
    class Connection:
        def run(self, command, **kwargs):
            assert kwargs["timeout"] is None
            return type("Result", (), {"exited": 0, "stdout": "", "stderr": ""})()

    monkeypatch.setattr(ssh, "build_connection", lambda cfg, **kwargs: Connection())
    SshTransport(_cfg()).run(["true"], timeout_s=None)


@pytest.mark.parametrize("value", [0, -1, float("inf"), float("nan"), True])
def test_run_rejects_invalid_explicit_timeout(monkeypatch, value):
    monkeypatch.setattr(ssh, "build_connection", lambda cfg, **kwargs: object())
    with pytest.raises(ModelValidationError, match="timeout_s"):
        SshTransport(_cfg()).run(["true"], timeout_s=value)


def test_rsync_uses_configured_timeout(monkeypatch):
    seen = {}
    monkeypatch.setattr(
        ssh.subprocess,
        "run",
        lambda argv, **kwargs: seen.update(kwargs) or subprocess.CompletedProcess(argv, 0),
    )
    SshTransport(_cfg(), rsync_timeout_s=23).push("/a", "/b")
    assert seen["timeout"] == 23


def test_rsync_timeout_raises_transport_error(monkeypatch):
    def boom(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, timeout=23)

    monkeypatch.setattr(ssh.subprocess, "run", boom)
    with pytest.raises(TransportError, match="rsync timed out after 23"):
        SshTransport(_cfg(), rsync_timeout_s=23).push("/a", "/b")


def test_rsync_launch_error_raises_transport_error(monkeypatch):
    def boom(argv, **kwargs):
        raise FileNotFoundError("rsync missing")

    monkeypatch.setattr(ssh.subprocess, "run", boom)
    with pytest.raises(TransportError, match="rsync launch failed: rsync missing"):
        SshTransport(_cfg()).push("/a", "/b")
