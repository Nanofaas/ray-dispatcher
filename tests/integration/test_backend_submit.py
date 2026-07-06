import time

import pytest

from ray_dispatcher.backends import ssh_ray
from ray_dispatcher.backends.ssh_ray import SshRayBackend
from ray_dispatcher.errors import SessionLockLostError
from ray_dispatcher.models import (
    HostProvisioningResult,
    Inventory,
    Job,
    JobHandle,
    JobStatus,
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


@pytest.mark.parametrize("operation", ["submit", "status"])
def test_backend_refuses_work_and_tears_down_after_session_loss(operation):
    calls: list[str] = []

    class Lock:
        def release(self):
            calls.append("release")

    class Heartbeat:
        def raise_if_failed(self):
            raise SessionLockLostError("lost")

        def stop(self):
            calls.append("stop")

    backend = SshRayBackend()
    backend._outcome = ProvisioningOutcome(
        ProvisioningReport(()), {"host": (Lock(), Heartbeat())}  # type: ignore[arg-type]
    )

    with pytest.raises(SessionLockLostError, match="lost"):
        if operation == "submit":
            backend.submit("batch", Job(id="job", command=("true",)))
        else:
            backend.status(JobHandle(batch_id="batch", job_id="job", token="missing"))

    assert calls == ["stop", "release"]
    assert backend._outcome is None


def test_submit_and_resolve_succeeds(tmp_path, monkeypatch):
    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu", slots=2),))
    monkeypatch.setattr(ssh_ray, "provision", lambda *a, **k: _canned_outcome("10.0.0.1"))

    # ponytail: inline closure so cloudpickle serializes it without a module reference
    def _ok_transport(host: RemoteHost) -> FakeTransport:
        def results(argv: list[str]) -> CommandResult:
            if 'printf %s "$HOME"' in " ".join(argv):
                return CommandResult(0, "/home/ubuntu", "", 0.0)
            if argv[0] == "cat":
                return CommandResult(0, '{"returncode": 0, "duration_s": 0.05}', "", 0.0)
            return CommandResult(0, "", "", 0.0)
        return FakeTransport(run_results=results)

    backend = SshRayBackend(transport_factory=_ok_transport, results_dir=str(tmp_path))
    try:
        backend.setup(inv, _project())
        job = Job(id="j1", command=("echo", "hi"))
        handle = backend.submit("batch1", job)
        result = backend.resolve(handle)
        assert result.status == JobStatus.SUCCEEDED
        assert result.id == "j1"
    finally:
        backend.teardown()


def test_submit_failed_command_returns_failed_not_ray_retry(tmp_path, monkeypatch):
    """COMMAND failure: result has 1 attempt (max_retries=0 — Ray never auto-retries) (§11)."""
    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu", slots=1),))
    monkeypatch.setattr(ssh_ray, "provision", lambda *a, **k: _canned_outcome("10.0.0.1"))

    # ponytail: inline closure so cloudpickle serializes it without a module reference
    def _fail_transport(host: RemoteHost) -> FakeTransport:
        def results(argv: list[str]) -> CommandResult:
            if 'printf %s "$HOME"' in " ".join(argv):
                return CommandResult(0, "/home/ubuntu", "", 0.0)
            if argv[0] == "cat":
                return CommandResult(0, '{"returncode": 1, "duration_s": 0.05}', "", 0.0)
            return CommandResult(0, "", "", 0.0)
        return FakeTransport(run_results=results)

    backend = SshRayBackend(transport_factory=_fail_transport, results_dir=str(tmp_path))
    try:
        backend.setup(inv, _project())
        job = Job(id="j2", command=("failing",))
        handle = backend.submit("batch1", job)
        result = backend.resolve(handle)
        assert result.status == JobStatus.FAILED
        assert len(result.attempts) == 1  # no retry for COMMAND failure
    finally:
        backend.teardown()


def test_status_returns_running_then_succeeded(tmp_path, monkeypatch):
    """status() returns RUNNING while the task blocks, then SUCCEEDED after resolve."""
    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu", slots=1),))
    monkeypatch.setattr(ssh_ray, "provision", lambda *a, **k: _canned_outcome("10.0.0.1"))

    # ponytail: inline closure — threading.Event is not Ray-serializable (cross-process);
    # time.sleep keeps the task running long enough to assert RUNNING, add Event if needed
    def _blocking_transport(host: RemoteHost) -> FakeTransport:
        def results(argv: list[str]) -> CommandResult:
            if 'printf %s "$HOME"' in " ".join(argv):
                return CommandResult(0, "/home/ubuntu", "", 0.0)
            if "python3" in " ".join(argv):  # runner invocation — block for a bit
                time.sleep(2.0)
                return CommandResult(0, "", "", 0.0)
            if argv[0] == "cat":
                return CommandResult(0, '{"returncode": 0, "duration_s": 0.1}', "", 0.0)
            return CommandResult(0, "", "", 0.0)
        return FakeTransport(run_results=results)

    backend = SshRayBackend(transport_factory=_blocking_transport, results_dir=str(tmp_path))
    try:
        backend.setup(inv, _project())
        job = Job(id="j3", command=("sleep",))
        handle = backend.submit("batch1", job)
        # Poll until RUNNING (Ray task takes a moment to start).
        for _ in range(40):
            if backend.status(handle) == JobStatus.RUNNING:
                break
            time.sleep(0.05)
        assert backend.status(handle) == JobStatus.RUNNING
        result = backend.resolve(handle)
        assert result.status == JobStatus.SUCCEEDED
    finally:
        backend.teardown()


def test_status_unknown_handle_raises(tmp_path, monkeypatch):
    import pytest

    from ray_dispatcher.errors import DispatcherError
    from ray_dispatcher.models import JobHandle

    monkeypatch.setattr(ssh_ray, "provision", lambda *a, **k: _canned_outcome("10.0.0.1"))
    backend = SshRayBackend(
        transport_factory=lambda h: FakeTransport(), results_dir=str(tmp_path)
    )
    with pytest.raises(DispatcherError, match="unknown job handle"):
        backend.status(JobHandle(batch_id="b", job_id="ghost", token="nope"))


def test_status_returns_cancelled_for_cancelled_task(tmp_path, monkeypatch):
    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu", slots=1),))
    monkeypatch.setattr(ssh_ray, "provision", lambda *a, **k: _canned_outcome("10.0.0.1"))

    # ponytail: inline closure so cloudpickle serializes it without a module reference
    def _blocking_transport(host: RemoteHost) -> FakeTransport:
        def results(argv: list[str]) -> CommandResult:
            if 'printf %s "$HOME"' in " ".join(argv):
                return CommandResult(0, "/home/ubuntu", "", 0.0)
            if "python3" in " ".join(argv):
                time.sleep(5.0)
                return CommandResult(0, "", "", 0.0)
            if argv[0] == "cat":
                return CommandResult(0, '{"returncode": 0, "duration_s": 0.1}', "", 0.0)
            return CommandResult(0, "", "", 0.0)
        return FakeTransport(run_results=results)

    backend = SshRayBackend(transport_factory=_blocking_transport, results_dir=str(tmp_path))
    try:
        backend.setup(inv, _project())
        handle = backend.submit("batch-c", Job(id="jc", command=("sleep",)))
        backend.cancel(handle)
        # poll: cancellation is async; status must settle on CANCELLED, never raise
        for _ in range(100):
            if backend.status(handle) == JobStatus.CANCELLED:
                break
            time.sleep(0.1)
        assert backend.status(handle) == JobStatus.CANCELLED
    finally:
        backend.teardown()


def test_cancel_running_job_writes_cancelled_result_json(tmp_path, monkeypatch):
    import json

    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu", slots=1),))
    monkeypatch.setattr(ssh_ray, "provision", lambda *a, **k: _canned_outcome("10.0.0.1"))

    # ponytail: inline closure so cloudpickle serializes it without a module reference
    def _blocking_transport(host: RemoteHost) -> FakeTransport:
        def results(argv: list[str]) -> CommandResult:
            if 'printf %s "$HOME"' in " ".join(argv):
                return CommandResult(0, "/home/ubuntu", "", 0.0)
            if "python3" in " ".join(argv):
                time.sleep(10.0)
                return CommandResult(0, "", "", 0.0)
            if argv[0] == "cat":
                return CommandResult(0, '{"returncode": 0, "duration_s": 0.1}', "", 0.0)
            return CommandResult(0, "", "", 0.0)
        return FakeTransport(run_results=results)

    backend = SshRayBackend(transport_factory=_blocking_transport, results_dir=str(tmp_path))
    try:
        backend.setup(inv, _project())
        handle = backend.submit("batch-k", Job(id="jk", command=("sleep",)))
        # wait until the task is actually running before cancelling
        for _ in range(100):
            if backend.status(handle) == JobStatus.RUNNING:
                break
            time.sleep(0.05)
        # status()==RUNNING only means "not finished yet" (ray.wait can't see inside
        # a worker that's still spinning up); give the fresh per-test Ray worker time
        # to actually enter the task body, otherwise cancel lands pre-start and the
        # in-task handler this test targets never runs.
        time.sleep(1.5)
        backend.cancel(handle)
        result = backend.resolve(handle)
        assert result.status == JobStatus.CANCELLED
        result_json = tmp_path / "batch-k" / "jk" / "result.json"
        # the in-task handler must persist the cancelled result locally
        for _ in range(100):
            if result_json.exists():
                break
            time.sleep(0.1)
        assert json.loads(result_json.read_text())["status"] == "cancelled"
    finally:
        backend.teardown()
