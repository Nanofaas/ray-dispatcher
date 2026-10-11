"""run_job registers the attempt's remote pid path right after acquiring a lease."""

import pytest

from it.unimib.data.nanofaas.ray_dispatcher.models import Job, JobStatus, RetryPolicy
from it.unimib.data.nanofaas.ray_dispatcher.provisioning import RemoteLayout
from it.unimib.data.nanofaas.ray_dispatcher.results import JobLayout
from it.unimib.data.nanofaas.ray_dispatcher.scheduling import HostRuntime, Lease, run_job
from it.unimib.data.nanofaas.ray_dispatcher.ssh import CommandResult, FakeTransport


class _RecordingLease:
    def __init__(self):
        self.registered: list[tuple[str, str]] = []

    def raise_if_lost(self):
        pass

    def acquire(self, attempt_id, *, exclude=()):
        return Lease(
            token="t1", host="h1", slot=0, attempt_id=attempt_id, expiry_s=60.0, heartbeat_s=0.0
        )

    def release(self, token):
        pass

    def register_pid(self, token, pid_path):
        self.registered.append((token, pid_path))


def _runtime():
    layout = RemoteLayout("/home/u", "proj")
    return HostRuntime(
        host="h1",
        layout=layout,
        environment_digest="e" * 8,
        runner_digest="r" * 8,
        project_path="/proj",
        secret_env={},
    )


def _ok_transport():
    def results(argv):
        if argv[0] == "cat":
            return CommandResult(0, '{"returncode": 0, "duration_s": 0.1}', "", 0.0)
        return CommandResult(0, "", "", 0.0)

    return FakeTransport(run_results=results)


def test_run_job_registers_pid_path_after_acquire(tmp_path):
    lease = _RecordingLease()
    job = Job(id="j1", command=("echo",))
    result = run_job(
        job,
        batch_id="b1",
        lease=lease,
        runtime_for=lambda h: _runtime(),
        transport_for=lambda h: _ok_transport(),
        local=JobLayout(str(tmp_path), "b1", "j1"),
        policy=RetryPolicy(),
    )
    assert result.status is JobStatus.SUCCEEDED
    assert lease.registered == [("t1", "/home/u/.ray_dispatcher/runs/proj/b1/j1/1/pid.json")]


def test_run_job_releases_lease_when_register_pid_raises(tmp_path):
    class _ExplodingLease(_RecordingLease):
        def __init__(self):
            super().__init__()
            self.released: list[str] = []

        def register_pid(self, token, pid_path):
            raise RuntimeError("actor unreachable")

        def release(self, token):
            self.released.append(token)

    lease = _ExplodingLease()
    job = Job(id="j1", command=("echo",))
    with pytest.raises(RuntimeError, match="actor unreachable"):
        run_job(
            job,
            batch_id="b1",
            lease=lease,
            runtime_for=lambda h: _runtime(),
            transport_for=lambda h: _ok_transport(),
            local=JobLayout(str(tmp_path), "b1", "j1"),
            policy=RetryPolicy(),
        )
    assert lease.released == ["t1"]  # the lease is released even though register_pid blew up
