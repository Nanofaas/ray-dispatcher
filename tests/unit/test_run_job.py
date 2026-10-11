import json

from ray_dispatcher.models import AttemptResult, FailureKind, Job, JobStatus, RetryPolicy
from ray_dispatcher.provisioning import RemoteLayout
from ray_dispatcher.results import JobLayout
from ray_dispatcher.scheduling import (
    HostRuntime,
    Lease,
    _HostCleanupError,
    run_job,
)
from ray_dispatcher.ssh import CommandResult, FakeTransport, TransportError


class FakeLease:
    """Hands out hosts in order; records the exclude set seen on each acquire."""

    def __init__(self, hosts):
        self._hosts = list(hosts)
        self._n = 0
        self.excludes = []
        self.quarantined = []

    def raise_if_lost(self):
        pass

    def acquire(self, attempt_id, *, exclude=()):
        self.excludes.append(set(exclude))
        host = self._hosts[self._n]
        self._n += 1
        return Lease(
            token=f"tok{self._n}",
            host=host,
            slot=0,
            attempt_id=attempt_id,
            expiry_s=0.0,
            heartbeat_s=0.0,
        )

    def release(self, token):
        pass

    def register_pid(self, token, pid_path):
        pass

    def quarantine(self, host):
        self.quarantined.append(host)


def _runtime(host):
    return HostRuntime(
        host=host,
        layout=RemoteLayout("/home/u", "dfaas"),
        environment_digest="env1",
        runner_digest="run1",
        project_path="/proj",
        secret_env={},
    )


def _ok_transport(rc=0):
    def results(argv):
        if argv[0] == "cat" and argv[1].endswith("result.json"):
            return CommandResult(
                0,
                json.dumps(
                    {"returncode": rc, "started_at": 1.0, "ended_at": 2.0, "duration_s": 1.5}
                ),
                "",
                0.0,
            )
        return CommandResult(0, "", "", 0.0)

    return FakeTransport(run_results=results)


def _ssh_failing_transport():
    def results(argv):
        if argv[0] == "python3":  # the runner invocation
            raise TransportError("ssh dropped")
        return CommandResult(0, "", "", 0.0)

    return FakeTransport(run_results=results)


def _layout(tmp_path, job_id="jobA"):
    return JobLayout(str(tmp_path / "results"), "b1", job_id)


def test_success_first_attempt(tmp_path):
    lease = FakeLease(["a"])
    job = Job(id="jobA", command=("python", "run.py"))  # no outputs
    r = run_job(
        job,
        batch_id="b1",
        lease=lease,
        runtime_for=_runtime,
        transport_for=lambda h: _ok_transport(0),
        local=_layout(tmp_path),
        policy=RetryPolicy(),
    )
    assert r.status is JobStatus.SUCCEEDED
    assert r.host == "a"
    assert len(r.attempts) == 1
    assert lease.excludes == [set()]  # first acquire excludes nothing


def test_ssh_failure_retries_on_different_host(tmp_path):
    lease = FakeLease(["a", "b"])
    transports = {"a": _ssh_failing_transport(), "b": _ok_transport(0)}
    job = Job(id="jobA", command=("python", "run.py"))
    r = run_job(
        job,
        batch_id="b1",
        lease=lease,
        runtime_for=_runtime,
        transport_for=lambda h: transports[h],
        local=_layout(tmp_path),
        policy=RetryPolicy(),
    )
    assert r.status is JobStatus.SUCCEEDED
    assert r.host == "b"  # retried elsewhere
    assert len(r.attempts) == 2
    assert r.attempts[0].failure_kind is FailureKind.SSH
    assert lease.excludes == [set(), {"a"}]  # second acquire excludes the tried host


def test_ssh_failure_exhausts_attempts(tmp_path):
    lease = FakeLease(["a", "b"])
    job = Job(id="jobA", command=("python", "run.py"))
    r = run_job(
        job,
        batch_id="b1",
        lease=lease,
        runtime_for=_runtime,
        transport_for=lambda h: _ssh_failing_transport(),
        local=_layout(tmp_path),
        policy=RetryPolicy(),
    )  # max_attempts=2
    assert r.status is JobStatus.FAILED
    assert r.attempts[-1].failure_kind is FailureKind.SSH
    assert len(r.attempts) == 2  # stopped at the budget


def test_command_failure_is_not_retried(tmp_path):
    lease = FakeLease(["a", "b"])
    job = Job(id="jobA", command=("python", "run.py"))
    r = run_job(
        job,
        batch_id="b1",
        lease=lease,
        runtime_for=_runtime,
        transport_for=lambda h: _ok_transport(rc=3),
        local=_layout(tmp_path),
        policy=RetryPolicy(),
    )
    assert r.status is JobStatus.FAILED
    assert r.returncode == 3
    assert r.attempts[-1].failure_kind is FailureKind.COMMAND
    assert len(r.attempts) == 1  # COMMAND not retried


def test_unclean_timeout_quarantines_host(monkeypatch, tmp_path):
    lease = FakeLease(["a"])
    timed_out = AttemptResult(
        number=1,
        host="a",
        status=JobStatus.TIMED_OUT,
        returncode=None,
        duration_s=0.1,
        stdout_log="out",
        stderr_log="err",
        failure_kind=FailureKind.TIMEOUT,
        error="job timed out",
    )
    monkeypatch.setattr(
        "ray_dispatcher.scheduling.execute_attempt",
        lambda *a, **k: (_ for _ in ()).throw(_HostCleanupError(result=timed_out)),
    )
    result = run_job(
        Job(id="jobA", command=("sleep", "9"), timeout_s=0.1),
        batch_id="b1",
        lease=lease,
        runtime_for=_runtime,
        transport_for=lambda h: _ok_transport(),
        local=_layout(tmp_path),
        policy=RetryPolicy(),
    )
    assert result.status is JobStatus.TIMED_OUT
    assert lease.quarantined == ["a"]


def test_unclean_cancel_quarantines_then_preserves_interrupt(monkeypatch, tmp_path):
    lease = FakeLease(["a"])
    interrupt = KeyboardInterrupt()
    monkeypatch.setattr(
        "ray_dispatcher.scheduling.execute_attempt",
        lambda *a, **k: (_ for _ in ()).throw(_HostCleanupError(interrupted=interrupt)),
    )
    result = run_job(
        Job(id="jobA", command=("sleep", "9")),
        batch_id="b1",
        lease=lease,
        runtime_for=_runtime,
        transport_for=lambda h: _ok_transport(),
        local=_layout(tmp_path),
        policy=RetryPolicy(),
    )
    assert result.status is JobStatus.CANCELLED
    assert lease.quarantined == ["a"]


def test_lost_capacity_preserves_completed_attempts(tmp_path):
    from ray_dispatcher.errors import NoHealthyHostsError

    class LostCapacity(FakeLease):
        def acquire(self, attempt_id, *, exclude=()):
            if self._n:
                raise NoHealthyHostsError("all hosts lost")
            return super().acquire(attempt_id, exclude=exclude)

    result = run_job(
        Job("jobA", ("true",)),
        batch_id="b1",
        lease=LostCapacity(["a"]),
        runtime_for=_runtime,
        transport_for=lambda _: _ssh_failing_transport(),
        local=_layout(tmp_path),
        policy=RetryPolicy(),
    )
    assert result.status is JobStatus.FAILED
    assert len(result.attempts) == 1
    assert "all hosts lost" in result.error


def test_cancel_preserves_attempt_history(tmp_path, monkeypatch):
    calls = []

    def execute(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise TransportError("retry")
        raise KeyboardInterrupt()

    monkeypatch.setattr("ray_dispatcher.scheduling.execute_attempt", execute)
    result = run_job(
        Job("jobA", ("true",)),
        batch_id="b1",
        lease=FakeLease(["a", "b"]),
        runtime_for=_runtime,
        transport_for=lambda _: _ok_transport(),
        local=_layout(tmp_path),
        policy=RetryPolicy(),
    )
    assert result.status is JobStatus.CANCELLED
    assert len(result.attempts) == 2
    assert result.attempts[0].failure_kind is FailureKind.SSH


def test_collection_policy_retries_download_failure(tmp_path):
    class BrokenPull(FakeTransport):
        def pull(self, *args, **kwargs):
            raise TransportError("download failed")

    transports = {"a": BrokenPull(run_results=_ok_transport()._run_results), "b": _ok_transport()}
    result = run_job(
        Job("jobA", ("true",)),
        batch_id="b1",
        lease=FakeLease(["a", "b"]),
        runtime_for=_runtime,
        transport_for=transports.__getitem__,
        local=_layout(tmp_path),
        policy=RetryPolicy(2, frozenset({FailureKind.COLLECTION})),
    )
    assert result.status is JobStatus.SUCCEEDED
    assert result.attempts[0].failure_kind is FailureKind.COLLECTION


def test_command_failure_dominates_collection_failure(tmp_path):
    class BrokenPull(FakeTransport):
        def pull(self, *args, **kwargs):
            raise TransportError("download failed")

    transport = BrokenPull(run_results=_ok_transport(rc=7)._run_results)
    result = run_job(
        Job("jobA", ("true",)),
        batch_id="b1",
        lease=FakeLease(["a"]),
        runtime_for=_runtime,
        transport_for=lambda _: transport,
        local=_layout(tmp_path),
        policy=RetryPolicy(2, frozenset({FailureKind.COLLECTION})),
    )
    assert result.returncode == 7
    assert result.attempts[0].failure_kind is FailureKind.COMMAND


def test_host_loss_is_quarantined_before_retry(tmp_path):
    from ray_dispatcher.scheduling import _LeaseLostError

    class LostLease(FakeLease):
        def raise_if_lost(self):
            if self._n == 1:
                raise _LeaseLostError("lease lost")

        def acquire(self, attempt_id, *, exclude=()):
            if self._n:
                assert self.quarantined == ["a"]
            return super().acquire(attempt_id, exclude=exclude)

    result = run_job(
        Job("jobA", ("true",)),
        batch_id="b1",
        lease=LostLease(["a", "b"]),
        runtime_for=_runtime,
        transport_for=lambda _: _ok_transport(),
        local=_layout(tmp_path),
        policy=RetryPolicy(2, frozenset({FailureKind.HOST_LOST})),
    )
    assert result.status is JobStatus.SUCCEEDED
    assert result.attempts[0].failure_kind is FailureKind.HOST_LOST
