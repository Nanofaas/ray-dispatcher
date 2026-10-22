import asyncio
from unittest.mock import MagicMock

import pytest

from it.unimib.data.nanofaas.ray_dispatcher.backends.base import ExecutionBackend
from it.unimib.data.nanofaas.ray_dispatcher.dispatcher import Dispatcher
from it.unimib.data.nanofaas.ray_dispatcher.models import Inventory, Project, RemoteHost
from it.unimib.data.nanofaas.ray_dispatcher.scheduling import LeasePool, LeaseService


def test_retired_host_finishes_existing_lease_but_gets_no_new_work():
    pool = LeasePool({"first": 1})
    lease = pool.acquire("running")
    pool.remove_hosts(["first"])
    assert pool.heartbeat(lease.token)
    assert pool.acquire("queued") is None
    pool.add_hosts({"second": 2})
    assert pool.acquire("queued").host == "second"
    pool.release(lease.token)
    assert pool.acquire("next").host == "second"


def test_waiting_job_sees_added_host_and_live_runtime():
    async def scenario():
        service = LeaseService({"first": 1})
        await service.acquire("running")
        pending = asyncio.create_task(service.acquire("queued"))
        await asyncio.sleep(0)
        assert not pending.done()
        await service.add_hosts({"second": 1}, {"second": ("runtime", "host")})
        leased = await asyncio.wait_for(pending, 0.5)
        assert leased.host == "second"
        assert await service.host_context(leased.host) == ("runtime", "host")
        await service.remove_hosts(["first"])
        assert await service.available_hosts() == ("second",)

    asyncio.run(scenario())


def test_no_capacity_wait_is_bounded_and_addition_wakes_it():
    async def scenario():
        service = LeaseService({}, capacity_wait_s=0.02)
        pending = asyncio.create_task(service.acquire("queued"))
        await asyncio.sleep(0)
        await service.add_hosts({"added": 1}, {})
        assert (await pending).host == "added"
        await service.remove_hosts(["added"])
        from it.unimib.data.nanofaas.ray_dispatcher.errors import NoHealthyHostsError

        with pytest.raises(NoHealthyHostsError):
            await asyncio.wait_for(service.acquire("timeout"), 0.5)

    asyncio.run(scenario())


def test_dispatcher_elastic_api_delegates_after_setup():
    backend = MagicMock(spec=ExecutionBackend)
    backend.setup.return_value.hosts = ()
    dispatcher = Dispatcher(
        Inventory((RemoteHost("first", "user"),)),
        Project(".", "p", "3.10.0", "0.11.25"),
        backend=backend,
        elastic=True,
    )
    dispatcher.setup()
    hosts = [RemoteHost("second", "user")]
    assert dispatcher.add_hosts(hosts) is backend.add_hosts.return_value
    dispatcher.remove_hosts(["first"])
    assert dispatcher.available_hosts() is backend.available_hosts.return_value
    backend.add_hosts.assert_called_once_with(hosts)
    backend.remove_hosts.assert_called_once_with(["first"])


def backend_fixture(monkeypatch, tmp_path):
    from it.unimib.data.nanofaas.ray_dispatcher.backends import ssh_ray

    backend = ssh_ray.SshRayBackend(elastic=True, results_dir=str(tmp_path))
    backend._actor = MagicMock()
    backend._project = Project(".", "p", "3.10.0", "0.11.25")
    backend._inv_hosts = {"first": RemoteHost("first", "user")}
    backend._runtimes = {"first": object()}
    backend._actor.available_hosts.remote.return_value = ("first",)
    backend._actor.assigned_hosts.remote.return_value = {}
    monkeypatch.setattr(ssh_ray.ray, "get", lambda value, **kw: value)
    monkeypatch.setattr(ssh_ray.ray, "wait", lambda refs, **kw: ([], list(refs)))
    return backend, ssh_ray


def test_added_capacity_starts_already_queued_jobs_without_fixed_ray_slots(monkeypatch, tmp_path):
    from it.unimib.data.nanofaas.ray_dispatcher.models import Job

    backend, module = backend_fixture(monkeypatch, tmp_path)
    task = MagicMock()
    task.options.return_value.remote.side_effect = [object(), object()]
    monkeypatch.setattr(module, "_attempt_task", task)
    first = backend.submit("b", Job("first", ("true",)))
    second = backend.submit("b", Job("second", ("true",)))
    assert first.token in backend._refs
    assert second.token not in backend._refs
    backend._inv_hosts["second"] = RemoteHost("second", "user")
    backend._actor.available_hosts.remote.return_value = ("first", "second")
    backend.status(second)
    assert second.token in backend._refs
    task.options.assert_called_with(resources={})


def test_addition_prepares_before_registering_and_retains_partial_report(monkeypatch, tmp_path):
    from it.unimib.data.nanofaas.ray_dispatcher.models import (
        HostProvisioningResult,
        ProvisioningReport,
    )
    from it.unimib.data.nanofaas.ray_dispatcher.provisioning import ProvisioningOutcome

    backend, module = backend_fixture(monkeypatch, tmp_path)
    report = ProvisioningReport(
        (
            HostProvisioningResult("second", True, "src", "env"),
            HostProvisioningResult("bad", False, None, None),
        )
    )
    outcome = ProvisioningOutcome(report)
    monkeypatch.setattr(module, "provision", lambda *a, **kw: outcome)
    runtime = object()

    def build(*a):
        backend._actor.add_hosts.remote.assert_not_called()
        return runtime

    monkeypatch.setattr(backend, "_build_runtime", build)
    result = backend.add_hosts([RemoteHost("second", "user"), RemoteHost("bad", "user")])
    assert result is report
    assert backend._runtimes["second"] is runtime
    assert "bad" not in backend._runtimes
    assert backend._actor.add_hosts.remote.call_args.args[0] == {"second": 1}


def test_slow_preparation_does_not_block_status_and_teardown_waits(monkeypatch, tmp_path):
    import threading

    from it.unimib.data.nanofaas.ray_dispatcher.models import (
        HostProvisioningResult,
        Job,
        ProvisioningReport,
    )
    from it.unimib.data.nanofaas.ray_dispatcher.provisioning import ProvisioningOutcome

    backend, module = backend_fixture(monkeypatch, tmp_path)
    entered, release = threading.Event(), threading.Event()
    outcome = ProvisioningOutcome(
        ProvisioningReport((HostProvisioningResult("second", True, "src", "env"),))
    )
    outcome.release_all = MagicMock()

    def prepare(*a, **kw):
        entered.set()
        assert release.wait(2)
        return outcome

    monkeypatch.setattr(module, "provision", prepare)
    monkeypatch.setattr(backend, "_build_runtime", lambda *a: object())
    monkeypatch.setattr(module, "_attempt_task", MagicMock())
    handle = backend.submit("b", Job("running", ("true",)))
    errors = []

    def add():
        try:
            backend.add_hosts([RemoteHost("second", "user")])
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=add)
    thread.start()
    assert entered.wait(1)
    assert backend.status(handle).value == "running"
    monkeypatch.setattr(backend, "_drain_jobs", lambda: None)
    monkeypatch.setattr(module.ray, "kill", lambda a: None)
    teardown = threading.Thread(target=backend.teardown)
    teardown.start()
    teardown.join(0.02)
    assert teardown.is_alive()
    release.set()
    thread.join(2)
    teardown.join(2)
    assert errors
    outcome.release_all.assert_called_once()
    assert backend._actor is None


def test_host_lock_loss_retires_only_that_host(monkeypatch, tmp_path):
    backend, module = backend_fixture(monkeypatch, tmp_path)
    cancel = MagicMock()
    monkeypatch.setattr(module.ray, "cancel", cancel)
    backend._refs["running"] = object()
    backend._on_host_lost("first", module.SessionLockLostError("evicted"))
    backend._actor.lose_host.remote.assert_called_once_with("first")
    cancel.assert_not_called()
    backend._fail_if_session_lost()


def test_idle_lost_host_does_not_create_unresolvable_cleanup():
    async def scenario():
        service = LeaseService({"idle": 1, "busy": 1})
        await service.lose_host("idle")
        assert await service.quarantined_hosts() == []
        leased = await service.acquire("running")
        await service.register_pid(leased.token, "/pid")
        await service.lose_host("busy")
        assert not await service.heartbeat(leased.token)
        assert await service.orphan_paths("busy") == ["/pid"]
        assert await service.available_hosts() == ()

    asyncio.run(scenario())


def test_elastic_worker_fetches_added_runtime_instead_of_submission_snapshot(monkeypatch, tmp_path):
    from it.unimib.data.nanofaas.ray_dispatcher.backends import ssh_ray
    from it.unimib.data.nanofaas.ray_dispatcher.models import Job, JobResult, JobStatus, RetryPolicy
    from it.unimib.data.nanofaas.ray_dispatcher.results import JobLayout

    actor = MagicMock()
    runtime, host, transport = object(), object(), object()
    actor.host_context.remote.return_value = (runtime, host)
    monkeypatch.setattr(ssh_ray.ray, "get", lambda x: x)

    def run(job, **kw):
        assert kw["runtime_for"]("new") is runtime
        assert kw["transport_for"]("new") is transport
        return JobResult("j", "b", JobStatus.SUCCEEDED, 0, 0.0, "new", None, ())

    monkeypatch.setattr(ssh_ray, "run_job", run)
    result = ssh_ray._attempt_task._function(
        Job("j", ("true",)),
        "b",
        JobLayout(str(tmp_path), "b", "j"),
        {},
        {},
        actor,
        lambda h: transport if h is host else None,
        RetryPolicy(),
        elastic=True,
    )
    assert result.status is JobStatus.SUCCEEDED


def test_session_loss_before_actor_start_is_applied_after_actor_starts(monkeypatch, tmp_path):
    backend, module = backend_fixture(monkeypatch, tmp_path)
    actor = backend._actor
    backend._actor = None
    backend._on_host_lost("first", module.SessionLockLostError("early loss"))
    backend._actor = actor
    backend._on_host_lost("first", module.SessionLockLostError("early loss"))
    actor.lose_host.remote.assert_called_once_with("first")


def test_pending_capacity_wait_times_out_without_launching_or_spending_attempts(
    monkeypatch, tmp_path
):
    from it.unimib.data.nanofaas.ray_dispatcher.models import Job, JobStatus

    backend, module = backend_fixture(monkeypatch, tmp_path)
    backend._actor.available_hosts.remote.return_value = ()
    now = [0.0]
    monkeypatch.setattr(module.time, "monotonic", lambda: now[0])
    task = MagicMock()
    monkeypatch.setattr(module, "_attempt_task", task)
    handle = backend.submit("b", Job("queued", ("true",)))
    assert backend.status(handle) is JobStatus.PENDING
    now[0] = 901.0
    assert backend.status(handle) is JobStatus.FAILED
    assert backend.resolve(handle).attempts == ()
    task.options.assert_not_called()


def test_session_callbacks_use_scheduler_host_identity_not_ssh_label(monkeypatch, tmp_path):
    from it.unimib.data.nanofaas.ray_dispatcher.models import ProvisioningReport
    from it.unimib.data.nanofaas.ray_dispatcher.provisioning import ProvisioningOutcome

    backend, module = backend_fixture(monkeypatch, tmp_path)
    heartbeat = MagicMock()
    outcome = ProvisioningOutcome(ProvisioningReport(()), {"user@first:22": (object(), heartbeat)})
    backend._watch_outcome(outcome, [RemoteHost("first", "user")])
    callback = heartbeat.set_failure_callback.call_args.args[0]
    callback(module.SessionLockLostError("lost"))
    backend._actor.lose_host.remote.assert_called_once_with("first")


def test_lost_busy_host_invalidates_only_its_lease_and_cannot_be_readmitted():
    async def scenario():
        service = LeaseService({"a": 1, "b": 1})
        lost = await service.acquire("lost-job")
        healthy = await service.acquire("healthy-job")
        assert lost.host == "a" and healthy.host == "b"
        await service.register_pid(lost.token, "/lost/pid")
        await service.lose_host("a")
        assert not await service.heartbeat(lost.token)
        assert await service.heartbeat(healthy.token)
        await service.mark_reconciled("a")
        assert await service.available_hosts() == ("b",)
        assert await service.current_hosts() == {"healthy-job": "b"}

    asyncio.run(scenario())


def test_explicit_empty_inventory_can_bootstrap_elastic_dispatcher(monkeypatch):
    from it.unimib.data.nanofaas.ray_dispatcher.backends import ssh_ray

    backend = ssh_ray.SshRayBackend(elastic=True)
    monkeypatch.setattr(ssh_ray.ray, "is_initialized", lambda: False)
    monkeypatch.setattr(ssh_ray.ray, "init", lambda **kw: None)
    monkeypatch.setattr(ssh_ray.HostLease, "remote", lambda *a, **kw: MagicMock())
    monkeypatch.setattr(backend, "_sweep_loop", lambda: None)
    monkeypatch.setattr(
        ssh_ray, "provision", lambda *a, **kw: pytest.fail("empty startup provisions")
    )
    report = backend.setup(Inventory((), allow_empty=True), Project(".", "p", "3.10.0", "0.11.25"))
    assert report.hosts == ()
    assert backend._actor is not None


def test_retired_busy_task_does_not_block_healthy_idle_capacity(monkeypatch, tmp_path):
    from it.unimib.data.nanofaas.ray_dispatcher.models import Job

    backend, module = backend_fixture(monkeypatch, tmp_path)
    task = MagicMock()
    task.options.return_value.remote.side_effect = [object(), object()]
    monkeypatch.setattr(module, "_attempt_task", task)
    first = backend.submit("b", Job("first", ("true",)))
    second = backend.submit("b", Job("second", ("true",)))
    backend._inv_hosts["healthy"] = RemoteHost("healthy", "user")
    backend._actor.available_hosts.remote.return_value = ("healthy",)
    backend._actor.assigned_hosts.remote.return_value = {"b/first": "first"}
    backend.status(second)
    assert first.token in backend._refs
    assert second.token in backend._refs


def test_confirm_stopped_waits_for_old_attempt_release_before_reusing_identity():
    async def scenario():
        service = LeaseService({"host": 1})
        leased = await service.acquire("old")
        await service.register_pid(leased.token, "/old/pid")
        await service.lose_host("host")
        assert not await service.forget_stopped_host("host")
        await service.release(leased.token)
        assert await service.forget_stopped_host("host")
        await service.add_hosts({"host": 1}, {})
        assert (await service.acquire("new")).host == "host"
        assert await service.quarantined_hosts() == []

    asyncio.run(scenario())


def test_confirm_stopped_releases_only_local_heartbeat_and_allows_replacement(
    monkeypatch, tmp_path
):
    from it.unimib.data.nanofaas.ray_dispatcher.models import ProvisioningReport
    from it.unimib.data.nanofaas.ray_dispatcher.provisioning import ProvisioningOutcome

    backend, module = backend_fixture(monkeypatch, tmp_path)
    lock, heartbeat = MagicMock(), MagicMock()
    backend._outcome = ProvisioningOutcome(ProvisioningReport(()), {"first": (lock, heartbeat)})
    backend._actor.forget_stopped_host.remote.return_value = True
    backend.confirm_hosts_stopped(["first", "unknown"])
    assert "first" not in backend._inv_hosts
    assert "first" not in backend._runtimes
    assert backend._outcome.sessions == {}
    heartbeat.stop.assert_called_once()
    lock.release.assert_not_called()


def test_elastic_failed_drain_releases_healthy_sessions_only(monkeypatch, tmp_path):
    from it.unimib.data.nanofaas.ray_dispatcher.models import ProvisioningReport
    from it.unimib.data.nanofaas.ray_dispatcher.provisioning import ProvisioningOutcome

    backend, module = backend_fixture(monkeypatch, tmp_path)
    bad_lock, bad_heartbeat, good_lock, good_heartbeat = [MagicMock() for _ in range(4)]
    backend._outcome = ProvisioningOutcome(
        ProvisioningReport(()),
        {"bad": (bad_lock, bad_heartbeat), "good": (good_lock, good_heartbeat)},
    )
    backend._actor.current_hosts.remote.return_value = {}
    backend._actor.quarantined_hosts.remote.return_value = ["bad"]
    monkeypatch.setattr(
        backend,
        "_drain_jobs",
        lambda: (_ for _ in ()).throw(module.DispatcherError("remote cleanup not confirmed")),
    )
    with pytest.raises(module.DispatcherError):
        backend.teardown()
    good_lock.release.assert_called_once()
    good_heartbeat.stop.assert_called_once()
    bad_lock.release.assert_not_called()
    assert set(backend._outcome.sessions) == {"bad"}


def test_explicit_empty_inventory_still_rejected_by_static_backend():
    from it.unimib.data.nanofaas.ray_dispatcher.backends.ssh_ray import SshRayBackend
    from it.unimib.data.nanofaas.ray_dispatcher.errors import ModelValidationError

    with pytest.raises(ModelValidationError, match="elastic"):
        SshRayBackend().setup(
            Inventory((), allow_empty=True), Project(".", "p", "3.10.0", "0.11.25")
        )


def test_old_heartbeat_callback_cannot_retire_a_replacement(monkeypatch, tmp_path):
    backend, module = backend_fixture(monkeypatch, tmp_path)
    old, current = MagicMock(), MagicMock()
    backend._host_heartbeats["first"] = current
    backend._on_host_lost("first", module.SessionLockLostError("stale"), heartbeat=old)
    backend._actor.lose_host.remote.assert_not_called()
    backend._on_host_lost("first", module.SessionLockLostError("current"), heartbeat=current)
    backend._actor.lose_host.remote.assert_called_once_with("first")


def test_confirmed_deleted_host_can_forget_a_finished_worker_that_never_released():
    async def scenario():
        service = LeaseService({"host": 1})
        await service.acquire("batch/dead-worker")
        await service.lose_host("host")
        assert await service.forget_stopped_host("host", ("batch/dead-worker",))
        await service.add_hosts({"host": 1}, {})
        assert (await service.acquire("new")).host == "host"

    asyncio.run(scenario())
