import json

import pytest

from it.unimib.data.nanofaas.ray_dispatcher.backends import ssh_ray
from it.unimib.data.nanofaas.ray_dispatcher.errors import NoHealthyHostsError
from it.unimib.data.nanofaas.ray_dispatcher.models import Job, JobStatus, RetryPolicy
from it.unimib.data.nanofaas.ray_dispatcher.results import JobLayout


def test_attempt_task_has_correct_ray_decoration():
    options = ssh_ray._attempt_task._default_options
    assert options["num_cpus"] == 0
    assert options["resources"] == {"vm_slot": 1}
    assert options["max_retries"] == 0


@pytest.mark.parametrize(
    "error", [NoHealthyHostsError("no healthy hosts remain"), KeyboardInterrupt()]
)
def test_early_failure_persists_terminal_result(tmp_path, monkeypatch, error):
    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(ssh_ray, "run_job", fail)
    local = JobLayout(str(tmp_path), "batch", "job")
    result = ssh_ray._attempt_task._function(
        Job("job", ("true",)), "batch", local, {}, {}, None, None, RetryPolicy()
    )
    assert result.status in {JobStatus.FAILED, JobStatus.CANCELLED}
    assert json.loads(local.result_json.read_text())["status"] == result.status.value


def test_persistence_error_is_not_retried(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(ssh_ray, "run_job", lambda *a, **k: object())

    def fail(*args):
        calls.append(1)
        raise PermissionError("read only")

    monkeypatch.setattr(ssh_ray, "write_result_json", fail)
    with pytest.raises(PermissionError):
        ssh_ray._attempt_task._function(
            Job("job", ("true",)),
            "batch",
            JobLayout(str(tmp_path), "batch", "job"),
            {},
            {},
            None,
            None,
            RetryPolicy(),
        )
    assert calls == [1]
