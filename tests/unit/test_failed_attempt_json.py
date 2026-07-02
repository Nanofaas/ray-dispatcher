import json

from ray_dispatcher.models import FailureKind, JobStatus
from ray_dispatcher.results import JobLayout
from ray_dispatcher.scheduling import _failed_attempt


def test_failed_attempt_writes_attempt_json(tmp_path):
    local = JobLayout(str(tmp_path), "b1", "j1")
    result = _failed_attempt(1, "10.0.0.1", FailureKind.SSH, "boom", local)
    assert result.status is JobStatus.FAILED
    doc = json.loads(local.attempt_json(1).read_text())
    assert doc["failure_kind"] == "ssh"
    assert doc["error"] == "boom"
