import pytest

from ray_dispatcher.errors import ModelValidationError
from ray_dispatcher.models import FailureKind, RetryPolicy
from ray_dispatcher.scheduling import should_retry


def test_retryable_kind_under_budget_retries():
    p = RetryPolicy()  # max_attempts=2, retry_on={SSH, HOST_LOST, COLLECTION}
    assert should_retry(p, FailureKind.SSH, completed_attempts=1) is True


def test_retryable_kind_at_budget_stops():
    p = RetryPolicy()
    assert should_retry(p, FailureKind.SSH, completed_attempts=2) is False  # used the 2 allowed


def test_non_retryable_kinds_never_retry():
    p = RetryPolicy()
    assert should_retry(p, FailureKind.COMMAND, completed_attempts=1) is False
    assert should_retry(p, FailureKind.OUTPUT_MISSING, completed_attempts=1) is False
    assert should_retry(p, FailureKind.TIMEOUT, completed_attempts=1) is False


def test_success_never_retries():
    p = RetryPolicy()
    assert should_retry(p, None, completed_attempts=1) is False


def test_opt_in_retry_on_command():
    p = RetryPolicy(retry_on=frozenset({FailureKind.COMMAND}))
    assert should_retry(p, FailureKind.COMMAND, completed_attempts=1) is True


@pytest.mark.parametrize("max_attempts", ["2", 2.5, True])
def test_retry_policy_rejects_noninteger_max_attempts(max_attempts):
    with pytest.raises(ModelValidationError):
        RetryPolicy(max_attempts=max_attempts)
