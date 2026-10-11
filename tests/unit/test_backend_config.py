import pytest

from it.unimib.data.nanofaas.ray_dispatcher.backends.ssh_ray import SshRayBackend
from it.unimib.data.nanofaas.ray_dispatcher.errors import ModelValidationError


@pytest.mark.parametrize(
    "kwargs",
    [
        {"min_disk_mb": -1},
        {"lease_ttl_s": 0},
        {"lease_ttl_s": float("nan")},
        {"sweep_interval_s": 0},
        {"sweep_interval_s": float("inf")},
    ],
)
def test_backend_rejects_invalid_numeric_configuration(kwargs):
    with pytest.raises(ModelValidationError):
        SshRayBackend(**kwargs)


def test_heartbeat_interval_is_below_short_lease_ttl():
    backend = SshRayBackend(lease_ttl_s=0.6)
    assert 0 < backend._heartbeat_interval_s < 0.6
