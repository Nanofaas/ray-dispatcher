import threading

import pytest

from it.unimib.data.nanofaas.ray_dispatcher.backends import ssh_ray
from it.unimib.data.nanofaas.ray_dispatcher.backends.ssh_ray import _ActorLeaseHandle


@pytest.mark.parametrize("failed", [False, True])
def test_lost_heartbeat_becomes_host_loss(monkeypatch, failed):
    class Remote:
        def remote(self, *args):
            return object()

    class Actor:
        heartbeat = Remote()

    def get(*args, **kwargs):
        if failed:
            raise RuntimeError("actor unavailable")
        return False

    monkeypatch.setattr(ssh_ray.ray, "get", get)
    handle = _ActorLeaseHandle(Actor(), heartbeat_interval_s=0.001)
    worker = threading.Thread(target=handle._heartbeat, args=("token",))
    worker.start()
    worker.join(1)
    with pytest.raises(Exception, match="lease lost"):
        handle.raise_if_lost()
