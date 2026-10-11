from ray_dispatcher.backends import ssh_ray
from ray_dispatcher.backends.ssh_ray import SshRayBackend


def test_backend_preserves_same_job_id_in_two_batches(monkeypatch):
    state = {"a/same": "host-a", "b/same": "host-b"}

    class Current:
        def remote(self):
            return state

    class Actor:
        current_hosts = Current()

    backend = SshRayBackend()
    backend._actor = Actor()
    monkeypatch.setattr(ssh_ray.ray, "get", lambda ref: ref)
    assert backend.running_hosts() == {("a", "same"): "host-a", ("b", "same"): "host-b"}
    state.pop("a/same")
    assert backend.running_hosts() == {("b", "same"): "host-b"}
