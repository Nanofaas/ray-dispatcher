import threading

import pytest

from ray_dispatcher.backends import ssh_ray
from ray_dispatcher.backends.ssh_ray import SshRayBackend
from ray_dispatcher.errors import DispatcherError
from ray_dispatcher.models import Inventory, JobHandle, Project, RemoteHost
from ray_dispatcher.dispatcher import Dispatcher


def test_teardown_keeps_resources_until_drain_finishes(monkeypatch):
    backend = SshRayBackend()
    actor = backend._actor = object()
    backend._owns_runtime = True
    entered, release = threading.Event(), threading.Event()
    calls = []
    def drain():
        entered.set()
        assert release.wait(2)
        calls.append('clean')
    monkeypatch.setattr(backend, '_drain_jobs', drain, raising=False)
    monkeypatch.setattr(ssh_ray.ray, 'kill', lambda a: calls.append('kill'))
    monkeypatch.setattr(ssh_ray.ray, 'shutdown', lambda: calls.append('shutdown'))
    worker = threading.Thread(target=backend.teardown)
    worker.start()
    try:
        assert entered.wait(1)
        assert backend._actor is actor
        assert calls == []
    finally:
        release.set()
        worker.join(2)
    assert calls == ['clean', 'kill', 'shutdown']


def test_failed_drain_preserves_resources_for_retry(monkeypatch):
    backend = SshRayBackend()
    actor = backend._actor = object()
    backend._refs['token'] = object()
    backend._owns_runtime = True
    def fail():
        raise DispatcherError('host unreachable')
    monkeypatch.setattr(backend, '_drain_jobs', fail, raising=False)
    monkeypatch.setattr(ssh_ray.ray, 'kill', lambda _: pytest.fail('actor killed too soon'))
    monkeypatch.setattr(ssh_ray.ray, 'shutdown', lambda: pytest.fail('runtime stopped too soon'))
    with pytest.raises(DispatcherError, match='host unreachable'):
        backend.teardown(purge=True)
    assert backend._actor is actor
    assert backend._refs
    monkeypatch.setattr(backend, '_drain_jobs', lambda: None)
    monkeypatch.setattr(ssh_ray.ray, 'kill', lambda _: None)
    monkeypatch.setattr(ssh_ray.ray, 'shutdown', lambda: None)
    backend.teardown()
    assert backend._actor is None


def test_dispatcher_keeps_handles_on_failed_teardown(monkeypatch):
    backend = SshRayBackend()
    dispatcher = Dispatcher(Inventory((RemoteHost('host', 'user'),)),
                            Project('.', 'test', '3.10.0', '0.11.25'), backend=backend)
    handle = JobHandle('b', 'j', 'token')
    dispatcher._outstanding = [handle]
    monkeypatch.setattr(backend, 'cancel', lambda h: None)
    def fail(**kwargs):
        raise DispatcherError('not clean')
    monkeypatch.setattr(backend, 'teardown', fail)
    with pytest.raises(DispatcherError):
        dispatcher.teardown()
    assert dispatcher._outstanding == [handle]
