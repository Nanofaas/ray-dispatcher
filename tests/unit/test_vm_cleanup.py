import importlib.util
from pathlib import Path

import pytest

pytest.importorskip('multipass_vm_sdk')


@pytest.mark.parametrize('caller', ['fixture', 'example'])
@pytest.mark.parametrize('stage', ['ready', 'cloud'])
@pytest.mark.parametrize('failed_vm', [1, 2])
def test_created_vms_are_deleted_on_initialization_failure(monkeypatch, caller, stage, failed_vm):
    path = Path('tests/e2e/conftest.py' if caller == 'fixture' else 'examples/toy_batch/run_batch.py')
    spec = importlib.util.spec_from_file_location('vm_cleanup_target', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    created = []
    deleted = []
    class VM:
        def __init__(self, number):
            self.number = number
            self.name = f'vm-{number}'
        def wait_ready(self, **kwargs):
            if self.number == failed_vm and stage == 'ready':
                raise RuntimeError('initialization failed')
            return '127.0.0.1'
        def exec(self, argv):
            if self.number == failed_vm and stage == 'cloud':
                raise RuntimeError('initialization failed')
        def delete(self, *, purge):
            assert purge
            deleted.append(self.number)
    class Client:
        def launch(self, **kwargs):
            vm = VM(len(created)+1)
            created.append(vm)
            return vm
    monkeypatch.setattr(module, 'MultipassClient', Client)
    monkeypatch.setattr(module, '_cloud_init', lambda: {})
    with pytest.raises(RuntimeError, match='initialization failed'):
        if caller == 'example':
            module.main()
        else:
            next(module.vm_fleet.__wrapped__(None))
    assert sorted(deleted) == list(range(1, failed_vm+1))
