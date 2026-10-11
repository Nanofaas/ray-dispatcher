import json
import time

import pytest

from ray_dispatcher import Dispatcher, Job


@pytest.mark.e2e
def test_context_exit_kills_process_group_before_next_session(tmp_path, inventory, synth_project):
    project, _ = synth_project
    dispatcher = Dispatcher(inventory, project, results_dir=str(tmp_path))
    observed = None
    try:
        dispatcher.setup()
        dispatcher.submit([Job('parent', ('python', '-c',
            "import subprocess,sys,time; subprocess.Popen([sys.executable,'-c',"
            "'import time; time.sleep(60)']); time.sleep(60)"))], batch_id='teardown')
        backend = dispatcher._backend
        deadline = time.monotonic()+30
        while time.monotonic() < deadline and observed is None:
            for host, runtime in backend._runtimes.items():
                transport = backend._transport_factory(backend._inv_hosts[host])
                result = transport.run(['cat', runtime.layout.run_paths('teardown', 'parent', 1).pid])
                if result.returncode == 0:
                    observed = transport, json.loads(result.stdout)['pgid']
                    break
            time.sleep(.05)
        assert observed is not None, 'job never started remotely'
    finally:
        dispatcher.teardown()
    transport, pgid = observed
    assert transport.run(['kill', '-0', f'-{pgid}']).returncode != 0
    with Dispatcher(inventory, project, results_dir=str(tmp_path/'next')) as following:
        following.setup()
