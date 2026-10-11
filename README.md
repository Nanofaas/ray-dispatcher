# ray-dispatcher

Generic, Ray-scheduled dispatcher for subprocess jobs on SSH-accessible VMs.
See `docs/superpowers/specs/` for the design and `docs/superpowers/plans/` for
the implementation plan.

## Development checks

```sh
uv sync --locked
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest tests/unit -q
uv run pytest tests/integration -q
uv run pytest -q
```

CI runs lint, formatting, strict type checking and the complete local test suite
on Python 3.10 and 3.13. Integration tests start a local Ray runtime; unit tests
do not start Ray.

Real-VM tests require Python >=3.11, a running Multipass daemon and an SSH public
key. They create disposable VMs and delete them after the session:

```sh
uv sync --locked --group e2e
uv run --group e2e pytest -o addopts= tests/e2e -m e2e -q
```

The example and E2E fixtures use the PyPI distribution `multipass-vm-sdk==0.6.0`
and import `multipass_vm_sdk`.

## Lifecycle and API changes

`running_hosts()` returns `{(batch_id, job_id): host}`. Update consumers from
`hosts.get(job_id)` to `hosts.get((handle.batch_id, handle.job_id))` to distinguish
jobs with the same name in separate batches.

`teardown()` confirms worker completion and orphan cleanup before releasing
session ownership. If cleanup cannot be confirmed within 30 seconds, it raises
`DispatcherError`, keeps the session heartbeats and job metadata, and rejects new
submissions. Restore connectivity and retry `teardown()`; do not discard the
Dispatcher while it still protects unfinished work. A process crash ends those
heartbeats, so ownership protection then lasts only until the session lock TTL.

Child logs stream to remote files and are collected into local attempt logs.
The dispatcher disables the runner's duplicate forwarding over SSH to avoid
capturing unbounded child output in worker memory. The standalone runner still
forwards output by default.
