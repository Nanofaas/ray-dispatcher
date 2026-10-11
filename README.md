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
uv run deptry src
uv audit --locked
uv run pytest tests/unit -q --cov --cov-report=term-missing
uv build
uv run twine check --strict dist/*
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


## Quality gates

Use uv 0.12.24, pinned in CI, and `uv sync --locked`. Ruff checks security rules
(`S`) and cyclomatic complexity (`C901`) in addition to formatting and existing
lint rules. Complexity is capped at the current maximum of 16; lower the cap
as lifecycle functions are simplified. Security exceptions are scoped to
intentional subprocess execution, test assertions and fixture credentials.

Unit tests measure statement and branch coverage with a minimum of 80%
(current baseline: 81%). Ray integration and real-VM tests run separately;
the coverage number does not include remote processes. `deptry` checks direct,
transitive, unused and development-only imports. The unused legacy `progress`
extra is retained for installation compatibility and explicitly excluded from
the unused-dependency check.

`uv audit --locked` checks known vulnerabilities in the resolved dependencies.
CI audits the root and example locks and runs weekly as well as on pushes and
pull requests, so newly disclosed vulnerabilities are detected without a code
change. Dependabot proposes weekly Python and GitHub Actions updates; updates
require review and passing checks. Secret scanning and push protection are
enabled on GitHub.

CI builds wheel and sdist, validates their metadata with Twine and installs the
wheel in a clean environment outside the checkout. No package is published by
these checks. See the [repository](https://github.com/Nanofaas/ray-dispatcher).
