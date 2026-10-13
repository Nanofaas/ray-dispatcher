# ray-dispatcher

Generic, Ray-scheduled dispatcher for subprocess jobs on SSH-accessible VMs.
See `docs/superpowers/specs/` for the design and `docs/superpowers/plans/` for
the implementation plan.

## Installation and imports

Install with `pip install ray-dispatcher` or `uv add ray-dispatcher`.
Requires Python >=3.10. For development, install from a checkout with
`uv sync --locked`.

```python
from it.unimib.data.nanofaas.ray_dispatcher import Dispatcher, Inventory, Job, Project
```

The installable distribution remains `ray-dispatcher`. Imports have migrated
from `ray_dispatcher` to `it.unimib.data.nanofaas.ray_dispatcher`; update existing
consumers. There is no legacy import alias. Parent packages are implicit
PEP 420 namespaces, so other distributions can share the Nanofaas namespace.

Licensed under the [MIT License](LICENSE).

## Publishing

The [release workflow](.github/workflows/release.yml) publishes to PyPI when a
push to `main` increases the version. Update both `version` in `pyproject.toml`
and `__version__` in `src/it/unimib/data/nanofaas/ray_dispatcher/__init__.py`,
then update both locks and commit them together with the version bump:

```sh
uv lock
uv lock --project examples/toy_batch
```

Version decreases and mismatched versions fail the workflow.

Before the first release, configure a GitHub **pending trusted publisher** at
[PyPI account Publishing](https://pypi.org/manage/account/publishing/):

| Setting | Value |
| --- | --- |
| PyPI project name | `ray-dispatcher` |
| GitHub owner | `Nanofaas` |
| Repository | `ray-dispatcher` |
| Workflow filename | `release.yml` |
| Environment | `pypi` |

See the [official setup instructions](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/).
The pending publisher creates the PyPI project on the first successful upload;
it does not reserve the name. No API token or repository secret is needed.

Run **PyPI release → Run workflow** in GitHub Actions to verify the complete
release pipeline without uploading anything. Download the `python-distributions`
artifact to inspect wheel and sdist. A version bump on `main` runs the same
checks on Python 3.10 and 3.13, including integration tests, audits, metadata
validation and an isolated wheel install, before uploading those exact artifacts.
Only the upload job receives OIDC permission. Unchanged versions do not publish;
re-running an already successful release will fail because PyPI files cannot
be overwritten. Fix failures and re-run the failed jobs before making another
version bump.

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
.venv/bin/python -m pytest tests/integration -q
.venv/bin/python -m pytest -q
```

CI runs lint, formatting, strict type checking and the complete local test suite
on Python 3.10 and 3.13. Integration tests start a local Ray runtime; unit tests
do not start Ray. Run integration tests with the interpreter created by
`uv sync`: wrapping them in `uv run` activates Ray’s automatic runtime-environment
repackaging, which is unnecessary for this exclusively local runtime.

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

File transfers use OpenSSH `rsync` multiplexed over one master connection per
host, user and port (`ControlMaster=auto`, socket `~/.ssh/rd-%C`, closed after
60 s idle): a job's inputs and outputs open one SSH connection instead of one
each, so networks that rate-limit SSH connections do not block the host.

`Project(post_setup=(...))` runs an argv once per host as the last provisioning
step, in the synced source with the project environment on `PATH`, as jobs see
it: warm the first imports so no measured job pays a cold start, or smoke-test
the environment. A non-zero exit marks the host unhealthy.


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
wheel in a clean environment outside the checkout. The release workflow reuses
these checks before publishing a version bump. See the
[repository](https://github.com/Nanofaas/ray-dispatcher).
