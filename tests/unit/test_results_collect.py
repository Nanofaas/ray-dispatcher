import pytest

from ray_dispatcher.errors import PathValidationError
from ray_dispatcher.models import OutputSpec
from ray_dispatcher.results import collect_outputs
from ray_dispatcher.ssh import FakeTransport, TransportError


def _pulls(t):
    return [c[1] for c in t.calls if c[0] == "pull"]


def test_collect_issues_pull_per_output_with_contained_dest(tmp_path):
    staging = tmp_path / "staging"
    staging.mkdir()
    t = FakeTransport()  # pull is a no-op (records the call); files do not appear
    outputs = (
        OutputSpec(source="solutions/a.json", required=True),
        OutputSpec(source="logs/run.log", destination="run.log", required=False),
    )
    collect_outputs(t, "/home/u/.ray_dispatcher/runs/b/j/1", outputs, staging)
    pulls = _pulls(t)
    # remote source is run-dir-relative; local dest is contained under staging.
    # ensure_within returns a resolved path, so compare against staging.resolve()
    # (pytest's tmp_path can sit under a macOS /var -> /private/var symlink).
    root = staging.resolve()
    assert pulls[0][0] == "/home/u/.ray_dispatcher/runs/b/j/1/solutions/a.json"
    assert pulls[0][1] == str(root / "solutions" / "a.json")
    assert pulls[1][0] == "/home/u/.ray_dispatcher/runs/b/j/1/logs/run.log"
    assert pulls[1][1] == str(root / "run.log")  # destination override


def test_collect_classifies_missing_required_and_optional(tmp_path):
    staging = tmp_path / "staging"
    staging.mkdir()
    # FakeTransport.pull does not create files; simulate a successful pull for
    # 'present.txt' by pre-creating its local destination.
    (staging / "present.txt").write_text("ok")
    t = FakeTransport()
    outputs = (
        OutputSpec(source="present.txt", required=True),
        OutputSpec(source="missing_req.txt", required=True),
        OutputSpec(source="missing_opt.txt", required=False),
    )
    res = collect_outputs(t, "/runs/b/j/1", outputs, staging)
    assert res.present == ("present.txt",)
    assert res.missing_required == ("missing_req.txt",)
    assert res.missing_optional == ("missing_opt.txt",)


def test_collect_swallows_rsync_file_not_found(tmp_path):
    """TransportError with returncode 23 and file genuinely absent remotely → classified as missing."""
    from ray_dispatcher.ssh import CommandResult
    staging = tmp_path / "staging"
    staging.mkdir()

    class _PullRaises(FakeTransport):
        def pull(self, remote: str, local: str, *, delete: bool = False,
                 excludes: object = ()) -> None:
            raise TransportError("rsync failed (23): no such file", returncode=23)

    outputs = (OutputSpec(source="missing.txt", required=True),)
    # `test -e` returns 1: the probe confirms the file is genuinely absent remotely
    t = _PullRaises(run_results=lambda argv: CommandResult(1, "", "", 0.0))
    res = collect_outputs(t, "/runs/b/j/1", outputs, staging)
    assert res.missing_required == ("missing.txt",)


def test_collect_reraises_ssh_transport_error(tmp_path):
    """TransportError with returncode != 23/24 (e.g. SSH failure) is re-raised."""
    staging = tmp_path / "staging"
    staging.mkdir()

    class _PullRaises(FakeTransport):
        def pull(self, remote: str, local: str, *, delete: bool = False,
                 excludes: object = ()) -> None:
            raise TransportError("rsync failed (255): ssh refused", returncode=255)

    outputs = (OutputSpec(source="out.txt", required=True),)
    with pytest.raises(TransportError):
        collect_outputs(_PullRaises(), "/runs/b/j/1", outputs, staging)


class _FailingPullTransport(FakeTransport):
    """pull always fails with rsync's partial-transfer code; run is programmable."""

    def __init__(self, run_results=None):
        super().__init__(run_results)
        self.pulls: list[str] = []

    def pull(self, remote, local, *, delete=False, excludes=()):
        self.pulls.append(remote)
        raise TransportError("rsync failed (23)", returncode=23)


def test_pull_failure_with_file_present_remotely_reraises(tmp_path):
    from ray_dispatcher.ssh import CommandResult
    staging = tmp_path / "staging"
    staging.mkdir()
    # `test -e` returns 0: the file exists remotely -> this is a transfer failure
    t = _FailingPullTransport(run_results=lambda argv: CommandResult(0, "", "", 0.0))
    with pytest.raises(TransportError):
        collect_outputs(t, "/runs/b/j/1", (OutputSpec(source="out.txt"),), staging)


def test_pull_failure_with_file_absent_remotely_is_missing(tmp_path):
    from ray_dispatcher.ssh import CommandResult
    staging = tmp_path / "staging"
    staging.mkdir()
    # `test -e` returns 1: genuinely absent -> classified missing, no raise
    t = _FailingPullTransport(run_results=lambda argv: CommandResult(1, "", "", 0.0))
    res = collect_outputs(t, "/runs/b/j/1", (OutputSpec(source="out.txt"),), staging)
    assert res.missing_required == ("out.txt",)


def test_collect_rejects_destination_symlinked_outside_staging(tmp_path):
    # OutputSpec.__post_init__ already rejects lexical escapes ('..', absolute),
    # so this exercises collect_outputs's OWN containment: a lexically-clean
    # destination that resolves through a symlink OUT of staging (§4.3). Only
    # ensure_within's filesystem-resolving check catches this.
    staging = tmp_path / "staging"
    staging.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (staging / "evil").symlink_to(outside)  # symlink inside staging -> outside it
    t = FakeTransport()
    bad = (OutputSpec(source="ok.txt", destination="evil/escape.txt", required=True),)
    with pytest.raises(PathValidationError):
        collect_outputs(t, "/runs/b/j/1", bad, staging)
