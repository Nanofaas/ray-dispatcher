import runpy
import subprocess
from pathlib import Path

import pytest

from it.unimib.data.nanofaas.ray_dispatcher import __version__

SCRIPT = Path(__file__).parents[2] / ".github/scripts/release_version.py"


@pytest.mark.parametrize(
    ("previous", "current", "expected"),
    [
        ("0.1.1", "0.1.1", False),
        ("0.1.1", "0.1.2", True),
        ("0.1.9", "0.1.10", True),
        ("0.1.1", "0.2.0rc1", True),
    ],
)
def test_release_only_on_version_increase(previous, current, expected):
    should_publish = runpy.run_path(str(SCRIPT))["should_publish"]
    assert should_publish(previous, current, current) is expected


@pytest.mark.parametrize(
    ("previous", "current", "package_version"),
    [("0.2.0", "0.1.1", "0.1.1"), ("0.1.1", "0.1.2", "0.1.1")],
)
def test_rejects_decrease_or_mismatched_package_version(previous, current, package_version):
    should_publish = runpy.run_path(str(SCRIPT))["should_publish"]
    with pytest.raises(ValueError):
        should_publish(previous, current, package_version)


def test_push_compares_entire_push_and_writes_release_outputs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    def git(*args):
        return subprocess.run(
            ["git", "-c", "user.name=Test", "-c", "user.email=test@example.org", *args],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    git("init")
    project = tmp_path / "pyproject.toml"
    project.write_text('[project]\nversion = "0.0.0"\n')
    git("add", ".")
    git("commit", "-m", "previous version")
    before = git("rev-parse", "HEAD")
    project.write_text(f'[project]\nversion = "{__version__}"\n')
    git("add", ".")
    git("commit", "-m", "bump version")
    (tmp_path / "unrelated").touch()
    git("add", ".")
    git("commit", "-m", "another commit in the same push")
    output = tmp_path / "output"
    monkeypatch.setenv("GITHUB_EVENT_NAME", "push")
    monkeypatch.setenv("BEFORE_SHA", before)
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    runpy.run_path(str(SCRIPT))["main"]()
    assert output.read_text() == f"publish=true\nversion={__version__}\n"


def test_manual_run_never_publishes(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "pyproject.toml").write_text(f'[project]\nversion = "{__version__}"\n')
    output = tmp_path / "output"
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    runpy.run_path(str(SCRIPT))["main"]()
    assert output.read_text() == f"publish=false\nversion={__version__}\n"


@pytest.mark.parametrize("before", ["0" * 40, "main", "--help", "a" * 39])
def test_push_rejects_invalid_previous_commit(tmp_path, monkeypatch, before):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "pyproject.toml").write_text('[project]\nversion = "0.1.1"\n')
    monkeypatch.setenv("GITHUB_EVENT_NAME", "push")
    monkeypatch.setenv("BEFORE_SHA", before)
    with pytest.raises(ValueError, match="previous commit SHA"):
        runpy.run_path(str(SCRIPT))["main"]()
