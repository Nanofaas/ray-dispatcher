import os
import subprocess
from pathlib import Path

import pytest

from it.unimib.data.nanofaas.ray_dispatcher.models import Project, RemoteHost
from it.unimib.data.nanofaas.ray_dispatcher.provisioning import HostProvisioner, RemoteLayout
from it.unimib.data.nanofaas.ray_dispatcher.ssh import CommandResult, FakeTransport


@pytest.mark.parametrize("kind", ["source", "env"])
@pytest.mark.parametrize("failed_move", ["first", "second"])
def test_publish_failure_preserves_previous_state(tmp_path, kind, failed_move):
    project = tmp_path / "project"
    project.mkdir()
    (project / "pyproject.toml").write_text('[project]\nname="test"\n')
    (project / "uv.lock").write_text("")
    transport = FakeTransport(
        lambda argv: CommandResult(
            1 if argv[0] == "sh" and "test -f" in argv[2] else 0,
            "Linux x86_64" if argv[0] == "uname" else "",
            "",
            0,
        )
    )
    prov = HostProvisioner(
        transport,
        Project(str(project), "test", "3.10.0", "0.11.25"),
        RemoteHost("host", "user"),
        runner_path="x",
        session_id="s",
    )
    prov.layout = RemoteLayout(str(tmp_path / "remote"), "test")
    if kind == "source":
        prov._sync_source()
        target = Path(prov.layout.source)
    else:
        digest = prov._publish_env("uv")
        target = Path(prov.layout.env_dir(digest))
    script = next(
        call[1][2]
        for call in transport.calls
        if call[0] == "run" and call[1][0] == "sh" and "mv " in call[1][2] and ".old" in call[1][2]
    )
    target.mkdir(parents=True)
    (target / "sentinel").write_text("previous")
    staging = Path(str(target) + ".staging")
    staging.mkdir()
    (staging / "sentinel").write_text("new")
    tools = tmp_path / "tools"
    tools.mkdir()
    pattern = "*.staging" if failed_move == "second" else str(target)
    wrapper = tools / "mv"
    wrapper.write_text(f'#!/bin/sh\ncase "$1" in {pattern}) exit 17;; esac\nexec /bin/mv "$@"\n')
    wrapper.chmod(0o755)
    completed = subprocess.run(
        ["sh", "-c", script],
        capture_output=True,
        env={**os.environ, "PATH": f"{tools}:" + os.environ["PATH"]},
    )
    assert completed.returncode != 0
    copies = [target / "sentinel", Path(str(target) + ".old") / "sentinel"]
    assert any(p.exists() and p.read_text() == "previous" for p in copies)
