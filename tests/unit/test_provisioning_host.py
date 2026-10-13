from it.unimib.data.nanofaas.ray_dispatcher.models import Project, RemoteHost
from it.unimib.data.nanofaas.ray_dispatcher.provisioning import HostProvisioner
from it.unimib.data.nanofaas.ray_dispatcher.ssh import CommandResult, FakeTransport


def _ok_results(tmp_path):
    (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\n")
    (tmp_path / "uv.lock").write_text("# lock\n")
    (tmp_path / "run.py").write_text("print('x')")

    def results(argv):
        joined = " ".join(argv)
        if "$HOME" in joined:
            return CommandResult(0, "/home/ubuntu", "", 0.0)
        if argv[:2] == ["command", "-v"]:
            return CommandResult(0, "/usr/bin/x", "", 0.0)
        if "df -Pk" in joined:
            return CommandResult(0, "/dev/sda1 100 1 999999999 1% /\n", "", 0.0)
        if argv[-1] == "--version":
            return CommandResult(0, "uv 0.11.25", "", 0.0)
        if argv[1:3] == ["python", "find"]:
            return CommandResult(0, "/interp", "", 0.0)
        if argv[0] == "/interp":
            return CommandResult(0, "3.10.18", "", 0.0)
        if argv[0] == "uname":
            return CommandResult(0, "Linux x86_64", "", 0.0)
        if "stat" in joined:
            return CommandResult(0, "ubuntu", "", 0.0)
        if "import sys" in joined:
            return CommandResult(0, "", "", 0.0)
        return CommandResult(0, "", "", 0.0)

    return results


def _prov(tmp_path, results, runner_path):
    return HostProvisioner(
        FakeTransport(run_results=results),
        Project(path=str(tmp_path), project_id="dfaas", python="3.10.18", uv_version="0.11.25"),
        RemoteHost(host="10.0.0.1", user="ubuntu"),
        runner_path=runner_path,
        session_id="sess-1",
        heartbeat_interval_s=1000.0,
    )


def test_provision_success_returns_live_session(tmp_path):
    runner = tmp_path / "remote_runner.py"
    runner.write_text("print('r')")
    p = _prov(tmp_path, _ok_results(tmp_path), str(runner))
    result, session = p.provision()
    assert result.succeeded is True
    assert result.host == "10.0.0.1"
    assert len(result.source_digest) == 64 and len(result.environment_digest) == 64
    assert session is not None
    lock, hb = session
    hb.stop()  # test cleanup
    assert lock.session_id == "sess-1"


def test_provision_step_failure_releases_lock_and_marks_failed(tmp_path):
    runner = tmp_path / "remote_runner.py"
    runner.write_text("print('r')")

    def failing(argv):
        if argv[:2] == ["command", "-v"]:
            return CommandResult(1, "", "missing", 0.0)  # preflight fails
        if argv[:2] == ["python3", "-c"] and argv[-3] == "release":
            return CommandResult(17, "", "release broken", 0.0)
        if "$HOME" in " ".join(argv):
            return CommandResult(0, "/home/ubuntu", "", 0.0)
        return CommandResult(0, "", "", 0.0)

    p = _prov(tmp_path, failing, str(runner))
    result, session = p.provision()
    assert result.succeeded is False
    assert session is None
    assert "missing" in result.error or "python3" in result.error
    assert "release broken" in result.error
    # the lock was released after the failure
    assert any(
        c[0] == "run" and c[1][:2] == ("python3", "-c") and c[1][-3] == "release" for c in p.t.calls
    )


def test_provision_host_in_use_returns_failed_no_session(tmp_path):
    runner = tmp_path / "remote_runner.py"
    runner.write_text("print('r')")

    def held(argv):
        if argv[:2] == ["python3", "-c"] and argv[-3] == "acquire":
            return CommandResult(3, "other\n", "", 0.0)
        return CommandResult(0, "", "", 0.0)

    p = HostProvisioner(
        FakeTransport(run_results=held),
        Project(path=str(tmp_path), project_id="dfaas", python="3.10.18", uv_version="0.11.25"),
        RemoteHost(host="10.0.0.1", user="ubuntu"),
        runner_path=str(runner),
        session_id="sess-1",
    )
    result, session = p.provision()
    assert result.succeeded is False and session is None
    assert "other" in result.error


def _prov_with(tmp_path, results, runner_path, post_setup):
    return HostProvisioner(
        FakeTransport(run_results=results),
        Project(
            path=str(tmp_path),
            project_id="dfaas",
            python="3.10.18",
            uv_version="0.11.25",
            post_setup=post_setup,
        ),
        RemoteHost(host="10.0.0.1", user="ubuntu"),
        runner_path=runner_path,
        session_id="sess-1",
        heartbeat_interval_s=1000.0,
    )


def _hook_calls(p):
    return [c for c in p.t.calls if c[0] == "run" and "exec python -c" in " ".join(c[1])]


def test_post_setup_runs_last_in_the_project_environment(tmp_path):
    runner = tmp_path / "remote_runner.py"
    runner.write_text("print('r')")
    p = _prov_with(tmp_path, _ok_results(tmp_path), str(runner), ("python", "-c", "import methods"))
    result, session = p.provision()
    session[1].stop()
    assert result.succeeded is True
    [hook] = _hook_calls(p)
    script = hook[1][-1]
    venv = p._lo.env_venv(result.environment_digest)
    assert script.startswith(f"cd {p._lo.source} && ")
    assert f"PATH={venv}/bin:" in script and f"VIRTUAL_ENV={venv}" in script
    assert script.endswith("exec python -c 'import methods'")
    assert p.t.calls.index(hook) > max(i for i, c in enumerate(p.t.calls) if c[0] == "push")


def test_post_setup_failure_marks_the_host_unhealthy(tmp_path):
    runner = tmp_path / "remote_runner.py"
    runner.write_text("print('r')")
    ok = _ok_results(tmp_path)

    def failing(argv):
        if "exec python -c" in " ".join(argv):
            return CommandResult(1, "", "ModuleNotFoundError: methods", 0.0)
        return ok(argv)

    p = _prov_with(tmp_path, failing, str(runner), ("python", "-c", "import methods"))
    result, session = p.provision()
    assert result.succeeded is False and session is None
    assert "post-setup command failed" in result.error and "ModuleNotFoundError" in result.error


def test_no_post_setup_runs_no_extra_command(tmp_path):
    runner = tmp_path / "remote_runner.py"
    runner.write_text("print('r')")
    p = _prov(tmp_path, _ok_results(tmp_path), str(runner))
    result, session = p.provision()
    session[1].stop()
    assert result.succeeded is True
    assert not any(c[0] == "run" and "exec " in " ".join(c[1]) for c in p.t.calls)


def test_post_setup_parts_must_be_strings_without_nul(tmp_path):
    import pytest

    from it.unimib.data.nanofaas.ray_dispatcher.errors import ModelValidationError

    for bad in (("python", 3), ("python", "a\x00b")):
        with pytest.raises(ModelValidationError, match="post_setup"):
            Project(
                path=str(tmp_path),
                project_id="x",
                python="3.10.18",
                uv_version="0.11.25",
                post_setup=bad,
            )
