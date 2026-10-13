from it.unimib.data.nanofaas.ray_dispatcher.ssh import SshConfig, build_rsync_argv


def _cfg():
    return SshConfig(
        host="h", user="u", port=2222, identity_file="/keys/id", known_hosts_file="/keys/kh"
    )


def test_argv_has_archive_and_ssh_options():
    argv = build_rsync_argv(_cfg(), "/local/", "u@h:/remote/", delete=False, excludes=())
    assert argv[0] == "rsync"
    assert "-a" in argv
    # the -e value is one argument holding the ssh invocation
    e_index = argv.index("-e")
    ssh_opt = argv[e_index + 1]
    assert ssh_opt.startswith("ssh ")
    assert "-p 2222" in ssh_opt
    assert "-i /keys/id" in ssh_opt
    assert "UserKnownHostsFile=/keys/kh" in ssh_opt
    assert "StrictHostKeyChecking=yes" in ssh_opt
    assert "GlobalKnownHostsFile=/dev/null" in ssh_opt
    assert "BatchMode=yes" in ssh_opt
    assert argv[-2:] == ["/local/", "u@h:/remote/"]


def test_argv_delete_and_excludes():
    argv = build_rsync_argv(_cfg(), "/a", "/b", delete=True, excludes=(".git/", "solutions/"))
    assert "--delete" in argv
    assert argv.count("--exclude") == 2
    i = argv.index("--exclude")
    assert argv[i + 1] == ".git/"


def test_argv_no_identity_omits_i():
    cfg = SshConfig(host="h", user="u", port=22, identity_file=None, known_hosts_file="/kh")
    joined = " ".join(build_rsync_argv(cfg, "/a", "/b", delete=False, excludes=()))
    assert "-i " not in joined


def test_rsync_argv_includes_chmod_when_given():
    cfg = SshConfig(host="h", user="u", port=22, identity_file=None, known_hosts_file="/kh")
    argv = build_rsync_argv(cfg, "/src", "u@h:/dst", delete=False, excludes=(), chmod="F600")
    assert "--chmod=F600" in argv


def test_rsync_argv_omits_chmod_by_default():
    cfg = SshConfig(host="h", user="u", port=22, identity_file=None, known_hosts_file="/kh")
    argv = build_rsync_argv(cfg, "/src", "u@h:/dst", delete=False, excludes=())
    assert not any(a.startswith("--chmod") for a in argv)


def test_argv_multiplexes_over_one_master_per_host():
    # Networks that rate-limit SSH connections block a job's burst of rsync
    # calls; one persistent master per (host, user, port) absorbs it.
    argv = build_rsync_argv(_cfg(), "/a", "u@h:/b", delete=False, excludes=())
    ssh_opt = argv[argv.index("-e") + 1]
    assert "ControlMaster=auto" in ssh_opt
    assert "ControlPersist=60s" in ssh_opt
    path = next(p for p in ssh_opt.split() if p.startswith("ControlPath="))
    assert path.endswith("/.ssh/rd-%C")
    assert len(path.removeprefix("ControlPath=").replace("%C", "x" * 40)) < 104  # socket path limit


def test_rsync_creates_the_control_directory(tmp_path, monkeypatch):
    import subprocess

    from it.unimib.data.nanofaas.ray_dispatcher.ssh import SshTransport

    monkeypatch.setenv("HOME", str(tmp_path))
    ran = []
    monkeypatch.setattr(subprocess, "run", lambda argv, **kw: ran.append(argv))
    SshTransport(_cfg()).push("/local", "/remote")
    control = tmp_path / ".ssh"
    assert ran and control.is_dir() and (control.stat().st_mode & 0o777) == 0o700
