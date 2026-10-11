from ray_dispatcher.scheduling import LeasePool


def _pool(**kw):
    return LeasePool(
        {"h1": 2, "h2": 1}, token_factory=iter(f"tok{i}" for i in range(100)).__next__, **kw
    )


def test_quarantine_captures_registered_pid_paths():
    pool = _pool()
    a = pool.acquire("job-a")
    b = pool.acquire("job-b")
    assert a is not None and b is not None and a.host == b.host == "h1"
    pool.register_pid(a.token, "/runs/b/ja/1/pid.json")
    pool.register_pid(b.token, "/runs/b/jb/1/pid.json")
    pool.quarantine("h1")
    orphans = pool.orphan_paths("h1")
    assert sorted(orphans) == ["/runs/b/ja/1/pid.json", "/runs/b/jb/1/pid.json"]
    assert pool.orphan_paths("h1") == orphans  # failed reconciliation must be retryable
    pool.mark_reconciled("h1")
    assert pool.orphan_paths("h1") == []


def test_release_clears_pid_path_no_orphan():
    pool = _pool()
    lease = pool.acquire("job-a")
    pool.register_pid(lease.token, "/runs/b/ja/1/pid.json")
    pool.release(lease.token)
    pool.quarantine(lease.host)
    assert pool.orphan_paths(lease.host) == []  # released cleanly -> nothing orphaned


def test_register_pid_unknown_token_is_noop():
    pool = _pool()
    pool.register_pid("ghost", "/runs/x/pid.json")  # must not raise
    pool.quarantine("h1")
    assert pool.orphan_paths("h1") == []
