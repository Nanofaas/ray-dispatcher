import io
import json
import subprocess
import sys
from pathlib import Path

from it.unimib.data.nanofaas.ray_dispatcher.remote_runner import _tee


class _BrokenForward:
    def write(self, b):
        raise OSError("broken pipe")

    def flush(self):
        pass


def test_tee_keeps_draining_to_file_when_forward_breaks(tmp_path):
    src = io.BytesIO(b"abc" * 10000)  # larger than one 4096 chunk
    out = tmp_path / "raw.log"
    _tee(src, str(out), _BrokenForward())  # must NOT raise
    assert out.read_bytes() == b"abc" * 10000


RUNNER = str(Path("src/it/unimib/data/nanofaas/ray_dispatcher/remote_runner.py").resolve())


def _manifest(tmp_path, argv):
    m = {
        "argv": argv,
        "cwd": str(tmp_path),
        "env": {},
        "secret_env": {},
        "venv_bin": str(tmp_path / "v" / "bin"),
        "virtual_env": str(tmp_path / "v"),
        "stdout_path": str(tmp_path / "out.log"),
        "stderr_path": str(tmp_path / "err.log"),
        "pid_path": str(tmp_path / "pid.json"),
        "result_path": str(tmp_path / "result.json"),
    }
    p = tmp_path / "m.json"
    p.write_text(json.dumps(m))
    return m, str(p)


def test_child_output_is_forwarded_live_to_runner_streams(tmp_path):
    m, path = _manifest(
        tmp_path,
        [
            sys.executable,
            "-c",
            "import sys; print('forward-me'); print('to-stderr', file=sys.stderr)",
        ],
    )
    proc = subprocess.run([sys.executable, RUNNER, path], capture_output=True, text=True)
    assert "forward-me" in proc.stdout
    assert "to-stderr" in proc.stderr


def test_binary_output_preserved_in_file_and_forwarded_raw(tmp_path):
    m, path = _manifest(
        tmp_path, [sys.executable, "-c", "import sys; sys.stdout.buffer.write(b'\\xff\\xfe')"]
    )
    proc = subprocess.run([sys.executable, RUNNER, path], capture_output=True)
    # raw bytes preserved on the VM-side file AND forwarded byte-for-byte
    assert Path(m["stdout_path"]).read_bytes() == b"\xff\xfe"
    assert b"\xff\xfe" in proc.stdout


def test_short_output_is_written_before_child_exit(tmp_path):
    import time

    marker = tmp_path / "release"
    code = (
        "import time,pathlib; print('live',flush=True); "
        f"p=pathlib.Path({str(marker)!r});\nwhile not p.exists(): time.sleep(.01)"
    )
    m, path = _manifest(tmp_path, [sys.executable, "-c", code])
    child = subprocess.Popen(
        [sys.executable, RUNNER, path], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    try:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            log = Path(m["stdout_path"])
            if log.exists() and log.read_bytes() == b"live\n":
                break
            time.sleep(0.01)
        assert log.read_bytes() == b"live\n"
    finally:
        marker.touch()
        child.communicate(timeout=5)


def test_disabled_forwarding_preserves_large_binary_log(tmp_path):
    m, path = _manifest(
        tmp_path, [sys.executable, "-c", "import sys;sys.stdout.buffer.write(b'\\xff'*100000)"]
    )
    m["forward_output"] = False
    Path(path).write_text(json.dumps(m))
    child = subprocess.run([sys.executable, RUNNER, path], capture_output=True, timeout=5)
    assert child.returncode == 0
    assert child.stdout == b""
    assert Path(m["stdout_path"]).read_bytes() == b"\xff" * 100000


def test_log_write_failure_is_not_reported_as_success(tmp_path):
    m, path = _manifest(tmp_path, [sys.executable, "-c", "print('cannot log')"])
    m["stdout_path"] = str(tmp_path)
    Path(path).write_text(json.dumps(m))
    child = subprocess.run([sys.executable, RUNNER, path], capture_output=True, timeout=5)
    assert child.returncode != 0
    assert not Path(m["result_path"]).exists()
