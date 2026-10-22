"""Per-host provisioning (spec §6.3) and inventory orchestration.

Drives each host through the §6.3 steps over the Phase 2 Transport seam. The
remote $HOME is resolved once per host (one `printf %s "$HOME"` probe), after
which every remote path is absolute — so both `run` (via shlex.join) and rsync
`push` (which uses --protect-args and would NOT expand `~`) are unambiguous.
All interpolated data is shlex-quoted; no user string is ever shelled (§7).
"""

from __future__ import annotations

import json
import secrets as _secrets
import shlex
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

from .digests import environment_digest, runner_digest, source_digest
from .errors import (
    ConfigurationError,
    DispatcherError,
    HostInUseError,
    NoHealthyHostsError,
    ProvisioningError,
    SessionLockLostError,
)
from .locking import HeartbeatThread, SessionLock
from .models import HostProvisioningResult, Inventory, Project, ProvisioningReport, RemoteHost
from .ssh import CommandResult, SshConfig, SshTransport, Transport, write_remote_file

# Base sync flags shared by environment_digest and the uv sync invocation (§6.3.6).
SYNC_FLAGS = (
    "--locked",
    "--no-install-project",
    "--no-install-workspace",
    "--no-default-groups",
)


class _StepError(Exception):
    """A provisioning step failed on one host. Caught by the host driver and
    turned into a failed HostProvisioningResult; never escapes provisioning.py."""


@dataclass(frozen=True)
class RunPaths:
    """Absolute remote paths inside one attempt's run dir (spec §6.1, §7).

    Control files (manifest/logs/pid/result) live in ``base``; the job runs in
    ``run_root`` (a copy of the provisioned source) so they never pollute outputs.
    """

    base: str

    @property
    def run_root(self) -> str:
        return f"{self.base}/run"

    @property
    def venv(self) -> str:
        return f"{self.run_root}/.venv"

    @property
    def manifest(self) -> str:
        return f"{self.base}/manifest.json"

    @property
    def stdout(self) -> str:
        return f"{self.base}/stdout.log"

    @property
    def stderr(self) -> str:
        return f"{self.base}/stderr.log"

    @property
    def pid(self) -> str:
        return f"{self.base}/pid.json"

    @property
    def result(self) -> str:
        return f"{self.base}/result.json"


class RemoteLayout:
    """Absolute remote paths under <home>/.ray_dispatcher (spec §6.1)."""

    def __init__(self, home: str, project_id: str) -> None:
        home = home.rstrip("/")
        self.root = f"{home}/.ray_dispatcher"
        self.project = f"{self.root}/projects/{project_id}"
        self.source = f"{self.project}/source"
        self.source_manifest = f"{self.project}/source-manifest.json"
        self.secrets = f"{self.root}/secrets/{project_id}"
        self.runs = f"{self.root}/runs/{project_id}"
        self.uv_root = f"{self.root}/uv"

    def env_dir(self, environment_digest: str) -> str:
        return f"{self.project}/envs/{environment_digest}"

    def env_venv(self, environment_digest: str) -> str:
        return f"{self.env_dir(environment_digest)}/.venv"

    def env_manifest(self, environment_digest: str) -> str:
        return f"{self.env_dir(environment_digest)}/environment-manifest.json"

    def runner_dir(self, runner_digest: str) -> str:
        return f"{self.root}/bin/{runner_digest}"

    def runner(self, runner_digest: str) -> str:
        return f"{self.runner_dir(runner_digest)}/remote_runner.py"

    def uv_bin(self, uv_version: str) -> str:
        return f"{self.uv_root}/{uv_version}/uv"

    def run_dir(self, batch_id: str, job_id: str, attempt: int) -> str:
        return f"{self.runs}/{batch_id}/{job_id}/{attempt}"

    def run_paths(self, batch_id: str, job_id: str, attempt: int) -> RunPaths:
        return RunPaths(self.run_dir(batch_id, job_id, attempt))


class HostProvisioner:
    """Provisions one host. `layout` is None until the driver resolves $HOME."""

    def __init__(
        self,
        transport: Transport,
        project: Project,
        host: RemoteHost,
        *,
        runner_path: str,
        session_id: str,
        force: bool = False,
        min_disk_mb: int = 500,
        heartbeat_interval_s: float = 20.0,
    ) -> None:
        self.t = transport
        self.project = project
        self.host = host
        self.runner_path = runner_path
        self.session_id = session_id
        self.force = force
        self.min_disk_mb = min_disk_mb
        self.heartbeat_interval_s = heartbeat_interval_s
        self.layout: RemoteLayout | None = None

    # --- helpers -------------------------------------------------------------

    def _checked(self, argv: list[str], what: str) -> CommandResult:
        r = self.t.run(argv)
        if r.returncode != 0:
            detail = (r.stderr or r.stdout).strip()
            raise _StepError(f"{what} failed on {self.host.host} (rc={r.returncode}): {detail}")
        return r

    def _write_remote_file(self, path: str, content: str, *, mode: int | None = None) -> None:
        # TransportError is an Exception: the provision() driver already turns any
        # step exception into a failed HostProvisioningResult, same as _StepError.
        write_remote_file(self.t, path, content, mode=mode)

    def _resolve_layout(self) -> RemoteLayout:
        home = self._checked(["sh", "-c", 'printf %s "$HOME"'], "resolve $HOME").stdout.strip()
        if not home:
            raise _StepError(f"could not resolve remote $HOME on {self.host.host}")
        return RemoteLayout(home, self.project.project_id)

    @property
    def _lo(self) -> RemoteLayout:
        if self.layout is None:  # pragma: no cover - guarded by driver ordering
            raise _StepError("layout not resolved")
        return self.layout

    def _preflight(self) -> None:
        for tool in ("python3", "rsync"):
            if self.t.run(["command", "-v", tool]).returncode != 0:
                raise _StepError(f"required tool {tool!r} missing on {self.host.host}")
        root = shlex.quote(self._lo.root)
        # df -Pk gives POSIX one-line-per-fs output; available 1024-blocks is column 4.
        r = self._checked(["sh", "-c", f"mkdir -p {root} && df -Pk {root} | tail -1"], "disk check")
        try:
            avail_kb = int(r.stdout.split()[3])
        except (IndexError, ValueError) as exc:
            msg = f"could not parse disk free on {self.host.host}: {r.stdout!r}"
            raise _StepError(msg) from exc
        if avail_kb < self.min_disk_mb * 1024:
            raise _StepError(
                f"insufficient disk on {self.host.host}: "
                f"{avail_kb // 1024} MB < {self.min_disk_mb} MB required"
            )

    def _install_uv(self) -> str:
        ver = self.project.uv_version
        uv = self._lo.uv_bin(ver)
        if not self.force and ver in self.t.run([uv, "--version"]).stdout.split():
            return uv
        install_dir = f"{self._lo.uv_root}/{ver}"
        # ponytail: official version-pinned installer; exact on-disk layout
        #           (UV_INSTALL_DIR -> <dir>/uv) is reconfirmed by the Phase 7 e2e.
        # ver is safe in the URL: Project validates uv_version as r"\d+\.\d+\.\d+"
        # (fullmatch) -> only digits and dots, no shell metacharacters (§7).
        script = (
            f"set -e; mkdir -p {shlex.quote(install_dir)}; "
            f"curl -LsSf https://astral.sh/uv/{ver}/install.sh "
            f"| env UV_INSTALL_DIR={shlex.quote(install_dir)} INSTALLER_NO_MODIFY_PATH=1 sh"
        )
        self._checked(["sh", "-c", script], "uv install")
        reported = self._checked([uv, "--version"], "uv version check").stdout
        if ver not in reported.split():
            raise _StepError(
                f"installed uv on {self.host.host} reports {reported.strip()!r}, expected {ver}"
            )
        return uv

    def _install_python(self, uv: str) -> None:
        want = self.project.python
        self._checked([uv, "python", "install", want], "uv python install")
        interp = self._checked([uv, "python", "find", want], "uv python find").stdout.strip()
        if not interp:
            raise _StepError(f"uv could not locate Python {want} on {self.host.host}")
        got = self._checked(
            [interp, "-c", "import sys;print('.'.join(map(str,sys.version_info[:3])))"],
            "python version check",
        ).stdout.strip()
        if got != want:
            raise _StepError(f"interpreter on {self.host.host} is {got!r}, expected {want!r}")

    def _sync_source(self) -> str:
        staging = f"{self._lo.source}.staging"
        self._checked(["sh", "-c", f"mkdir -p {shlex.quote(staging)}"], "source staging mkdir")
        # trailing slashes: copy the *contents* of the local tree into staging.
        self.t.push(
            self.project.path.rstrip("/") + "/",
            staging + "/",
            delete=True,
            excludes=self.project.exclude,
        )
        src = shlex.quote(self._lo.source)
        stg = shlex.quote(staging)
        old = shlex.quote(self._lo.source + ".old")
        # The `mv staging source` rename is atomic within one parent dir; the brief
        # window where source is absent is acceptable pre-runtime (no readers yet).
        self._checked(
            [
                "sh",
                "-c",
                f"set -e; if [ ! -e {src} ] && [ -e {old} ]; then mv {old} {src}; fi; "
                f"rm -rf {old}; if [ -e {src} ]; then mv {src} {old}; fi; "
                f"mv {stg} {src}; rm -rf {old}",
            ],
            "source atomic replace",
        )
        digest = source_digest(self.project.path, self.project.exclude)
        manifest = json.dumps({"source_digest": digest, "project_id": self.project.project_id})
        self._write_remote_file(self._lo.source_manifest, manifest)
        return digest

    def _publish_env(self, uv: str) -> str:
        platform = self._checked(["uname", "-sm"], "platform probe").stdout.strip()
        digest = environment_digest(self.project, platform=platform, sync_flags=SYNC_FLAGS)
        env_dir = self._lo.env_dir(digest)
        venv = self._lo.env_venv(digest)
        manifest_path = self._lo.env_manifest(digest)
        valid = (
            self.t.run(
                [
                    "sh",
                    "-c",
                    f"test -f {shlex.quote(manifest_path)} && "
                    f"test -x {shlex.quote(venv)}/bin/python",
                ]
            ).returncode
            == 0
        )
        if valid and not self.force:
            return digest

        staging = f"{env_dir}.staging"
        staging_venv = f"{staging}/.venv"
        self._checked(
            ["sh", "-c", f"rm -rf {shlex.quote(staging)}; mkdir -p {shlex.quote(staging)}"],
            "env staging mkdir",
        )
        sync = [
            uv,
            "sync",
            "--project",
            self._lo.source,
            *SYNC_FLAGS,
            "--python",
            self.project.python,
        ]
        for group in self.project.dependency_groups:
            sync += ["--group", group]
        self._checked(
            ["sh", "-c", f"UV_PROJECT_ENVIRONMENT={shlex.quote(staging_venv)} {shlex.join(sync)}"],
            "uv sync",
        )
        # ponytail: venv relocatability after the atomic move is reconfirmed by the
        #           Phase 7 e2e; bin/python is a symlink to the uv interpreter and
        #           survives a move, console-script shebangs would not (jobs use python).
        self._checked(
            ["sh", "-c", f"{shlex.quote(staging_venv)}/bin/python -c 'import sys'"],
            "venv smoke check",
        )
        manifest = json.dumps(
            {
                "environment_digest": digest,
                "python": self.project.python,
                "uv_version": self.project.uv_version,
                "platform": platform,
                "dependency_groups": list(self.project.dependency_groups),
                "sync_flags": list(SYNC_FLAGS),
            }
        )
        self._write_remote_file(f"{staging}/environment-manifest.json", manifest)
        qenv, qstg = shlex.quote(env_dir), shlex.quote(staging)
        qold = shlex.quote(env_dir + ".old")
        self._checked(
            [
                "sh",
                "-c",
                f"set -e; mkdir -p {shlex.quote(self._lo.project)}/envs; "
                f"if [ ! -e {qenv} ] && [ -e {qold} ]; then mv {qold} {qenv}; fi; "
                f"rm -rf {qold}; "
                f"if [ -e {qenv} ]; then mv {qenv} {qold}; fi; "
                f"mv {qstg} {qenv}; rm -rf {qold}",
            ],
            "env atomic publish",
        )
        return digest

    def _install_runner(self) -> str:
        digest = runner_digest(self.runner_path)
        remote = self._lo.runner(digest)
        present = self.t.run(["test", "-f", remote]).returncode == 0
        if present and not self.force:
            return digest
        self._checked(
            ["sh", "-c", f"mkdir -p {shlex.quote(self._lo.runner_dir(digest))}"],
            "runner dir mkdir",
        )
        self.t.push(self.runner_path, remote)
        return digest

    def _copy_secrets(self) -> None:
        if not self.project.secrets:
            return
        sdir = shlex.quote(self._lo.secrets)
        self._checked(["sh", "-c", f"mkdir -p {sdir} && chmod 700 {sdir}"], "secrets dir")
        for secret in self.project.secrets:
            remote = f"{self._lo.secrets}/{secret.remote_name}"
            self.t.push(secret.source, remote, chmod=f"F{secret.mode:o}")
            # Owner check only — never reads the secret's contents (§6.3.8).
            # GNU stat first, BSD/macOS stat as a dev fallback.
            qr = shlex.quote(remote)
            owner = self._checked(
                ["sh", "-c", f"stat -c '%U' {qr} 2>/dev/null || stat -f '%Su' {qr}"],
                f"verify secret {secret.remote_name}",
            ).stdout.strip()
            if owner != self.host.user:
                raise _StepError(
                    f"secret {secret.remote_name!r} on {self.host.host} owned by "
                    f"{owner!r}, expected {self.host.user!r}"
                )

    def _post_setup(self, env_digest: str) -> None:
        if not self.project.post_setup:
            return
        venv = shlex.quote(self._lo.env_venv(env_digest))
        self._checked(
            [
                "sh",
                "-c",
                f"cd {shlex.quote(self._lo.source)} && "
                f'PATH={venv}/bin:"$PATH" VIRTUAL_ENV={venv} '
                f"exec {shlex.join(self.project.post_setup)}",
            ],
            "post-setup command",
        )

    def provision(
        self,
    ) -> tuple[HostProvisioningResult, tuple[SessionLock, HeartbeatThread] | None]:
        lock = SessionLock(self.t, self.session_id)
        try:
            lock.acquire()
        except HostInUseError as exc:
            return HostProvisioningResult(self.host.host, False, None, None, error=str(exc)), None
        hb = HeartbeatThread(lock, interval_s=self.heartbeat_interval_s)
        hb.start()
        source_dig: str | None = None
        env_dig: str | None = None
        try:
            self.layout = self._resolve_layout()
            hb.raise_if_failed()
            self._preflight()
            hb.raise_if_failed()
            uv = self._install_uv()
            hb.raise_if_failed()
            self._install_python(uv)
            hb.raise_if_failed()
            source_dig = self._sync_source()
            hb.raise_if_failed()
            env_dig = self._publish_env(uv)
            hb.raise_if_failed()
            self._install_runner()
            hb.raise_if_failed()
            self._copy_secrets()
            hb.raise_if_failed()
            self._post_setup(env_dig)
            hb.raise_if_failed()
        except Exception as exc:  # noqa: BLE001 — any step failure marks the host unhealthy
            cleanup_errors = []
            try:
                hb.stop()  # stop the heartbeat BEFORE releasing (avoids a beat/rm race)
            except Exception as cleanup_exc:  # noqa: BLE001 — preserve the primary failure
                cleanup_errors.append(f"heartbeat stop failed: {cleanup_exc}")
            try:
                lock.release()
            except Exception as cleanup_exc:  # noqa: BLE001 — preserve the primary failure
                cleanup_errors.append(f"session lock release failed: {cleanup_exc}")
            error = str(exc)
            if cleanup_errors:
                error += f"; cleanup failed: {'; '.join(cleanup_errors)}"
            return HostProvisioningResult(
                self.host.host, False, source_dig, env_dig, error=error
            ), None
        return HostProvisioningResult(self.host.host, True, source_dig, env_dig), (lock, hb)


def _label(host: RemoteHost) -> str:
    return f"{host.user}@{host.host}:{host.port}"


@dataclass
class ProvisioningOutcome:
    """Report plus the live session locks for healthy hosts (held until teardown)."""

    report: ProvisioningReport
    sessions: dict[str, tuple[SessionLock, HeartbeatThread]] = field(default_factory=dict)

    def raise_if_session_lost(self) -> None:
        for _, heartbeat in self.sessions.values():
            heartbeat.raise_if_failed()

    def set_session_loss_callback(self, callback: Callable[[SessionLockLostError], None]) -> None:
        for _, heartbeat in self.sessions.values():
            heartbeat.set_failure_callback(callback)

    def release_all(self) -> None:
        sessions = self.sessions
        self.sessions = {}
        errors = []
        for host, (_, hb) in sessions.items():
            try:
                hb.stop()
            except Exception as exc:  # noqa: BLE001 — every heartbeat must be stopped
                errors.append(f"{host} heartbeat stop failed: {exc}")
        for host, (lock, _) in sessions.items():
            try:
                lock.release()
            except Exception as exc:  # noqa: BLE001 — every lock must be attempted
                errors.append(f"{host} release failed: {exc}")
        if errors:
            raise DispatcherError(f"session cleanup failed: {'; '.join(errors)}")


def _default_transport(host: RemoteHost) -> Transport:
    return SshTransport(SshConfig.from_host(host))


def provision(
    inventory: Inventory,
    project: Project,
    *,
    runner_path: str,
    require_all_hosts: bool = False,
    force: bool = False,
    transport_factory: Callable[[RemoteHost], Transport] | None = None,
    session_id: str | None = None,
    min_disk_mb: int = 500,
    allow_empty: bool = False,
) -> ProvisioningOutcome:
    for fname in ("pyproject.toml", "uv.lock"):
        if not Path(project.path, fname).is_file():
            raise ConfigurationError(
                f"project at {project.path} is missing {fname} "
                "(required to compute the environment digest, §6.2)"
            )
    factory = transport_factory or _default_transport
    sid = session_id or _secrets.token_hex(16)

    def work(
        host: RemoteHost,
    ) -> tuple[HostProvisioningResult, tuple[SessionLock, HeartbeatThread] | None]:
        try:
            prov = HostProvisioner(
                factory(host),
                project,
                host,
                runner_path=runner_path,
                session_id=sid,
                force=force,
                min_disk_mb=min_disk_mb,
            )
            return prov.provision()
        except Exception as exc:  # noqa: BLE001 — factory/connect failure => host unavailable, never crash the run
            return HostProvisioningResult(host.host, False, None, None, error=str(exc)), None

    _SessionPair = tuple[SessionLock, HeartbeatThread] | None
    results: dict[str, tuple[HostProvisioningResult, _SessionPair]] = {}
    with ThreadPoolExecutor(max_workers=len(inventory.hosts)) as pool:
        futures = {pool.submit(work, h): h for h in inventory.hosts}
        for fut in futures:
            host = futures[fut]
            results[_label(host)] = fut.result()

    report = ProvisioningReport(tuple(results[_label(h)][0] for h in inventory.hosts))
    # Keep only the live sessions of healthy hosts. An explicit loop lets mypy
    # narrow `sess` to a concrete tuple inside the `if` (a dict comprehension would not).
    sessions: dict[str, tuple[SessionLock, HeartbeatThread]] = {}
    for host in inventory.hosts:
        sess = results[_label(host)][1]
        if sess is not None:
            sessions[_label(host)] = sess
    outcome = ProvisioningOutcome(report, sessions)

    healthy = [r for r in report.hosts if r.succeeded]
    if require_all_hosts and len(healthy) != len(inventory.hosts):
        outcome.release_all()
        raise ProvisioningError(
            report, f"require_all_hosts=True: not every host provisioned: {report.failures()}"
        )
    if not healthy and not allow_empty:
        outcome.release_all()
        raise NoHealthyHostsError(f"no host provisioned successfully: {report.failures()}")
    return outcome
