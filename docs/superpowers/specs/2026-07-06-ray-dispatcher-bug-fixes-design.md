# Ray Dispatcher Bug Fixes Design

## Scope

Fix only the confirmed correctness and reliability bugs from the 2026-07-06 audit. Exclude dependency hardening, supply-chain changes, stale-secret cleanup, and complexity-only refactors.

## Design

- Validate caller-controlled identifiers and numeric runtime settings at their public boundaries so local and remote paths cannot escape their roots and invalid timing configurations cannot enter scheduling.
- Make the remote session lock fail closed. Ownership-changing operations use one atomic remote critical section; heartbeat and release never overwrite or remove a newer owner.
- Keep orphan process metadata until every process is confirmed gone. A failed reconciliation leaves the host quarantined and retryable.
- Treat timeout and cancellation as incomplete until the runner thread stops or the host is quarantined. Never release capacity while a remote process may still be alive.
- Make backend setup transactional: any failure releases acquired locks and shuts down only the Ray runtime started by that setup attempt.
- Implement the documented project-scoped purge after active work is excluded, without touching other project IDs.
- Make multi-job submission rollback already-submitted jobs if a later submission fails.
- Apply bounded SSH and rsync operation timeouts and classify local transport launch failures as transport errors.

## Error handling

Cleanup is best-effort only when another mechanism preserves safety. A failed process kill quarantines the host; a failed setup rolls back all acquired resources; a partial submit cancels every handle already created before re-raising the original error.

## Testing

Each bug receives a focused regression test written before its fix. Existing unit and integration tests must remain green, followed by the complete default pytest suite, Ruff, and strict mypy. Multipass E2E remains opt-in because it requires external VMs.
