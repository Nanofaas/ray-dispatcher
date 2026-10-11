# Python Namespace Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Native execution explicitly requested; no subagents.

**Goal:** Importare la libreria da `it.unimib.data.nanofaas.ray_dispatcher`.

**Architecture:** Namespace impliciti PEP 420 per i parent. Package finale ed export invariati; nessun alias legacy. Hatch include l’albero sotto `src/it`.

**Tech Stack:** Python >=3.10, Hatch, uv, Ruff, mypy, pytest.

**Spec:** [design approvato](../specs/2026-10-11-python-namespace-design.md).

## Global Constraints

- Distribuzione `ray-dispatcher`, versione 0.1.1; nessuna pubblicazione PyPI.
- Nessun `__init__.py` in `it/unimib/data/nanofaas`; nessun package alias.
- Percorsi remoti `.ray_dispatcher` e formati di risultato invariati.
- Tre piani non tracciati dell’utente e documenti storici preservati.

## Review Focus

- Wheel installato fuori dal checkout: import pubblico deve funzionare.
- Nessuna collisione fra distribuzioni: i parent devono restare namespace PEP 420.
- Runner remoto standalone: il file deve essere presente nel wheel e trovato dal backend.
- Ray/cloudpickle e monkeypatch: tutti i riferimenti importabili devono essere migrati.
- Distribuzione invariata: pip/uv continuano a installare `ray-dispatcher`.

### Task 1: Package e consumatori

**Files:** spostare `src/ray_dispatcher/` in `src/it/unimib/data/nanofaas/ray_dispatcher/`; modificare import in `tests/` ed `examples/`, `pyproject.toml`, README; aggiungere `tests/unit/test_namespace.py`.

**Interfaces:** `from it.unimib.data.nanofaas.ray_dispatcher import Dispatcher, Job`; firme invariate.

- [x] Test `test_namespace_exports_and_parents`: importare namespace, verificare export, parent con `__file__ is None`, runner adiacente esistente. Eseguirlo prima del move: deve fallire.
- [x] Spostare package; sostituire solo riferimenti a moduli e percorsi sorgente, preservando path remoti `.ray_dispatcher`. Aggiornare Hatch e sorgente coverage.
- [x] Aggiornare README con installazione invariata e migrazione degli import.
- [x] Ruff check/format, mypy, deptry e unit test devono passare. Commit locale dedicato.

### Task 2: Distribuzioni e verifica finale

**Files:** CI e documentazione qualità, piano per esiti; nessuna nuova API.

**Interfaces:** wheel contiene `it/unimib/data/nanofaas/ray_dispatcher/remote_runner.py` e metadata `ray-dispatcher`.

- [x] `uv build` e `twine check --strict dist/*`: entrambe le distribuzioni valide.
- [x] Installare wheel in venv temporaneo; da `/tmp` verificare import, parent namespace e runner, senza PYTHONPATH né editable checkout.
- [x] Eseguire copertura unit con branch >=80%, suite completa Ray, test esempio ed E2E Multipass; verificare inventario finale vuoto.
- [x] Registrare risultati, revisione nativa, commit e push al nuovo remote Nanofaas già richiesto per il progetto. Nessun rilascio.


## Execution results — 2026-10-11

- Native execution in an isolated worktree; namespace test failed before migration.
- Full suite: 451 passed; unit suite: 420 passed, branch coverage 81.25%.
- Example: 5 passed. Multipass E2E: 13 passed in 217.30s; final VM inventory empty.
- Ruff, formatting, strict mypy, deptry, root/example audits passed.
- Wheel and sdist passed strict Twine checks. Isolated wheel verified namespace
  parents, public exports, runner, version, absence of old imports and MIT metadata.
- Native review confirmed runtime source is unchanged except a runner docstring.
- Ruling: namespace and release CI are committed together because their packaging
  and import verification share one contract. PyPI setup was separately approved;
  version stays 0.1.1 and no upload is performed during setup.
- Release CI uses the SDK's Trusted Publishing pattern, environment `pypi`,
  filename `release.yml`, and pinned PyPI action. The approved trigger differs:
  version increase on main rather than the SDK's `v*` tags.
- Account-side PyPI publisher requires user setup; no browser is available here.
