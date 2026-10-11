# Ray Dispatcher Audit Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` for sequential execution, or `superpowers:subagent-driven-development` only if the user chooses delegation. Steps use checkbox syntax for tracking. This document plans the work; it does not authorize deployment or publication.

**Goal:** Correggere gli 11 rilievi dell'audit, preservare dati e processi durante i guasti e rendere ripetibili i controlli di qualità.

**Architecture:** Correzioni nei moduli esistenti. Il backend possiede cancellazione, attesa e riconciliazione; il runner gestisce il processo remoto; la pubblicazione conserva lo stato precedente finché il nuovo è valido. Un solo helper interno raccoglie invocazione, timeout e cancellazione: nessuna nuova gerarchia di backend o framework.

**Tech Stack:** Python >=3.10, Ray, Fabric/Paramiko, rsync, uv, pytest, mypy, Ruff; Python >=3.11 per gli esempi/test con `multipass-vm-sdk==0.6.0`.

**Spec:** Audit della conversazione del 2026-10-11; [design originale](../specs/2026-06-27-ray-dispatcher-design.md), [correzioni precedenti](../specs/2026-07-06-ray-dispatcher-bug-fixes-design.md), [visibilità host](../specs/2026-06-30-live-host-visibility-design.md). Le decisioni sotto sono gli emendamenti proposti; i vecchi piani non sono una checklist da rieseguire.

## Stato e decisioni di progetto

Piano pronto per revisione, implementazione non iniziata. Approccio scelto: fix incrementali con regressioni prima del codice. Una riscrittura dei backend avrebbe più rischio e non serve; limitarsi a formatter e dipendenze lascerebbe irrisolti i problemi di sicurezza dello stato remoto.

- Nessun nuovo backend Azure: il core continua a ricevere host già esistenti tramite `Inventory`.
- Il teardown restituisce successo solo dopo la pulizia verificata. Richiedere `ray.cancel()` o osservare un riferimento Ray cancellato non prova da solo che il processo remoto sia morto.
- Se la pulizia fallisce, il backend conserva lo stato necessario per ritentare e i heartbeat delle sessioni ancora possedute; non rilascia i lock dei relativi host e non effettua purge. Il chiamante riceve `DispatcherError`. Se il processo del dispatcher muore, la garanzia non può estendersi oltre il TTL esistente: il recupero automatico dopo crash è fuori scope e va documentato.
- La chiave proposta per `running_hosts()` è `(batch_id, job_id)`, con risultato `dict[tuple[str, str], str]`. È una modifica incompatibile rispetto a `{job_id: host}`: aggiornare tutti i chiamanti nel repository e documentare la migrazione per quelli esterni. La preferenza dell'utente sulla compatibilità, se diversa, richiede di rivedere solo Task 7 prima di implementarlo.
- I log del figlio restano byte per byte su file remoti, con flush tempestivo. Il dispatcher non deve riceverne una seconda copia illimitata in memoria. Il runner standalone conserva il forwarding predefinito; il manifest generato dal dispatcher lo disabilita. Gli errori del supervisore restano visibili su stderr.
- Migrare la dipendenza distribuita, non installare l'omonimo `multipass-sdk` di un altro progetto. Metadati verificati durante l'audit: [multipass-vm-sdk 0.6.0 su PyPI](https://pypi.org/project/multipass-vm-sdk/0.6.0/) e [API pubblica](https://github.com/Nanofaas/multipass-vm-sdk/blob/main/src/multipass_vm_sdk/__init__.py).

## Global Constraints

- Il core mantiene `requires-python = ">=3.10"`; Multipass rimane una dipendenza opzionale di test/esempio con Python >=3.11.
- Nessuna nuova dipendenza runtime nel core. Riutilizzare `Transport`, `FakeTransport`, `LeasePool`, `reconcile_host` e `terminate_process_group`.
- Non modificare i tre piani già presenti come file non tracciati. Nessun commit deve inglobare modifiche estranee.
- Prima di ogni fix cercare tutti i chiamanti, fake e test del simbolo interessato con `rg`.
- Per ogni comportamento nuovo: test di regressione rosso, correzione minima, test verde. Nessun test nuovo che controlli soltanto la presenza di una stringa nel comando shell.
- Test unitari senza avvio di Ray; test di integrazione con Ray locale; E2E Multipass espliciti e separati.
- Non dichiarare superata la suite completa finché non termina con exit code 0. Un errore ambientale o exit 143 non è un risultato verde.
- Formattazione in un commit separato dopo i fix funzionali. Nessuna modifica a versioni di rilascio, tag o pubblicazione in questo piano.

## Review Focus

1. Cancellazione prima della creazione di `pid.json`, durante la raccolta e con figlio che ignora SIGTERM: Task 3 e 5.
2. Perdita del lock di sessione durante il teardown: non cancellare file né rilasciare il lock del nuovo proprietario; Task 2 e 5.
3. Guasto del secondo rename con una vecchia versione valida: conservarla e non scrivere un manifest di successo; Task 1.
4. Log binari, output breve seguito da attesa, errore di scrittura log e perdita della connessione: Task 3, con assenza di successi falsi o pipe bloccate.
5. Un batch senza capacità dopo tentativi già falliti deve conservare la storia; due batch omonimi devono restare distinguibili: Task 4 e 7.

## Mappa dei file

| Area | File di implementazione/configurazione | Responsabilità |
| --- | --- | --- |
| Pubblicazione | `src/ray_dispatcher/provisioning.py` | Rename verificati, conservazione copia precedente |
| Lock | `src/ray_dispatcher/locking.py` | Timestamp remoto e ownership |
| Processo e log | `src/ray_dispatcher/scheduling.py`, `remote_runner.py` | Invocazione, timeout, cleanup, log senza cattura illimitata |
| Risultati e retry | `src/ray_dispatcher/results.py`, `scheduling.py`, `backends/ssh_ray.py` | Persistenza iniziale, classificazione errori, cronologia |
| Lifecycle | `src/ray_dispatcher/dispatcher.py`, `backends/ssh_ray.py`, `provisioning.py` | Drain, riconciliazione, rilascio sessioni |
| Visibilità | `scheduling.py`, `backends/base.py`, `backends/ssh_ray.py`, `dispatcher.py` | Identità batch/job in tutti gli strati |
| SDK e VM | `pyproject.toml`, `uv.lock`, `examples/toy_batch/{pyproject.toml,uv.lock,run_batch.py,README.md}`, `tests/e2e/conftest.py` | Distribuzione PyPI, import, cleanup VM |
| Controlli | `.github/workflows/ci.yml` (nuovo), `pyproject.toml`, `README.md`, `tests/conftest.py` | Lint, format, tipi, test riproducibili |

## Ordine e dipendenze

Eseguire Task 1–10 nell'ordine. Task 1 e 2 sono correzioni isolate; Task 3 definisce il lifecycle del processo usato da Task 5 e 6; Task 4 rende affidabili i risultati usati da Task 5. Task 8 abilita gli E2E della verifica finale. Task 9 non cambia comportamento. Task 10 è il gate di chiusura.

### Task 1 — Pubblicazione senza perdita dello stato precedente (rilievo 3)

**Files:** modificare `src/ray_dispatcher/provisioning.py`; estendere `tests/unit/test_provisioning_source.py` e `tests/unit/test_provisioning_env.py`.

**Interfaces:** mantenere `HostProvisioner._sync_source() -> str` e `_publish_env(uv: str) -> str`; un fallimento resta `_StepError` e rende negativo `HostProvisioningResult`.

- [x] Scrivere `test_source_publish_failure_preserves_previous_tree` e `test_env_publish_failure_preserves_previous_env`. Eseguire realmente lo script locale in directory temporanee, simulando un `mv` fallito tramite un eseguibile controllato nel PATH. Assert: errore propagato, vecchi contenuti ancora presenti nella destinazione o in `.old`, nessun manifest nuovo di successo.
- [x] Aggiungere il caso di fallimento del primo rename e quello di pubblicazione riuscita. Non usare percorsi reali della macchina o comandi di cancellazione fuori dal temporaneo.
- [x] Eseguire `uv run pytest tests/unit/test_provisioning_source.py tests/unit/test_provisioning_env.py -q`: i nuovi casi devono fallire prima del fix.
- [x] Sostituire le sequenze separate da `;` con operazioni controllate; eliminare `.old` solo dopo il rename riuscito. In caso di errore conservare il backup e non far mascherare l'errore primario da cleanup/rollback. Scrivere il manifest soltanto a pubblicazione completata.
- [x] Ripetere i test fino a exit 0; commit mirato `fix: preserve previous state on publish failure`.

### Task 2 — TTL del lock calcolato sulla VM (rilievo 4)

**Files:** `src/ray_dispatcher/locking.py`; `tests/unit/test_session_lock_acquire.py`, `test_session_lock_heartbeat.py`, `test_session_lock_contention.py`, `test_heartbeat_thread.py`.

**Interfaces:** `SessionLock.acquire/heartbeat/release` invariati. `_LOCK_SCRIPT` ricava `time.time()` sul remoto dentro il lock `fcntl`; rimuovere il timestamp del client dagli argomenti. Rimuovere il parametro interno `now` e `_payload` se nessun chiamante residuo li usa; i test controllano il tempo nel trasporto/script remoto.

- [x] Aggiungere `test_client_clock_skew_cannot_steal_live_lock`: client simulati con differenza di 61 s, stesso clock remoto, seconda acquisizione rifiutata. Aggiungere lock realmente scaduto e heartbeat del precedente proprietario rifiutato.
- [x] Eseguire `uv run pytest tests/unit/test_session_lock* tests/unit/test_heartbeat_thread.py -q` e confermare la regressione.
- [x] Usare lo stesso clock remoto per acquisizione, rinnovo e confronto TTL; mantenere scrittura atomica e ownership check.
- [x] Ripetere i test, inclusa la contesa concorrente già esistente; commit `fix: evaluate session lock expiry on remote host`.

### Task 3 — Unificare supervisione e pulizia del processo; correggere i log (rilievi 5 e 10)

**Files:** `src/ray_dispatcher/scheduling.py`, `src/ray_dispatcher/remote_runner.py`; `tests/unit/test_execute_attempt_cancel.py`, `test_execute_attempt_timeout.py`, `test_remote_runner.py`, `test_remote_runner_forward.py`, `test_runner_manifest.py`.

**Interfaces:** estrarre nello stesso `scheduling.py` `_invoke_runner(transport: Transport, runner: str, run: RunPaths, *, timeout_s: float | None, check_lease: Callable[[], None] | None = None) -> bool`, dove `True` indica timeout e `False` conclusione normale. Aggiungere lo stesso keyword opzionale `check_lease` a `execute_attempt`; Task 6 lo collegherà al lease handle. Errori di trasporto e interrupt mantengono il tipo originario; cleanup non confermato usa `_HostCleanupError`. `execute_attempt` conserva preparazione, raccolta e composizione di `AttemptResult`.

- [x] Scrivere test con eventi sincronizzati per timeout e cancellazione durante l'invocazione. Assert: il worker SSH termina oppure il cleanup viene segnalato non confermato; mai rilascio sicuro dedotto soltanto da `ray.cancel`. Coprire assenza temporanea del PID e figlio che ignora SIGTERM.
- [x] Scrivere `test_short_output_is_written_before_child_exit`: il figlio scrive e resta in attesa di un segnale; verificare i byte nel file prima di sbloccarlo. Niente semplice `communicate()` dopo la fine per provare lo streaming.
- [x] Aggiungere `test_dispatcher_manifest_disables_child_forwarding` e `test_large_binary_output_stays_in_remote_logs`: manifest dispatcher con `forward_output=False`, log binari completi, stdout del supervisore vuoto. Il comportamento standalone predefinito conserva il forwarding.
- [x] Eseguire i test interessati e osservare i nuovi fallimenti.
- [x] Usare una sola invocazione in thread con polling del termine, del timeout e di `check_lease`; un solo percorso kill/join decide se il cleanup è confermato. Nessuna nuova classe orchestratrice. Trasferire senza perdere gli errori già gestiti.
- [x] Nel runner usare `read1` sui pipe bufferizzati; adattare `_tee` al forwarding opzionale, con default compatibile. Il manifest del dispatcher disabilita soltanto il forwarding del figlio: traceback e diagnostica del supervisore rimangono su stderr.
- [x] Propagare i guasti dei thread di logging al supervisore, terminando il gruppo se necessario: un disco pieno non deve produrre successo o lasciare una pipe bloccata. Test con errore di scrittura iniettato.
- [x] `uv run pytest tests/unit/test_execute_attempt*.py tests/unit/test_remote_runner*.py tests/unit/test_runner_manifest.py -q` deve terminare con exit 0; commit `fix: unify remote process cleanup and stream logs to disk`.

### Task 4 — Risultati affidabili anche prima del primo tentativo (rilievo 2)

**Files:** `src/ray_dispatcher/results.py`, `src/ray_dispatcher/backends/ssh_ray.py`, `src/ray_dispatcher/scheduling.py`; `tests/unit/test_results_manifest.py`, `test_run_job.py`; spostare i casi con Ray reale da `tests/unit/test_attempt_task.py` a `tests/integration/test_attempt_task.py` e aggiungere unit test del corpo del task con dipendenze simulate.

**Interfaces:** `write_result_json(path: Path, result: JobResult) -> None` crea il parent e pubblica via file temporaneo/replace nella stessa directory. `status` e `resolve` devono produrre uno stesso risultato terminale quando la cancellazione avviene prima dell'avvio.

- [x] Test `test_no_capacity_before_first_attempt_persists_failed_result`: acquisizione che solleva `NoHealthyHostsError`, nessuna directory preesistente; assert risultato FAILED, errore originale leggibile, `result.json` valido.
- [x] Test cancellazione pre-start: `status == CANCELLED`, `resolve` restituisce CANCELLED e il file locale corrisponde. Test perdita capacità dopo un tentativo: storia già accumulata conservata.
- [x] Test errore reale di filesystem: un `PermissionError` durante la persistenza deve essere riportato come tale, senza loop di riscrittura né falsa dichiarazione di file salvato.
- [x] Eseguire i test nuovi rossi, poi creare il parent nello scrittore condiviso e rendere coerente la finalizzazione nei percorsi task/backend. Non sovrascrivere una cronologia più completa con un risultato cancellato vuoto.
- [x] `uv run pytest tests/unit/test_results_manifest.py tests/unit/test_attempt_task.py tests/unit/test_run_job.py tests/integration/test_attempt_task.py tests/integration/test_backend_submit.py -q` deve passare; commit `fix: persist terminal results for early failures and cancellation`.

### Task 5 — Teardown ordinato e verificabile (rilievo 1)

**Files:** `src/ray_dispatcher/dispatcher.py`, `src/ray_dispatcher/backends/ssh_ray.py`, `src/ray_dispatcher/provisioning.py`, `src/ray_dispatcher/scheduling.py`, `src/ray_dispatcher/ssh.py`; test in `tests/unit/test_dispatcher.py`, `test_reconcile_host.py`, `test_terminate_pgroup.py`, `tests/integration/test_backend_teardown.py`, `test_backend_sweeper.py`; nuovo `tests/e2e/test_teardown.py`.

**Interfaces:** firme pubbliche di `teardown(*, purge: bool = False) -> None` invariate. Il backend possiede il drain; aggiungere un helper privato `_drain_jobs() -> None` che usa refs e lease già posseduti e una costante privata `_DRAIN_TIMEOUT_S = 30.0`. Aggiungere il keyword interno `deadline_s: float | None = None` a `reconcile_host` e `terminate_process_group`, espresso come deadline monotonic assoluta. Conservare le risorse in caso di drain fallito per consentire un secondo teardown.

- [x] Test con eventi: job avviato e cleanup bloccato, iniziare teardown; assert lock/actor/runtime ancora disponibili. Sbloccare cleanup; assert ordine `cancel -> conferma processi terminati -> actor stop -> rilascio lock -> shutdown`.
- [x] Test host irraggiungibile o PID non ancora disponibile: teardown fallisce senza purge, senza cancellare metadati/refs e senza rilasciare il lock; secondo tentativo dopo ripristino termina correttamente. Un PID assente da solo non prova la pulizia.
- [x] Test session lock perso: nessun purge o modifica di ownership del nuovo proprietario; preservare l'errore originario, limitando il cleanup ai processi identificati della vecchia sessione.
- [x] Rendere i test rossi, poi impedire nuove submission durante il teardown; richiedere cancellazione una volta, attendere i task e riconciliare i PID registrati/orfani prima di distruggere l'actor. Fermare il sweeper solo quando non serve più al drain. Non azzerare `_outstanding`, `_refs` e `_outcome` prima del successo della fase che li usa.
- [x] Limitare il drain a 30 s complessivi, preservando la grace SIGTERM di 10 s quando il budget residuo lo permette. Limitare attese Ray, join e singole chiamate `transport.run(timeout_s=...)` al tempo residuo; alla scadenza non iniziare altre sonde e sollevare errore conservando sessione e diagnostica. Test con clock controllato e assert sul timeout passato al trasporto: il default SSH di 300 s non deve rendere illusorio il limite del drain.
- [ ] E2E reale non eseguito (daemon Multipass irraggiungibile); test aggiunto: job con processo figlio, attendere una prova remota dell'avvio, uscire dal contesto, controllare che il gruppo sia morto prima di avviare una seconda sessione sulla stessa VM. Non accontentarsi dello stato CANCELLED restituito da Ray.
- [x] `uv run pytest tests/unit/test_dispatcher.py tests/unit/test_reconcile_host.py tests/unit/test_terminate_pgroup.py tests/integration/test_backend_teardown.py tests/integration/test_backend_sweeper.py -q` deve passare; E2E nel Task 10. Commit `fix: drain remote work before releasing session ownership`.

### Task 6 — Classificare COLLECTION e HOST_LOST dove avvengono (rilievo 8)

**Files:** `src/ray_dispatcher/scheduling.py`, `src/ray_dispatcher/backends/ssh_ray.py`; `tests/unit/test_run_job.py`, `test_results_collect.py`, `test_should_retry.py`; nuovo `tests/unit/test_actor_lease_handle.py`.

**Interfaces:** mantenere `FailureKind` e `RetryPolicy`. Estendere il protocollo interno `LeaseHandle` con `raise_if_lost() -> None`; implementarlo nel wrapper Ray e in tutti i fake. Il heartbeat registra in modo thread-safe perdita/rifiuto della lease; `check_lease` del Task 3 la osserva. Usare un'eccezione interna `_LeaseLostError(DispatcherError)` per distinguerla dal trasporto SSH.

- [x] Policy selettiva `RetryPolicy(max_attempts=2, retry_on=frozenset({FailureKind.COLLECTION}))`: pull fallisce, secondo tentativo riesce; assert due tentativi e primo `failure_kind == COLLECTION`. Un guasto nell'invocazione resta SSH e non viene ritentato da questa policy.
- [x] Test heartbeat rifiutato o actor perso durante il runner: stop/quarantena prima del retry, primo tentativo HOST_LOST. Una policy solo SSH non deve ritentare HOST_LOST.
- [x] Aggiungere caso errore di raccolta dopo exit nonzero: conservare il returncode già noto e classificare il fallimento COMMAND come dominante, senza rieseguire un comando fallito solo per recuperare log.
- [x] Eseguire test rossi, introdurre la classificazione ai confini di invocazione/raccolta/lease. Non dedurre HOST_LOST dal testo di un messaggio. Aggiornare le docstring obsolete che dichiarano queste funzionalità ancora da implementare.
- [x] `uv run pytest tests/unit/test_run_job*.py tests/unit/test_results_collect.py tests/unit/test_should_retry.py tests/unit/test_actor_lease_handle.py tests/integration/test_host_lease_actor.py -q` deve passare; commit `fix: honor collection and host loss retry policies`.

### Task 7 — Identità completa dei job nella visibilità host (rilievo 7)

**Files:** `src/ray_dispatcher/scheduling.py`, `backends/base.py`, `backends/ssh_ray.py`, `dispatcher.py`; test `test_lease_pool_current_hosts.py`, `test_lease_service_current_hosts.py`, `test_run_job*.py`, `test_dispatcher.py`, `tests/integration/test_backend_running_hosts.py`; `README.md` e specifica visibilità host.

**Interfaces:** pubblico `running_hosts() -> dict[tuple[str, str], str]`. Mantenere `Lease.attempt_id` come stringa opaca interna: `run_job` passa `f"{batch_id}/{job.id}"`, delimitatore non ammesso negli ID validati. Il backend converte la chiave interna nella tupla pubblica; niente nuovo modello pubblico.

- [x] Test due job `same` nei batch `a` e `b` contemporanei: assert entrambe le chiavi presenti e associate agli host corretti; al termine del primo scompare solo `('a', 'same')`.
- [x] Eseguire i nuovi test rossi, aggiornare produttore, interfaccia backend, Dispatcher e tutti i consumatori trovati con `rg 'running_hosts|current_hosts|attempt_id'`.
- [x] Documentare prima/dopo per i consumer esterni, senza fallback che perda silenziosamente le collisioni. Se l'utente richiede compatibilità, rivedere questo contratto prima di modificare codice.
- [x] `uv run pytest tests/unit/test_lease_pool_current_hosts.py tests/unit/test_lease_service_current_hosts.py tests/unit/test_run_job*.py tests/unit/test_dispatcher.py tests/integration/test_backend_running_hosts.py -q` deve passare; commit `fix: distinguish jobs across batches in running host snapshots`.

### Task 8 — Migrare Multipass e registrare subito le VM per il cleanup (rilievi 6 e 9)

**Files:** `pyproject.toml`, `uv.lock`, `examples/toy_batch/pyproject.toml`, `examples/toy_batch/uv.lock`, `examples/toy_batch/run_batch.py`, `examples/toy_batch/README.md`, `tests/e2e/conftest.py`; nuovi `examples/toy_batch/tests/test_vm_cleanup.py` e `tests/unit/test_e2e_vm_cleanup.py`.

**Interfaces:** usare `from multipass_vm_sdk import MultipassClient, MultipassVM, find_ssh_public_key`; dipendenza `multipass-vm-sdk==0.6.0`, marker `python_version >= '3.11'` nel gruppo E2E root. Nessuna modifica al contratto `Inventory`.

- [x] Test con client fake: `launch` restituisce una VM, `wait_ready` solleva; assert `delete(purge=True)` chiamato esattamente una volta. Ripetere per errore cloud-init e errore sulla seconda VM: tutte e sole le VM già create vengono eliminate. I test non avviano il daemon. Il test root usa `pytest.importorskip("multipass_vm_sdk")` prima di importare la fixture E2E: la CI esegue esplicitamente questo test anche con il gruppo E2E installato, così lo skip nella suite base non nasconde la regressione.
- [x] Eseguire i test rossi. Registrare l'oggetto VM immediatamente dopo `launch`, separando l'elenco cleanup dalle coppie VM/IP pronte. Applicare il fix sia all'esempio sia alla fixture, senza creare un nuovo servizio VM condiviso.
- [x] Sostituire URL Git/override uv con la distribuzione PyPI e aggiornare gli import. Rigenerare entrambi i lockfile tramite uv; verificare che il diff non aggiorni dipendenze estranee senza necessità. Non modificare `examples/toy_batch/experiment/uv.lock`.
- [x] Verificare installazione/import in ambiente Python >=3.11 con `uv sync --group e2e` e `uv run --group e2e python -c 'from multipass_vm_sdk import MultipassClient, MultipassVM, find_ssh_public_key'`; poi `uv sync --project examples/toy_batch` e test dell'esempio.
- [x] Verificare che il wheel contenga le API usate e `uv run --group e2e pytest -o addopts= --collect-only tests/e2e -q` raccolga i test senza creare VM. Aggiornare istruzioni e marker descrittivo.
- [x] Commit `fix: migrate Multipass SDK and clean up partially initialized VMs`.

### Task 9 — Formatter e CI obbligatori (rilievo 11)

**Files:** nuovo `.github/workflows/ci.yml`; `pyproject.toml`, `README.md`, `tests/conftest.py`; formattazione dei file Python già esistenti e nuovi. Non introdurre pre-commit in aggiunta alla CI.

**Interfaces:** comandi ufficiali `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy src`, `uv run pytest -q`. La stessa toolchain risolta da `uv.lock` deve essere usata in locale e CI.

- [x] Spostare qualsiasi test residuo che avvia Ray da `tests/unit` a `tests/integration`; mantenere isolamento e teardown tramite fixture. Verificare che la suite unit possa girare senza avviare processi Ray.
- [x] Applicare `uv run ruff format .` in un commit esclusivamente meccanico. Non aggiungere una collezione di nuove regole lint per ampliare artificialmente lo scope.
- [x] Creare workflow per pull request/push: `uv sync --locked`, lint, format-check, mypy e suite completa su Linux/Python 3.10 e 3.13. Esempio/SDK smoke su Python 3.13. Dipendenze e azioni CI devono usare versioni verificate all'implementazione, non valori inventati nel piano.
- [x] Usare il lockfile senza ricalcolarlo in CI; impostare timeout del job per evitare blocchi Ray infiniti. Multipass non parte nella pipeline ordinaria.
- [x] Documentare comandi separati per unit, integration ed E2E. Eseguire localmente tutti i controlli; commit `ci: enforce lint format typing and test checks`.

### Task 10 — Verifica finale e consegna

**Files:** test E2E aggiunti/estesi nei task precedenti; questo piano per registrare gli esiti. Nessuna nuova funzione di prodotto.

- [x] Verificare corrispondenza audit/task: 1→5, 2→4, 3→1, 4→2, 5→3, 6→8, 7→7, 8→6, 9→8, 10→3, 11→9.
- [x] Eseguire `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy src` e `uv run pytest -q`. Tutti devono terminare con exit 0; indagare l'exit 143 osservato durante l'audit se si ripete, senza catalogarlo automaticamente come problema del codice.
- [x] Eseguire `uv run --project examples/toy_batch pytest examples/toy_batch/tests -q` e controllare entrambi i lockfile con `uv lock --check` / `uv lock --check --project examples/toy_batch`.
- [ ] Ambiente Multipass non disponibile: `uv run --group e2e pytest -o addopts= tests/e2e -m e2e -q`. Coprire smoke, timeout, teardown con figli attivi, secondo dispatcher, e cleanup delle VM al termine. Se manca l'ambiente, indicare gli E2E come non verificati, non come passati.
- [x] Revisionare i percorsi errore e la compatibilità di `running_hosts`; controllare che nessun lock venga rilasciato prima della conferma di cleanup e nessun vecchio source/env sia eliminato dopo rename fallito.
- [x] Consegnare riepilogo dei fix, risultati dei comandi e limitazioni residue. Nessuna pubblicazione o merge automatico.

## Baseline dell'audit e criterio di completamento

Baseline: `ruff check .` e `mypy src` verdi; `ruff format --check .` segnala 69 file; 373 unit test senza avvio Ray verdi. Due test classificati unitari tentavano Ray nella sandbox e fallivano per permessi; la suite completa fuori sandbox è terminata con exit 143, quindi non è stata validata.

Il lavoro è completo quando ogni rilievo ha il fix e una verifica pertinente, la suite completa termina correttamente, i contratti modificati sono documentati e gli E2E hanno un esito esplicito. La copertura percentuale e il conteggio righe non sostituiscono i test sui guasti identificati.

## Esclusioni e rischi residui

- Esclusi backend Azure, riscrittura delle classi, nuovo framework di configurazione, cambiamenti estetici oltre Ruff, aggiornamenti indiscriminati delle dipendenze e recupero persistente dopo crash del dispatcher.
- La nuova chiave di `running_hosts` richiede la migrazione dei consumer esterni, che non sono presenti in questo checkout.
- Una VM irraggiungibile impedisce di provare la morte dei processi: il teardown deve segnalarlo e conservare la protezione finché il processo dispatcher resta vivo; non può promettere cleanup fisico della VM offline.
- Stato originale del piano prima dell’implementazione: nessun fix ancora applicato. Esecuzione scelta: sequenziale nella sessione, con un commit per unità verificata; nessun agente aggiuntivo necessario per preparare questo documento.


## Esito dell’implementazione nativa — 2026-10-11

Branch locale: `codex/audit-fixes-2026-10-11`. Nessun agente aggiuntivo, merge,
push o rilascio. I tre piani preesistenti non tracciati sono stati preservati.

| Task | Commit / esito |
| --- | --- |
| 1 | `44fbe8a`: pubblicazione source/env senza perdita del backup |
| 2 | `4e60d41`: scadenza lock calcolata sul remoto |
| 3 | `c1e11c0`: supervisione comune e log su file |
| 4 | `b91232a`: risultati atomici e cronologia dei tentativi |
| 5 | `0ba9719`: drain prima del rilascio sessioni, deadline e retry teardown |
| 6 | `28fb6d3`: classificazione COLLECTION/HOST_LOST e policy selettive |
| 7 | `feb93e1`: chiavi `(batch_id, job_id)` per gli host correnti |
| 8 | `eb4b798`: SDK PyPI 0.6.0 e cleanup VM inizializzate parzialmente |
| 9 | `3c90a11` e `7c1c7df`: Ruff e CI su Python 3.10/3.13 |
| 10 | `ff72bfd`: revisione nativa e verifiche finali riportate sotto |

La revisione finale ha aggiunto due regressioni: un errore Ray durante il drain
mantiene le risorse e diventa DispatcherError; la perdita del lock e il teardown
non cancellano due volte un worker già impegnato nel cleanup. Una submission
interrotta dalla perdita di sessione conserva il riferimento per il drain.
I fake dei test di cancellazione ora confermano la morte del processo simulato.

Verifiche: Ruff check e format-check verdi (110 file), mypy verde (15 moduli),
entrambi i lockfile validi, 5 test dell’esempio passati, import del SDK 0.6.0
riuscito e 13 E2E raccolti senza avviare VM. Suite completa iniziale dopo i fix:
437 test passati, exit 0. Verifica definitiva dopo la revisione: **438 test passati**, exit 0, in 73,43 s;
un solo FutureWarning di Ray. L’exit 143 osservato durante l’audit e una prima
esecuzione non si è ripetuto nelle due suite complete terminate con successo;
non è stato attribuito a una causa non dimostrata.

Gli E2E reali restano non verificati: `multipass list --format json` termina con
exit 2, «cannot connect to the multipass socket». Nessuna VM creata.
La matrice Linux/Python 3.10 e 3.13 è configurata in CI; localmente la suite è
stata eseguita su macOS/Python 3.13. Nessuna riscrittura generale delle classi:
è stata rimossa la duplicazione nella supervisione del processo.
