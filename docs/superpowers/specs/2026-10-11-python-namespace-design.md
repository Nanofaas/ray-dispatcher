# Namespace Python Nanofaas

## Obiettivo e contratto

Adottare `it.unimib.data.nanofaas.ray_dispatcher`, come scelto dall’utente,
rispecchiando la gerarchia Java e usando il nome Python `ray_dispatcher`.
Il nome della distribuzione installabile rimane `ray-dispatcher` e la versione
rimane 0.1.1; non è previsto alcun rilascio PyPI.

## Struttura

Spostare il package in `src/it/unimib/data/nanofaas/ray_dispatcher/`.
I parent `it`, `unimib`, `data` e `nanofaas` sono namespace impliciti PEP 420,
senza `__init__.py`, così altre distribuzioni possono contribuire al namespace.
Il package finale mantiene il proprio `__init__.py`, gli export e gli import
relativi interni.

Gli import pubblici diventano, per esempio:

```python
from it.unimib.data.nanofaas.ray_dispatcher import Dispatcher, Job
```

Non aggiungere un secondo package di compatibilità `ray_dispatcher`: sarebbe
una seconda superficie da mantenere. È una migrazione esplicita degli import;
i consumer esterni devono aggiornarsi. Non cambiano le firme né il comportamento
operativo del dispatcher.

## Superficie da aggiornare

Aggiornare import e monkeypatch in test, fixture ed esempio; configurazione
Hatch del wheel, copertura, deptry, documentazione attiva, percorsi del runner e
qualunque riferimento operativo al vecchio percorso trovato con `rg`.
I vecchi piani rimangono documenti storici; i tre piani non tracciati dell’utente
non vengono modificati.

## Verifica

Testare gli export pubblici attraverso il nuovo namespace, l’assenza di
`__init__.py` nei parent e i percorsi del runner. Costruire wheel e sdist,
verificarne i metadati e installare il wheel in un ambiente isolato per provare
l’import fuori dal checkout. Eseguire Ruff, mypy, deptry, copertura e suite
completa; E2E Multipass dopo il cambio del percorso del runner.

## Rischio principale

Gli import `from ray_dispatcher ...` smettono di funzionare. Documentare
prima/dopo nel README; l’identità GitHub resta `Nanofaas/ray-dispatcher`.
