"""Persistance des runs scheduler + adaptation trigger() -> handlers P0.

Ce module concentre tout ce que le registre a besoin de connaître au-delà du
dispatch en mémoire, sans jamais importer ``trendx.services.worker``
(contrat d'isolation MR18) :

- ``resolve_run_mode`` : validation fail-closed du mode d'exécution ;
- ``adapt_p0_call`` : mapping pur payload 1-arg -> (json_job, task_id,
  execution_id) exigé par les handlers P0 ``(json_job, task_id,
  execution_id)`` ;
- ``adapt_p0_handler`` : adaptateur 1-arg enregistrable dans le registre pour
  un handler P0 (mode direct = exécution in-process, mode enqueue = création
  de tâche via TaskService SANS exécution in-process) ;
- ``RunStore`` (Protocol) + ``DbRunStore`` : persistance PostgreSQL
  (sessions courtes séparées, commits explicites).

Aucun retry immédiat, aucune élection leader ici (MR-3) : la reprise
opérationnelle reste le prochain tick (coalesce/max_instances existants).
"""

from __future__ import annotations

import socket
import uuid
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from typing import Any, Protocol
from uuid import uuid4

from loguru import logger
from sqlalchemy.orm import Session
from trendx.database.connection import manager as db_manager
from trendx.database.repositories import SchedulerRunRepository

RUN_MODES = ("direct", "enqueue")


def resolve_run_mode(raw: Any) -> str:
    """Valide le mode d'exécution (défaut 'direct'). Fail-closed sinon."""
    if raw is None or raw == "":
        return "direct"
    mode = str(raw)
    if mode not in RUN_MODES:
        raise ValueError(f"Unknown run mode: {mode!r} (expected 'direct' or 'enqueue')")
    return mode


def default_instance_id() -> str:
    """Identifiant d'instance stable pour la durée d'un registre."""
    return f"{socket.gethostname()}-{uuid4().hex[:8]}"


def adapt_p0_call(
    payload: Mapping[str, Any] | None,
    *,
    execution_id: str | None,
    task_id: str | None = None,
    mode: str = "direct",
) -> tuple[dict[str, Any], str | None, str | None]:
    """Mappe un payload 1-arg vers (json_job, task_id, execution_id).

    Pur et sans effet de bord : `json_job` est le payload tel quel (le contrat
    des handlers ignore les clés inconnues) ; en mode direct `task_id` est
    forcé à None ; en mode enqueue `task_id` est requis (fail-closed).
    """
    resolved = resolve_run_mode(mode)
    json_job = dict(payload or {})
    if resolved == "direct":
        return json_job, None, execution_id
    if task_id is None:
        raise ValueError("enqueue mode requires an explicit task_id (none provided)")
    return json_job, str(task_id), execution_id


def adapt_p0_handler(
    fn: Callable[[dict[str, Any], str | None, str], dict[str, Any]],
    *,
    mode: str = "direct",
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """Adaptateur 1-arg enregistrable pour un handler P0 3-args.

    - direct : exécution in-process ``fn(json_job, None, execution_id)`` ;
      execution_id = payload["execution_id"] ou UUID frais.
    - enqueue : création de la tâche via TaskService (atomique, persistée,
      exécutée plus tard par le worker claim) SANS exécution in-process ;
      retourne {"status": "enqueued", "task_id": ...}.
    Les handlers P0 restent inchangés ; TaskService est importé localement
    (même pattern lazy que les handlers).
    """
    resolved = resolve_run_mode(mode)

    def _adapted(payload: dict[str, Any]) -> dict[str, Any]:
        data = dict(payload or {})
        if resolved == "enqueue":
            from trendx.services.tasks import TaskService

            job_type = str(data.get("job_type") or "trendx_train")
            name = str(data.get("name") or f"scheduler-{job_type}")
            task = TaskService().create_task(name=name, job_type=job_type, json_job=data)
            return {"status": "enqueued", "task_id": str(task.id), "job_type": job_type}

        execution_id = str(data.get("execution_id") or uuid4())
        json_job, _, _ = adapt_p0_call(data, execution_id=execution_id, mode="direct")
        return fn(json_job, None, execution_id)

    return _adapted


class RunStore(Protocol):
    """Contrat de persistance consommé par SchedulerRegistry.trigger()."""

    def begin_run(
        self,
        *,
        run_id: str,
        job_id: str,
        job_type: str,
        instance_id: str,
        mode: str = "direct",
        task_id: str | None = None,
    ) -> None: ...

    def finish_run(
        self,
        *,
        run_id: str,
        status: str,
        error: str | None = None,
        task_id: str | None = None,
    ) -> None: ...


class DbRunStore:
    """RunStore PostgreSQL via SchedulerRunRepository.

    Sessions courtes séparées + commits explicites : un échec handler ne rend
    jamais la transaction de persistance inutilisable. Les erreurs de
    `begin_run` se propagent (fail fast avant exécution) ; les erreurs de
    `finish_run` sont journalisées sans masquer le résultat du handler.
    """

    def __init__(
        self,
        session_factory: Callable[[], AbstractContextManager[Session]] | None = None,
    ) -> None:
        self._sessions = session_factory or (lambda: db_manager.get_session("catalog"))

    def begin_run(
        self,
        *,
        run_id: str,
        job_id: str,
        job_type: str,
        instance_id: str,
        mode: str = "direct",
        task_id: str | None = None,
    ) -> None:
        with self._sessions() as session:
            SchedulerRunRepository(session).create_run(
                run_id=run_id,
                job_id=job_id,
                job_type=job_type,
                instance_id=instance_id,
                mode=resolve_run_mode(mode),
                task_id=task_id,
            )
            session.commit()

    def record_heartbeat(
        self,
        *,
        instance_id: str,
        leader: bool,
        version: str = "",
        host: str = "",
    ) -> None:
        with self._sessions() as session:
            SchedulerRunRepository(session).record_heartbeat(
                instance_id=instance_id, leader=leader, version=version, host=host
            )
            session.commit()

    def finish_run(
        self,
        *,
        run_id: str,
        status: str,
        error: str | None = None,
        task_id: str | None = None,
    ) -> None:
        try:
            with self._sessions() as session:
                repo = SchedulerRunRepository(session)
                if status == "enqueued":
                    # Chemin enqueue : la ligne reste running (l'exécution aura
                    # lieu via le claim worker) ; on y associe task_id si fourni.
                    row = repo.get(uuid.UUID(str(run_id)))
                    if row is not None and task_id is not None and row.task_id is None:
                        row.task_id = uuid.UUID(str(task_id))
                        session.flush()
                    session.commit()
                    return
                repo.mark_terminal(run_id, status, error=error)
                session.commit()
        except Exception as exc:
            # La persistance est de l'observabilité : elle ne doit jamais
            # masquer le résultat réel du handler.
            logger.error("Scheduler run persistence failed for {}: {}", run_id, exc)
