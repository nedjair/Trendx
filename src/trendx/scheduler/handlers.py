"""Handlers P0 partagés (extraction depuis services/worker.py — comportement identique).

Source de vérité unique des exécuteurs P0 : importés par le worker (boucle
d'exécution ``JOB_DISPATCH``) et destinés au futur service ``trendx-scheduler``
dédicace. Contrat d'isolation (MR18) : ce module n'importe JAMAIS
``trendx.services.worker`` et ne retombe jamais sur un stub. Les imports
métier lourds (TrainingService, InferenceService, Strategy) restent locaux
aux fonctions, comme dans le worker d'origine (mêmes timings, mêmes cycles
d'import évités).

Aucune modification métier : signatures ``(json_job, task_id, execution_id)``,
docstrings, valeurs de retour et effets de bord sont ceux d'origine. Le
writeback TB n'est jamais invoqué ici (dry_run forcé côté forecast).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from trendx.config import settings
from trendx.services.ingestion import IngestionService
from trendx.thingsboard.discovery import TopologyDiscoveryService


def _run_topology_discovery(
    json_job: dict[str, Any], task_id: str, execution_id: str
) -> dict[str, Any]:
    """Découverte topologique complète + persistance du catalog."""
    svc = TopologyDiscoveryService()
    asyncio.run(svc.full_sync())
    asyncio.run(svc.update_catalog())
    return {"job_type": "topology_discovery", "status": "ok"}


def _run_topology_sync(json_job: dict[str, Any], task_id: str, execution_id: str) -> dict[str, Any]:
    """Synchro topologique incrémentale + persistance du catalog."""
    svc = TopologyDiscoveryService()
    asyncio.run(svc.incremental_sync())
    asyncio.run(svc.update_catalog())
    return {"job_type": "topology_sync", "status": "ok"}


def _run_ingestion(json_job: dict[str, Any], task_id: str, execution_id: str) -> dict[str, Any]:
    """Ingestion incrémentale (auto-découvre devices/metrics)."""
    svc = IngestionService()
    results = asyncio.run(svc.run_incremental_ingest())
    return {"job_type": "ingestion", "status": "ok", "tasks": len(results) if results else 0}


def _require_forecast_keys(json_job: dict[str, Any], required: list[str]) -> None:
    """Valide les paramètres minimaux d'un job forecast/train.

    Lève ValueError si une clé requise est absente ou vide. L'appelant
    (_process_one_task côté worker) convertit l'exception en fail_task isolé
    par device/métrique/fenêtre, sans impacter les autres tâches.
    """
    missing = [k for k in required if not json_job.get(k)]
    if missing:
        msg = f"Missing required job params: {', '.join(missing)}"
        raise ValueError(msg)


def _parse_strategy(raw: Any) -> Any:
    """Parse la stratégie PER_DEVICE/PER_PROFILE/GLOBAL/AUTO (défaut PER_DEVICE).

    Accepté en string insensible à la casse ou en membre Strategy. Lève
    ValueError si inconnue. Import local pour éviter tout cycle.
    """
    from trendx.forecasting.base import Strategy

    if raw is None or raw == "":
        return Strategy.PER_DEVICE
    if isinstance(raw, Strategy):
        return raw
    try:
        return Strategy[str(raw).upper()]
    except KeyError as exc:
        msg = f"Unknown strategy: {raw!r}. Expected PER_DEVICE/PER_PROFILE/GLOBAL/AUTO"
        raise ValueError(msg) from exc


def _run_trendx_train(json_job: dict[str, Any], task_id: str, execution_id: str) -> dict[str, Any]:
    """Job trendx_train : entraînement idempotent, reprenable, isolé par device/métrique.

    Paramètres minimaux (tous requis sauf défauts indiqués) :
      tenant_id, entity_type, entity_id, metric_name,
      strategy (défaut PER_DEVICE : PER_DEVICE/PER_PROFILE/GLOBAL/AUTO),
      lookback_days (défaut settings.training_lookback_days),
      frequency (défaut settings.forecast_frequency),
      horizon (défaut settings.forecast_horizon),
      algorithm (défaut Prophet), params/model_params (défaut None),
      min_val/max_val (défaut None, clipping métier).

    Contraintes : dispatch dynamique via JOB_DISPATCH (aucun job statique par
    device), timestamps UTC, lectures DB bornées par fenêtre + LIMIT côté
    TrainingService, erreur isolée (exception -> fail_task de cette tâche
    uniquement), writeback jamais invoqué ici.
    """
    _require_forecast_keys(json_job, ["tenant_id", "entity_type", "entity_id", "metric_name"])
    tenant_id = str(json_job["tenant_id"])
    entity_type = str(json_job["entity_type"])
    entity_id = str(json_job["entity_id"])
    metric_name = str(json_job["metric_name"])
    strategy = _parse_strategy(json_job.get("strategy", "PER_DEVICE"))
    lookback_days = json_job.get("lookback_days")
    frequency = str(json_job.get("frequency") or settings.forecast_frequency)
    horizon = json_job.get("horizon") or settings.forecast_horizon
    algorithm = str(json_job.get("algorithm") or "Prophet")
    params = json_job.get("params", json_job.get("model_params"))
    min_val = json_job.get("min_val")
    max_val = json_job.get("max_val")

    from trendx.services.training import TrainingService

    svc = TrainingService()
    # AUTO : compétition + sélection champion ; autres stratégies : entraînement
    # ciblé (PER_DEVICE/PER_PROFILE/GLOBAL routés via le même service, la
    # segmentation fine par profil/global étant portée par ModelSelector).
    if strategy.name == "AUTO":
        champion = svc.auto_select_strategy(entity_id, metric_name)
        model_id = str(getattr(champion, "id", "") or "") if champion is not None else None
        status = "ok" if champion is not None else "no_data"
        return {
            "job_type": "trendx_train",
            "status": status,
            "tenant_id": tenant_id,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "metric_name": metric_name,
            "strategy": strategy.name,
            "model_id": model_id,
            "generated_at": datetime.now(UTC).isoformat(),
        }
    model = svc.train_model(
        entity_id=entity_id,
        metric_key=metric_name,
        algorithm=algorithm,
        params=params,
        lookback_days=lookback_days,
        frequency=frequency,
        horizon=int(horizon),
        min_val=min_val,
        max_val=max_val,
    )
    if model is None:
        return {
            "job_type": "trendx_train",
            "status": "no_data",
            "tenant_id": tenant_id,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "metric_name": metric_name,
            "strategy": strategy.name,
            "model_id": None,
            "generated_at": datetime.now(UTC).isoformat(),
        }
    return {
        "job_type": "trendx_train",
        "status": "ok",
        "tenant_id": tenant_id,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "metric_name": metric_name,
        "strategy": strategy.name,
        "model_id": str(getattr(model, "id", "")),
        "generated_at": datetime.now(UTC).isoformat(),
    }


def _run_trendx_forecast(
    json_job: dict[str, Any], task_id: str, execution_id: str
) -> dict[str, Any]:
    """Job trendx_forecast : prévision idempotente, isolée, sans writeback TB.

    Paramètres minimaux :
      tenant_id, entity_type, entity_id, metric_name,
      horizon (défaut settings.forecast_horizon),
      strategy (défaut PER_DEVICE).

    Utilise le modèle/version champion via InferenceService, calcule les
    métriques lorsque la vérité est disponible (côté TrainingService), persiste
    via save_forecast_results (upsert idempotent sur PK composite). Aucun
    writeback TB par défaut : writeback_forecast/post_telemetry ne sont jamais
    appelés ici ; dry_run forcé à True.
    """
    _require_forecast_keys(json_job, ["tenant_id", "entity_type", "entity_id", "metric_name"])
    tenant_id = str(json_job["tenant_id"])
    entity_type = str(json_job["entity_type"])
    entity_id = str(json_job["entity_id"])
    metric_name = str(json_job["metric_name"])
    horizon = int(json_job.get("horizon") or settings.forecast_horizon)
    strategy = _parse_strategy(json_job.get("strategy", "PER_DEVICE"))

    from trendx.services.inference import InferenceService

    svc = InferenceService(dry_run=True)
    # Garde-fou : même si la config autorisait le writeback, ce job reste
    # sans écriture TB (séparation stricte persistance locale vs writeback).
    svc.dry_run = True
    forecast = svc.generate_forecast(entity_id, metric_name, horizon=horizon)
    if forecast is None:
        msg = f"No forecast for {entity_id[:12]}/{metric_name} (no champion or no data)"
        raise RuntimeError(msg)
    saved = svc.save_forecast_results(entity_id, metric_name, forecast)
    return {
        "job_type": "trendx_forecast",
        "status": "ok",
        "tenant_id": tenant_id,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "metric_name": metric_name,
        "strategy": strategy.name,
        "horizon": horizon,
        "points": len(forecast.values),
        "saved": saved,
        "writeback": "disabled",
        "generated_at": datetime.now(UTC).isoformat(),
    }


def _run_anomaly_scan(json_job: dict[str, Any], task_id: str, execution_id: str) -> dict[str, Any]:
    """Job anomaly_scan : scan idempotent, isolé, sans alarme ni writeback TB.

    Paramètres minimaux : tenant_id, entity_type, entity_id, metric_name.
    Utilise AnomalyDetectorService.scan (persistance des épisodes dans la table
    ``anomaly`` existante). AlertingService n'est jamais invoqué ici.
    """
    _require_forecast_keys(json_job, ["tenant_id", "entity_type", "entity_id", "metric_name"])
    tenant_id = str(json_job["tenant_id"])
    entity_type = str(json_job["entity_type"])
    entity_id = str(json_job["entity_id"])
    metric_name = str(json_job["metric_name"])

    from trendx.anomalies.detectors import AnomalyDetectorService

    svc = AnomalyDetectorService()
    episodes = svc.scan(entity_id, metric_name)
    return {
        "job_type": "anomaly_scan",
        "status": "ok",
        "tenant_id": tenant_id,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "metric_name": metric_name,
        "episodes": len(episodes or []),
        "alarms": "disabled",
        "generated_at": datetime.now(UTC).isoformat(),
    }


def _run_ml_pipeline(json_job: dict[str, Any], task_id: str, execution_id: str) -> dict[str, Any]:
    """Job ml_pipeline : Quality → Preprocessing → Train → Forecast → Anomaly.

    Paramètres minimaux : tenant_id, entity_type, entity_id, metric_name
    (+ lookback_days/frequency/horizon/algorithm/customer_id optionnels ;
    customer_id explicite recommandé, sinon résolution fail-closed via
    TRENDX_DEFAULT_CUSTOMER_ID dans TrainingService).
    Délègue au MLPipelineOrchestrator (contexte propagé, dry_run forcé côté
    InferenceService, aucun writeback/alarme). Échec → RuntimeError pour
    fail_task explicite (pas de passage silencieux).
    """
    _require_forecast_keys(json_job, ["tenant_id", "entity_type", "entity_id", "metric_name"])

    from trendx.services.pipeline import MLPipelineOrchestrator, PipelineContext

    context = PipelineContext(
        tenant_id=str(json_job["tenant_id"]),
        entity_type=str(json_job["entity_type"]),
        entity_id=str(json_job["entity_id"]),
        metric_name=str(json_job["metric_name"]),
        lookback_days=json_job.get("lookback_days"),
        frequency=str(json_job.get("frequency") or settings.forecast_frequency),
        horizon=json_job.get("horizon"),
        algorithm=str(json_job.get("algorithm") or "Prophet"),
        customer_id=json_job.get("customer_id"),
        execution_id=execution_id,
    )
    result = MLPipelineOrchestrator().run_single(context)
    if result.status != "ok":
        msg = (
            f"ml_pipeline failed for {context.entity_id[:12]}/{context.metric_name}: "
            f"{result.error}"
        )
        raise RuntimeError(msg)
    payload = result.to_dict()
    payload["job_type"] = "ml_pipeline"
    return payload


# Registre partagé job_type -> exécuteur. Le worker expose JOB_DISPATCH
# (contrat exécuteur, clés identiques) ; le futur service scheduler dédié
# consommera ce mapping sans jamais importer trendx.services.worker.
JOB_HANDLERS: dict[str, Any] = {
    "topology_discovery": _run_topology_discovery,
    "topology_sync": _run_topology_sync,
    "ingestion": _run_ingestion,
    "trendx_train": _run_trendx_train,
    "trendx_forecast": _run_trendx_forecast,
    "anomaly_scan": _run_anomaly_scan,
    "ml_pipeline": _run_ml_pipeline,
}
