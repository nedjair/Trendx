# Référence API — chaîne d'exécution et d'audit

> **Source de vérité : l'OpenAPI généré par l'application**
> (`GET /openapi.json`). Ce document en est une **dérivation vérifiée** sur le
> commit `d9a01d7`. En cas de divergence, l'OpenAPI fait foi.

Généré depuis `trendx.main:app` (OpenAPI 3.1.0). Au total : 62 `paths`, dont
**21 routes** de la chaîne d'exécution et d'audit, 64 `operationId` au total
dans l'application.

## Toutes les routes sont authentifiées

Les 21 routes déclarent toutes :

```json
"security": [{"BearerAuth": []}, {"ApiKeyAuth": []}]
```

et répondent `401` sans credential valide. Voir
[la référence de sécurité](execution-audit-security.md).

## Table des routes

| Méthode | Route | Lecture/écriture | Réponses | operationId |
|---|---|---|---|---|
| `GET` | `/api/v1/forecast/executions` | lecture | `200,401,403,404,422,503` | `list_executions_api_v1_forecast_executions_get` |
| `GET` | `/api/v1/forecast/executions/statistics` | lecture | `200,401,403,404,422,503` | `execution_statistics_api_v1_forecast_executions_statistics_get` |
| `GET` | `/api/v1/forecast/executions/analytics` | lecture | `200,401,403,404,422,503` | `execution_analytics_api_v1_forecast_executions_analytics_get` |
| `GET` | `/api/v1/forecast/executions/report` | lecture | `200,401,403,404,422,503` | `execution_report_api_v1_forecast_executions_report_get` |
| `GET` | `/api/v1/forecast/executions/{execution_id}` | lecture | `200,401,403,404,422,503` | `get_execution_api_v1_forecast_executions__execution_id__get` |
| `GET` | `/api/v1/forecast/executions/{execution_id}/diagnostic` | lecture | `200,401,403,404,422,503` | `get_execution_diagnostic_api_v1_forecast_executions__execution_id__diagnostic_get` |
| `GET` | `/api/v1/forecast/executions/reference/{reference_key}` | lecture | `200,401,403,404,422,503` | `get_execution_by_reference_api_v1_forecast_executions_reference__reference_key__get` |
| `POST` | `/api/v1/forecast/executions/restore` | **mutation** | `200,401,403,404,409,422,503` | `restore_execution_api_v1_forecast_executions_restore_post` |
| `GET` | `/api/v1/forecast/executions/audit` | lecture | `200,401,403,422,503` | `query_execution_audit_api_v1_forecast_executions_audit_get` |
| `GET` | `/api/v1/forecast/executions/audit/statistics` | lecture | `200,401,403,422,503` | `execution_audit_statistics_api_v1_forecast_executions_audit_statistics_get` |
| `GET` | `/api/v1/forecast/executions/audit/integrity` | lecture | `200,401,403,422,503` | `execution_audit_integrity_api_v1_forecast_executions_audit_integrity_get` |
| `GET` | `/api/v1/forecast/executions/audit/reconciliation` | lecture | `200,401,403,422,503` | `execution_audit_reconciliation_api_v1_forecast_executions_audit_reconciliation_get` |
| `GET` | `/api/v1/forecast/executions/audit/export` | lecture | `200,401,403,422,503` | `execution_audit_export_api_v1_forecast_executions_audit_export_get` |
| `GET` | `/api/v1/forecast/executions/audit/export/checksum` | lecture | `200,401,403,422,503` | `execution_audit_export_checksum_api_v1_forecast_executions_audit_export_checksum_get` |
| `GET` | `/api/v1/forecast/executions/audit/health` | lecture | `200,401,403,422,503` | `execution_audit_health_api_v1_forecast_executions_audit_health_get` |
| `GET` | `/api/v1/forecast/executions/audit/capacity` | lecture | `200,401,403,422,503` | `execution_audit_capacity_api_v1_forecast_executions_audit_capacity_get` |
| `GET` | `/api/v1/forecast/executions/audit/readiness` | lecture | `200,401,403,422,503` | `execution_audit_readiness_api_v1_forecast_executions_audit_readiness_get` |
| `POST` | `/api/v1/forecast/executions/audit/lifecycle/preview` | **mutation** | `200,401,403,409,422,503` | `preview_audit_lifecycle_api_v1_forecast_executions_audit_lifecycle_preview_post` |
| `POST` | `/api/v1/forecast/executions/audit/lifecycle/archive` | **mutation** | `200,401,403,409,422,503` | `archive_audit_lifecycle_api_v1_forecast_executions_audit_lifecycle_archive_post` |
| `POST` | `/api/v1/forecast/executions/audit/lifecycle/purge` | **mutation** | `200,401,403,409,422,503` | `purge_audit_lifecycle_api_v1_forecast_executions_audit_lifecycle_purge_post` |
| `POST` | `/api/v1/forecast/executions/audit/lifecycle/restore` | **mutation** | `200,401,403,409,422,503` | `restore_audit_lifecycle_api_v1_forecast_executions_audit_lifecycle_restore_post` |

**16 routes en lecture seule, 5 routes mutantes.** Toutes les routes mutantes
sont des `POST` : aucune suppression ni modification n'est exposed en `GET`,
`PUT`, `PATCH` ou `DELETE`.

## Détail par endpoint

### Lectures d'exécution

| Route | Paramètres d'entrée | Sortie | Effets de bord |
|---|---|---|---|
| `/executions` | 17 filtres W99 + `limit`, `offset` | `ExecutionListOut` | **aucun** |
| `/executions/statistics` | 17 filtres W99 + `limit`, `offset` | `ExecutionStatsOut` | **aucun** |
| `/executions/{execution_id}` | `execution_id` | `ExecutionOut` | **aucun** |
| `/executions/reference/{reference_key}` | `reference_key` | `ExecutionOut` | **aucun** |
| `/executions/{execution_id}/diagnostic` | `execution_id` | `ExecutionDiagnosticOut` | **aucun** |
| `/executions/analytics` | 15 filtres, **sans** `limit`/`offset` | `ExecutionAnalyticsOut` | **aucun** |
| `/executions/report` | 15 filtres, **sans** `limit`/`offset` | `ExecutionAnalyticsReportOut` | **aucun** |

### `POST /executions/restore` — W109

Corps `ExecutionRestoreRequestIn`, champs **obligatoires** :

| Champ | Contrainte |
|---|---|
| `execution_id` | 1 à 512 caractères |
| `tenant_id` | 1 à 256 caractères |
| `expected_archive_checksum` | exactement 64 caractères hexadécimaux `^[0-9a-f]{64}$` |
| `conflict_policy` | constante `FAIL_IF_EXISTS` — seul `conflict_policy` accepté |

Règles HTTP-only, appliquées **avant** toute résolution de backend :

1. tout paramètre de requête présent → `422 invalid_request` ;
2. `TRENDX_API_TOKEN` absent ou placeholder → `503 recovery_authentication_not_configured` ;
3. `tenant_id` du corps ≠ tenant du contexte → `403 tenant_forbidden` ;
4. seule la stratégie `FAIL_IF_EXISTS` est acceptée.

Codes de statut possibles : `200`, `403`, `404`, `409`, `422`, `503`. Le statut
HTTP est aligné sur le statut métier retourné.

### Lectures d'audit

| Route | Sortie | Effets de bord |
|---|---|---|
| `/audit` | `ExecutionAuditListOut` (≤ 1000 événements) | **aucun** |
| `/audit/statistics` | `ExecutionAuditStatsOut` | **aucun** |
| `/audit/integrity` | rapport d'intégrité | **aucun** |
| `/audit/reconciliation` | rapport de réconciliation | **aucun** |
| `/audit/export` | export (`format=JSON` ou `JSONL`) | **aucun** |
| `/audit/export/checksum` | SHA-256 de l'export | **aucun** |
| `/audit/health` | `AuditHealthReport` | **aucun** |
| `/audit/capacity` | `AuditCapacitySnapshot` | **aucun** |
| `/audit/readiness` | `AuditReadinessReport` | **aucun** |

Filtres d'audit : `tenant_id`, `event_id`, `operation`, `outcome`,
`execution_id`, `reference_key`, `occurred_at_from`, `occurred_at_to`,
`request_id`, `actor_type`, `source`, `reason_code`, `limit`, `offset`.

`X-Request-ID` est un en-tête de corrélation **optionnel**, borné à 128
caractères. Il est echoed dans la réponse.

### `POST /audit/lifecycle/*` — W112

Corps partagé `AuditRetentionPolicyIn` :

| Champ | Type | Défaut | Obligatoire |
|---|---|---|---|
| `retention_days` | entier | — | **oui** |
| `reference_time` | datetime | — | **oui** |
| `archive_before_purge` | booléen | `true` | non |
| `minimum_events_to_keep` | entier | `0` | non |
| `dry_run` | booléen | `true` | non |

`AuditRestoreRequestIn` : `event_id` (obligatoire), `tenant_id` (obligatoire),
`request_id` (optionnel), `actor_id` (défaut `api-key-context`).

`extra="forbid"` : un champ inconnu est un `422` dur, pas un no-op silencieux.
`reference_time` est obligatoire **exprès**, pour qu'une exécution de lifecycle
n'hérite jamais d'une horloge murale implicite.

`preview` et `archive` sont idempotents. `purge` ne supprime qu'après archive
vérifiée. `restore` n'écrase jamais.

## Configuration des store

| Setting | Alias | Store |
|---|---|---|
| `trendx_execution_history_path` | `TRENDX_EXECUTION_HISTORY_PATH` | historique d'exécutions W100 |
| `trendx_execution_archive_path` | `TRENDX_EXECUTION_ARCHIVE_PATH` | archive d'exécutions W107/W108 |
| `trendx_execution_audit_path` | `TRENDX_EXECUTION_AUDIT_PATH` | journal d'audit W110 |
| `trendx_execution_audit_archive_path` | `TRENDX_EXECUTION_AUDIT_ARCHIVE_PATH` | archive d'audit W112 |
| `trendx_default_tenant_id` | `TRENDX_DEFAULT_TENANT_ID` | tenant du déploiement |
| `trendx_api_token` | `TRENDX_API_TOKEN` | token API |

Le chemin d'audit ne doit **pas** chevaucher le store d'exécutions ni son
archive : la factory d'audit refuse un chevauchement de chemins.
