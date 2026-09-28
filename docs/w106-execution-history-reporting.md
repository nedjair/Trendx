# W106 — Rapport de l'historique d'exécutions

## Objet et périmètre

W106 donne à l'analytics W104 une **représentation stable** destinée à être
consommée, exportée ou comparée dans le temps. W106 ne recalcule rien : il
représente l'agrégat W104 sous une forme versionnée.

Code source : `src/trendx/forecasting/reporting.py`.
Tests : `tests/unit/test_w106_execution_history_reporting.py` (504 lignes) et
`tests/integration/test_w106_execution_history_reporting_ops.py` (907 lignes),
commit `7c99c34`.

## Contrat du rapport

```python
ExecutionAnalyticsReportingService.report(query) -> ExecutionAnalyticsReport
```

`REPORT_CONTRACT_VERSION = 1`.

| Bloc | Type | Contenu |
|---|---|---|
| `contract_version` | `int` | `1` |
| `generated_at` | `datetime \| None` | `None` en mode déterministe |
| `tenant_context` | `ReportTenantContext` | `tenant_id` agrégé |
| `applied_filters` | `ExecutionReportFilters` | les filtres **effectivement** appliqués |
| `pagination` | `ReportPagination` | `applied: bool`, `reason: str` |
| `summary` | `ExecutionStatistics` | les compteurs W99 |
| `dimensions` | `ReportDimensions` | `by_metric`, `by_entity`, `by_algorithm`, `by_model`, `by_status` |
| `temporal` | `ReportTemporalData` | `created_at_trend` + `trend_contract` |
| `failures` | `tuple[FailureAnalytics, ...]` | répartition par `error_code` |

## Mode déterministe

Quand `generated_at` n'est pas fourni, il vaut **`None`**. Le rapport ne
contient alors aucun champ d'horloge : deux rapports de la même donnée sont
**byte-identiques**. C'est ce qui permet de comparer deux snapshots, ou de
checksummer un rapport sans que l'horloge le rende différent.

`pagination.applied` vaut `False` sur un rapport, et `reason` explique pourquoi
l'agrégat ignore la pagination W99. Le rapport **assume** ce choix au lieu de
le taire.

## `ReportTrendContract`

Le rapport ré-embarque le contrat temporel :

```json
{"dimension": "created_at", "bucket": "exact_timestamp",
 "timezone": "UTC", "inclusivity": "inclusive", "ordering": "ascending"}
```

Il est identique à celui renvoyé par W104 : les deux couches ne divergent pas.

## Erreurs

`ExecutionReportingDataError` est levée si la source est inexploable. L'API la
convertit en `503 execution_history_unavailable` ou `422 invalid_query`.

## API

| Méthode | Route | Lecture/écriture |
|---|---|---|
| GET | `/api/v1/forecast/executions/report` | **lecture seule** |

La pagination n'est pas appliquée : la route ne déclare ni `limit` ni `offset`.

Documentation voisine : [W104](w104-execution-history-analytics.md),
[W105](w105-analytics-operationalization.md).
