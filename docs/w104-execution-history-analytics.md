# W104 — Analytics de l'historique d'exécutions

## Objet et périmètre

W104 calcule une **projection agrégée en lecture seule** sur l'historique W99.
Il ne modifie jamais le store et ne réécrit pas W99 : il lit, regroupe, compte.

Code source : `src/trendx/forecasting/analytics.py`.
Tests : `tests/unit/test_w104_execution_history_analytics.py` (687 lignes,
commit `84af67e`).

## Objetif

```python
ExecutionHistoryAnalytics.analyze(query) -> ExecutionAnalytics
```

`ExecutionAnalytics` est composed de :

| Bloc | Type | Contenu |
|---|---|---|
| `summary` | `ExecutionStatistics` | les compteurs W99, non recalculés |
| `by_metric` | `tuple[AnalyticsGroup, ...]` | par `target_metric` |
| `by_entity` | `tuple[EntityAnalytics, ...]` | par couple `(entity_type, entity_id)` |
| `by_algorithm` | `tuple[AnalyticsGroup, ...]` | par `algorithm` |
| `by_model` | `tuple[ModelAnalytics, ...]` | par couple `(model_id, model_version)` |
| `by_status` | `tuple[AnalyticsGroup, ...]` | par `ExecutionStatus` |
| `failures` | `tuple[FailureAnalytics, ...]` | par `error_code` |
| `created_at_trend` | `tuple[TimeBucket, ...]` | par `created_at` exact |
| `tenant_id` | `str` | tenant agrégé |

`summary` est l'objet `ExecutionStatistics` de W99, **relu tel quel** : W104 ne
définit pas sa propre notion de taux de succès.

## Formes de ligne

| Type | Champs |
|---|---|
| `AnalyticsGroup` | `key`, `total`, `success_count`, `failed_count`, `success_rate`, `prediction_count` |
| `EntityAnalytics` | `entity_type`, `entity_id`, `total`, `success_count`, `failed_count`, `success_rate`, `prediction_count` |
| `ModelAnalytics` | `model_id`, `model_version`, `total`, `success_count`, `failed_count`, `success_rate`, `prediction_count` |
| `FailureAnalytics` | `error_code`, `count`, `proportion` |
| `TimeBucket` | `bucket`, `total`, `success_count`, `failed_count`, `prediction_count` |

## Sémantique temporelle

Le contrat `TREND_CONTRACT`, retourné tel quel dans la réponse, est :

```json
{"dimension": "created_at", "bucket": "exact_timestamp",
 "timezone": "UTC", "inclusivity": "inclusive", "ordering": "ascending"}
```

Points essential :

- la dimension est `created_at`, **jamais** `completed_at` ;
- le seau est le **timestamp exact**, pas une troncature à la journée, à
  l'heure ou à la minute ;
- le fuseau est **UTC** ;
- l'ordre est **ascendant** ;
- les bornes de filtre sont **inclusives**.

`proportion` dans `failures` est la part de chaque `error_code` dans le total des
échecs du périmètre filtré.

## Pagination

L'analytics est un **agrégat** : la pagination W99 n'est **pas** appliquée. Le
paramètre `limit` de la route est ignoré par le contrat. C'est intentionnel et
documenté dans la description de la route.

## Erreurs

`ExecutionHistoryDataError` est levée si les données sont inexploitables
(datastore indisponible, document illisible). L'API la convertit en `503
execution_history_unavailable` ou `422 invalid_query` — jamais en traceback.

## LIMITES

- **Partition par jour non disponible.** `bucket = exact_timestamp` signifie
  qu'une série quotidienne doit être dérivée par le consommateur, ou par W106.
- **`created_at` uniquement.** Analyser la durée ou la latence de complétion
  n'est pas fourni ; `created_at_trend` ne le permet pas.
- **Coût linéaire en enregistrements filtrés.** L'agrégat parcourt le
  périmètre filtré en mémoire du service. Voir W105 pour les observations
  opérationnelles.

Documentation voisine : [W99](w99-execution-history.md),
[W103](w103-execution-history-observability.md),
[W105](w105-analytics-operationalization.md),
[W106](w106-execution-history-reporting.md).
