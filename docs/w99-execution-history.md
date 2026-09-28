# W99 — Historique des exécutions

## Objet et périmètre

W99 fait d'un store d'exécutions un **historique consultable** : filtrage,
tri déterministe, pagination bornée, statistiques et diagnostic des échecs.
W99 ne modifie jamais le store : toutes ses opérations sont en lecture.

Code source : `src/trendx/forecasting/history.py`.
Tests : `tests/unit/test_w99_forecast_execution_history.py` (672 lignes,
commit `d027884`).

## `ExecutionQuery`

Tous les filtres sont optionnels. Filtrer sur plusieurs champs est une
**intersection (AND)**.

| Champ | Type | Effet |
|---|---|---|
| `tenant_id` | `str \| None` | filtre de tenant ; `None` = pas de filtre de tenant |
| `entity_type` | `str \| None` | type d'entité |
| `entity_id` | `str \| None` | identifiant d'entité |
| `target_metric` | `str \| None` | métrique prédite |
| `execution_id` | `str \| None` | identité primaire exacte |
| `reference_key` | `str \| None` | clé d'idempotence exacte |
| `model_id` | `str \| None` | modèle |
| `model_version` | `str \| None` | version du modèle |
| `algorithm` | `str \| None` | algorithme |
| `feature_schema_version` | `str \| None` | version du schéma de features |
| `feature_schema_fingerprint` | `str \| None` | empreinte du schéma de features |
| `status` | `ExecutionStatus \| str \| None` | statut |
| `created_at_from` / `created_at_to` | `datetime \| str \| None` | fenêtre de création, bornes **inclusives** |
| `completed_at_from` / `completed_at_to` | `datetime \| str \| None` | fenêtre de fin, bornes **inclusives** |
| `limit` | `int \| None` | taille de page |
| `offset` | `int` | décalage, `0` par défaut |

`MAX_HISTORY_PAGE_SIZE = 1000`. Une requête qui dépasse cette borne est refusée
par l'API avec `422 invalid_pagination` — elle n'est pas silencieusement
tronquée.

## Sémantique temporelle

- Tous les timestamps sont normalisés en **UTC**. Le fuseau est une préoccupation
  d'affichage uniquement.
- Les bornes sont **inclusives** des deux côtés : `created_at_from = T` inclut un
  enregistré creaté exactement à `T`.
- Le tri est **déterministe** : `HISTORY_ORDER` fixe l'ordre de tri, de sorte que
  deux requêtes identiques rendent des résultats identiques.

## `ExecutionHistoryService`

| Méthode | Rôle |
|---|---|
| `query(query)` | page de `ExecutionRecord` |
| `get(execution_id)` | lecture unitaire |
| `get_by_reference_key(reference_key)` | lecture par clé d'idempotence |
| `list_records(...)` / `list_history(...)` | parcours filtré |
| `count(...)` | nombre d'éléments retenus |
| `recent(...)` | derniers enregistrements |
| `search(...)` | recherche |
| `find_by_model(...)` | regroupement par modèle |
| `statistics(query)` | `ExecutionStatistics` |
| `diagnostics(...)` | diagnostic d'un échec |
| `failure_diagnostics(query)` | diagnostic de tous les échecs |

### `ExecutionHistoryPage`

`records`, `total`, `limit`, `offset` — et une propriété calculée `has_more`
qui indique s'il reste une page au-delà de `offset + limit`.

### `ExecutionStatistics`

| Champ | Rôle |
|---|---|
| `total_executions` | nombre total sur le filtre |
| `success_count` | exécutions `SUCCESS` |
| `failure_count` | exécutions `FAILED` |
| `success_rate` | `success_count / total_executions` |
| `prediction_count_total` | somme des prédictions produites |
| `duration_sample_count` | exécutions avec `started_at` **et** `completed_at` |
| `average_duration_seconds` | durée moyenne, `None` si aucun échantillon |
| `total_duration_seconds` | durée cumulée, `None` si aucun échantillon |

`duration_sample_count` est distinct de `total_executions` : une exécution
jamais terminée, ou jamais démarrée, n'entre pas dans la durée.

### `FailureDiagnostic`

Projection d'un échec, strictement limitée à :
`execution_id`, `reference_key`, `tenant_id`, `entity_type`, `entity_id`,
`target_metric`, `model_id`, `model_version`, `algorithm`,
`feature_schema_version`, `feature_schema_fingerprint`, `artifact_uri`,
`status`, `error_code`, `error_reason`, `created_at`, `started_at`,
`completed_at`.

Le diagnostic ne contient **ni** la charge utile complète, **ni** le corps de la
requête, **ni** de traceback.

## API

Trois routes, toutes en lecture, authentifiées :

| Méthode | Route | Objet |
|---|---|---|
| GET | `/api/v1/forecast/executions` | page d'historique |
| GET | `/api/v1/forecast/executions/statistics` | `ExecutionStatistics` |
| GET | `/api/v1/forecast/executions/{execution_id}` | un enregistrement |
| GET | `/api/v1/forecast/executions/reference/{reference_key}` | lecture par clé |
| GET | `/api/v1/forecast/executions/{execution_id}/diagnostic` | diagnostic d'échec |

Les paramètres de requête de `/executions` et `/executions/statistics` sont
exactement les champs de `ExecutionQuery` ci-dessus ; le paramètre `tenant_id`
est **imposé** par le contexte de tenant et non choisi par le client.

Codes d'erreur : `401`, `403 tenant_forbidden`, `404 execution_not_found`,
`422 invalid_pagination` / `invalid_query`, `503 execution_history_unavailable`.

## LIMITES ET SUIVIS

- `tenant_id=None` est techniquement accepté par `ExecutionQuery`. L'API ne
  l'expose jamais de la sorte : elle surcharge toujours le tenant du contexte.
  Un appel direct au service avec `None` n'est pas filtré par tenant.
  *Suivi : restreindre `ExecutionQuery` à un tenant obligatoire — hors périmètre.*
- Les bornes temporelles sont inclusives mais sans granularité de fuseau :
  une requête par jour utilise l'UTC, pas le fuseau du tenant.

## Vérification

```bash
.venv/bin/python -m pytest tests/unit/test_w99_forecast_execution_history.py -q
```

Documentation voisine : [W98](w98-forecast-execution-persistence.md),
[W100](w100-durable-execution-history.md),
[W101](w101-execution-history-api.md).
