# W98 — Persistance des exécutions de prévision

## Objet et périmètre

W98 introduit le contrat de persistance d'une **exécution de prévision** : ce
qui a été demandé, par quel modèle, avec quelle version de schéma de
features, et ce qui en est ressorti. W98 ne lit rien, n'agrège rien et
n'expose aucune route HTTP.

Trois objets portent le contrat :

- `ExecutionProvenance` — l'identité du calcul ;
- `ExecutionRecord` — l'exécution complète, cycle de vie inclus ;
- `ExecutionStore` — le port de stockage, implémenté par `MemoryExecutionStore`
  (W98) puis `DurableExecutionStore` (W100).

Code source : `src/trendx/forecasting/execution.py`.
Tests : `tests/unit/test_w98_forecast_execution_persistence.py` (896 lignes,
commit `806da40`).

## `ExecutionStatus`

Quatre valeurs, et uniquement celles-là :

| Statut | Signification |
|---|---|
| `PENDING` | enregistrement créé, aucun début d'exécution |
| `RUNNING` | `mark_started()` appelé, pas de résultat final |
| `SUCCESS` | `mark_success()` appelé : `completed_at` renseigné |
| `FAILED` | `mark_failed()` appelé : `error_code` et `error_reason` renseignés |

Les transitions autorisées sont strictes et unidirectionnelles :

```text
PENDING -> RUNNING -> SUCCESS
                  -> FAILED
```

`FAILED` et `SUCCESS` sont des états **terminaux** ; `PENDING` ne l'est pas.
Aucune transition inverse n'est admise par le store.

## `ExecutionProvenance`

Champs réels, tous optionnels sauf identification :

| Champ | Rôle |
|---|---|
| `model_id` | identifiant du modèle |
| `model_version` | version du modèle |
| `algorithm` | algorithme (ex. `prophet`) |
| `feature_schema_version` | version du schéma de features d'entrée |
| `feature_schema_fingerprint` | empreinte du schéma de features |
| `artifact_uri` | localisation de l'artefact de modèle |
| `prediction_count` | nombre de prédictions produites |
| `metadata` | dictionnaire libre, **strictement JSON** |
| `result` | résultat, dictionnaire libre, **strictement JSON** |

`feature_schema_version` et `feature_schema_fingerprint` existent pour pouvoir
prouver *avec quelle version de schéma* une série a été produite. Sans eux,
deux exécutions seemingly identiques seraient indiscernables.

## `ExecutionRecord`

| Champ | Rôle |
|---|---|
| `execution_id` | identité primaire, unique dans le store |
| `reference_key` | clé d'idempotence métier, unique dans le store |
| `tenant_id` | tenant propriétaire — **clé d'isolation** |
| `entity_type` | type d'entité ThingsBoard (`DEVICE`, `ASSET`, …) |
| `entity_id` | identifiant de l'entité |
| `target_metric` | métrique prédite |
| `frequency` | pas de temps demandé (ex. `1h`) |
| `horizon` | horizon de prédiction, en pas de temps |
| `model_id`, `model_version`, `algorithm` | modèle employé |
| `feature_schema_version`, `feature_schema_fingerprint` | schéma de features |
| `artifact_uri` | localisation de l'artefact |
| `status` | `ExecutionStatus` |
| `created_at` | création, ISO 8601 **UTC** |
| `started_at` | début d'exécution, `None` tant que `PENDING` |
| `completed_at` | fin, `None` tant que non terminal |
| `error_code` | code d'erreur, rempli seulement si `FAILED` |
| `error_reason` | motif lisible, rempli seulement si `FAILED` |
| `prediction_count` | nombre de prédictions |
| `metadata` | dictionnaire libre, JSON strict |
| `result` | résultat, dictionnaire libre, JSON strict |

### Règle de clé de série

La clé de série minimale est :

```text
tenant_id + entity_type + entity_id + target_metric
```

`device_id` n'est **jamais** codé en dur. Ajouter un device dans ThingsBoard ne
requiert aucun redéploiement de Trendx : il suffit qu'il apparaisse dans
l'historique du tenant.

## `ExecutionStore` — le port

Méthodes du port, identiques pour les deux implémentations :

| Méthode | Effet |
|---|---|
| `create(record)` | crée ; refuse un `reference_key` déjà présent |
| `mark_started(execution_id)` | `PENDING -> RUNNING` |
| `mark_success(execution_id, provenance, *, completed_at=None)` | `RUNNING -> SUCCESS` |
| `mark_failed(execution_id, *, error_code, error_reason, provenance=None, completed_at=None)` | `RUNNING -> FAILED` |
| `get(execution_id)` | lecture unitaire |
| `get_by_reference_key(reference_key)` | lecture par clé d'idempotence |
| `list(...)` | lecture filtrée |
| `delete_if_unchanged(record)` | suppression compare-and-delete (ajouté par W107) |
| `restore_if_absent(...)` | restauration sans écrasement (ajouté par W108) |

## `MemoryExecutionStore` vs `DurableExecutionStore`

| | `MemoryExecutionStore` | `DurableExecutionStore` |
|---|---|---|
| Support | dictionnaire en mémoire | fichier JSON unique sur disque |
| Format | aucun | `{"format": 1, "records": {...}}` |
| Version de format | — | `FORMAT_VERSION = 1` |
| Perte au redémarrage | **oui** | non |
| Multi-processus | non | oui, via `flock` |
| Verrouillage | `threading.RLock` | `RLock` + `fcntl.flock(LOCK_EX)` |
| Écriture atomique | non applicable | `tempfile` + `fsync` + `os.replace` |
| Usage prévu | tests, DryRun | production |

`MemoryExecutionStore` est un double de test, pas un mode dégradé. W100
documente le store durable.

## LIMITES ET SUIVIS

- `metadata` et `result` sont des dictionnaires libres validés comme JSON
  strict. Ils ne sont pas typés : une dérive de schéma y reste possible.
  *Suivi : typer `result` par version de schéma — hors périmètre W98.*
- `ExecutionProvenance.metadata` n'est pas persisté par le `DurableExecutionStore`
  au format de record, qui expose `feature_schema_*` au premier niveau.
  *Suivi : vérifier l'alignement des deux contrats — hors périmètre.*

## Vérification

```bash
.venv/bin/python -m pytest tests/unit/test_w98_forecast_execution_persistence.py -q
```

Documentation voisine : [W99](w99-execution-history.md),
[W100](w100-durable-execution-history.md).
