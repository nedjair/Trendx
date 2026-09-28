# W103 — Observabilité de l'historique d'exécutions

## Objet et périmètre

W103 rend l'historique **observable** : chaque opération d'API émet une ligne
de log structurée et minimale. W103 n'ajoute ni métrique, ni trace distribuée,
ni nouvelle plateforme de logging : il utilise le logger du projet.

Code source : `src/trendx/forecasting/api.py` (fonction `_observe`).
Tests : `tests/unit/test_w103_execution_history_observability.py` (362 lignes,
commit `b3fdb64`).

## Le contrat d'observation

Chaque opération d'API passe par :

```python
_observe(operation=<str>, outcome=<str>, status_code=<int>)
```

qui produit exactement une ligne :

```text
execution_history operation=<operation> outcome=<outcome> status_code=<code>
```

Trois champs, jamais plus. Ce sont les champs réellement définis par le
contrat ; il n'y a pas de `duration_ms`, ni de `tenant_id`, ni de payload.

## Valeurs observées

Les **20** libellés d'opération réellement émis par la chaîne W99–W113 :

| Opération | Émet quand |
|---|---|
| `list` | lecture d'historique réussie ou en échec |
| `get` | lecture par `execution_id` |
| `get_by_reference` | lecture par `reference_key` |
| `diagnostic` | diagnostic d'échec |
| `statistics` | statistiques d'exécution |
| `analytics` | analytics |
| `analytics_query` | construction de la requête analytics rejetée |
| `report` | rapport |
| `query` | construction de requête rejetée |
| `audit` | requête d'audit |
| `audit_statistics` | statistiques d'audit |
| `audit_integrity` | intégrité |
| `audit_reconciliation` | réconciliation |
| `audit_export` | export |
| `audit_export_checksum` | checksum d'export |
| `audit_health` | health, capacity, readiness |
| `audit_lifecycle` | preview, archive, purge, restore d'audit |
| `restore` | appel de recovery, y compris ses refus |
| `service` | résolution du service de lecture |
| `tenant` | contexte de tenant absent ou rejeté |

`audit_health` sert les **trois** routes de diagnostic (health, capacity,
readiness) : elles partagent le même service et donc le même libellé.

Les trois derniers (`restore`, `service`, `tenant`) ne sont émis que sur des
chemins d'échec : un recovery non configuré, un store illisible, un contexte de
tenant vide. Leur absence d'une ligne de log signifie donc que le chemin
nominal s'est déroulé sans incident.

## Interdits

La ligne d'observabilité ne doit contenir **jamais** :

- un token, une API key, un mot de passe, un secret ;
- un corps de requête ou une charge utile métier ;
- un chemin de datastore ou d'archive ;
- une trace d'exception non assainie.

L'opérateur dispose donc de l'opération, de l'issue et du statut HTTP — et de
rien qui puisse être ré-identifié ou fuité.

## Exploitation

```text
# taux d'échec d'une opération
grep 'execution_history operation=audit ' access.log | grep -c 'status_code=4'

# distribution des issues d'une opération
grep -o 'operation=[a-z_]* outcome=[a-z_]*' access.log | sort | uniq -c
```

Documentation voisine : [W101](w101-execution-history-api.md),
[W104](w104-execution-history-analytics.md).
