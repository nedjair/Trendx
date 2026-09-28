# Chaîne d'exécution et d'audit — guide opérateur

Référence opérateur pour la chaîne de prévision **W98 → W115**, certifiée
`PASS-W115-EXECUTION-AUDIT-PRODUCTION-READINESS` sur `d9a01d7`.

## Réponses rapides

**Que stocke Trendx ?** Des exécutions de prévision (qui a demandé quoi, pour
quelle entité, quelle métrique, avec quel modèle, quel résultat) et un journal
d'audit opérationnel (qui a fait quoi, quand, avec quel résultat).

**Où sont stockées les exécutions ?** Un fichier JSON unique, chemin
`TRENDX_EXECUTION_HISTORY_PATH`, format `{"format": 1, "records": {...}}`,
écrit de façon atomique.

**Où sont stockés les événements d'audit ?** Un répertoire d'un document par
événement, chemin `TRENDX_EXECUTION_AUDIT_PATH`, plus une archive au
`TRENDX_EXECUTION_AUDIT_ARCHIVE_PATH`.

**Quelle différence entre historique d'exécution et audit ?**

| | Historique d'exécution (W99) | Audit (W110) |
|---|---|---|
| Unité | une **prévision** | un **fait opérationnel** |
| Clé | `tenant_id + entity_type + entity_id + target_metric` | `event_id` |
| Né quand | une prévision est demandée | quand une **opération** est tentée |
| Contenu | requête, modèle, prédictions, erreur | opération, issue, acteur, motif |
| Effet | métier | traçabilité, conformité, diagnostic |
| Cycle de vie | archive/purge/restore W107–W109 | archive/purge/restore W112 |

L'historique dit **ce qui a été prédit**. L'audit dit **ce que le système a
fait**. Un échec de prévision produit un enregistrement `FAILED` dans
l'historique *et* un événement `FAILED` dans l'audit : les deux Causes sont
distinguées par leur `source`.

**Consulter l'historique ?** `GET /api/v1/forecast/executions` — 17 filtres,
pagination bornée à 1000.

**Consulter les statistiques ?** `GET /api/v1/forecast/executions/statistics`.

**Consulter analytics / reporting ?**
`GET /api/v1/forecast/executions/analytics` (agrégats) et
`GET /api/v1/forecast/executions/report` (représentation versionnée,
déterministe).

**Vérifier l'intégrité ?** `GET /api/v1/forecast/executions/audit/integrity`.

**Archiver ?** `POST /api/v1/forecast/executions/audit/lifecycle/archive`.
**Prévisualiser ?** `POST .../lifecycle/preview` — ne modifie rien.
**Purger ?** `POST .../lifecycle/purge` — uniquement après archive vérifiée.
**Restaurer ?** `POST .../lifecycle/restore` et
`POST /api/v1/forecast/executions/restore`.

**Health / capacity / readiness ?**
`GET /api/v1/forecast/executions/audit/health`, `.../capacity`, `.../readiness`.

**Diagnostiquer une corruption ?** Intégrité d'abord, puis
`reason_codes` de health, puis readiness. Voir
[troubleshooting](execution-audit-troubleshooting.md).

**Diagnostiquer un problème de tenant ?** `403 tenant_forbidden` signifie un
`tenant_id` divergent du contexte ; `403 tenant_context_unavailable` signifie
`TRENDX_DEFAULT_TENANT_ID` vide. Le modèle est mono-tenant par déploiement :
aligner la configuration en premier.

**Comportements fail-closed.** Archive absente ou corrompue → rien n'est
restauré. Purge sans archive vérifiée → rien n'est supprimé. Contexte de tenant
vide → `403`. Restore d'un événement modifié entre-temps → `CONFLICT`. Métrique
non mesurable → `None`, jamais une valeur inventée.

**Comportements idempotents.** `reference_key` (création), archivage, purge,
restore, appels lifecycle répétés. Un restore répété donne `ALREADY_PRESENT`,
jamais une duplication.

**Opérations read-only.** Les 16 routes `GET` de la chaîne. La preuve : le
SHA-256 du store d'exécutions, du journal d'audit et de l'archive est inchangé
avant et après la chaîne de lecture complète.

**Opérations mutantes.** Exactement 5, toutes en `POST` : les quatre
`audit/lifecycle/*` et `POST /executions/restore`. Aucune suppression ni
modification n'est exposée en `GET`, `PUT`, `PATCH` ou `DELETE`.

**Flags de production.** Tous désactivés et vérifiés lors de la certification :
`TRENDX_SCHEDULER_FORECAST_ENABLED`, `TRENDX_INGEST_ENABLED`,
`TRENDX_WORKER_INGESTION_ENABLED`, `TB_WRITEBACK_ENABLED`, `TB_ALARMS_ENABLED`,
`ANOMALY_DETECTION_ENABLED`. Rien ne s'active automatiquement.

**Limites connues.** Mono-tenant par déploiement ; store mono-hôte sans
réplication ; coût d'écriture du store linéaire ; métriques `filesystem`
environnementales ; token API unique sans portée. Liste complète dans
[W115](w115-execution-audit-production-readiness.md).

## Table des phases

| Phase | Fonction | Module | API | Persistance | Mutant / read-only |
|---|---|---|---|---|---|
| W98 | Persistance des exécutions | `forecasting/execution.py` | — | mémoire (`MemoryExecutionStore`) | mutant |
| W99 | Historique des exécutions | `forecasting/history.py` | 7 routes GET | — | **lecture** |
| W100 | Durabilité de l'historique | `forecasting/execution.py` | — | fichier JSON atomique | mutant |
| W101 | API de l'historique | `forecasting/api.py` | adaptateur HTTP | — | **lecture** |
| W102 | *phase sans code* | — | — | — | — |
| W103 | Observabilité | `forecasting/api.py` | logs `execution_history` | — | **lecture** |
| W104 | Analytics | `forecasting/analytics.py` | GET `/executions/analytics` | — | **lecture** |
| W105 | Opérationnalisation analytics | *aucun module* | preuve opérationnelle | — | **lecture** |
| W106 | Reporting | `forecasting/reporting.py` | GET `/executions/report` | — | **lecture** |
| W107 | Lifecycle d'exécution | `forecasting/lifecycle.py` | — | archive d'exécutions | mutant |
| W108 | Recovery d'exécution | `forecasting/recovery.py` | — | archive d'exécutions | mutant |
| W109 | API de recovery | `forecasting/api.py` | POST `/executions/restore` | archive d'exécutions | mutant |
| W110 | Audit opérationnel | `forecasting/audit.py` | GET `/audit`, `/audit/statistics` | journal d'audit | **lecture** |
| W111 | Intégrité / réconciliation / export | `forecasting/audit_control.py` | GET `/audit/integrity`, `/reconciliation`, `/export`, `/export/checksum` | — | **lecture** |
| W112 | Lifecycle d'audit | `forecasting/audit_lifecycle.py` | POST `/audit/lifecycle/*` | archive d'audit | mutant |
| W113 | Health / capacity / readiness | `forecasting/audit_health.py` | GET `/audit/health`, `/capacity`, `/readiness` | — | **lecture** |
| W114 | Cohérence bout en bout | `forecasting/audit_e2e.py` | *aucune route* | — | **lecture** |
| W115 | Production readiness | `forecasting/api.py` (déclaratif) | *aucune route* | — | — |

**W102 n'a jamais eu de code** : le numéro a été sauté. Voir
[W101 §W102](w101-execution-history-api.md#w102--phase-sans-code).

## Documentation de la chaîne

| Phase | Document |
|---|---|
| W98 | [Persistance des exécutions](w98-forecast-execution-persistence.md) |
| W99 | [Historique des exécutions](w99-execution-history.md) |
| W100 | [Historique durable](w100-durable-execution-history.md) |
| W101 | [API de l'historique](w101-execution-history-api.md) |
| W102 | [phase sans code](w101-execution-history-api.md#w102--phase-sans-code) |
| W103 | [Observabilité](w103-execution-history-observability.md) |
| W104 | [Analytics](w104-execution-history-analytics.md) |
| W105 | [Opérationnalisation](w105-analytics-operationalization.md) |
| W106 | [Reporting](w106-execution-history-reporting.md) |
| W107 | [Lifecycle d'exécution](w107-execution-history-lifecycle.md) |
| W108 | [Recovery](w108-execution-history-recovery.md) |
| W109 | [API de recovery](w109-execution-history-recovery-api.md) |
| W110 | [Audit opérationnel](w110-execution-history-operational-audit.md) |
| W111 | [Intégrité, export](w111-execution-audit-integrity-export.md) |
| W112 | [Lifecycle d'audit](w112-execution-audit-lifecycle.md) |
| W113 | [Health opérationnel](w113-execution-audit-operational-health.md) |
| W114 | [Cohérence bout en bout](w114-execution-audit-e2e-consistency.md) |
| W115 | [Production readiness](w115-execution-audit-production-readiness.md) |

Guides transverses :

| Sujet | Document |
|---|---|
| Toutes les routes | [Référence API](execution-audit-api-reference.md) |
| Auth, tenant, sanitisation | [Référence de sécurité](execution-audit-security.md) |
| Diagnostic opérateur | [Troubleshooting](execution-audit-troubleshooting.md) |
| Traçabilité phase / code / tests / API | [Matrice de documentation croisée](execution-audit-cross-reference.md) |

> La **source de vérité** de l'API est l'OpenAPI généré par l'application
> (`GET /openapi.json`). La [référence API](execution-audit-api-reference.md)
> en est une dérivation vérifiée sur `d9a01d7`.
