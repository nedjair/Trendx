# W101 — API de l'historique d'exécutions

## Objet et périmètre

W101 expose l'historique W99/W100 sur HTTP. W101 ajoute **un adaptateur mince** :
il ne calcule rien, ne décide rien, et ne connaît pas le format sur disque. Il
résout un service depuis le câblage applicatif, applique la politique d'auth et
de tenant, puis délègue.

Code source : `src/trendx/forecasting/api.py` (routeur
`/api/v1/forecast/executions`).
Tests : `tests/unit/test_w101_execution_history_api.py` (600 lignes,
commit `0c4d533`).

## Principes

1. **Le client ne choisit jamais son tenant.** Le paramètre `tenant_id` est
   injectable dans l'URL, mais il est **écrasé** par le contexte de tenant. S'il
   est fourni et différent, la requête est refusée en `403`.
2. **Le client ne fournit jamais un chemin de datastore.** Tous les chemins
   viennent de la configuration serveur.
3. **Une erreur ne divulgue jamais sa cause interne.** Une exception de store
   devient un `503` au code stable, sans traceback ni chemin.
4. **Toute réponse passe par la frontière de redaction** : `_redact_json`
   remplace les clés de type credential par `[REDACTED]`, sans toucher aux
   données métier.

## Codes d'erreur

Ce sont les 17 codes stables de l'API (`_ApiErrorCode`) :

| Code | Statut typique |
|---|---|
| `execution_not_found` | 404 |
| `execution_history_unavailable` | 503 |
| `tenant_forbidden` | 403 |
| `tenant_context_unavailable` | 403 |
| `invalid_query` | 422 |
| `invalid_pagination` | 422 |
| `diagnostic_unavailable` | 503 |
| `recovery_service_unavailable` | 503 |
| `recovery_authentication_not_configured` | 503 |
| `audit_service_unavailable` | 503 |
| `audit_authentication_not_configured` | 503 |
| `invalid_audit_query` | 422 |
| `audit_lifecycle_unavailable` | 503 |
| `invalid_audit_retention` | 422 |
| `invalid_audit_restore` | 422 |
| `audit_restore_conflict` | 409 |
| `audit_health_unavailable` | 503 |

Tous sont en minuscules, sans espace. Ils sont disjoints des codes
opérationnels W113, qui sont en majuscules.

## Résolution du service

`get_execution_history_service` lit `request.app.state.execution_history_service_factory`.
Si la factory est absente, si elle lève, ou si elle renvoie `None`, la route
répond `503 execution_history_unavailable`. Le store n'est **jamais** créé à la
volée par une requête : les factories sont paresseuses et le chemin doit déjà
exister.

## Configuration

| Setting | Alias | Rôle |
|---|---|---|
| `trendx_execution_history_path` | `TRENDX_EXECUTION_HISTORY_PATH` | fichier d'historique |

Documentation voisine : [W99](w99-execution-history.md),
[W100](w100-durable-execution-history.md), [W102](#w102--phase-sans-code),
[W103](w103-execution-history-observability.md).

---

## W102 — phase sans code

**W102 n'existe pas.** Le numéro n'a jamais été utilisé : aucun commit, aucun
fichier source, aucun test et aucun document ne référence `W102` dans
l'historique git de `fix/ml-w49-master-conflicts`.

Le commit `b3fdb64` (`test(forecasting): harden execution history contract`)
est le seul commit sans numéro de phase entre W101 et W103 : il durcit le
contrat de `api.py` et introduit `tests/unit/test_w103_execution_history_observability.py`.

W102 est donc **un numéro sauté**, pas une fonctionnalité manquante et non
documentée. Rien n'est inventé ici, et il n'y a pas de "FOLLOW-UP" de code à
ouvrir : il n'y a pas de contrat W102 à documenter.
