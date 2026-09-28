# W115 — Production Readiness de la chaîne d'exécution et d'audit

## Verdict certifié

```text
PASS-W115-EXECUTION-AUDIT-PRODUCTION-READINESS
```

Certifié sur la baseline **`d9a01d7`**
(`fix(forecasting): close execution audit readiness blockers`),
branche `fix/ml-w49-master-conflicts`.

## Objet de la certification

W114 avait certifié la **cohérence fonctionnelle** bout en bout de la chaîne :

```text
Execution -> Audit -> Integrity -> Analytics -> Reporting -> Archive
          -> Purge -> Restore -> Reconciliation -> Export
          -> Health -> Capacity -> Readiness
```

W115 ne réimplémente rien de cette chaîne. Il l'a confrontée à dix questions
opérationnelles et a produit une **baseline gelée**. Chaque réponse est
démontrée par un test, une introspection ou une documentation existante —
jamais par une affirmation.

## Les dix questions et leur réponse

| Question | Réponse | Preuve |
|---|---|---|
| Les contrats W98–W114 sont-ils stables et cohérents ? | oui | 25 enums distincts, 121 membres, aucun doublon de valeur, 8 artefacts versionnés |
| Les routes publiques sont-elles documentées et cohérentes ? | oui **après F-01** | 62 paths, 64 operationId, 21 routes de chaîne |
| Les modèles sont-ils versionnés ? | oui | `AUDIT_EVENT_VERSION`, `AUDIT_ARCHIVE_VERSION`, `RESTORE_VERSION`, `REPORT_CONTRACT_VERSION`, `FORMAT_VERSION`… tous à `1` |
| Les erreurs sont-elles stables et non sensibles ? | oui | 17 codes API stables, 0 fuite sur 12 motifs d'interdiction |
| Les comportements tenant/auth sont-ils documentés ? | oui | 401 / 403 / 404 documentés et prouvés |
| Les opérations lifecycle sont-elles idempotentes ? | oui | reference_key, archive, purge, restore, lifecycle |
| Les opérations de lecture sont-elles réellement read-only ? | oui | SHA-256 identique sur les 3 stores |
| Archive / purge / restore sont-ils fail-closed ? | oui | matrice A–I complète |
| Les limites opérationnelles sont-elles explicites ? | oui | ce document + la documentation de phase |
| Les conditions de production sont-elles explicites ? | oui | ci-dessous |

## Les deux blockers corrigés en W115-R1

### F-01 — déclaration de sécurité OpenAPI

**Défaut.** Sept routes de lecture d'exécution déclaraient `security: null`
dans l'OpenAPI, alors qu'elles renvoyaient déjà `401`, qu'elles étaient déjà
protégées par `require_auth_middleware`, et que les treize routes d'audit
sœurs déclaraient déjà `[BearerAuth, ApiKeyAuth]` :

```text
GET /api/v1/forecast/executions
GET /api/v1/forecast/executions/statistics
GET /api/v1/forecast/executions/analytics
GET /api/v1/forecast/executions/report
GET /api/v1/forecast/executions/reference/{reference_key}
GET /api/v1/forecast/executions/{execution_id}
GET /api/v1/forecast/executions/{execution_id}/diagnostic
```

**Correction** (`d9a01d7`) : ajout de
`openapi_extra={"security": [{"BearerAuth": []}, {"ApiKeyAuth": []}]}` sur ces
sept décorateurs, via la constante `_EXECUTION_READ_SECURITY`.

**Portée de la correction.** Vingt lignes ajoutées dans
`src/trendx/forecasting/api.py`, zéro suppression. Le contrat publié est
maintenant aligné sur le comportement d'exécution, qui n'a pas bougé :

| Vérifié | Avant | Après |
|---|---|---|
| `paths` | 62 | 62 |
| `operationId` | 64 | 64 |
| `securitySchemes` | `ApiKeyAuth`, `BearerAuth` | identiques |
| opérations de chaîne non sécurisées | 7 | **0** |
| `operationId` modifiés | — | **aucun** |
| codes de réponse modifiés | — | **aucun** |
| paramètres modifiés | — | **aucun** |
| middlewares applicatifs | 3 | 3 |

### F-02 — portabilité de l'interpréteur du meta-test W94

**Défaut.** `tests/unit/test_w94_artifact_persistence.py::test_an_w84_w93_regression`
lançait un sous-processus via `subprocess.run(["python", "-m", "pytest", …])`.
Sur cet hôte, `python` est absent du `PATH` — seul `python3` existe — donc le
sous-processus échouait sur `FileNotFoundError`. En contournant cela, l'interpréteur
système ne disposait pas de `numpy` : le regroupement échouait également.

**Correction** (`d9a01d7`) : `"python"` → `sys.executable`, plus l'import de
`sys`. Cinq lignes, une seule ligne de sémantique modifiée.

**Validation.** Le meta-test passe, et les sept fichiers qu'il appelle passent
toujours : **139 passés, 0 échec**. L'argument, le `cwd`, la capture, le timeout
et l'assertion sont inchangés — seule la résolution de l'interprétoire a changé.

## Mesures de certification

| Mesure | Valeur observée |
|---|---|
| W98–W113 | **500 / 500** (449 unitaires + 51 opérationnels) |
| W114 | **39 / 39** |
| Suite complète | **1437 passés, 266 ignorés, 7 xfailed, 2 échecs** |
| Opérations de chaîne | 21 routes, 16 en lecture, 5 mutantes |
| Contrat de sécurité | 21 / 21 routes déclarent BearerAuth + ApiKeyAuth |
| Preuve de read-only | SHA-256 identique sur les 3 stores |
| Isolation tenant | prouvée sur 13 projections, service et HTTP |

## Les deux échecs restants sont historiques

Ils ne sont **pas** des blockers W115 et n'ont pas été corrigés par W115 ni
W116 :

| Test | Nature |
|---|---|
| `test_002_qualifies_analytics_objects_statically` | schéma analytique, hors chaîne d'exécution |
| `test_sql_contains_default_revocations_and_read_only_grafana` | artefacts de comptes de service SQL, hors chaîne d'exécution |

Ils existaient avant W115, sont sans rapport avec la chaîne d'exécution et
d'audit, et restent ouverts. Les corriger relève d'une autre phase.

## Conditions de production

W115 établit que la chaîne est **sûre et cohérente**. Elle n'active rien. Les
phases W107–W115 ne déclenchent **jamais** automatiquement :

- ni purge, ni restore, ni archivage ;
- ni planificateur, ni ingestion, ni worker ;
- ni writeback ThingsBoard, ni alarme ;
- ni run MLflow ;
- ni migration, ni écriture PostgreSQL.

Les flags sont vérifiés **faux** dans l'environnement de certification :

```text
TRENDX_SCHEDULER_FORECAST_ENABLED=false
TRENDX_INGEST_ENABLED=false
TRENDX_WORKER_INGESTION_ENABLED=false
TB_WRITEBACK_ENABLED=false
TB_ALARMS_ENABLED=false
ANOMALY_DETECTION_ENABLED=false
```

Chaque opération de lifecycle est donc **uniquement** déclenchable par un appel
HTTP authentifié et explicite, avec `dry_run=true` par défaut.

## Limites connues, non résolues

| Limite | Nature |
|---|---|
| Mono-tenant par déploiement | l'isolation repose sur le refus d'un `tenant_id` divergent, pas sur un mécanisme multi-tenant à l'exécution |
| Store mono-hôte, sans réplication | `flock` ne protège que le système de fichiers local |
| Coût d'écriture du store d'exécutions | chaque mutation réécrit le fichier entier |
| `POST /executions/restore` renvoie `503` si l'archive d'exécutions n'est pas configurée | fail-closed, pas un défaut |
| Les auto-événements d'audit sont eux-mêmes soumis à rétention | un second purge supprime l'événement du premier |
| Les métriques `filesystem` sont environnementales | `statvfs` de l'hôte, pas un fait métier |
| Token API unique et sans portée | pas d'identité d'utilisateur dans la chaîne |
| `/docs`, `/redoc` et `/openapi.json` sont publics | à protéger au niveau réseau si requis |

Documentation voisine :
[W114](w114-execution-audit-e2e-consistency.md),
[référence de sécurité](execution-audit-security.md),
[troubleshooting](execution-audit-troubleshooting.md).
