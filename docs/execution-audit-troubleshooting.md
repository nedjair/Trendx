# Troubleshooting — chaîne d'exécution et d'audit

Diagnostic opérateur pour la chaîne W99–W115. Chaque symptôme est rattaché à
des reason codes et des codes d'erreur **réellement présents dans le code**.

## Prérequis de tout diagnostic

```bash
# 1. santé du service
curl -s localhost:8080/health

# 2. santé du journal d'audit, pour le tenant du déploiement
curl -s -H "Authorization: Bearer $TRENDX_API_TOKEN" \
  localhost:8080/api/v1/forecast/executions/audit/health

# 3. readiness — le verdict d'exploitation
curl -s -H "Authorization: Bearer $TRENDX_API_TOKEN" \
  localhost:8080/api/v1/forecast/executions/audit/readiness
```

---

## Symptôme : l'historique d'exécutions est indisponible

`503 execution_history_unavailable`

| Vérification | Commande | Attendu |
|---|---|---|
| Auth | appel sans token | `401 Missing authentication credentials` |
| Tenant | `TRENDX_DEFAULT_TENANT_ID` | non vide, sans blanc, sinon `403 tenant_context_unavailable` |
| Datastore | `TRENDX_EXECUTION_HISTORY_PATH` | fichier existant, non symlink |
| Format | contenu du fichier | `{"format": 1, "records": {...}}` |
| Health | `/audit/health` | `HEALTHY` ou `DEGRADED` |
| Readiness | `/audit/readiness` | `READY` |
| Intégrité | `/audit/integrity` | `VALID` |

Causes réelles de ce `503` :

- `execution_history_service_factory` absente du câblage applicatif ;
- la factory lève (fichier absent, chemin non fichier, symlink) ;
- la factory renvoie `None`.

Un store **corrompu** (JSON invalide, `format` ≠ 1, record malformé) produit
aussi ce `503` : le store n'est jamais renvoyé partiellement.

---

## Symptôme : une archive est corrompue

| Vérification | Attendu |
|---|---|
| `/audit/integrity` | statut `INVALID` ou `ERROR`, avec findings |
| `/audit/health` → `reason_codes` | `ARCHIVE_CORRUPTED` et/ou `AUDIT_DATA_CORRUPTED` |
| `/audit/health` → statut d'archive | `CORRUPTED` |
| `/audit/readiness` | `NOT_READY` avec `ARCHIVE_CORRUPTED` |
| vérification unitaire d'un document | `verified: false`, `error_code: archive_corrupt` |

L'archive **n'est jamais réparée automatiquement**. Un document corrompu
n'est pas lu comme valide : `get()` lève `AuditArchiveIntegrityError`, et un
`restore` répond `INTEGRITY_FAILURE`. Rien n'est restauré.

---

## Symptôme : un restore est refusé

Réponses possibles, toutes réelles :

| Statut métier | HTTP | Signification | Vérification |
|---|---|---|---|
| `NOT_FOUND` | 404 | aucun document archivé pour cet `event_id` | `archive_not_found` |
| `INTEGRITY_FAILURE` | 422/503 | archive corrompue, checksum, schéma ou version invalide | `archive_integrity_failure` |
| `TENANT_FORBIDDEN` | 403 | le tenant du document ≠ tenant authentifié | `tenant_forbidden` |
| `CONFLICT` | 409 | identité active en conflit | conflit d'identité |
| `ALREADY_PRESENT` | 200 | l'événement est déjà actif — **ce n'est pas une erreur** | rien à faire |
| `RESTORE_FAILED` | — | échec du backend de restauration | cause backend |

Points de contrôle :

1. **Tenant** — `tenant_id` du corps/requête égal au tenant du contexte, sinon
   `403 tenant_forbidden` avant toute résolution de backend.
2. **Existence** — `NOT_FOUND` si l'`event_id` n'est pas dans l'archive.
3. **Checksum** — le conteneur d'archive et le checksum de l'événement W110
   doivent tous deux correspondre.
4. **Schéma / version** — `event_version` doit valoir `"1"`, `archive_version`
   doit valoir `1`.
5. **Déjà présent** — un restore répété donne `ALREADY_PRESENT`, pas une
   duplication.

---

## Symptôme : un purge ne supprime rien

Un `purge` qui rapporte `purged: 0` n'est pas un bug. Causes réelles, dans
l'ordre de fréquence :

| Cause | Champ du rapport |
|---|---|
| `dry_run` est vrai (**défaut**) | `status: DRY_RUN` |
| politique non satisfaite | `eligible: 0` |
| `minimum_events_to_keep` protège les plus récents | `protected: N` |
| archive absente et `archive_before_purge=false` | `guards: {"archive_missing": N}` |
| archive corrompue | `guards: {"archive_corrupt": N}` |
| checksum d'archive incohérent | `guards: {"checksum_mismatch": N}` |
| archive déjà présente | `guards: {"already_archived": N}` |
| conflict d'identité au compare-and-delete | `guards: {"conflict": N}` |
| événement modifié entre sélection et suppression | `conflicts: N` |
| événement protégé par statut | `guards: {"protected": N}` |

`reference_time` est obligatoire et sert de référence de rétention ; une
rétention `N` jours sur un `reference_time` ancien ne sélectionne rien.

**Aucun purge automatique n'existe.** Une opération de lifecycle n'est
déclenchée que par un appel HTTP authentifié et explicite.

### Les événements d'auto-audit influencent le résultat

Chaque `archive` et chaque `purge` enregistre **son propre** événement
d'audit (`AUDIT_ARCHIVE`, `AUDIT_PURGE`). Ces événements sont eux-mêmes
soumis à la rétention. Conséquence observée : un second `purge` exécuté peu
après supprime exactement **un** événement, celui du `purge` précédent, et
aucun événement métier. Un opérateur peut donc voir `purged: 1` alors qu'il
n'a demandé la suppression d'aucun événement métier. C'est attendu, et c'est
la cause pour laquelle `minimum_events_to_keep` peut sembler avoir un effet
different de l'intuition.

---

## Symptôme : readiness `NOT_READY`

`/audit/readiness` renvoie `readiness_status` et deux ensembles : `reason_codes`
(toutes) et `blocking_reason_codes`. Seuls ces reason codes font échouer la
readiness :

| Reason code | Signification |
|---|---|
| `AUDIT_STORE_UNAVAILABLE` | le journal d'audit ne peut pas être lu |
| `AUDIT_SCHEMA_INVALID` | un document ne respecte pas le schéma attendu |
| `AUDIT_DATA_CORRUPTED` | document illisible ou checksum invalide |
| `ARCHIVE_UNAVAILABLE` | l'archive ne peut pas être atteinte |
| `ARCHIVE_CORRUPTED` | l'archive contient des documents invalides |
| `TENANT_CONTEXT_INVALID` | le tenant du contexte est inutilisable |
| `INTEGRITY_CHECK_FAILED` | la vérification W111 échoue |

Reason codes **informationnels** — ils nbloquent pas la readiness :

| Reason code | Signification |
|---|---|
| `AUDIT_JOURNAL_EMPTY` | journal vide, état valide |
| `ARCHIVE_NOT_CONFIGURED` | aucune archive configurée |
| `CAPACITY_MEASUREMENT_INCOMPLETE` | métrique de capacité non mesurable |

---

## Symptôme : `EMPTY` alors qu'on attend `HEALTHY`

**Un journal vide est valide, pas défaillant.** `AuditOperationalStatus`
distingue `EMPTY` de `HEALTHY` volontairement, et `AUDIT_JOURNAL_EMPTY` est
déclaré comme informationnel : un journal vide ne doit jamais déclencher une
alerte.

Ne pas traiter `EMPTY` comme une panne. C'est l'état attendu d'un tenant qui
n'a encore rien produit.

---

## Symptôme : les métriques de capacité changent entre deux appels

Le bloc `filesystem` de `/audit/capacity` (`filesystem_free_bytes`,
`filesystem_used_bytes`, `inode_free`) est une mesure `statvfs` **du hôte**. Il
bouge dès que quoi que ce soit d'autre touche le disque. Deux processus qui
lisent le **même journal inchangé** à deux instants différents peuvent donc
légitimement différer sur ces champs.

Ces métriques sont **environnementales**, ce n'est pas un invariant métier. Tous les
autres champs de `capacity` — effectifs d'événements, octets actifs et
archivés, bornes d'âge, statut, reason codes — sont en revanche des faits
métier et doivent être stables.

Si la métrique n'est pas obtenable, elle vaut `None` et le champ
`capacity_measurement_complete` passe à faux. Rien n'est inventé pour combler.

---

## Symptôme : `403 tenant_forbidden` alors que le client est légitime

| Cause | Vérification |
|---|---|
| `?tenant_id=` différent du contexte | retirer le paramètre, il doit venir du contexte |
| `TRENDX_DEFAULT_TENANT_ID` nonaligné sur le tenant attendu | vérifier la valeur sur **chaque** instance |
| restore : tenant du corps ≠ tenant du contexte | corriger le corps |
| restore : document archivé appartenant à un autre tenant | le document est réellement hors périmètre |

Le modèle est mono-tenant par déploiement : aligner la configuration est la
première chose à vérifier, pas le code.

---

## Symptôme : `503 recovery_service_unavailable` sur `POST /executions/restore`

`TRENDX_EXECUTION_ARCHIVE_PATH` est vide ou pointe un répertoire absent.
La factory de recovery refuse de démarrer, et la route échoue **fermement** :
elle ne pretendra jamais avoir restauré.

---

## Symptôme : `405` sur `GET /executions/restore`

`restore` est un `POST`. Un `GET` sur ce chemin est explicitement rejeté par
`reject_reserved_restore_read`, afin qu'une lecture accidentelle ne soit pas
confondue avec une restauration.

---

## Suivre une requête de bout en bout

Chaque requête reçoit un `request_id` (`X-Request-ID` s'il est fourni et
valide, sinon généré). Il est propagé :

- dans la ligne d'observabilité `execution_history operation=… status_code=…` ;
- dans le champ `request_id` des événements d'audit ;
- comme filtre de la requête d'audit.

```bash
# retrouver toutes les traces d'un request_id
curl -s -H "Authorization: Bearer $TRENDX_API_TOKEN" \
  "localhost:8080/api/v1/forecast/executions/audit?request_id=req-1234"
```

---

## Ce qui n'est pas un défaut

| Comportement observé | Pourquoi c'est normal |
|---|---|
| `EMPTY` sur un tenant neuf | journal vide valide |
| `purged: 1` après un second purge | auto-audit du purge précédent |
| `ALREADY_PRESENT` sur un restore répété | contrat d'idempotence |
| métriques `filesystem` instables | mesure `statvfs` de l'hôte |
| `ARCHIVE_NOT_CONFIGURED` | archive non configurée, pas une panne |
| `503` sur restore sans archive configurée | fail-closed volontaire |
