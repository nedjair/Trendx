# W107 — Cycle de vie de l’historique d’exécution

## Objet et périmètre

W107 ajoute une couche de maintenance explicite autour du `DurableExecutionStore`
existant. La couche ne lit jamais le fichier JSON W100 directement et ne
réimplémente pas W99, W104 ou W106. Elle dépend d’un port de lecture/écriture
d’exécution, d’un port de purge atomique et d’un `ArchiveStore`. W100 conserve
son format et ses lectures ; W107 ajoute seulement la capacité additive
`delete_if_unchanged` aux implémentations de store qui doivent autoriser une
suppression atomique.

Aucune route de purge, aucun scheduler, aucun worker et aucune activation de
production ne font partie de W107. Les opérations réelles ne sont donc jamais
déclenchées implicitement.

## Politique de rétention

`ExecutionRetentionPolicy` est un contrat injecté, sans période métier implicite :

- `retention_days` est obligatoire et doit être un entier positif ou nul ;
- `archive_before_purge=True` est le défaut conservateur ;
- `minimum_records_to_keep=0` est le défaut technique ;
- `dry_run=True` est le défaut : preview, validation et appel de politique ne
  modifient ni le datastore actif ni les archives.

La référence de rétention est `created_at`, en UTC. Le calcul est :

```text
eligible_at = created_at + retention_days
```

La borne est inclusive : un record est candidate lorsque
`eligible_at <= now`. L’horloge est injectée ; la logique testable n’appelle
pas `datetime.now()`.

Les statuts `PENDING` et `RUNNING` sont toujours protégés. Un statut inconnu
et un `created_at` invalide sont ignorés avec une reason explicite. Les
statuts `SUCCESS` et `FAILED` peuvent être candidates. Les records les plus
récents sont protégés si `minimum_records_to_keep` est supérieur à zéro.

Chaque décision est sérialisée dans `ExecutionLifecycleReport.decisions` avec
son état (`eligible`, `protected`, `skipped`), sa reason et `eligible_at`.

## Archive

`FileSystemArchiveStore` utilise un fichier JSON textuel par exécution, avec :

- `archive_format_version` ;
- le `ExecutionRecord` complet, sans introduire de champ ;
- la politique, la reason, le tenant, l’identifiant et les timestamps ;
- un SHA-256 du record et un SHA-256 du contenu document ;
- un schéma strict : tous les champs W98 sont présents, sans champ superflu,
  et le round-trip canonique est vérifié ;
- des permissions `0700` pour le répertoire et `0600` pour les fichiers ;
- un refus de lecture si le propriétaire ou les permissions sont trop
  permissifs.

Le nom de fichier est le SHA-256 de l’`execution_id`. Les IDs contenant
`../`, des chemins absolus, des octets nuls ou des chaînes de tenant ne
peuvent donc pas provoquer de traversal.

L’écriture utilise un fichier temporaire, `flush`/`fsync`, remplacement
atomique, `fsync` du répertoire parent et nettoyage du temporaire en cas
d’erreur. Une archive existante n’est jamais écrasée silencieusement : record,
tenant et policy doivent correspondre. Une archive corrompue est refusée. Si le
`fsync` du répertoire échoue après publication, l’archive peut être visible
mais l’opération reste en échec et le record source est conservé ; une
relance vérifie l’archive avant toute suppression.

## Ordre des opérations

La purge réelle suit obligatoirement :

```text
SELECT/SCAN → ELIGIBILITY → ARCHIVE → VERIFY → COMPARE-AND-DELETE
```

Un échec d’archive, une archive corrompue, un checksum invalide, un record
modifié ou une erreur de suppression laisse le record actif conservé. La
suppression utilise `delete_if_unchanged` sous le verrou du store W100 ; un
concurrent ne peut donc pas supprimer une version différente de celle qui a
été archivée.

`purge_eligible()` est idempotent. Une archive identique est réutilisée et une
seconde purge ne recrée pas d’archive. Si un processus concurrent a déjà
supprimé le record, le processus perdant vérifie l’archive et rapporte
`already_purged`, sans erreur ni seconde suppression. Une archive différente
ou corrompue provoque un échec contrôlé.

## Preview et lecture

`preview()` force `dry_run=True`, même si la politique reçue est en mode réel.
Le rapport indique les records scannés, candidates, protégés, déjà archivés,
ignorés, les IDs concernés, les décisions et les erreurs. Un preview vérifie
éventuellement une archive existante mais ne crée aucun fichier définitif.
L’adaptateur d’archive initialise son répertoire uniquement lors d’une
écriture réelle ; un preview contre un répertoire absent ne le crée pas.

Les archives ne sont pas fusionnées dans W99, W104 ou W106. Après une purge,
les vues actives montrent uniquement les records encore présents dans le
store actif. La lecture d’une archive se fait uniquement via `ArchiveStore`.

## Observabilité et sécurité

Le logger existant émet uniquement des événements `operation=lifecycle` avec
`outcome=preview`, `archive`, `purge` ou `error`. Les IDs, payloads, chemins,
credentials et métadonnées sensibles ne sont pas journalisés.

## Exemple de test explicite

```python
policy = ExecutionRetentionPolicy(
    retention_days=30,
    dry_run=True,
)
```

La valeur `30` est uniquement une fixture de test ; elle ne constitue pas une
politique métier ou une valeur de production.

## Limites W107

- pas de scheduler ou purge automatique ;
- pas d’API destructive ;
- pas de migration de schéma ;
- pas de nouveau datastore, cache, modèle, artifact ou writeback ;
- pas de promesse de transaction distribuée au-delà du verrou et du
  compare-and-delete du store concret ;
- une archive existante sous une policy différente reste conservée mais
  n’est pas réutilisée pour une purge.
