# Garde-fous ingestion Phase 3

Avant TOUTE ingestion (backfill ou incrémentale) : périmètre limité d'abord.
Règles AGENTS : ingestion incrémentale, idempotente, paginée, reprenable, avec
checkpoints, fenêtre de recouvrement, upsert, déduplication, table d'erreurs.
Jamais tout l'historique en mémoire.

## 1. Périmètre limité d'abord (premier run de validation)

1. Un seul tenant (celui du compte de service).
2. Quelques devices seulement — filtrage explicite par liste de `entity_id` en
   Phase 3 (pas encore de filtre natif dans `IngestionService.run_incremental_ingest` ;
   lancer `ingest_device_metric` sur 1 à 3 clés choisies).
3. Fenêtre courte : 24 à 72 h récentes (`start_ts = now - 72h`).
4. Objectif : valider flux complet (API → validation → upsert → dédup → checkpoints)
   avant tout backfill.

## 2. Limitation du débit vers l'API ThingsBoard

- `TB_PAGE_SIZE` (défaut 100) borne chaque page.
- `TB_RETRY_MAX_ATTEMPTS` (défaut 5) + `TB_RETRY_BACKOFF_SECONDS` (défaut 2) :
  backoff exponentiel côté `ThingsBoardClient._request`.
- `TB_REQUEST_TIMEOUT_SECONDS` (défaut 30) : timeout par requête.
- La lecture SQL des fenêtres est bornée : pagination par fenêtre temporelle,
  `LIMIT`, curseur serveur, `statement_timeout` (rôle `trendx_ro` = 60 s).
- Aucune requête Trendx ne doit dégrader ThingsBoard (prioritaire sur les
  ressources de l'hôte) : le worker reste seul consommateur Canal 1, séquence de
  fenêtres, pas de parallélisme par défaut.

## 3. Points de reprise

- Checkpoints par device/metric (`CheckpointRepository`, table d'eau) ;
  en l'absence de table checkpoint dans le schéma Trendz 1.15.0, la reprise se
  fait par fenêtres temporelles bornées : relancer la même fenêtre est idempotent
  (upsert + déduplication `_deduplicate`).
- Fenêtre de recouvrement : 1 h de chevauchement entre deux runs incrémentaux.
- Table d'erreurs / dead-letter (`_write_dead_letter`) : un échec de device ne
  bloque pas les autres ; statut par device / metric / fenêtre / job.
- Arrêt/reprise : tout run peut être interrompu et relancé sans double ingestion
  (upsert sur clé tenant_id + entity_type + entity_id + metric_name + ts).

## 4. Arrêt automatique sous TRENDX_DISK_MIN_FREE_GB (test réel)

- `IngestionService.ensure_disk_available()` vérifie `/` et `/var/lib/docker`
  (mounts présents) avant chaque run et avant chaque device/metric
  (`check_disk_min_free`, seuil `TRENDX_DISK_MIN_FREE_GB`, défaut 50).
- Sous le seuil → `DiskCapacityError` : le run s'arrête immédiatement, loggué,
  aucun point de télémétrie supplémentaire écrit.
- **Test réel exigé avant tout backfill** :
  1. `TRENDX_DISK_MIN_FREE_GB` très élevé dans `.env` (ex. 999999) ;
  2. recréer le worker (`docker compose up -d --no-deps --force-recreate worker`) ;
  3. déclencher une ingestion (via job ou appel direct `IngestionService`) ;
  4. constater l'arrêt immédiat (`DiskCapacityError` au log du worker, aucun
     INSERT dans `trendx_analytics.ts_kv`) ;
  5. remettre la valeur 50 et recréer le worker ; vérifier la reprise idempotente.

## 5. Contrôles pré-ingestion (make doctor)

- `trendx_ro` : INSERT refusé sur `thingsboard` (lecture seule).
- Auth ThingsBoard : `scripts/tb_auth_check.py` (`OK`/`SKIP`, jamais de secret).
- Espace libre `/` et `/var/lib/docker` >= `TRENDX_DISK_MIN_FREE_GB`.
- Search paths des moteurs (aucun schéma Trendx sur `tb_readonly`).
- Liste blanche des bases PostgreSQL (thingsboard, trendx, postgres, template*).
