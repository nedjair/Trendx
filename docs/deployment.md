# Plan de déploiement Docker — Trendx sur 10.0.0.1

**Version :** 1.1 — Phase 2 (corrections post-validation)
**Date :** 2026-08-02
**Profil par défaut :** `TRENDX_PROFILE=minimal`
**Décisions applicables :** B1, B2, B3, B4, B5, B6, validations Phase 2 conditionnelles

---

## 1. Prérequis hôte

| Élément | Valeur requise | État observé |
|---|---|---|
| OS | Ubuntu 22.04+ / Debian 12+ | ✅ Ubuntu 24.04.4 LTS |
| Docker | Engine 24+ / Compose v2+ | ✅ Docker 29.3.0 / Compose v5.1.0 |
| RAM | ≥ 16 GB | ✅ 62 GB |
| Disk libre | ≥ 50 GB | ✅ 374 GB |
| Python | 3.11+ | À vérifier sur hôte |
| Accès réseau | 10.0.0.1 | ✅ |
| Réseau Docker externe | `mobili_dahsboard_default` existant | ✅ |
| Volume PostgreSQL | Partage du volume `tb-postgres-data` (ThingsBoard) | ✅ Contrainte acceptée |

---

## 2. Réseau Docker existant ThingsBoard

Trendx se rattache au réseau Docker existant `mobili_dahsboard_default` :

```yaml
networks:
  mobili_dahsboard_default:
    external: true
    name: mobili_dahsboard_default
```

**B2 :** Connexion Canal 2 par nom de conteneur PostgreSQL TB (`mobili_dahsboard-postgres-1:5432`).
REFUSÉ exposition hôte 5433 et tunnel SSH.

---

## 3. Docker Compose — services (profil minimal)

```yaml
services:
  reverse-proxy:
    build:
      context: .
      dockerfile: docker/Dockerfile.ui
    image: trendx/reverse-proxy:latest
    container_name: trendx_reverse_proxy
    restart: unless-stopped
    ports:
      - "127.0.0.1:8443:8443"
    environment:
      VITE_API_BASE_URL: "http://api:8000"
    depends_on:
      api:
        condition: service_started
    networks:
      - trendx_edge
      - trendx_internal
    # TLS reporté à une phase ultérieure : HTTP simple sur 127.0.0.1,
    # chiffrement délégué au tunnel SSH. Aucun certificat embarqué.
    cpus: "0.5"
    mem_limit: 256m
    pids_limit: 100
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
    healthcheck:
      test: ["CMD-SHELL", "wget --no-verbose --tries=1 --spider http://127.0.0.1:8443/healthz || exit 1"]
      interval: 20s
      timeout: 10s
      retries: 10
      start_period: 30s
    volumes:
      - /opt/trendx/data/reverse-proxy/logs:/var/log/nginx:rw

  api:
    build:
      context: .
      dockerfile: docker/Dockerfile.api
    image: trendx/api:latest
    container_name: trendx_api
    restart: unless-stopped
    command: >
      uvicorn trendx.main:app
      --host 0.0.0.0
      --port 8000
      --workers 2
      --log-level info
    environment:
      PYTHONPATH: "/app/src:/app:$PYTHONPATH"
      PYTHONUNBUFFERED: "1"
      PYTHONDONTWRITEBYTECODE: "1"
      PG_ADMIN_HOST: mobili_dahsboard-postgres-1
    networks:
      - trendx_internal
      - mobili_dahsboard_default
    cpus: "1.0"
    mem_limit: 512m
    pids_limit: 200
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
    healthcheck:
      test: ["CMD", "python3", "-c", "import urllib.request,sys; r=urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3); sys.exit(0 if r.status==200 else 1)"]
      interval: 20s
      timeout: 10s
      retries: 10
      start_period: 30s
    volumes:
      - ./src:/app/src:ro
      - ./migrations:/app/migrations:ro
      - ./scripts:/app/scripts:ro
      - ./tests:/app/tests:ro
      - ./.secrets:/app/.secrets:ro
      - /opt/trendx/data:/opt/trendx/data:rw

  worker:
    build:
      context: .
      dockerfile: docker/Dockerfile.worker
    image: trendx/worker:latest
    container_name: trendx_worker
    restart: unless-stopped
    command: python3 -m trendx.services.worker
    environment:
      PYTHONPATH: "/app/src:/app:$PYTHONPATH"
      PYTHONUNBUFFERED: "1"
      PYTHONDONTWRITEBYTECODE: "1"
      SERVER_PORT: "${TRENDX_PYTHON_EXECUTOR_PORT:-8181}"
      SCRIPT_ENGINE_RUNTIME_TIMEOUT: "10000"
      EXECUTOR_MANAGER: "1"
      EXECUTOR_SCRIPT_ENGINE: "6"
      THROTTLING_QUEUE_CAPACITY: "10"
    user: "python-executor:1000"
    depends_on:
      api:
        condition: service_healthy
    networks:
      - trendx_internal
      - mobili_dahsboard_default
    cpus: "0.5"
    mem_limit: 512m
    pids_limit: 100
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
    healthcheck:
      test: ["CMD", "python3", "-c", "import os, sys, socket; s=socket.socket(); s.settimeout(2); r=s.connect_ex(('127.0.0.1', 8181)); sys.exit(0)"]
      interval: 20s
      timeout: 10s
      retries: 10
      start_period: 30s
    volumes:
      - ./src:/app/src:ro
      - ./scripts:/app/scripts:ro
      - ./.secrets:/app/.secrets:ro
      - /opt/trendx/data:/opt/trendx/data:rw

networks:
  trendx_internal:
    name: trendx_internal
    driver: bridge
  trendx_edge:
    name: trendx_edge
    driver: bridge
  mobili_dahsboard_default:
    external: true
    name: mobili_dahsboard_default

# Pas de volume nommé Trendx : bind mount direct /opt/trendx/data sur le worker
# Les sauvegardes sont dans /opt/trendx/data/backups/ sur l'hôte
# Aucun bloc `secrets:` ni certificat TLS dans le Compose : les secrets sont lus
# via `env_file: .env` (cf. security.md §1), le TLS est reporté (architecture.md §12).
```

---

## 4. Limites de ressources (obligatoires)

Chaque service déclare `cpus`, `mem_limit`, `pids_limit` et `logging max-size/max-file` (format court Docker Compose, identique à `docker-compose.trendx.yml`).

| Service | CPUs limit | Memory limit | PIDs limit | Logging |
|---|---|---|---|---|
| reverse-proxy | 0.5 | 256M | 100 | 10m × 3 |
| api | 1.0 | 512M | 200 | 10m × 3 |
| worker | 0.5 | 512M | 100 | 10m × 3 |

---

## 5. Ports et conflits

**B6 : AUCUN port ouvert** sur l'hôte pour les services Trendx.
Un seul point d'entrée : reverse-proxy Trendx HTTP 8443 sur `127.0.0.1` uniquement.
TLS reporté à une phase ultérieure — tant qu'il est absent, ne jamais publier ce
port ailleurs que sur `127.0.0.1` (chiffrement délégué au tunnel SSH).

| Service Trendx | Port interne | Port hôte | Conflit ? |
|---|---|---|---|
| reverse-proxy (HTTP) | 8443 | 127.0.0.1:8443 | Non |
| API | 8000 | interne seulement | N/A |
| Worker | 8181 | interne seulement | N/A |
| PostgreSQL Trendx | 5432 | Aucun (interne Docker) | N/A |

**Règle :** Aucun service Trendx ne publie de port sur `0.0.0.0` sauf le reverse-proxy sur 127.0.0.1:8443.

---

## 6. Accès ThingsBoard et Canal 2

**B2 :** Canal 2 PostgreSQL ThingsBoard accessible par :
- Nom conteneur : `mobili_dahsboard-postgres-1`
- Port interne : `5432`
- Base : `thingsboard`
- Réseau : `mobili_dahsboard_default`

**B1 :** Rôle `trendx_ro` autorisé avec droits stricts :
- `CONNECT` + `USAGE` sur base `thingsboard`
- `SELECT` uniquement
- `default_transaction_read_only = on`
- `statement_timeout = 60000`
- `idle_in_transaction_session_timeout = 30000`
- `CONNECTION LIMIT 5`
- Aucun droit de création, aucune propriété d'objet
- Mot de passe hors dépôt, stocké dans `/opt/trendx/.secrets/` (dir 700, fichiers 600, root)

---

## 7. Writeback et alarmes

**B3 — Writeback AUTORISÉ SOUS CONDITIONS — Phase 2 = dry-run uniquement :**
- `TB_WRITEBACK_ENABLED=false` par défaut
- Activation explicite requise
- Dry-run + validation liste clés par opérateur
- `TB_WRITEBACK_DEVICE_ALLOWLIST=ALG16025001`
- Une seule métrique : `mppt_main_battery_voltage_v`
- Horizon court
- Préfixe `_EPD_` obligatoire
- Écriture par API ThingsBoard uniquement, jamais par SQL direct
- Journal des clés écrites dans `writeback_log` (table)
- Runbook `docs/runbooks/writeback-rollback.md` obligatoire avant toute écriture

**B4 — Alarmes REFUSÉES pour l'instant :**
- `TB_ALARMS_ENABLED=false`
- Réexamen après démonstration interne >= 30 jours

---

## 8. Stockage analytique

**B5 — TimescaleDB REFUSÉ :**
- Aucune extension installée dans PostgreSQL production
- Utiliser partitionnement déclaratif par temps + index BRIN + tables d'agrégats alimentées par UPSERT incrémental
- Réexamen sur benchmark chiffré uniquement

**Rétention Phase 2 :**
- Brut `trendx_analytics.ts_kv` : 180 jours (`TRENDX_RETENTION_DAYS=180`)
- Agrégats horaires : 2 ans
- Agrégats jour/semaine : 5 ans
- Prédictions : 1 an
- Journaux : 90 jours

**Partitionnement :**
- Mensuel, pré-création à +3 mois par job
- PAS de partition DEFAULT
- Échec explicite si partition manquante
- DROP PARTITION pour purge

**Tables d'agrégats (Q3 — VALIDÉ avec correction, UPSERT incrémental) :**
- Horaire : toutes les 15 min sur les 3 dernières heures, UPSERT sur plage recalculée
- Jour : toutes les heures sur les 2 derniers jours, UPSERT
- Semaine/mois : 1x/jour hors pointe, UPSERT
- Watermark par agrégat (`trendx_analytics.aggregate_watermarks`)
- JAMAIS `REFRESH MATERIALIZED VIEW` (verrouille l'instance PostgreSQL partagée avec ThingsBoard)
- Job APScheduler : `aggregate_hourly` (IntervalTrigger 15 min), `aggregate_daily` (1 h), `aggregate_weekly` (cron 02:30 UTC)
- Fonctions : `trendx_analytics.refresh_aggregate('hourly|daily|weekly')`, propriétaire `trendx_migration`

**Partitionnement — automatisation APScheduler :**
- Job `partitions_monthly` (cron 1er du mois 00:15 UTC) + `partitions_boot_check` au démarrage du worker
- Fonction `trendx_analytics.ensure_partitions_forward(3)` (SECURITY DEFINER, exécutable par `trendx_app`)
- Contrôle : `trendx_analytics.partition_coverage(3)` — `make doctor` échoue si moins de 3 mois de partitions

---

## 9. Planificateur

**Correction C — APScheduler :**
- En Phase 2, le worker utilise APScheduler en mode standalone
- Pas de Redis nécessaire pour la planification
- Les jobs sont gérés en mémoire avec persistance dans `trendx_catalog.trendz_task`
- Justification : simplifier l'infrastructure en Phase 2, éviter dépendance externe

---

## 10. Procédure de déploiement (post-approbation Phase 2)

1. `cp .env.example .env && chmod 600 .env`
2. `make doctor` — vérifications pre-flight
3. `make build-images` — build 3 images (reverse-proxy, api, worker)
4. `make up-infra` — vérification PostgreSQL existant + réseau Docker
5. `make migrate` — création base `trendx` + schémas + tables
6. `make up-core` — démarrage API + worker + reverse-proxy
7. `make health` — vérification tous services

**Accès :**
- UI/API Trendx : `http://127.0.0.1:8443` (reverse-proxy HTTP — chiffrement via tunnel SSH, TLS reporté)

**Rollback :**
```bash
make down          # stop, conserve volumes
make down-clean    # ⚠ nécessite TRENDX_CONFIRM_APPLY=YES, détruit volumes
```

---

## 11. Points d'arrêt avant déploiement

- [ ] Création rôle `trendx_ro` sur PostgreSQL ThingsBoard (B1)
- [ ] Rattachement réseau `mobili_dahsboard_default`
- [ ] Validation writeback device test `ALG16025001` (B3) — dry-run uniquement
- [ ] Vérification `.env` permissions 600
- [ ] Vérification `TRENDX_DISK_MIN_FREE_GB` avant création volumes
- [ ] Runbook `docs/runbooks/writeback-rollback.md` validé
- [ ] Partition mensuelle +3 mois pré-créée
- [ ] Schéma toujours qualifié (`trendx_analytics.ts_kv`) dans toutes les requêtes

---

## 12. Volumes et chemins

| Volume / bind mount | Point de montage | Rôle |
|---|---|---|
| `/opt/trendx/data` (bind mount) | `/opt/trendx/data` (worker) | Données persistantes Trendx : backups, logs, données ingestion |
| `/opt/trendx/data/reverse-proxy/logs` (bind mount) | `/var/log/nginx` (reverse-proxy) | Journaux nginx |
| `tb-postgres-data` (partagé) | `/var/lib/postgresql/data` | Base `trendx` (schémas `trendx_catalog`, `trendx_analytics`) |

**Correction B — Montage direct host :** Le volume nommé `trendx_data` a été remplacé par un bind mount direct du chemin hôte `/opt/trendx/data`. Cela garantit que :
- Les sauvegardes (`/opt/trendx/data/backups/`) sont accessibles depuis l'hôte ET les conteneurs
- Le dernier dump connu est : `/opt/trendx/data/backups/20260802/trendx.dump.gz` (42 707 octets, 2026-08-02 15:44)
- Pas de divergence entre chemin hôte et point de montage conteneur

**Protection :** Arrêt automatique ingestion sous `TRENDX_DISK_MIN_FREE_GB` + alerte < 100 GB.

---

## 12bis. Sauvegardes et restauration

### Sauvegarde

- Cible : base `trendx` uniquement (jamais `thingsboard` — lecture seule)
- Convention : `/opt/trendx/data/backups/YYYYMMDD/trendx.dump.gz`
  (`pg_dump -Fc --no-owner --clean` pipé dans `gzip -9`, cible `make backup`)
- Droits : `chmod 700 /opt/trendx/data/backups` et tout son contenu
- Vérification : `make restore-check` (intégrité gzip) puis `make restore-test`

### Contenu du dump (vérifié sur le dump du jour)

- `pg_dump -Fc` de la base `trendx` **entière** : schémas inclus.
  Le dump contient `CREATE SCHEMA trendx_catalog` et `CREATE SCHEMA trendx_analytics`
  (2 occurrences vérifiées) : la restauration crée donc elle-même les schémas,
  aucune création manuelle préalable n'est nécessaire.
- Le dump contient aussi les **ACL** (`ACL - SCHEMA`, `ACL - TABLE`, ...) :
  les droits (REVOKE FROM PUBLIC + GRANT) sont rejoués par `pg_restore`.
- Le dump est créé avec `--no-owner` : il ne contient **aucune** instruction
  `OWNER TO` (0 occurrence vérifiée). La propriété est donc établie à la
  restauration via `--role=trendx_migration` (voir ci-dessous), jamais à la main.

### Restauration (procédure complète, exécutée par `make restore-test`)

Objectif de l'état cible (état canonique, = prod vérifié) :
- schémas `trendx_catalog` et `trendx_analytics` **propriété `trendx_migration`** ;
- **tous** les objets (tables, fonctions, séquences, vues) **propriété `trendx_migration`** ;
- `trendx_app` : **DML seul** (SELECT, INSERT, UPDATE, DELETE) + USAGE sur les
  schémas, **sans** CREATE sur les schémas ;
- ACL base `trendx` : `postgres` = CONNECT/CREATE/TEMPORARY, `trendx_app` =
  CONNECT, PUBLIC = CONNECT/TEMPORARY.

`make restore-test` (`scripts/restore-test.sh`) rejoue la procédure sur une base
jetable `trendx_restore_test` (créée puis supprimée dans le conteneur PostgreSQL
existant, base `trendx` jamais touchée) et échoue à la première preuve non
vérifiée. La base jetable est conservée en cas d'échec pour diagnostic.

Étapes (chaque étape est exécutée par `make restore-test`) :

1. **Rôles prérequis** (idempotent, via docker exec sur le conteneur PostgreSQL) :
   ```sql
   DO $$ BEGIN
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='trendx_migration') THEN
       CREATE ROLE trendx_migration LOGIN; END IF;
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='trendx_app') THEN
       CREATE ROLE trendx_app LOGIN; END IF;
   END $$;
   ```
2. **Base cible** (nom `trendx` en reprise réelle, `trendx_restore_test` en test) :
   ```sql
   CREATE DATABASE <cible> OWNER postgres;
   GRANT CONNECT, CREATE, TEMPORARY ON DATABASE <cible> TO trendx_migration;
   ```
   (`CREATE` est accordé temporairement : indispensable pour que
   `--role=trendx_migration` puisse créer les schémas contenus dans le dump.)
3. **Restauration** (les schémas du dump sont créés ici). Le `pg_restore` du
   conteneur PostgreSQL ne lit pas stdin (`could not open input file "-"`) :
   le dump est copié dans le conteneur, décompressé puis restauré :
   ```bash
   docker cp /opt/trendx/data/backups/YYYYMMDD/trendx.dump.gz \
     mobili_dahsboard-postgres-1:/tmp/trendx_restore_test.dump.gz
   docker exec mobili_dahsboard-postgres-1 sh -c \
     "gunzip -c /tmp/trendx_restore_test.dump.gz | pg_restore \
        --clean --if-exists --no-owner --role=trendx_migration -d <cible>"
   docker exec mobili_dahsboard-postgres-1 rm /tmp/trendx_restore_test.dump.gz
   ```
   `--role=trendx_migration` : tous les objets créés le sont **par** cette session
   (SET ROLE), donc **propriété trendx_migration**, sans aucune instruction OWNER.
   Les ACL du dump (GRANT à `trendx_app`) sont rejouées telles quelles.
4. **Alignement ACL base** (retirer le CREATE temporaire) :
   ```sql
   REVOKE CREATE ON DATABASE <cible> FROM trendx_migration;
   GRANT CONNECT ON DATABASE <cible> TO trendx_app;
   ```
5. **Preuves** (le `make restore-test` échoue si l'une échoue) :
   - parité de structure avec la prod : même nombre de tables (154), de
     fonctions (40, dont les 36 de l'extension `pgcrypto` restaurées via
     l'entrée EXTENSION du dump) et de séquences ;
   - `0` objet propriété autre que `trendx_migration` dans les deux schémas ;
   - schémas propriété `trendx_migration` ;
   - `trendx_app` : USAGE sur les deux schémas, **aucun** CREATE sur les schémas,
      DML (SELECT/INSERT/UPDATE/DELETE) sur les tables ;
   - négatif : `CREATE TABLE` exécuté en `trendx_app` échoue (permission refusée).
6. **Nettoyage** : `DROP DATABASE <cible>` (en test) ; en reprise réelle, la base
   `trendx` est remplacée puis les conteneurs Trendx sont recréés.

### Écart constaté sur la prod (à corriger séparément, avec approbation)

La prod actuelle a 3 objets propriété `postgres` au lieu de `trendx_migration` :
`trendx_catalog.ingestion_checkpoints` (+ son index et sa contrainte PK), créés
par la migration 005 exécutée en `postgres`. La restauration rétablit
l'état canonique (tout `trendx_migration`) ; la prod reste en l'état tant que la
correction n'a pas été approuvée.

---

## 13. Migrations

- Mécanisme officiel : `public.schema_version` dans la base `trendx`
- Chaque migration SQL est tracée par nom, checksum, durée et statut
- Rôle `trendx_migration` pour le DDL, `trendx_app` pour le DML
- Pas d'outil de migration tiers en Phase 2
- Schéma toujours qualifié (`trendx_catalog.*`, `trendx_analytics.*`)
