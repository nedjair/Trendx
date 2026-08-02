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
    container_name: trendx_reverse_proxy
    restart: unless-stopped
    ports:
      - "127.0.0.1:8443:8443"
    networks:
      - trendx_internal
      - trendx_edge
    # TLS reporté à une phase ultérieure : HTTP simple sur 127.0.0.1,
    # chiffrement délégué au tunnel SSH. Aucun certificat embarqué.
    healthcheck:
      test: ["CMD-SHELL", "wget --no-verbose --tries=1 --spider http://127.0.0.1:8443/healthz || exit 1"]
      interval: 20s
      timeout: 10s
      retries: 10
      start_period: 30s
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
    deploy:
      resources:
        limits:
          cpus: "0.5"
          memory: 512M
        reservations:
          cpus: "0.25"
          memory: 256M
    pids_limit: 256

  api:
    build:
      context: .
      dockerfile: docker/Dockerfile.api
    container_name: trendx_api
    restart: unless-stopped
    command: >
      uvicorn trendx.main:app
      --host 0.0.0.0
      --port 8000
      --workers 2
      --log-level info
    networks:
      - trendx_internal
      - mobili_dahsboard_default
    env_file:
      - .env
    secrets:
      - db_password
      - jwt_signing_key
    healthcheck:
      test: ["CMD", "python3", "-c", "import urllib.request,sys; r=urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3); sys.exit(0 if r.status==200 else 1)"]
      interval: 20s
      timeout: 10s
      retries: 10
      start_period: 30s
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
    deploy:
      resources:
        limits:
          cpus: "2.0"
          memory: 4G
        reservations:
          cpus: "0.5"
          memory: 1G
    pids_limit: 1024
    volumes:
      - trendx_data:/opt/trendx/data

  worker:
    build:
      context: .
      dockerfile: docker/Dockerfile.worker
    container_name: trendx_worker
    restart: unless-stopped
    command: >
      python3 -m trendx.worker
    networks:
      - trendx_internal
      - mobili_dahsboard_default
    env_file:
      - .env
    secrets:
      - db_password
      - jwt_signing_key
    healthcheck:
      test: ["CMD-SHELL", "python3 -c \"import socket;s=socket.socket();s.settimeout(2);s.connect(('127.0.0.1', 8181));s.close()\""]
      interval: 20s
      timeout: 10s
      retries: 10
      start_period: 30s
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
    deploy:
      resources:
        limits:
          cpus: "4.0"
          memory: 8G
        reservations:
          cpus: "1.0"
          memory: 2G
    pids_limit: 2048
    volumes:
      - trendx_data:/opt/trendx/data

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

volumes:
  trendx_data:
    name: trendx_data
    driver: local

secrets:
  tls_cert:
    file: /opt/trendx/.secrets/tls_cert.pem
  tls_key:
    file: /opt/trendx/.secrets/tls_key.pem
  db_password:
    file: /opt/trendx/.secrets/db_password.txt
  jwt_signing_key:
    file: /opt/trendx/.secrets/jwt_signing_key.txt
```

---

## 4. Limites de ressources (obligatoires)

Chaque service déclare `cpus`, `memory`, `pids_limit` et `logging max-size/max-file`.

| Service | CPUs limit | Memory limit | PIDs limit | Logging |
|---|---|---|---|---|
| reverse-proxy | 0.5 | 512M | 256 | 10m × 3 |
| api | 2.0 | 4G | 1024 | 10m × 3 |
| worker | 4.0 | 8G | 2048 | 10m × 3 |

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

| Volume Docker | Point de montage | Rôle |
|---|---|---|
| `trendx_data` | `/opt/trendx/data` | Données Trendx |
| `tb-postgres-data` (partagé) | `/var/lib/postgresql/data` | Base `trendx` (schémas `trendx_catalog`, `trendx_analytics`) |

**Correction B — Partage volume :** Les bases Trendx partagent le volume PostgreSQL existant de ThingsBoard. Ce choix est accepté pour Phase 2. Conséquences :
- Performance : risque de contention si ThingsBoard est très chargé
- Espace : ts_kv TB (~25–30 GB) + `trendx_analytics.ts_kv` (estimé 2–8 GB) sur même volume
- Sauvegarde : la sauvegarde de `trendx` inclut le volume PostgreSQL partagé

**Protection :** Arrêt automatique ingestion sous `TRENDX_DISK_MIN_FREE_GB` + alerte < 100 GB.

---

## 12bis. Sauvegardes

- Cible : base `trendx` uniquement (jamais `thingsboard` — lecture seule)
- Convention : `/opt/trendx/data/backups/YYYYMMDD/trendx.dump.gz`
  (`pg_dump -Fc --no-owner --clean` pipé dans `gzip -9`, cible `make backup`)
- Droits : `chmod 700 /opt/trendx/data/backups` et tout son contenu
- Vérification : `make restore-check` (intégrité gzip + restauration de test)
- Le fichier plat hérité `trendx_2026-08-02.dump` (non compressé, à la racine) est obsolète : archivé dans `backups/archive/`

---

## 13. Migrations

- Mécanisme officiel : `public.schema_version` dans la base `trendx`
- Chaque migration SQL est tracée par nom, checksum, durée et statut
- Rôle `trendx_migration` pour le DDL, `trendx_app` pour le DML
- Pas d'outil de migration tiers en Phase 2
- Schéma toujours qualifié (`trendx_catalog.*`, `trendx_analytics.*`)
