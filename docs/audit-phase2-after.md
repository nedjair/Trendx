# Audit Phase 2 — État final

**Date :** 2026-08-02  
**Opérateur :** Kilo  
**Séquence :** Étape 12 — snapshot après modifications (+ correctifs post-refus de tag)

---

## 1. État disque (après)

```
Sys. de fichiers Taille Utilisé Dispo Uti% Monté sur
/dev/sdb2          1,1T    682G  361G  66% /
```

**Espace libre :** 361 GB  
**Seuil minimal (TRENDX_DISK_MIN_FREE_GB) :** 50 GB  
**Marge :** 311 GB  
**Consommation Trendx :** ~14 GB (images + containers + données)

---

## 2. Conteneurs Docker (après)

| Conteneur | Image | État | Santé | Créé le (UTC) | StartedAt (UTC) | RestartCount |
|---|---|---|---|---|---|---|
| trendx_reverse_proxy | trendx/reverse-proxy:latest | running | healthy | 2026-08-02 13:51:38 | 2026-08-02 13:51:38 | 0 |
| trendx_api | trendx/api:latest | running | healthy | 2026-08-02 15:05:09 | 2026-08-02 15:05:10 | 0 |
| trendx_worker | trendx/worker:latest | running | healthy | 2026-08-02 15:05:09 | 2026-08-02 15:05:22 | 0 |
| thingsboard_thingsboard-ce_1 | thingsboard/tb-node:4.3.1.1-mobilis | running | — | 2026-06-15 15:27:16 | 2026-08-02 07:47:09 | 1 |
| mobili_dahsboard-postgres-1 | postgres:16 | running | healthy | 2026-06-01 11:05:40 | 2026-08-02 07:43:09 | 0 |

**Conteneurs Trendx :** 3 (reverse-proxy, api, worker)

---

## 3. Bases de données (après)

| Base | Propriétaire | Contenu |
|---|---|---|
| thingsboard | postgres | Tables ThingsBoard CE (lecture seule Trendx) |
| trendx | postgres | Schémas `public` (schéma Trendz natif), `trendx_catalog`, `trendx_analytics` |
| postgres / template0 / template1 | postgres | Système |
| test_gexec3 | postgres | Reliquat vide (0 table, 7,4 MB) — **à confirmer suppression** |

**Bases supprimées :** `trendx_airflow`, `trendx_mlflow` (hors périmètre Phase 2, correctif M4)

**Rôles PostgreSQL :**
- postgres (superuser)
- trendx_ro : read-only sur `thingsboard`, CONNECTION LIMIT 5
- trendx_app : DML sur `trendx`, CONNECTION LIMIT **24**
- trendx_migration : DDL/migrations, `NOCREATEDB` (correctif M5)

**Rôles supprimés :** `trendx_airflow`, `trendx_mlflow` (correctif M4)

**max_connections :** 100

---

## 3bis. Stockage analytique (correctifs bloquants 1 et 3)

**Partitionnement mensuel (+3 mois) :**
- Tables partitionnées : `ts_kv`, `predictions`, `anomaly_scores`, `data_quality`, `ml_metrics`
- Aucune partition DEFAULT
- Dernière partition créée : `*_2026_11` (borne max 2026-12-01) → couverture garantie à +3 mois
- `trendx_analytics.ensure_partitions_forward(3)` (SECURITY DEFINER) : création mensuelle automatisée (APScheduler)
- `trendx_analytics.partition_coverage(3)` : `{"missing": 0, ...}` — contrôle échouant dans `make doctor`

**Tables d'agrégats (remplacement des vues matérialisées) :**
- `ts_kv_hourly`, `ts_kv_daily`, `ts_kv_weekly` : tables réelles (UPSERT incrémental)
- `aggregate_watermarks` : filigrane par agrégat (watermark, début/fin, lignes)
- `trendx_analytics.refresh_aggregate('hourly|daily|weekly')` : fenêtres 3 h / 2 j / 7 j
- Jobs APScheduler : `aggregate_hourly` (15 min), `aggregate_daily` (1 h), `aggregate_weekly` (cron 02:30 UTC)
- Propriétaire : `trendx_migration` ; `GRANT` DML à `trendx_app`
- **Aucun `REFRESH MATERIALIZED VIEW`** (verrouillage de l'instance PostgreSQL partagée avec ThingsBoard)

**Budget connexions `trendx_app` (correctif bloquant 2) :**
- Pools SQLAlchemy : `pool_size=2`, `max_overflow=1` par moteur
- 2 moteurs (catalog + analytics) × 3 = 6 connexions max / processus
- API = 2 workers uvicorn (PID vérifiés) → 12 max ; worker = 1 processus → 6 max
- **Total réel maximal : 18 ≤ LIMIT 24 (marge 6)**

---

## 4. Réseau Docker (après)

| Réseau | État |
|---|---|
| mobili_dahsboard_default | existe (external, non modifié) |
| trendx_internal | existe |
| trendx_edge | existe |

---

## 5. Compose utilisé

**Fichier :** /opt/trendx/docker-compose.trendx.yml  
**Projet :** trendx  
**Services :** reverse-proxy, api, worker

---

## 6. Migrations tracées

| Migration | Appliquée le (UTC) | Statut |
|---|---|---|
| 001_trendz_native_schema.sql | 2026-08-02 13:50:42 | OK |
| 003_schema_alignment.sql | 2026-08-02 13:50:42 | OK |
| 004_aggregates_partitions.sql | 2026-08-02 14:35:29 | OK |

---

## 6bis. Rotation des secrets PostgreSQL (correctif BLOQUANT credentials)

**Constat (2026-08-02) :** `.env` et `.secrets/service-accounts.env` ne contenaient
que des placeholders (`CHANGE_ME`) ; les mots de passe réels des rôles étaient
introuvables (le vault ne correspondait pas à PostgreSQL — `pg_hba` n'autorise que
`127.0.0.1` en `trust`, le test local était donc faussement positif).

**Correctif approuvé par l'utilisateur** (rotation complète, périmètre strict
`trendx_app` / `trendx_migration` / `trendx_ro`) :
- Génération `openssl rand -base64 32` (secret distinct par rôle) ;
- Application via `psql \password <role>` (stdin, aucun secret en clair sur la ligne
  de commande, `HISTFILE=/dev/null`) ;
- Persistance dans `.secrets/service-accounts.env` (dir 700, fichier 600) et
  `.env` (repo + /opt/trendx, 600) ; `TB_DB_READONLY_USER/PASSWORD` désormais renseignés ;
- Aucun secret dans `docker-compose.trendx.yml` (le `TRENDX_JWT_SIGNING_KEY` du bloc
  `environment:` a été retiré, fourni désormais uniquement via `env_file`) ;
- Rôle `postgres` et rôles ThingsBoard **non touchés**.

**Vérifications :** authentification réseau OK pour les 3 rôles ; attributs inchangés
(`trendx_ro` LIMIT 5, `trendx_app` LIMIT 24, `trendx_migration` NOCREATEDB, aucun
superuser) ; tests négatifs rejoués (INSERT `trendx_ro` refusé, CREATE TABLE
`trendx_app` refusé) ; conteneurs TB/PG (StartedAt/RestartCount) inchangés ; aucune
erreur d'authentification dans les logs api/worker.

**Risque accepté consigné :** `pg_hba` `trust` sur 127.0.0.1 = `docker exec` équivaut
à un accès superutilisateur sans mot de passe. Non corrigé (imposerait de recharger la
config du conteneur PostgreSQL ThingsBoard). Mesures compensatoires dans
`docs/security.md` (§3) : restriction d'accès hôte/démon Docker + journalisation.

## 6ter. Correctif search_path des moteurs SQLAlchemy

**Constat :** les tables catalogue sont dans le schéma `trendx_catalog` (58 tables,
dont `business_entity`) et les tables analytiques dans `trendx_analytics` ; les
moteurs SQLAlchemy n'appliquaient pas de `search_path`, d'où
`relation "business_entity" does not exist` (HTTP 500 sur `/api/v1/catalog/devices`).

**Correctif :** `src/trendx/database/connection.py` — `search_path` par moteur
(`catalog` → `trendx_catalog,public`, `analytics` → `trendx_analytics,public`) via
`connect_args["options"]="-c search_path=..."`. Pool inchangé (2/1).

**Vérification :** `GET /api/v1/catalog/devices` → HTTP 200 `{"data":[],"total":0,...}`.

---

## 7. Vérifications doctor (après)

```
│  .env permissions (600 attendu) : OK
│  Compose file = docker-compose.trendx.yml ? : OK
│  Réseau mobili_dahsboard_default existe ? : OK
│  Port publié (seul 127.0.0.1:8443 autorisé) ? : OK
│  trendx_ro ne peut pas INSERT dans thingsboard ? : OK (INSERT refusé)
│  trendx_app ne peut pas CREATE TABLE dans trendx_catalog ? : OK (CREATE refusé)
│  Partitions trendx_analytics à +3 mois ? : OK ({"missing": 0, "target_bound": "2026-12-01T00:00:00+00:00"})
│  Ressources reverse-proxy/api/worker : OK
│  Pas de mélange pools TB/trendx dans .env ? : OK
│  Espace libre / >= 50 GB ? : OK (361 GB)
│  Espace libre /var/lib/docker >= 50 GB ? : OK (361 GB)
```

**Tous les contrôles sont verts** (exécution `make doctor` du 2026-08-02 16:03).

---

## 8. Comparaison avant/après

| Métrique | Avant | Après | Δ |
|---|---|---|---|
| Espace libre /dev/sdb2 | 376 GB | 361 GB | -15 GB |
| Conteneurs Trendx | 0 | 3 | +3 |
| Bases Trendx | 1 (déjà existante) + airflow + mlflow | 1 | -2 (airflow, mlflow) |
| Rôles trendx_* | 4 | 3 | -1 |
| Connexions PG actives | 22 | 22 | 0 |
| Conteneurs TB/PG | Inchangés | Inchangés (StartedAt 07:43/07:47, RestartCount TB=1) | 0 |

---

## 9. Conclusion

Phase 2 infrastructure déployée avec succès. Tous les correctifs bloquants appliqués :
partitions +3 mois automatisées, agrégats en tables à UPSERT (sans vue matérialisée),
budget connexions `trendx_app` = 18 ≤ 24, rôles hors périmètre supprimés,
`trendx_migration NOCREATEDB`, `schema_version` propriété `trendx_migration`,
sauvegarde au format canonique `YYYYMMDD/trendx.dump.gz` + `chmod 700`,
rotation des secrets PostgreSQL (vault + `.env` réalignés, `\password` sans clair),
correctif `search_path` des moteurs (catalogue opérationnel). Prêt pour Phase 3
(ingestion, entraînement, prévision).

---

## 10. Incident infrastructure — signalé à l'exploitant

**Redémarrage de l'hôte (et du démon Docker) le 2026-08-02 à 07:42 UTC**,
plus de 6 heures avant la création des conteneurs Trendx (13:51 UTC).

Preuves :
```text
$ uptime
 15:43:43 up  7:01, ...
$ last reboot
 reboot   system boot  7.0.0-28-generic Sun Aug  2 08:42   still running
$ journalctl -u docker --since "2026-08-02 07:00" --until "2026-08-02 09:00"
 août 02 08:42:47 iotserver systemd[1]: Starting docker.service - Docker Application Container Engine...
 août 02 08:43:05 iotserver dockerd: Restoring containers: start.
 août 02 08:43:11 iotserver dockerd: Loading containers: done.
```

Chronologie (UTC) : boot hôte 07:42:47 → dockerd restaure 07:43:05–07:43:11 →
PG/Kafka/Zookeeper/adminer redémarrés 07:43:09 → ThingsBoard-ce 07:47:09 (`RestartCount=1`) →
premiers conteneurs Trendx 13:51:38.

Le `RestartCount=1` du conteneur ThingsBoard est donc **antérieur** à toute activité
Trendx et s'explique par le redémarrage de l'hôte. Aucun impact attribuable à Trendx.
**Action requise :** signaler l'incident à ops@mobilis.dz.

---

## 7. Validation post-Phase 2 (2026-08-02, 16:30) — TLS, auth, search_path, nettoyage

### 7.1 TLS du reverse-proxy — réalité vs documentation
- Proxy en **HTTP simple** sur `127.0.0.1:8443` (nginx `listen 8443`, aucun `ssl`,
  healthcheck Compose en `http`). Le `https://` de la doc était faux.
- Corrigé : `docs/architecture.md`, `docs/deployment.md`, `docs/security.md` —
  « HTTP sur loopback, chiffrement délégué au tunnel SSH, TLS reporté à une phase
  ultérieure ; ne jamais publier ce port ailleurs que sur 127.0.0.1 tant que TLS absent ».
- Preuve API : sans jeton `401`, jeton invalide `403`, jeton valide `200`,
  `X-API-Key` `200`, `/health` public `200` (via `curl http://127.0.0.1:8443/...`).

### 7.2 Authentification API ajoutée
- Middleware `require_auth_middleware` (main.py) : `/api/v1/*` exige
  `Authorization: Bearer <jeton>` ou `X-API-Key`, comparaison temps constant
  (`hmac.compare_digest`). Public : `/`, `/health`, `/metrics`, `/docs`, `/redoc`,
  `/openapi.json`.
- Clé `TRENDX_API_TOKEN` générée (`openssl rand -hex 32`) dans `.env` (mode 600),
  placeholder dans `.env.example`. Jamais journalisée.

### 7.3 Test négatif trendx_app — cibles qualifiées (3/3 refusés)
```
trendx_app CREATE TABLE trendx_catalog.doctor_test  → OK (CREATE refusé)
trendx_app CREATE TABLE trendx_analytics.doctor_test → OK (CREATE refusé)
trendx_app CREATE TABLE public.doctor_test           → OK (CREATE refusé)
```
`make doctor` rejoué avec ces trois cibles explicitement qualifiées.

### 7.4 Search paths effectifs (SHOW search_path sur chaque connexion)
```
catalog:     'trendx_catalog,public'
analytics:   'trendx_analytics,public'
tb_readonly: 'public'
```
- `tb_readonly` (nouveau moteur, pool 1/0, rôle `trendx_ro` LIMIT 5 → 3 ≤ 5)
  **sans aucun schéma Trendx** ; aucun moteur ne porte les deux schémas.
- Contrôle automatisé : `scripts/check_search_path.py` (intégré à `make doctor`).

### 7.5 Nettoyage base partagée
- `\l` complet : postgres, template0, template1, thingsboard, trendx, test_gexec3.
- `test_gexec3` contrôlé : 0 table, ~7,4 MB, aucune relation avec ThingsBoard,
  aucun privilège → **supprimée** (`DROP DATABASE`).
- `make doctor` : contrôle liste blanche (thingsboard, trendx, postgres, template*),
  échoue si une base inattendue apparaît.

### 7.6 Compte de service ThingsBoard (préparation, création par l'opérateur)
- Variables : `TB_SERVICE_USER_EMAIL` / `TB_SERVICE_USER_PASSWORD` dans
  `.secrets/service-accounts.env` ; fallback runtime via `settings.tb_login` (config.py).
- Liste des endpoints REST Phase 3 : voir `docs/service-accounts.md`.
- Contrôle `make doctor` : `scripts/tb_auth_check.py` (POST `/api/auth/login`,
  sortie `OK`/`SKIP`/`FAIL`, jamais de mot de passe) — `SKIP` tant que placeholders.
- `TB_WRITEBACK_ENABLED=false` inchangé.

### 7.7 Ingestion Phase 3 — garde-fous
- `docs/phase3-ingestion-guardrails.md` : périmètre limité, débit API, reprise,
  test réel d'arrêt sous `TRENDX_DISK_MIN_FREE_GB`.
- Gate disque implémenté : `check_disk_min_free` + `ensure_disk_available`
  (levée `DiskCapacityError` sous le seuil, avant chaque run et chaque device/metric).
  Test unitaire `test_disk_min_free_gate`.
- `prompt2.md` (consigne de tâche historique exécutée) : ajouté au `.gitignore`.

### 7.8 Tests
- `tests/unit` : 162 passed / 43 failed (échecs préexistants hors périmètre :
  checkpoints stubs, mlflow absent, pydantic version). 2 tests config corrigés
  (alignés sur la conception une-base-deux-schémas, plus de fuite de DSN).

### 7.9 Bug rollback silencieux des jobs planifiés (découvert à la validation)
- Premier tick planifié du worker (15:36:55) : `refresh_aggregate('hourly')`
  renvoyait un JSON de succès, mais `aggregate_watermarks` restait à 14:36:12
  (valeur de la migration). Diagnostic :
  - `with engine.connect() as conn:` **ne commit pas** à la sortie du bloc
    (`Connection.close()` fait ROLLBACK d'une transaction ouverte). Seul
    `engine.begin()` commite à la sortie. Contrôle : un `UPDATE` simple via
    `engine.connect()` n'était pas persisté ; via `engine.begin()` oui.
  - `worker.py::_call_db_function` utilisait `engine.connect()` → les écritures
    du planificateur (agrégats, création de partitions) étaient **silencieusement
    annulées** à chaque tick.
- Correctif : `_call_db_function` passe à `engine.begin()`.
- Vérification end-to-end : tick de 15:59:23 → `hourly` watermark = 15:59:23
  (persisté, identique au log du worker).
- Audit des autres usages `engine.connect()` : `training.py`, `inference.py`,
  `tasks.py`, `connection.py` (search_path, SELECT 1), `check_search_path.py` =
  lecture seule → corrects. `ingestion.py` et `quality.py` utilisent déjà
  `engine.begin()`. Les sessions ORM passent par `session.commit()` explicite.
- Note sécurité : un diagnostic psycopg2 mal formé a fait apparaître le mot de
  passe applicatif dans la sortie de terminal ; rotation de
  `TRENDX_APP_PASSWORD` recommandée.

---

*Fin du snapshot final.*
