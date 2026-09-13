# Architecture Trendx

**Version :** 1.1 — Phase 2 (corrections post-validation)
**Date :** 2026-08-02
**Conformité :** AGENTS.md §1–22, prompt.md §7, décisions Phase 1 → Phase 2, validations Phase 2 conditionnelles

---

## 1. Principe d'installation

Trendx s'installe **à côté** de ThingsBoard sur le serveur unique `10.0.0.1`.
Aucun service Trendx ne s'exécute dans les conteneurs ThingsBoard existants.
Fichier Compose séparé, images propres, volumes propres sous `/opt/trendx/data`.

**Correction A — UNE SEULE BASE :** La base `trendx` regroupe tous les schémas Trendx.
`trendx_catalog` et `trendx_analytics` deviennent des schémas dans `trendx`.
`trendx_airflow` et `trendx_mlflow` ne sont pas créés en Phase 2.

---

## 2. Schéma logique

```
                10.0.0.1
                    │
    ┌───────────────┼───────────────┐
    │               │               │
ThingsBoard CE   TB PostgreSQL   Trendx
    │         (mobili_dahsboard_   │
    │          default network)    │
    │               │               │
    └───────Canal 1 (API REST)────►│
            https://10.0.0.1:8081  │
                                    │
    ┌───────Canal 2 (SQL RO)───────►│
    │      mobili_dahsboard-        │
    │      postgres-1:5432          │
    │      base : thingsboard       │
    │      (lecture seule)          │
    └──────────────────────────────┘
                                    │
    ┌───────────────────────────────┐
    │     Stack Trendx              │
    │  ┌─────────────────────────┐  │
    │  │  Reverse Proxy (HTTP)   │  │
    │  │  Port unique : 8443     │  │
    │  │  127.0.0.1 uniquement   │  │
    │  │  TLS reporté (voir §12) │  │
    │  └──────────┬──────────────┘  │
    │             │                 │
    │  ┌──────────▼──────────────┐  │
    │  │  trendx-api (FastAPI)   │  │
    │  │  interne : 8000         │  │
    │  └──────────┬──────────────┘  │
    │             │                 │
    │  ┌──────────▼──────────────┐  │
    │  │  trendx-worker          │  │
    │  │  (APScheduler)          │  │
    │  │  interne : 8181         │  │
    │  └──────────┬──────────────┘  │
    │             │                 │
    │  ┌──────────▼──────────────┐  │
    │  │  PostgreSQL existant    │  │
    │  │  base : trendx          │  │
    │  │  schémas :              │  │
    │  │   - trendx_catalog      │  │
    │  │   - trendx_analytics    │  │
    │  └─────────────────────────┘  │
    └───────────────────────────────┘
```

---

## 3. Composants Trendx (profil minimal)

| Composant | Rôle | Réseau |
|---|---|---|
| reverse-proxy | Nginx HTTP — seul exposant port 8443 (127.0.0.1 uniquement), auth obligatoire ; TLS reporté à une phase ultérieure | trendx_edge + trendx_internal |
| trendx-api | FastAPI — REST catalogue, analytics, auth | trendx_internal + mobili_dahsboard_default |
| trendx-worker | APScheduler + planificateur intégré | trendx_internal |
| PostgreSQL (existant) | Base `trendx` avec schémas `trendx_catalog` + `trendx_analytics` | mobili_dahsboard_default |

**Correction C — APScheduler :** Pas de Redis en Phase 2. Le worker utilise APScheduler en mode standalone, sans broker externe.

**Optionnels (non dans profil minimal) :**
- Airflow (reporté)
- MLflow (fichier local, puis conteneur)
- Grafana (optionnel)
- trendx-scheduler (MR-4, profil `scheduler`, OFF par défaut — voir §3bis)

---

### 3bis. Scheduler B1 dédié `trendx-scheduler` (MR-4, inactif par défaut)

- **Rôle** : planifier les jobs P0 (catalogue `P0_JOBS`) au lieu du worker, avec élection de leader, heartbeat, persistance des runs et fencing. Entrée : `python -m trendx.scheduler.service`.
- **Relation avec worker** : le worker reste l'exécuteur (file de tâches `trendz_task`, claim atomique) et conserve la maintenance infrastructure (partitions, rétention, agrégats) ainsi que `ingestion_hourly` jusqu'à MR-5. Le scheduler ne l'importe jamais (contrat MR-18) ; les jobs lourds (`trendx_train`, `trendx_forecast`, `anomaly_scan`) sont en mode `enqueue` (tâche créée, exécutée par le worker), les autres en `direct`.
- **Profil Compose** : service `scheduler` derrière `profiles: ["scheduler"]` — la stack minimal (`reverse-proxy`/`api`/`worker`) est inchangée sans ce profil. Activation = gate MR-5 séparé ; rollback = arrêt du conteneur.
- **Activation contrôlée** : `TRENDX_SCHEDULER_ENABLED=false` par défaut (aucune planification) ; guards refusant le démarrage si `TB_WRITEBACK_ENABLED`/`TB_ALARMS_ENABLED` ; leadership requis (`require_leadership=True`, standby sans planification, step-down gelant l'ordonnancement).
- **Healthcheck** : HTTP `127.0.0.1:9109` (interne uniquement) — `/healthz` (`disabled|leader|standby|degraded`, standby jamais reporté leader) et `/metrics` (texte Prometheus : `scheduler_up`, `scheduler_enabled`, `scheduler_leader`).
- **Leadership** : advisory lock PostgreSQL sur connexion dédiée (`TRENDX_SCHEDULER_LEADER_LOCK`), heartbeat toutes `TRENDX_SCHEDULER_HEARTBEAT_SECONDS=15`, stale `TRENDX_SCHEDULER_STALE_SECONDS=60` ; incertitude ⇒ non-leader.
- **Arrêt** : `SIGTERM`/`SIGINT` → gel planification → `release()` (publish `leader=false` best-effort, unlock, close) → fermeture HTTP ; idempotent, sans traceback.
- **Prérequis** : base `trendx` migrée 000→014 (tables `scheduler_run`, `scheduler_heartbeat`), flags TB OFF, `TRENDX_SCHEDULER_ENABLED=true` + instance explicite pour activation (gate séparé).

---

## 4. Réseaux Docker

| Réseau | Driver | Usage |
|---|---|---|
| trendx_internal | bridge | Backends uniquement (API, worker, PG) |
| trendx_edge | bridge | Front HTTP (reverse-proxy) |
| mobili_dahsboard_default | bridge (external) | Accès ThingsBoard + PostgreSQL TB (port interne 5432) |

**Règles dures :**
- Reverse-proxy : `trendx_edge` + `trendx_internal` SEULEMENT.
- API et worker : `trendx_internal` + `mobili_dahsboard_default` SEULEMENT.
- Aucun service Trendx ne publie de port sur `0.0.0.0` directement.
- Un seul point d'entrée : reverse-proxy Trendx HTTP 8443 sur `127.0.0.1` uniquement.
- Canal 2 SQL : accès par nom de conteneur PostgreSQL TB sur `mobili_dahsboard-postgres-1:5432` uniquement.
- B2 : REFUSÉ exposition hôte 5433 et tunnel SSH.

---

## 5. Base de données unique `trendx`

**Correction A — UNE SEULE BASE :** `trendx` avec schémas `trendx_catalog` et `trendx_analytics`.

| Base | Schéma | Rôle | Rôles PostgreSQL propriétaires |
|---|---|---|---|
| `thingsboard` | public | Base ThingsBoard — lecture seule via `trendx_ro` | — |
| `trendx` | `trendx_catalog` | Tables Trendz reproduites, métadonnées, modèles | `trendx_migration` (DDL), `trendx_app` (DML) |
| `trendx` | `trendx_analytics` | Partitionné déclaratif + BRIN + agrégats matérialisés : ts_kv, prédictions, anomalies | idem |

**B5 — TimescaleDB REFUSÉ** : Aucune extension. Partitionnement déclaratif + index BRIN + tables d'agrégats matérialisés. Réexamen sur benchmark chiffré uniquement.

**Isolation stricte :** Aucune jointure inter-bases. Ingestion : lecture TB → écriture `trendx_analytics` → requêtes locales. Schéma toujours qualifié (`trendx_analytics.ts_kv`).

> **Réconciliation schéma (2026-08-27, RÉSOLU 2026-08-27) :** l'affirmation « schémas `trendx_catalog` + `trendx_analytics` » est désormais vraie dans les migrations. `trendx_analytics` est créé par `009/011/012`. `trendx_catalog` est créé explicitement par `001` (qui y place le catalogue natif Trendz, idempotent) ; `003_relocate_catalog.sql` relocalise les bases historiques déjà migrées (catalogue en `public`) vers `trendx_catalog` sans perte de données. `005/006/007` référencent `trendx_catalog.*` et fonctionnent sur base vierge comme historique. Détail : `schema-mapping.md` §3.

---

## 6. Séparation des responsabilités

| Couche | Composant | Technologies |
|---|---|---|
| UI | trendx-ui | React + TypeScript + Vite + ECharts |
| API | trendx-api | FastAPI + Pydantic v2 + SQLAlchemy async |
| Worker | trendx-worker | APScheduler + planificateur intégré |
| Analytique | PostgreSQL natif | Partitionnement déclaratif + BRIN + agrégats matérialisés |
| Tracking ML | MLflow | Experiments + Model Registry (fichier local puis conteneur) |
| Observabilité | Grafana | Dashboards santé / KPI / ML perf (optionnel) |

---

## 7. Flux de données

1. **Découverte topologie** : Canal 1 API ThingsBoard → `trendx_catalog`
2. **Ingestion télémétrie** : Canal 2 SQL ThingsBoard (RO) → `trendx_analytics.ts_kv` (partitionné déclaratif)
3. **Qualité données** : préprocessing avant stockage (NaN, doublons, domaines)
4. **Entraînement** : extraction `trendx_analytics` → MLflow tracking
5. **Prévision** : Prophet / ARIMA / Fourier → `trendx_analytics.predictions`
6. **Anomalies** : PyOD → `trendx_analytics.anomaly_scores`
7. **Writeback TB** (B3 : dry-run uniquement en Phase 2, API uniquement, false par défaut) : `_EPD_<metric>` sur allowlist device/métrique
8. **Visualisation** : UI Trendx + Grafana (optionnel)

---

## 8. Contraintes dures

- Aucun device_id codé en dur
- Aucune écriture vers la base `thingsboard` sauf writeback autorisé B3 par API
- Aucune jointure inter-bases
- Tous les timestamps en UTC
- Lecture SQL bornée : fenêtre temporelle, LIMIT, curseur, statement_timeout
- Python utilisateur : sandbox avec limites CPU, mémoire, durée
- Deux pools de connexion strictement séparés (Canal 1 API, Canal 2 SQL RO)

---

## 9. Rôle PostgreSQL `trendx_ro` (B1)

**Décision : AUTORISÉ** avec droits stricts :
- `CONNECT` + `USAGE` sur base `thingsboard`
- `SELECT` uniquement sur tables existantes
- `default_transaction_read_only = on`
- `statement_timeout = 60000`
- `idle_in_transaction_session_timeout = 30000`
- `CONNECTION LIMIT 5`
- Aucun droit de création, aucune propriété d'objet
- Mot de passe hors dépôt, stocké dans `/opt/trendx/.secrets/` (dir 700, fichiers 600, root)

---

## 10. Writeback ThingsBoard (B3)

**Décision : AUTORISÉ SOUS CONDITIONS — Phase 2 = dry-run uniquement**
- `TB_WRITEBACK_ENABLED=false` par défaut
- Activation explicite requise
- Dry-run + validation liste clés par opérateur
- `TB_WRITEBACK_DEVICE_ALLOWLIST=ALG16025001`
- Une seule métrique : `mppt_main_battery_voltage_v`
- Horizon court
- Préfixe `_EPD_` obligatoire
- Écriture par API ThingsBoard uniquement, jamais par SQL direct
- Journal des clés écrites dans `writeback_log`
- Runbook `docs/runbooks/writeback-rollback.md` obligatoire avant toute écriture

---

## 11. Alarmes ThingsBoard (B4)

**Décision : REFUSÉ pour l'instant**
- `TB_ALARMS_ENABLED=false`
- Réexamen après démonstration interne >= 30 jours
- Conditions : hystérésis, durée minimale, cooldown, anti-duplication, zéro flapping
- Activation future sur un type d'alarme dédié et le seul device de test

---

## 12. Accès réseau et ports

- **B6 : AUCUN port ouvert** sur l'hôte pour les services Trendx
- Un seul point d'entrée : reverse-proxy Trendx HTTP 8443 sur `127.0.0.1` uniquement
- Auth obligatoire sur `/api/v1/*` ; aucun port 80

### 12.1 État TLS (constat Phase 2 — corrige la doc initiale)

- Le reverse-proxy sert en **HTTP simple** sur `127.0.0.1:8443` : aucun TLS dans le
  conteneur nginx, aucun certificat, aucun secret `tls_cert`/`tls_key`.
- Le chiffrement de bout en bout est **délégué au tunnel SSH** pour tout accès distant :
  `ssh -L 8443:127.0.0.1:8443 root@10.0.0.1`.
- **TLS reporté à une phase ultérieure** (certificat auto-signé ou ACME + HSTS).
- **Règle stricte tant que TLS est absent :** le port 8443 n'est JAMAIS publié ailleurs
  que sur `127.0.0.1`. Pas d'exposition sur `0.0.0.0`, pas d'IP LAN, pas de NAT, pas de
  port hôte supplémentaire.
- Airflow, MLflow, Grafana : exposition interne Docker seulement
- 8080 est pris par ThingsBoard : UI Trendx sur reverse-proxy interne

---

## 13. Stockage et disque

**Correction B — Partage volume PostgreSQL :** Les bases Trendx partagent le volume PostgreSQL existant de ThingsBoard (`tb-postgres-data`).
**Risque :** ts_kv TB occupe ~25–30 GB. Toute dégradation de performance ou d'espace sur ce volume impacte directement ThingsBoard.

**Protections :**
- Arrêt automatique ingestion sous `TRENDX_DISK_MIN_FREE_GB`
- Alerte si < 100 GB
- Purge automatique partitions `trendx_analytics` au-delà de `TRENDX_RETENTION_DAYS`
- Rétention Phase 2 :
  - Brut `trendx_analytics.ts_kv` : 180 jours
  - Agrégats horaires : 2 ans
  - Agrégats jour/semaine : 5 ans
  - Prédictions : 1 an
  - Journaux : 90 jours

**Règle :** Avant toute migration, vérifier l'espace libre du montage `/var/lib/docker` et du volume PostgreSQL. Si insuffisant, arrêter l'ingestion.

---

## 15. Migrations et traçabilité schéma

**Mécanisme officiel :** `public.schema_version` dans la base `trendx`.
Chaque migration SQL est enregistrée avec son nom, checksum, durée et statut.
Procédure :
1. Déposer le fichier SQL dans `migrations/`
2. L'appliquer via `make migrate` ou `psql`
3. Insérer une ligne dans `public.schema_version`

Aucun outil de migration tiers n'est utilisé en Phase 2.
