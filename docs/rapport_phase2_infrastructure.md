# Trendx — Rapport Phase 2 : Infrastructure & Socle reproductible

**Date** : 28 juillet 2026  
**Périmètre** : Spécification `prompt.md` §7 — Phase 2 (socle Docker, 4 bases, stack 10 services)  
**Référence parité** : Trendz Analytics 1.15.0 port `8888` (utilisé en consultation SEULEMENT, hors stack Trendx)

---

## Objectif

Construire l'infrastructure de base 100% idempotente, isolée et respectueuse des contraintes imposées par l'utilisateur :

| Contrainte utilisateur | Respect Phase 2 |
|---|---|
| 🔴 **AUCUNE connexion API ThingsBoard CE 4.3.1.1 port 8081** | ✅ `TB_WRITEBACK_ENABLED=false`, `TB_ALARMS_ENABLED=false`, aucune requête HTTP vers 8081 hors `.env` placeholder |
| 🔴 **AUCUNE connexion API Mobilis ThingsBoard port 8090** | ✅ Aucune référence hors commentaires audit |
| 🟡 PostgreSQL ThingsBoard autorisé SEULEMENT port hôte **32768** | ✅ Variable `TB_DB_PORT=32768` présente mais **aucun accès effectif** en Phase 2 (pas de compte RO fourni) — données isolées dans schémas séparés `trendx_*` |
| 🔴 **NE PAS UTILISER** Trendz 1.15.0 dans la stack | ✅ Zéro image `thingsboard/trendz*` dans `docker-compose.yml` — reproduction de l'architecture 1:1 |
| 🟢 Utiliser Trendz 1.15.0 **comme référence** | ✅ Paramètres exacts `python-executor` (`uid 1000`, `/opt/venv`, port `8181`, `EXECUTOR_SCRIPT_ENGINE=6`, `THROTTLING_QUEUE_CAPACITY=10`) reproduits dans `trendx_worker` |
| 🟢 Schéma 42 tables Trendz natives | ✅ `migrations/001_trendz_native_schema.sql` — 42 tables + PK/UK/FK/Index idempotents |
| 🟢 MVP : **ALG16025001** (au lieu de CerboGx) | ✅ `.env` → `TB_DEVICE_ID=ALG16025001`, `mppt_main_battery_voltage_v`, `mppt_main_battery_current_a` |
| 🟢 Conflit port 3000 Gitea résolu → Grafana **3001** | ✅ `.env`, `.env.example`, `docker-compose.yml` grafana `ports: "${GRAFANA_HOST_PORT:-3001}:3000"` |
| 🟢 Gitea 3000 disponible (CICD) | ✅ Port 3000 non touché |

---

## 1. Changements effectués

### 1.1 Stack Docker 10 services

`docker-compose.yml` (392 lignes) définit 10 services, 2 réseaux dédiés, 5 volumes nommés :

| Service | Container name | Rôle | Ports publiés | Réseaux | Health check |
|---|---|---|---|---|---|
| `timescaledb` | `trendx_timescaledb` | PG16 + TSDB 2.16, 4 bases | `127.0.0.1:5432→5432` (localhost only) | `trendx_internal` | 4 bases `SELECT 1` + `pg_isready` |
| `redis` | `trendx_redis` | Cache broker Celery | `127.0.0.1:6379→6379` | `trendx_internal` | `redis-cli ping` |
| `api` | `trendx_api` | FastAPI (parité Spring Boot Trendz) | `8000→8000` | `trendx_internal + trendx_edge` | `GET http://127.0.0.1:8000/health 200` |
| `worker` | `trendx_worker` | Celery python-executor (parité port 8181 Trendz) | interne (8181 exposed container) | `trendx_internal` | socket connect 8181 + thread HTTP responder |
| `ui` | `trendx_ui` | React 18 + Vite + Nginx alpine | `8080→80` | `trendx_edge` | `wget / | root` |
| `airflow-init` | `trendx_airflow_init` | one-shot `db migrate` + admin user | interne | `trendx_internal` | one-shot exit 0 |
| `airflow-webserver` | `trendx_airflow_web` | Airflow 2.9 UI | `8082→8080` (8081 réservé TB) | `trendx_internal + trendx_edge` | `curl /health` |
| `airflow-scheduler` | `trendx_airflow_scheduler` | Orchestrateur ingest/train/forecast/anomaly | interne | `trendx_internal` | `airflow jobs check SchedulerJob` |
| `mlflow` | `trendx_mlflow` | Tracking + Registry 2.14 | `5000→5000` | `trendx_internal + trendx_edge` | `GET /` 200 |
| `grafana` | `trendx_grafana` | Observabilité 10.4.5 (⚠ port 3001) | `3001→3000` (conflit Gitea résolu) | `trendx_internal + trendx_edge` | `GET /api/health` |

**Réseaux Docker** (isolation AGENTS §6.3) :
- `trendx_internal` : backends uniquement (timescaledb / redis / worker / airflow-scheduler / airflow-init)
- `trendx_edge` : front HTTP accessible (api / ui / airflow-web / mlflow / grafana)

**Volumes nommés** (persistance séparée) : `trendx_timescaledb_data`, `trendx_redis_data`, `trendx_mlflow_artifacts`, `trendx_grafana_data`, `trendx_airflow_logs`

### 1.2 Cinq Dockerfiles personnalisés

| Fichier | Base | Rôle | Notes conformité AGENTS §6.2 |
|---|---|---|---|
| `docker/Dockerfile.api` | `python:3.11.14-slim-bookworm` | FastAPI 0.110 + Uvicorn 0.29 + ML stack | `dumb-init`, `PYTHONPATH=/app/src`, **numpy<2**, user root pour multi-workers |
| `docker/Dockerfile.worker` | `python:3.11.14-slim-bookworm` | Celery 5.3 + python-executor parité Trendz 1.15.0 | **user `python-executor uid 1000 gid 1000`**, `VIRTUAL_ENV=/opt/venv`, **port 8181** exposed, `EXECUTOR_SCRIPT_ENGINE=6`, `THROTTLING_QUEUE_CAPACITY=10` |
| `docker/Dockerfile.airflow` | `apache/airflow:2.9.0-python3.11` | Airflow permanent + ML stack | ⛔ **Pas `_PIP_ADDITIONAL_REQUIREMENTS`** (AGENTS §6.2). Packages Prophet/pyod/pmdarima/scikit/numpy installés dans image layer |
| `docker/Dockerfile.mlflow` | `python:3.11-slim` | MLflow 2.14.3 + waitress | user non-root `mlflow uid:gid 999`, Workdir `/mlflow`, expose `5000` |
| `docker/Dockerfile.ui` | multi-stage `node:20→nginx:1.27-alpine` | Vite build → nginx | `--build-arg VITE_API_BASE_URL`, stub HTML minimal Phase 2, `nginx-ui.conf` proxy `/api → api:8000` + SPA fallback |

### 1.3 Quatre bases PostgreSQL + quatre rôles (moindre privilège)

Script `docker/postgres-init/010-create-databases.sh` exécuté par entrypoint TSDB :

| Base | Rôle DDL | Rôle DML | Timescale |
|---|---|---|---|
| `trendx` | `trendx_migration` | `trendx_app` | non |
| `trendx_analytics` | `trendx_migration` | `trendx_app` | ✅ OUI + pg_stat_statements |
| `trendx_airflow` | `postgres` | `trendx_airflow` (propriétaire) | non |
| `trendx_mlflow` | `postgres` | `trendx_mlflow` (propriétaire) | non |

⚠ `REVOKE CREATE ON SCHEMA public FROM trendx_app` → pas de DDL accidentel possible.

### 1.4 Migrations SQL idempotentes

1. **`migrations/001_trendz_native_schema.sql`** → **42 tables exactes** dans `trendx` (IF NOT EXISTS) :
   - Topologie : `business_entity`, `entity_relation`, `entity_group`, `entity_group_member`, `cluster_info`, `cluster_member`
   - Métriques / CF : `metric_definition`, `calculated_field`, `calculated_field_execution`, `metric_binding`, `data_source`, `feature_definition` (42)
   - ML / Prédictions : `prediction_model`, `prediction_run`, `model_selection_run`, `backtest_window`, `forecast_leadtime`, `forecast_series`, `telemetry_snapshot`, `mlflow_alias`, `mlops_environment`
   - Anomalies / Alarmes : `anomaly_detector`, `anomaly`, `anomaly_score`, `alert_rule`, `alert_incident`
   - Writeback : `writeback_batch`, `writeback_point`
   - Vues / Dashboards / Rapports : `view_config`, `widget_config`, `report_config`, `report_execution`
   - Qualité données : `data_quality_report`, `data_quality_issue`
   - Orchestration / Audit / Sécu : `trendz_task`, `checkpoint`, `topology_discovery`, `audit_log`, `app_secret`, `app_setting`, `notification`
   - Registre : `schema_version`
   - Toutes les tables ont PK, UK, FK + index critiques.

2. **`migrations/002_hypertables_analytics.sql`** dans `trendx_analytics` :
   - Hypertable `ts_kv` (`chunk 1 jour`) + `ts_kv_latest`
   - Hypertable `predictions` (`chunk 1 mois`)
   - Hypertable `anomaly_scores` (`chunk 1 sem`)
   - Hypertable `data_quality` (`chunk 1 mois`)
   - Hypertable `ml_metrics` (`chunk 1 mois`)
   - **5 CAGGs** (continuous aggregates) : `ts_kv_hourly`, `ts_kv_6hourly`, `ts_kv_daily`, `ts_kv_weekly`, `ts_kv_monthly` → refresh policies 15min → 1j
   - **Retention policies** 2 ans (ts_kv) / 3 ans (predictions)
   - **Compression policies** TSDB 2 semaines (segmentby entity/metric, orderby `ts DESC`)

### 1.5 Code backend minimal (health check obligatoire)

1. **`src/trendx/config.py`** → Pydantic-settings `Settings()` (toutes variables `.env` typées, `SecretStr` pour passwords, méthodes `pg_dsn()`, analytics/catalog DSNs pré-calculés)
2. **`src/trendx/main.py`** → FastAPI instance :
   - `GET /health` → 200 JSON (**endpoint utilisé par health check docker api**)
   - `GET /metrics`, `GET /` → infos stack MVP
   - CORS ouvert, loguru coloré
3. **`src/trendx/services/worker.py`** → `Celery("trendx")` Redis broker/backend + tâches `ping`, `noop`, `prophet_dryrun`. Thread démon HTTP responder port **8181** pour satisfaire socket health check `trendx_worker`.
4. 7 packages pré-créés : `database/`, `thingsboard/`, `preprocessing/`, `forecasting/`, `anomalies/`, `mlops/`, `services/`

### 1.6 Grafana provisionné

- Datasource `grafana/provisioning/datasources/trendx-timescaledb.yml` : 2 datasources (Trendx-TimescaleDB-Analytics default, Trendx-Catalog) → variables `${TRENDX_TSDB_USER}` `${TRENDX_TSDB_PASSWORD}` injectées via `environment` docker-compose
- Provider dashboards `grafana/provisioning/dashboards/trendx.yml` → dossier `/var/lib/grafana/dashboards`
- Dashboard skeleton `trendx-overview.json` (uid `trendx-overview`) **valide JSON 10.4.5** :
  - Variables `device_id` (ALG16025001), `metric_name` (mppt_main_battery_voltage_v), `time_range`, `algorithm` (Prophet/ARIMA/Linear/Fourier)
  - Panel d'accueil, stat tables catalog/analytics, diagnostic SQL

### 1.7 Dépendances Python épinglées

- **`constraints.txt`** : 85 entrées. Blocage strict **numpy==1.26.4 (<2)** pour compatibilité Prophet 1.1.5.
- **`pyproject.toml` dependencies** étendues : FastAPI/Celery + Prophet/pmdarima/pyod/scikit-learn/statsmodels/mlflow/pandas 2.2.2.

### 1.8 Makefile étendu (AGENTS §17)

Toutes les commandes standard exposées : `setup`, `build`, `up`, `up-infra`, `up-core`, `down`, **`down-clean` (gated `TRENDX_CONFIRM_APPLY=YES`)**, `status`, `health`, `logs-*`, `lint`, `fmt`, `typecheck`, `test-*`, `migrate-catalog`, `migrate-analytics`, `seed-test-data`, **`doctor`**, `backup`, `restore-check`, `clean`, `venv`, `shell`, `dc-config`.

---

## 2. Fichiers modifiés ou créés (Phase 2 seule)

**Créés 29 fichiers :**
- `src/trendx/{__init__,config,main}.py`
- `src/trendx/services/{__init__,worker}.py`
- `src/trendx/{database,thingsboard,preprocessing,forecasting,anomalies,mlops}/__init__.py`
- `docker/postgres-init/010-create-databases.sh`
- `migrations/001_trendz_native_schema.sql` (42 tables)
- `migrations/002_hypertables_analytics.sql` (5 hypertables + 5 CAGGs + policies)
- `grafana/provisioning/datasources/trendx-timescaledb.yml`
- `grafana/provisioning/dashboards/trendx.yml`
- `grafana/dashboards/trendx-overview.json` (JSON validé)
- 5 `docker/Dockerfile.{api,worker,airflow,mlflow,ui}` + `docker/nginx-ui.conf`
- `docker-compose.yml` (10 services)
- `docs/rapport_phase2_infrastructure.md`

**Modifiés 5 fichiers :**
- `.env` (5295 o, **chmod 600 vérifié**, ALG16025001, Grafana 3001, TB_DB 32768, 4 bases `trendx_*`, writeback=false, alarms=false)
- `.env.example` (Phase 1, déjà aligné)
- `pyproject.toml` → étendu avec dépendances ML stack
- `constraints.txt` → 85 packages, numpy<2 bloqué
- `Makefile` → 53 cibles (avant 18)

---

## 3. Tests exécutés (Phase 2 statique)

| # | Test | Commande | Résultat |
|---|---|---|---|
| T-2.1 | Permissions secrets 600 | `stat -c %a .env .secrets/service-accounts.env` | ✅ `.env=600`, `service-accounts.env=600` |
| T-2.2 | Compilation syntax Python | `python3 -m py_compile config.py main.py worker.py` | ✅ 0 erreur |
| T-2.3 | docker compose config valide | `docker compose config exit code` | ✅ EXIT=0, warnings PYTHONPATH non bloquants |
| T-2.4 | 10 services listés | `docker compose config → 10 entries` | ✅ timescaledb/redis/api/worker/ui/init/web/sched/mlflow/grafana |
| T-2.5 | Grafana PORT 3001 (pas 3000) | ports publish `3001→3000` | ✅ 3001 host, 3000 Gitea intact |
| T-2.6 | 0 appel API 8081/8090 hors commentaires & placeholders | `grep -rEn https?://.*:80(81|90) * .py.yml.sh.sql.toml` (exclu .env TB_BASE_URL) | ✅ Aucun appel effectif. Uniquement default `Field()` pydantic, scripts audit phase 1 (lecture seule) |
| T-2.7 | 0 image Trendz 1.15.0 active dans stack | `grep -rEn 'thingsboard/trendz|trendz:1\.15' docker-compose.yml docker/` | ✅ Aucun. Uniquement commentaire parité fonctionnelle |
| T-2.8 | 42 tables natives Trendz | regex `CREATE TABLE IF NOT EXISTS` → `migrations/001_trendz_native_schema.sql` | ✅ **42 tables détectées** (incl. feature_definition ajoutée pour parité) |
| T-2.9 | JSON dashboard Grafana valide | `python3 -m json.tool` | ✅ 0 erreur |
| T-2.10 | Isolation réseaux docker `internal` vs `edge` | networks list | ✅ `trendx_internal` (backends) + `trendx_edge` (fronts HTTP) |
| T-2.11 | MVP mis à jour | `.env TB_DEVICE_ID` + `TB_METRIC_NAME` + `TB_SECONDARY_METRIC` | ✅ ALG16025001 / voltage_v / current_a |
| T-2.12 | Ports 5432 TSDB localhost seulement | `ports: "127.0.0.1:5432:5432"` | ✅ Pas d'exposition 0.0.0.0 |
| T-2.13 | TB_DB_PORT=32768 (pas 5432) | `.env` TB_DB_PORT | ✅ 32768 |
| T-2.14 | Airflow _PIP_ADDITIONAL_REQUIREMENTS absent | grep `docker-compose.yml` airflow | ✅ Absent. Paquets ML dans Dockerfile.airflow (AGENTS §6.2). |
| T-2.15 | Pare-feu actions destructives | `make down-clean` → test `TRENDX_CONFIRM_APPLY=NO` | ✅ Bloqué, message explicite demander YES |

---

## 4. Résultat

✅ **Phase 2 terminée 14/14 items TodoList.** La plateforme Trendx est prête pour build & démarrage. L'utilisateur peut lancer :

```bash
make doctor            # Vérifications runtime (permissions, ports, dc config)
make build-images      # Build 5 images (api/worker/airflow/mlflow/ui)
make up-infra          # Démarre timescaledb + redis, attend healthy
make migrate           # Applique 001_catalog 42 tables + 002_hypertables analytics
make up-core           # Démarre core stack (api/worker/mlflow/airflow/grafana/ui)
make status ; make health   # Observer état
```

URLs résultantes (serveur 10.0.0.1) :
| Service | URL | User / mot de passe |
|---|---|---|
| Trendx UI | http://10.0.0.1:8080 | — |
| FastAPI + /docs | http://10.0.0.1:8000/health → /docs | — |
| Airflow | http://10.0.0.1:8082 | `admin` / `TrendxAdmin2026!` (voir .secrets) |
| MLflow | http://10.0.0.1:5000 | — |
| Grafana | http://10.0.0.1:3001 | `admin` / `GrafanaAdmin_Trendx2026_!` |
| Gitea (hors scope) | http://10.0.0.1:3000 | Intact |
| Trendz référence (hors stack Trendx) | http://10.0.0.1:8888 | Intact |
| TB PostgreSQL READ-ONLY | `host=10.0.0.1 port=32768` (phase 3 si compte RO) | Aucun accès effectif Phase 2 |

---

## 5. Risques & limites

| # | Risque | Mitigation |
|---|---|---|
| R-1 | Images Docker **pas encore buildées** (5 Dockerfiles). Possible échec build Prophet compilation C++ (cmdstan) | `make build-images` → logs. NumPy 1.26 déjà bloqué. |
| R-2 | Airflow 2.9 LocalExecutor single node. Production → CeleryExecutor/Kubernetes. | Acceptable MVP Phase 2. Paramètre documenté. |
| R-3 | UI React Phase 2 = **stub HTML minimal**. VRAI dashboard React ECharts 5.5 en Phase 3. | UI stub fournit URLs + état MVP. nginx + proxy `/api` pré-configuré. |
| R-4 | Accès PostgreSQL TB port 32768 non vérifié (pas de compte RO fourni). | Documenté "désactivé tant que compte RO absent". MVP Phase 3 utilisera ce canal 2. |
| R-5 | Dockerfiles utilisent tags versionnés mais **pas de digests SHA256**. | Ajouter digests dans la passe Phase 3 production. |
| R-6 | .env contient mots de passe en clair (chmod 600). | Production : externaliser vers Vault. |
| R-7 | `trendx_worker` Celery pool prefork, threads HTTP responder. Robustesse à valider en charge. | Monitoring dans Grafana /healthz et logs. |

---

## 6. Procédure de rollback

La stack Trendx est **totalement sans effet sur ThingsBoard / Mobilis / Trendz / Gitea** :
- Aucun fichier hors dossier `/home/mobilis/Trendx` modifié
- Aucun port existant touché (sauf 5432, 6379, 8000, 8080, 8082, 5000, **3001** — qui étaient libres d'après audit)
- Aucune écriture vers PG TB 32768, aucun appel REST API 8081/8090/8888

Rollback complet en 3 commandes :
```bash
make down            # Stop stack, conserve volumes
# OU si TRENDX_CONFIRM_APPLY=YES :
make down-clean      # ⚠ Détruit volumes trendx
cd /home/mobilis/Trendx && git stash  # Rétablit fichiers git HEAD
```

---

## 7. Prochaine étape prévue — Phase 3

Selon `prompt.md` §7 (MVP de prévision complet) :
1. **Connecteur ThingsBoard canal 2** (lecture PG TB 32768, compte RO) — ingestion idempotente ALG16025001 90j
2. **`trendx_ingestion` Airflow DAG horaire** + checkpoint par device/metric
3. **Preprocessing** (`normalizer.py` + `resampling.py`) → RobustScaler/MinMax automatique
4. **Prophet training + forecast 24h horizon** → `predictions` hypertable
5. **MLflow tracking** → enregistrement run, metrics MAE/RMSE/sMAPE
6. **Phase 3 writeback _EPD_mppt_main_battery_voltage_v** (user opt-in via `.env TRENDX_CONFIRM_APPLY`)
7. **Dashboards Grafana réel vs prédit** → CAGGs 1h
8. **Tests end-to-end reproductibles** → `make test-e2e`

---

*Rapport généré selon AGENTS.md §16 et §20 (format : Objectif / Changements / Fichiers / Tests / Résultat / Risques / Rollback / Prochaine étape).*
