# Trendx — Procédure de mise en œuvre (pipeline d'analyse/prévision open source équivalent à Trendz Analytics)

> **Objectif** : construire **Trendx**, une solution 100 % open source reproduisant les fonctions officielles de ThingsBoard Trendz Analytics — tableaux de bord analytiques, prédiction de tendances et détection d'anomalies — connectée à l'instance ThingsBoard existante (`http://10.0.0.1:8081`, Docker).
> 

**Stack retenue** (validée par l'étude comparative) : ThingsBoard CE (existant) + TimescaleDB + Apache Airflow + Python ML (scikit-learn, statsmodels, SciPy, Prophet, PyOD) + MLflow + Grafana.

**Fonctions Trendz reproduites** :

| Fonction Trendz officielle | Équivalent Trendx |
| --- | --- |
| Vues analytiques (table, line, bar, pie, heatmap, card…) | Dashboards Grafana + widgets ThingsBoard |
| Champs calculés (KPI) | Vues SQL / agrégats continus TimescaleDB |
| Prédiction (Fourier, Prophet, ARIMA, régression linéaire, modèle custom) | Modules Python + compétition de modèles |
| Sauvegarde des prévisions sous clé `_EPD_<key>` | Écriture télémétrie via API REST ThingsBoard |
| Accuracy (Confidence Level / Confidence Band) | Backtesting par segments + métriques d'erreur |
| Détection d'anomalies non supervisée (Anomaly Score / Score Index) | Isolation Forest + clustering (PyOD, scikit-learn) |
| Monitoring continu + alarmes ThingsBoard | Scans planifiés Airflow + POST /api/alarm |

---

# Phase 0 — Prérequis et préparation (Jour 1)

1. **Vérifier l'existant**
    - ThingsBoard opérationnel sur `http://10.0.0.1:8081` (conteneur Docker).
    - Accès SSH au serveur cloud, Docker ≥ 24 et Docker Compose v2 installés.
    - Un compte **Tenant Administrator** ThingsBoard (login/mot de passe).
2. **Réserver les ports** (ThingsBoard occupe déjà 8080) :
    - TimescaleDB : `5432` — Airflow : `8081` — MLflow : `5000` — Grafana : `3000`.
3. **Créer l'arborescence du projet**

```bash
mkdir -p /opt/trendx/{dags/ml_engine,grafana/provisioning/datasources,scripts,ml_models}
cd /opt/trendx
```

1. **Créer le fichier d'environnement** `.env` (jamais de secrets en dur) :

```bash
TB_BASE_URL=http://10.0.0.1:8081
TB_USERNAME=tenant@besteya.com
TB_PASSWORD=********
PG_USER=trendx
PG_PASSWORD=trendx_pwd
PG_DB=trendx_analytics
```

1. **Valider l'accès API ThingsBoard** (test JWT) :

```bash
curl -s -X POST $TB_BASE_URL/api/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"'$TB_USERNAME'","password":"'$TB_PASSWORD'"}'
# → doit retourner {"token":"...","refreshToken":"..."}
```

<aside>
⚠️

Ne redéployez pas de second ThingsBoard : Trendx se branche sur l'instance existante via API REST/WebSocket, exactement comme Trendz se connecte à ThingsBoard.

</aside>

---

# Phase 1 — Déploiement de l'infrastructure Docker (Jour 1–2)

Créer `/opt/trendx/docker-compose.yml` :

```yaml
services:
  timescaledb:
    image: timescale/timescaledb:latest-pg15
    container_name: trendx-db
    environment:
      POSTGRES_DB: ${PG_DB}
      POSTGRES_USER: ${PG_USER}
      POSTGRES_PASSWORD: ${PG_PASSWORD}
    ports: ["5432:5432"]
    volumes:
      - ts_data:/var/lib/postgresql/data
      - ./init-db.sql:/docker-entrypoint-initdb.d/init.sql

  airflow:
    image: apache/airflow:2.9.3-python3.11
    container_name: trendx-airflow
    command: standalone
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${PG_USER}:${PG_PASSWORD}@timescaledb:5432/${PG_DB}
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
      _PIP_ADDITIONAL_REQUIREMENTS: "scikit-learn statsmodels scipy prophet pmdarima pyod mlflow psycopg2-binary pandas numpy requests"
      TB_BASE_URL: ${TB_BASE_URL}
      TB_USERNAME: ${TB_USERNAME}
      TB_PASSWORD: ${TB_PASSWORD}
    ports: ["8081:8080"]
    volumes:
      - ./dags:/opt/airflow/dags
      - ./ml_models:/opt/airflow/ml_models
    depends_on: [timescaledb]

  mlflow:
    image: ghcr.io/mlflow/mlflow:v2.14.1
    container_name: trendx-mlflow
    command: mlflow server --backend-store-uri postgresql://${PG_USER}:${PG_PASSWORD}@timescaledb:5432/${PG_DB} --default-artifact-root /mlflow/artifacts --host 0.0.0.0
    ports: ["5000:5000"]
    volumes: [mlflow_artifacts:/mlflow/artifacts]
    depends_on: [timescaledb]

  grafana:
    image: grafana/grafana:11.1.0
    container_name: trendx-grafana
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    ports: ["3000:3000"]
    volumes:
      - grafana_data:/var/lib/grafana
      - ./grafana/provisioning:/etc/grafana/provisioning
    depends_on: [timescaledb]

volumes:
  ts_data:
  mlflow_artifacts:
  grafana_data:
```

Puis lancer et vérifier :

```bash
docker compose up -d
docker compose ps   # les 4 services doivent être "running"
```

---

# Phase 2 — Initialisation de la base TimescaleDB (Jour 2)

Créer `/opt/trendx/init-db.sql` — schéma inspiré du stockage interne de Trendz :

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Télémétrie brute extraite de ThingsBoard
CREATE TABLE IF NOT EXISTS sensor_data (
  time        TIMESTAMPTZ NOT NULL,
  device_id   TEXT NOT NULL,
  metric_name TEXT NOT NULL,
  value       DOUBLE PRECISION
);
SELECT create_hypertable('sensor_data','time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_sensor ON sensor_data (device_id, metric_name, time DESC);

-- Prédictions (équivalent des clés _EPD_ de Trendz)
CREATE TABLE IF NOT EXISTS predictions (
  prediction_time  TIMESTAMPTZ NOT NULL,
  device_id        TEXT NOT NULL,
  metric_name      TEXT NOT NULL,
  algorithm        TEXT NOT NULL,          -- linear | arima | fourier | prophet
  predicted_value  DOUBLE PRECISION,
  conf_lower       DOUBLE PRECISION,
  conf_upper       DOUBLE PRECISION,
  model_version    TEXT,
  created_at       TIMESTAMPTZ DEFAULT NOW()
);
SELECT create_hypertable('predictions','prediction_time', if_not_exists => TRUE);

-- Scores d'anomalie (Anomaly Score + Score Index, comme Trendz)
CREATE TABLE IF NOT EXISTS anomaly_scores (
  time                TIMESTAMPTZ NOT NULL,
  device_id           TEXT NOT NULL,
  metric_name         TEXT NOT NULL,
  anomaly_score       DOUBLE PRECISION,   -- intensité de la déviation
  anomaly_score_index DOUBLE PRECISION,   -- intensité x durée
  is_anomaly          BOOLEAN DEFAULT FALSE
);
SELECT create_hypertable('anomaly_scores','time', if_not_exists => TRUE);

-- Alertes de maintenance préventive
CREATE TABLE IF NOT EXISTS maintenance_alerts (
  id SERIAL PRIMARY KEY,
  device_id TEXT NOT NULL,
  alert_type TEXT NOT NULL,     -- anomaly | forecast_threshold | degradation
  severity   TEXT NOT NULL,     -- info | warning | critical
  message    TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  acknowledged BOOLEAN DEFAULT FALSE
);

-- Champs calculés / KPI : agrégats continus (équivalent Calculated Fields + Aggregation de Trendz)
CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_data_hourly
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', time) AS bucket, device_id, metric_name,
       AVG(value) AS avg_value, MIN(value) AS min_value,
       MAX(value) AS max_value, STDDEV(value) AS std_value, COUNT(*) AS n
FROM sensor_data GROUP BY bucket, device_id, metric_name WITH NO DATA;

SELECT add_continuous_aggregate_policy('sensor_data_hourly',
  start_offset => INTERVAL '3 hours', end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour', if_not_exists => TRUE);

-- Rétention : 2 ans de données brutes
SELECT add_retention_policy('sensor_data', INTERVAL '2 years', if_not_exists => TRUE);
```

Appliquer et vérifier :

```bash
docker exec -it trendx-db psql -U trendx -d trendx_analytics -c "\dt"
```

---

# Phase 3 — Connecteur ThingsBoard : découverte de topologie et extraction (Jour 3–4)

Équivalent du « Discover topology » de Trendz. Créer `dags/ml_engine/tb_client.py` :

```python
import os, requests

class TBClient:
    def __init__(self):
        self.base = os.environ["TB_BASE_URL"]
        self.token = None

    def login(self):
        r = requests.post(f"{self.base}/api/auth/login", json={
            "username": os.environ["TB_USERNAME"],
            "password": os.environ["TB_PASSWORD"]})
        r.raise_for_status()
        self.token = r.json()["token"]

    def _h(self):
        return {"X-Authorization": f"Bearer {self.token}"}

    # 1) Topologie : liste des devices du tenant
    def devices(self, page_size=100):
        r = requests.get(f"{self.base}/api/tenant/devices",
            params={"pageSize": page_size, "page": 0}, headers=self._h())
        return r.json()["data"]

    # 2) Extraction télémétrie historique agrégée
    def telemetry(self, device_id, keys, start_ms, end_ms, interval_ms=3600000, agg="AVG"):
        r = requests.get(
            f"{self.base}/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries",
            params={"keys": keys, "startTs": start_ms, "endTs": end_ms,
                    "interval": interval_ms, "agg": agg, "limit": 50000},
            headers=self._h())
        return r.json()

    # 3) Réécriture des prévisions dans ThingsBoard (clé _EPD_ comme Trendz)
    def save_telemetry(self, device_id, key, ts_value_pairs):
        payload = [{"ts": ts, "values": {f"_EPD_{key}": v}} for ts, v in ts_value_pairs]
        r = requests.post(
            f"{self.base}/api/plugins/telemetry/DEVICE/{device_id}/timeseries/ANY",
            json=payload, headers=self._h())
        r.raise_for_status()

    # 4) Création d'alarme (monitoring continu)
    def create_alarm(self, device_id, alarm_type, severity="WARNING"):
        r = requests.post(f"{self.base}/api/alarm", headers=self._h(), json={
            "originator": {"entityType": "DEVICE", "id": device_id},
            "type": alarm_type, "severity": severity, "status": "ACTIVE_UNACK"})
        r.raise_for_status()
```

**Étapes de validation** :

1. Lister les devices → vérifier que la topologie remonte.
2. Extraire 90 jours d'une télémétrie (ex. `energyConsumption`) en agrégat horaire.
3. Insérer le résultat dans `sensor_data` et contrôler avec `SELECT count(*) FROM sensor_data;`.

---

# Phase 4 — Moteur ML : normalisation et prédiction (Jour 5–8)

Reproduction du pipeline automatique Trendz : *filtrage → normalisation → segmentation → entraînement → forecast*.

1. **Normalisation automatique** (`normalizer.py`) : sélection auto de la méthode selon la distribution — `RobustScaler` si outliers > 5 % ou kurtosis > 5, `MinMaxScaler` si asymétrie > 2, sinon `StandardScaler` (z-score). Rééchantillonnage régulier + interpolation des trous.
2. **Segmentation** (comme Trendz) : découpage du jeu d'entraînement en segments égaux — stratégies `FIXED`, `SLIDING_WINDOW`, `STICK_TO_END`, `AUTO`.
3. **Algorithmes de prévision** (`forecasters.py`) — les méthodes officielles de Trendz :
    - **Régression linéaire** → `sklearn.linear_model.LinearRegression` (variables explicatives multiples).
    - **ARIMA/SARIMA** → `statsmodels.tsa.SARIMAX` + `pmdarima.auto_arima` pour l'auto-sélection (p,d,q).
    - **Transformation de Fourier** → `scipy.fft` : extraction des composantes fréquentielles dominantes, reconstruction et extrapolation des cycles (journalier/hebdo/saisonnier).
    - **Prophet** → tendance + saisonnalités + jours fériés, intervalles de confiance natifs.
    - *(Bonus)* modèle custom Python injectable, comme le « Custom Model » de Trendz.
4. **Compétition de modèles** (`model_selector.py`) : backtesting sur les derniers segments, calcul MAE/RMSE/MAPE par algorithme, sélection automatique du meilleur → journalisation dans **MLflow** (version, paramètres, métriques).
5. **Accuracy à la Trendz** :
    - *Confidence Level* : % d'unités de temps où |erreur valeur| ≤ seuil ET |décalage temporel| ≤ seuil.
    - *Confidence Band* : erreur en % normalisée par la plage MIN–MAX de la télémétrie (min/max/moyenne par segment, percentile configurable).
6. **Limites de prédiction** : borner les valeurs prédites (min/max métier), comme l'option « Set Limits » de Trendz.

---

# Phase 5 — Détection d'anomalies non supervisée (Jour 9–11)

Reproduction du module Anomaly Detection de Trendz (aucun seuil manuel, aucune donnée labellisée) :

1. **Fenêtrage** : découpage de la télémétrie en fenêtres glissantes ; extraction de features (moyenne, écart-type, pente, énergie spectrale).
2. **Modèles** : `IsolationForest` (scikit-learn) et/ou `KMeans`/`DBSCAN` avec distance euclidienne ou DTW ; bibliothèque **PyOD** pour varier les détecteurs.
3. **Scoring** (métriques officielles Trendz) :
    - `anomaly_score` = intensité de la déviation par rapport au comportement normal appris ;
    - `anomaly_score_index` = score composite **intensité × durée** pour prioriser les dérives longues (ex. dégradation lente d'une CRAC ou d'un UPS — cas SCADA).
4. **Paramètres utilisateur** : sensibilité (contamination) et granularité de détection (taille de fenêtre), comme le wizard « Quick Scan » de Trendz.
5. **Sorties** : insertion dans `anomaly_scores` + création d'alertes dans `maintenance_alerts`.

---

# Phase 6 — Orchestration Airflow : le pipeline complet (Jour 12–13)

Créer le DAG `dags/trendx_pipeline.py`, planifié toutes les heures (équivalent des « Jobs » et du « Periodic Scanning » de Trendz) :

```python
# Enchaînement des tâches (ordre chronologique d'exécution) :
# 1. extract_telemetry      → API ThingsBoard → sensor_data
# 2. normalize_data         → normalisation auto + rééchantillonnage
# 3. train_and_compete      → algos entraînés, meilleur modèle retenu (MLflow)
# 4. generate_forecast      → prévisions + bandes de confiance → predictions
# 5. detect_anomalies       → scores → anomaly_scores
# 6. writeback_thingsboard  → télémétrie _EPD_<key> + scores vers ThingsBoard
# 7. raise_alerts           → maintenance_alerts + POST /api/alarm si seuil dépassé
```

**Règles d'ordonnancement** :

- Ré-entraînement complet : 1×/jour (la nuit). Prévision + scan d'anomalies : toutes les heures.
- Chaque tâche écrit ses logs ; en cas d'échec → 3 retries puis alerte e-mail/webhook.

---

# Phase 7 — Visualisation : dashboards Grafana + ThingsBoard (Jour 14–15)

1. **Provisionner la datasource** Grafana → TimescaleDB (`grafana/provisioning/datasources/timescaledb.yml`).
2. **Créer les vues équivalentes à Trendz** :
    - *Line chart* réel vs prédit avec bande de confiance (`sensor_data` + `predictions`) ;
    - *Bar chart* consommation par jour/semaine ; *Heatmap* activité/anomalies ; *Table* KPI par device ; *Card/Stat* dernière valeur + tendance.
3. **Variables de dashboard** (`device_id`, `metric_name`) = équivalent des filtres et alias de vues Trendz.
4. **Alerting Grafana** sur écart réel/prévu et sur `anomaly_score`.
5. **Intégration ThingsBoard** (équivalent « Share to ThingsBoard ») :
    - les prévisions réécrites sous `_EPD_<key>` s'affichent nativement dans les widgets Time-series des dashboards ThingsBoard ;
    - option : intégrer un panneau Grafana en iframe dans un dashboard ThingsBoard.

---

# Phase 8 — Tests, validation et mise en production (Jour 16–18)

1. **Jeu de données de test** : script `scripts/generate_test_data.py` (série avec saisonnalité + bruit + anomalies injectées) si les historiques réels sont insuffisants.
2. **Validation fonctionnelle** (checklist) :
    - [ ]  Topologie ThingsBoard découverte et synchronisée
    - [ ]  90 jours de télémétrie extraits et agrégés
    - [ ]  Les 4 algorithmes s'entraînent sans erreur ; meilleur modèle tracé dans MLflow
    - [ ]  Confidence Level / Confidence Band calculés et cohérents
    - [ ]  Prévisions visibles dans ThingsBoard sous `_EPD_<key>`
    - [ ]  Anomalies injectées détectées (score élevé), peu de faux positifs
    - [ ]  Alarmes ThingsBoard créées automatiquement
    - [ ]  Dashboards Grafana opérationnels avec filtres
3. **Durcissement** : HTTPS via reverse proxy (nginx/traefik), rotation des mots de passe, sauvegarde PostgreSQL (`pg_dump` planifié), supervision des conteneurs.
4. **Gouvernance des données** (recommandations du rapport SCADA) : vérifier la qualité/cohérence de la télémétrie, valider les KPI avec le métier, prévoir une phase d'ajustement des seuils de sensibilité pour limiter les faux positifs.

---

# Récapitulatif chronologique

| Ordre | Phase | Durée indicative | Livrable |
| --- | --- | --- | --- |
| 1 | Prérequis & test API ThingsBoard | J1 | Accès JWT validé |
| 2 | Infrastructure Docker | J1–J2 | 4 services up |
| 3 | Schéma TimescaleDB | J2 | Hypertables + agrégats |
| 4 | Connecteur ThingsBoard | J3–J4 | Extraction + writeback |
| 5 | Moteur ML prédiction | J5–J8 | 4 algos + sélection auto |
| 6 | Détection d'anomalies | J9–J11 | Scores + alertes |
| 7 | DAG Airflow | J12–J13 | Pipeline automatisé |
| 8 | Dashboards | J14–J15 | Grafana + ThingsBoard |
| 9 | Tests & production | J16–J18 | Solution Trendx en service |

<aside>
💡

Conseil MVP : livrez d'abord la chaîne minimale « extraction → Prophet → writeback `_EPD_` → line chart réel vs prédit » (phases 1–4 + un seul algorithme), puis ajoutez ARIMA/Fourier, la compétition de modèles et la détection d'anomalies par itérations successives.

</aside>