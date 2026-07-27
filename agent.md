# Trendx — Instructions de l’agent de Vibe Coding

## 1. Mission

Tu es l’agent principal chargé de concevoir, développer, tester, déployer et maintenir **Trendx**, une solution open source d’analyse, de prévision et de détection d’anomalies pour ThingsBoard.

Trendx doit reproduire les fonctions essentielles de ThingsBoard Trendz Analytics :

- extraction de télémétrie ThingsBoard ;
- stockage et agrégation dans TimescaleDB ;
- calcul de KPI ;
- prévision de séries temporelles ;
- compétition et versionnement de modèles ;
- détection non supervisée d’anomalies ;
- réécriture des résultats dans ThingsBoard ;
- création d’alarmes ;
- orchestration avec Airflow ;
- suivi des modèles dans MLflow ;
- visualisation avec Grafana et ThingsBoard.

## 2. Architecture cible

La stack principale est :

- ThingsBoard Community Edition existant ;
- PostgreSQL avec TimescaleDB ;
- Apache Airflow ;
- Python 3.11 ;
- pandas et NumPy ;
- scikit-learn ;
- statsmodels ;
- SciPy ;
- Prophet ;
- pmdarima ;
- PyOD ;
- MLflow ;
- Grafana ;
- Docker et Docker Compose ;
- Git.

Ne jamais déployer une deuxième instance ThingsBoard sans demande explicite.

## 3. Principes de travail

### 3.1 Observer avant de modifier

Avant toute modification :

1. inspecter l’état actuel du projet ;
2. lire les fichiers existants ;
3. identifier les conventions utilisées ;
4. vérifier les versions installées ;
5. contrôler les dépendances ;
6. rechercher les tests associés ;
7. présenter ou enregistrer un plan court.

Ne jamais remplacer un fichier complet si une modification ciblée suffit.

### 3.2 Avancer par petits incréments

Pour chaque tâche :

1. définir le résultat attendu ;
2. effectuer le plus petit changement fonctionnel ;
3. exécuter les tests ciblés ;
4. vérifier les logs ;
5. corriger les erreurs ;
6. exécuter les tests de non-régression ;
7. documenter le résultat.

Ne pas développer plusieurs sous-systèmes critiques simultanément.

### 3.3 Ne jamais masquer les erreurs

Il est interdit de :

- ignorer silencieusement une exception ;
- retourner une valeur vide pour cacher un échec ;
- désactiver un test sans justification ;
- ajouter un `try/except Exception: pass` ;
- considérer un conteneur démarré comme fonctionnel sans health check ;
- considérer un pipeline réussi sans vérifier ses sorties.

Les erreurs doivent être :

- journalisées avec leur contexte ;
- classifiées ;
- propagées lorsque nécessaire ;
- associées à une stratégie de reprise.

## 4. Priorité d’implémentation

Toujours suivre cet ordre, sauf instruction contraire.

### Étape 1 — Audit

- vérifier l’OS et les ressources ;
- vérifier Docker et Docker Compose ;
- identifier les ports utilisés ;
- vérifier l’instance ThingsBoard ;
- vérifier l’accès à l’API ;
- identifier la version ThingsBoard ;
- inventorier les devices et clés de télémétrie disponibles.

### Étape 2 — Socle reproductible

- créer l’arborescence du projet ;
- créer les fichiers Docker ;
- créer les images personnalisées ;
- épingler les versions ;
- configurer les health checks ;
- préparer les secrets sans les versionner.

### Étape 3 — Base de données

- créer les bases et utilisateurs séparés ;
- activer TimescaleDB ;
- appliquer les migrations ;
- créer les hypertables ;
- créer les index ;
- créer les agrégats continus ;
- vérifier l’idempotence.

### Étape 4 — Connecteur ThingsBoard

- authentification ;
- renouvellement JWT ;
- découverte de topologie ;
- extraction paginée ;
- extraction par fenêtres temporelles ;
- retries et backoff ;
- ingestion idempotente ;
- réécriture de télémétrie ;
- gestion des alarmes.

### Étape 5 — MVP de prévision

Implémenter d’abord :

```

ThingsBoard

→ extraction

→ TimescaleDB

→ normalisation

→ Prophet

→ predictions

→ writeback *EPD*<key>

→ dashboard réel/prédit

```

N’ajouter ARIMA, Fourier, compétition de modèles et anomalies qu’après validation du MVP.

### Étape 6 — Pipeline complet

- régression linéaire ;
- ARIMA/SARIMA ;
- Fourier ;
- Prophet ;
- modèle personnalisé ;
- backtesting ;
- sélection du meilleur modèle ;
- MLflow ;
- détection d’anomalies ;
- alertes ;
- dashboards complets.

## 5. Structure recommandée du dépôt

```

trendx/

├── [agent.md](http://agent.md)

├── [README.md](http://README.md)

├── .env.example

├── .gitignore

├── docker-compose.yml

├── Makefile

├── pyproject.toml

├── constraints.txt

├── docker/

│   ├── airflow/

│   │   └── Dockerfile

│   └── mlflow/

│       └── Dockerfile

├── migrations/

│   ├── 001_extensions.sql

│   ├── 002_telemetry.sql

│   ├── 003_predictions.sql

│   ├── 004_anomalies.sql

│   └── 005_aggregates.sql

├── dags/

│   ├── trendx_[ingestion.py](http://ingestion.py)

│   ├── trendx_[forecasting.py](http://forecasting.py)

│   ├── trendx_anomaly_[detection.py](http://detection.py)

│   └── trendx_[training.py](http://training.py)

├── src/

│   └── trendx/

│       ├── **init**.py

│       ├── [config.py](http://config.py)

│       ├── [logging.py](http://logging.py)

│       ├── thingsboard/

│       │   ├── [client.py](http://client.py)

│       │   ├── [discovery.py](http://discovery.py)

│       │   ├── [telemetry.py](http://telemetry.py)

│       │   └── [alarms.py](http://alarms.py)

│       ├── database/

│       │   ├── [connection.py](http://connection.py)

│       │   ├── [repositories.py](http://repositories.py)

│       │   └── [models.py](http://models.py)

│       ├── preprocessing/

│       │   ├── [normalizer.py](http://normalizer.py)

│       │   ├── [resampling.py](http://resampling.py)

│       │   └── [segmentation.py](http://segmentation.py)

│       ├── forecasting/

│       │   ├── [base.py](http://base.py)

│       │   ├── [linear.py](http://linear.py)

│       │   ├── [arima.py](http://arima.py)

│       │   ├── [fourier.py](http://fourier.py)

│       │   ├── [prophet.py](http://prophet.py)

│       │   └── [selector.py](http://selector.py)

│       ├── anomalies/

│       │   ├── [features.py](http://features.py)

│       │   ├── [detectors.py](http://detectors.py)

│       │   └── [scoring.py](http://scoring.py)

│       ├── mlops/

│       │   ├── [tracking.py](http://tracking.py)

│       │   └── [registry.py](http://registry.py)

│       └── services/

│           ├── [ingestion.py](http://ingestion.py)

│           ├── [training.py](http://training.py)

│           ├── [inference.py](http://inference.py)

│           └── [alerting.py](http://alerting.py)

├── grafana/

│   ├── provisioning/

│   │   ├── datasources/

│   │   └── dashboards/

│   └── dashboards/

├── scripts/

│   ├── [bootstrap.sh](http://bootstrap.sh)

│   ├── [healthcheck.sh](http://healthcheck.sh)

│   ├── generate_test_[data.py](http://data.py)

│   ├── [backup.sh](http://backup.sh)

│   └── [restore.sh](http://restore.sh)

└── tests/

├── unit/

├── integration/

├── contract/

└── end_to_end/

```

## 6. Règles d’infrastructure

### 6.1 Ports

L’instance ThingsBoard cible utilise :

```

http://10.0.0.1:8081

```

Ne pas publier Airflow sur le port hôte `8081` si Airflow est déployé sur la même machine.

Utiliser par défaut :

```

ThingsBoard : 8081

Airflow     : 8082

MLflow      : 5000

Grafana     : 3000

PostgreSQL  : 5432

```

Avant publication d’un port, vérifier qu’il est disponible.

### 6.2 Images Docker

Ne pas utiliser de tag `latest` en production.

Utiliser :

- des versions fixes ;
- si possible, des digests ;
- un Dockerfile Airflow personnalisé ;
- des dépendances Python épinglées ;
- des health checks ;
- des utilisateurs non-root lorsque possible.

Ne pas utiliser `_PIP_ADDITIONAL_REQUIREMENTS` comme mécanisme permanent d’installation.

### 6.3 Bases séparées

Utiliser des bases ou schémas et utilisateurs séparés :

```

trendx_analytics

trendx_airflow

trendx_mlflow

```

Appliquer le principe du moindre privilège.

Airflow ne doit pas être propriétaire des tables analytiques.

MLflow ne doit pas pouvoir modifier les métadonnées Airflow.

### 6.4 Destruction de données

Toujours demander une confirmation explicite avant :

- `docker compose down -v` ;
- suppression d’un volume ;
- `DROP DATABASE` ;
- `DROP TABLE` ;
- suppression massive de télémétrie ;
- purge de modèles ;
- suppression d’un dashboard de production ;
- suppression ou fermeture massive d’alarmes.

## 7. Gestion des secrets

Ne jamais :

- écrire un mot de passe dans le code ;
- commiter `.env` ;
- afficher un JWT ;
- journaliser les en-têtes d’authentification ;
- recopier un secret dans un rapport ;
- utiliser les mots de passe d’exemple en production.

Les secrets attendus sont notamment :

```

TB_BASE_URL

TB_USERNAME

TB_PASSWORD

PG_HOST

PG_PORT

PG_USER

PG_PASSWORD

PG_DB

AIRFLOW_DB_URL

MLFLOW_TRACKING_URI

MLFLOW_DB_URL

GRAFANA_URL

GRAFANA_SERVICE_ACCOUNT_TOKEN

```

Le fichier `.env.example` doit contenir uniquement des valeurs factices.

Le fichier `.env` doit avoir des permissions restrictives :

```

chmod 600 .env

```

## 8. Standards Python

### 8.1 Qualité

Le code doit être :

- typé ;
- modulaire ;
- testable ;
- documenté ;
- compatible avec le système de logging ;
- sans dépendance implicite à Airflow dans le cœur métier.

Utiliser de préférence :

- `ruff` pour le lint et le formatage ;
- `mypy` ou `pyright` pour le typage ;
- `pytest` pour les tests ;
- `pydantic-settings` pour la configuration ;
- `tenacity` pour les retries contrôlés ;
- SQLAlchemy ou psycopg avec requêtes paramétrées.

### 8.2 Interfaces

Les modèles de prévision doivent respecter une interface commune :

```

from typing import Protocol

class ForecastModel(Protocol):

def fit(self, data, *, context=None) -> "ForecastModel":

...

def predict(self, horizon: int, *, context=None):

...

def save(self, path: str) -> None:

...

@classmethod

def load(cls, path: str) -> "ForecastModel":

...

```

Les détecteurs d’anomalies doivent respecter une interface comparable :

```

class AnomalyDetector(Protocol):

def fit(self, features):

...

def score(self, features):

...

def predict(self, features):

...

```

## 9. Connecteur ThingsBoard

Le client ThingsBoard doit gérer :

- timeout de connexion ;
- timeout de lecture ;
- retries avec backoff exponentiel ;
- expiration et renouvellement du token ;
- pagination ;
- fenêtres temporelles ;
- limite maximale de résultats ;
- erreurs 401, 403, 404, 409, 429 et 5xx ;
- journalisation sans secrets ;
- validation des réponses ;
- métriques d’exécution.

Ne jamais supposer qu’une seule page contient tous les devices.

### 9.1 Ingestion idempotente

Une nouvelle extraction ne doit pas dupliquer les données existantes.

Prévoir :

- une contrainte d’unicité adaptée ;
- un mécanisme d’upsert ;
- un checkpoint par device et métrique ;
- une légère fenêtre de recouvrement ;
- une déduplication transactionnelle.

Tous les timestamps doivent être stockés en UTC.

### 9.2 Writeback

Les prévisions doivent être écrites sous :

```

*EPD*<metric_name>

```

Avant un writeback massif :

1. vérifier le device ;
2. vérifier la clé ;
3. vérifier l’horizon ;
4. vérifier les timestamps ;
5. vérifier les valeurs nulles ou infinies ;
6. contrôler les limites métier ;
7. écrire par lots ;
8. vérifier un échantillon après écriture.

### 9.3 Alarmes

Les alarmes doivent être idempotentes.

Éviter de créer une nouvelle alarme à chaque exécution. Utiliser :

- une clé logique d’incident ;
- un cooldown ;
- une hystérésis ;
- un seuil d’ouverture ;
- un seuil de fermeture ;
- un mécanisme d’acquittement ;
- une durée minimale.

## 10. Règles de séries temporelles

### 10.1 Prévention de la fuite de données

Il est interdit :

- d’ajuster un scaler sur l’ensemble entraînement + test ;
- d’interpoler une donnée d’entraînement avec une valeur future ;
- d’utiliser une validation croisée aléatoire ;
- de sélectionner un modèle sur la période finale de test ;
- de calculer des features utilisant involontairement le futur.

Utiliser un backtesting temporel de type walk-forward.

### 10.2 Normalisation

Choisir automatiquement :

- `RobustScaler` lorsque les outliers sont importants ou la kurtosis élevée ;
- `MinMaxScaler` pour certaines distributions fortement asymétriques ;
- `StandardScaler` lorsque la distribution est suffisamment stable.

Enregistrer dans MLflow :

- le scaler choisi ;
- ses paramètres ;
- la période d’apprentissage ;
- la version des données ;
- la méthode d’interpolation ;
- la fréquence de rééchantillonnage.

### 10.3 Métriques

Calculer au minimum :

- MAE ;
- RMSE ;
- sMAPE ;
- MAPE uniquement lorsque les valeurs proches de zéro sont maîtrisées ;
- couverture des intervalles ;
- largeur moyenne des intervalles ;
- biais moyen ;
- durée d’entraînement ;
- durée d’inférence.

Ne jamais sélectionner un modèle uniquement sur MAPE.

### 10.4 Sélection du modèle

Le modèle champion doit satisfaire :

1. une métrique principale ;
2. une stabilité suffisante entre fenêtres ;
3. une couverture minimale des intervalles ;
4. les contraintes métier ;
5. un temps d’exécution acceptable ;
6. l’absence d’erreur ou de convergence instable.

Ne pas promouvoir automatiquement un nouveau modèle si le gain est marginal ou instable.

Conserver le champion précédent pour permettre un rollback.

## 11. Détection d’anomalies

Le pipeline doit :

1. rééchantillonner la série ;
2. produire des fenêtres ;
3. calculer les features ;
4. entraîner le détecteur sur une période de référence ;
5. calculer un score normalisé ;
6. regrouper les points en épisodes ;
7. calculer l’intensité ;
8. calculer la durée ;
9. calculer `anomaly_score_index` ;
10. appliquer une politique d’alerte.

Features minimales :

- moyenne ;
- écart-type ;
- minimum ;
- maximum ;
- pente ;
- amplitude ;
- énergie spectrale ;
- différence avec la fenêtre précédente ;
- taux de valeurs manquantes.

Les paramètres de contamination, sensibilité et durée minimale doivent être configurables.

## 12. MLflow

Chaque entraînement doit enregistrer :

- device ;
- métrique ;
- période des données ;
- version ou hash des données ;
- fréquence ;
- algorithme ;
- hyperparamètres ;
- scaler ;
- métriques par segment ;
- métriques agrégées ;
- artefact du modèle ;
- version du code ;
- date d’entraînement ;
- statut de validation.

La version MLflow doit être choisie explicitement.

Ne pas supposer que le MCP officiel de MLflow couvre automatiquement les expériences classiques et le Model Registry. Utiliser l’API MLflow ou un adaptateur dédié si nécessaire.

## 13. Airflow

Séparer de préférence les responsabilités :

```

trendx_ingestion_hourly

trendx_forecasting_hourly

trendx_anomaly_scan_hourly

trendx_training_daily

trendx_maintenance_daily

```

Les tâches doivent être :

- idempotentes ;
- rejouables ;
- limitées par timeout ;
- observables ;
- sans état local non versionné ;
- protégées contre les exécutions concurrentes incompatibles.

Éviter de transférer de gros DataFrames avec XCom.

Stocker les résultats volumineux dans TimescaleDB ou un stockage d’artefacts.

Configurer :

- retries ;
- retry delay ;
- exponential backoff ;
- execution timeout ;
- pools ;
- max active runs ;
- notifications d’échec.

## 14. Grafana

Les dashboards doivent être provisionnés et versionnés.

Variables minimales :

```

device_id

metric_name

time_range

algorithm

```

Dashboards minimaux :

1. santé de la plateforme ;
2. télémétrie réelle ;
3. réel versus prédit ;
4. bandes de confiance ;
5. anomalies ;
6. alertes de maintenance ;
7. qualité des données ;
8. performance des modèles.

Toutes les requêtes Grafana doivent :

- être paramétrées ;
- utiliser les index ;
- limiter le volume retourné ;
- respecter la plage temporelle ;
- éviter les scans complets inutiles.

## 15. Tests obligatoires

### 15.1 Tests unitaires

Tester notamment :

- normalisation ;
- segmentation ;
- génération des features ;
- calcul des métriques ;
- limites métier ;
- score d’anomalie ;
- sérialisation des modèles ;
- parsing des réponses ThingsBoard.

### 15.2 Tests d’intégration

Tester :

- authentification ThingsBoard ;
- extraction d’une petite fenêtre ;
- insertion TimescaleDB ;
- upsert sans doublon ;
- création d’un run MLflow ;
- déclenchement d’un DAG ;
- lecture Grafana ;
- writeback sur un device de test.

### 15.3 Tests end-to-end

Le scénario complet doit vérifier :

```

donnée ThingsBoard

→ extraction

→ stockage

→ entraînement

→ prédiction

→ writeback

→ affichage

→ anomalie

→ alarme

```

Ne jamais exécuter un test destructif sur un device de production.

## 16. Définition de terminé

Une tâche n’est terminée que si :

- le code est implémenté ;
- le code est formaté ;
- le lint passe ;
- les tests ciblés passent ;
- les tests de non-régression passent ;
- les logs ne contiennent pas de secret ;
- la documentation est actualisée ;
- le comportement a été vérifié ;
- une procédure de rollback existe pour les changements sensibles.

Pour un déploiement :

- les conteneurs sont healthy ;
- les migrations sont appliquées ;
- les API répondent ;
- un test fonctionnel est passé ;
- les dashboards sont accessibles ;
- les sauvegardes sont vérifiées.

## 17. Commandes de développement recommandées

Le projet doit exposer des commandes simples et stables :

```

make setup

make build

make up

make down

make status

make logs

make lint

make typecheck

make test

make test-integration

make test-e2e

make migrate

make seed-test-data

make backup

make restore-check

```

Ne jamais faire exécuter à l’utilisateur une longue suite de commandes lorsqu’une cible `make` peut l’encapsuler.

## 18. Utilisation des MCP

Lorsque les MCP sont disponibles, les utiliser dans cet ordre :

1. Git et filesystem pour comprendre le code ;
2. SSH pour auditer l’hôte ;
3. Docker pour inspecter les services ;
4. PostgreSQL pour vérifier données et schémas ;
5. ThingsBoard pour vérifier entités et télémétrie ;
6. Airflow pour contrôler les DAG ;
7. MLflow pour comparer les modèles ;
8. Grafana pour valider les dashboards.

Accès MCP attendus :

- SSH ;
- filesystem ;
- Docker ;
- Git ou GitHub ;
- ThingsBoard ;
- PostgreSQL/TimescaleDB ;
- Airflow ;
- MLflow ;
- Grafana ;
- HTTP/OpenAPI ;
- coffre de secrets ;
- observabilité et notifications si disponibles.

Ne jamais contourner les permissions d’un MCP.

Ne jamais envoyer un secret récupéré par un MCP vers un autre système.

## 19. Actions soumises à approbation

Demander une confirmation avant :

- modification de production ;
- arrêt de ThingsBoard ;
- arrêt de TimescaleDB ;
- suppression de volume ;
- migration destructive ;
- rotation d’un secret ;
- ouverture d’un port public ;
- changement de pare-feu ;
- modification du reverse proxy ;
- promotion d’un modèle en production ;
- writeback massif ;
- création massive d’alarmes ;
- restauration d’une sauvegarde ;
- purge de données.

## 20. Format des comptes rendus

Après chaque tâche, fournir un compte rendu court :

```

Objectif :

Changements :

Fichiers modifiés :

Tests exécutés :

Résultat :

Risques ou limites :

Rollback :

Prochaine étape prévue :

```

Ne pas annoncer qu’une opération a réussi sans preuve observable.

## 21. Règles spécifiques au vibe coding

Le mode vibe coding permet d’accélérer la création, mais ne réduit pas les exigences de qualité.

L’agent doit :

- transformer les intentions en petites tâches vérifiables ;
- produire du code exécutable, pas uniquement des exemples ;
- vérifier les signatures des API et bibliothèques ;
- préférer les composants simples ;
- éviter les abstractions prématurées ;
- supprimer le code mort ;
- conserver les conventions du projet ;
- écrire les tests en même temps que le code ;
- refactoriser après validation fonctionnelle ;
- expliquer clairement tout compromis technique.

Si une information manque :

1. inspecter le projet et l’environnement ;
2. rechercher dans la documentation officielle ;
3. choisir une valeur sûre et configurable ;
4. documenter l’hypothèse ;
5. ne demander à l’utilisateur que les informations impossibles à déduire.

## 22. Objectif MVP final

Le MVP est considéré comme réussi lorsque :

- au moins un device et une métrique sont découverts ;
- 90 jours de données peuvent être extraits ;
- les données sont stockées sans doublon ;
- un modèle Prophet est entraîné ;
- les métriques sont enregistrées dans MLflow ;
- des prévisions sont stockées dans TimescaleDB ;
- les valeurs sont réécrites dans ThingsBoard sous `_EPD_<key>` ;
- Grafana affiche réel et prédit ;
- le pipeline horaire est orchestré par Airflow ;
- un test end-to-end reproductible passe ;
- les secrets ne sont ni exposés ni versionnés ;
- une sauvegarde et une procédure de restauration existent.