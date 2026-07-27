# Analyse des SKILLS nécessaires à l’agent IA Trendx

Le document décrit bien plus qu’un moteur de prévision : il s’agit d’une **plateforme MLOps/IoT complète**, depuis ThingsBoard jusqu’aux tableaux de bord et aux alarmes. L’agent IA devra donc combiner des compétences d’architecture, d’administration système, de traitement de séries temporelles, de machine learning, de sécurité et d’exploitation.

Une simple skill « data science » serait insuffisante. Je recommande une architecture composée de **22 SKILLS spécialisées**, coordonnées par une skill d’orchestration principale.

---

## 1. Vue d’ensemble des SKILLS

| Domaine | SKILLS nécessaires |
| --- | --- |
| Pilotage | 1. Orchestration Trendx, 2. Planification et suivi d’exécution |
| Infrastructure | 3. Diagnostic des prérequis, 4. Docker Compose, 5. Configuration et secrets |
| Données | 6. TimescaleDB, 7. Connecteur ThingsBoard, 8. Ingestion incrémentale, 9. Qualité des données |
| Préparation ML | 10. Préparation des séries temporelles, 11. Segmentation et backtesting |
| Prévision | 12. Régression linéaire, 13. ARIMA/SARIMA, 14. Fourier, 15. Prophet, 16. Sélection de modèles |
| Anomalies | 17. Détection d’anomalies non supervisée |
| MLOps | 18. MLflow et cycle de vie des modèles, 19. Airflow |
| Restitution | 20. Writeback et alarmes ThingsBoard, 21. Grafana et visualisation |
| Production | 22. Tests, sécurité, observabilité et reprise |

Certaines SKILLS peuvent être regroupées lors d’un MVP, mais leurs responsabilités doivent rester clairement séparées.

---

# 2. SKILLS de pilotage

## SKILL 1 — `trendx-solution-orchestrator`

### Rôle

Piloter la mise en œuvre de bout en bout et sélectionner les SKILLS spécialisées à invoquer selon la phase du projet.

### Responsabilités

- Analyser l’état actuel de l’installation.
- Construire un plan d’exécution dépendant de cet état.
- Déterminer si l’on se trouve en mode :
    - installation initiale ;
    - MVP ;
    - ajout d’un modèle ;
    - diagnostic ;
    - maintenance ;
    - mise en production.
- Enchaîner les phases dans le bon ordre :
    1. prérequis ;
    2. infrastructure ;
    3. base ;
    4. connecteur ;
    5. données ;
    6. modèles ;
    7. orchestration ;
    8. visualisation ;
    9. validation ;
    10. production.
- Arrêter l’exécution si un contrôle bloquant échoue.
- Produire un rapport de progression et une synthèse des erreurs.

### Entrées attendues

- Environnement cible.
- URL ThingsBoard.
- liste des devices et métriques ciblées ;
- horizon historique et horizon de prévision ;
- contraintes métier des métriques ;
- état des conteneurs et composants existants.

### Sorties

- Plan d’exécution.
- Statut par phase.
- Liste des actions réalisées et restantes.
- Rapport de validation final.

### Garde-fous

Cette skill ne doit pas exécuter directement toutes les opérations. Elle doit déléguer aux SKILLS expertes, conserver l’état et contrôler leurs résultats.

---

## SKILL 2 — `trendx-implementation-planner`

### Rôle

Transformer la procédure générique en plan de déploiement concret et reproductible.

### Capacités

- Construire les jalons, dépendances et critères d’acceptation.
- Identifier le chemin critique.
- Distinguer :
    - MVP ;
    - version pilote ;
    - version de production.
- Générer des checklists idempotentes.
- Reprendre une installation partiellement terminée.
- Estimer les ressources nécessaires :
    - CPU ;
    - mémoire ;
    - stockage ;
    - rétention ;
    - volumétrie de télémétrie.

### Point important

Le document donne des durées indicatives, mais ne dimensionne pas l’infrastructure. La skill doit calculer les besoins à partir du nombre de devices, de métriques, de la fréquence d’échantillonnage et de la durée de rétention.

---

# 3. SKILLS d’infrastructure

## SKILL 3 — `trendx-environment-diagnostics`

### Rôle

Vérifier les prérequis avant toute modification.

### Contrôles nécessaires

- Accessibilité du serveur par SSH.
- Version du système d’exploitation.
- Version de Docker et de Docker Compose.
- Présence de l’instance ThingsBoard existante.
- Disponibilité des ports `5432`, `8081`, `5000` et `3000`.
- Espace disque et mémoire disponibles.
- Résolution réseau depuis les futurs conteneurs vers `10.0.0.1:8080`.
- Accès à Internet pour télécharger les images et dépendances.
- Permissions sur `/opt/trendx`.
- Santé des conteneurs déjà déployés.

### Sortie

Un diagnostic structuré :

- `PASS` ;
- `WARNING` ;
- `BLOCKING`;
- correction recommandée.

### Garde-fou critique

La skill doit détecter l’instance ThingsBoard existante et interdire le déploiement accidentel d’une seconde instance.

---

## SKILL 4 — `trendx-docker-compose-operations`

### Rôle

Créer, valider, déployer et maintenir la stack Docker.

### Connaissances requises

- Docker Compose v2.
- Réseaux, volumes et health checks.
- Ordre de démarrage.
- Substitution des variables d’environnement.
- Persistance des données.
- Lecture des logs et diagnostic des redémarrages.

### Actions

- Générer le fichier Compose.
- Valider sa syntaxe avec `docker compose config`.
- Télécharger les images.
- Démarrer ou mettre à jour les services.
- Vérifier la santé de TimescaleDB, Airflow, MLflow et Grafana.
- Inspecter les logs en cas d’échec.
- Éviter la suppression des volumes persistants.
- Produire une procédure de rollback.

### Améliorations nécessaires par rapport au document

La skill devrait ajouter :

- des `healthcheck`;
- des politiques de redémarrage ;
- des versions d’images figées ;
- un réseau Docker dédié ;
- des limites de ressources ;
- une initialisation Airflow explicite ;
- un service ou une base séparée pour les métadonnées Airflow et MLflow en production.

Le partage d’une seule base PostgreSQL logique entre données analytiques, Airflow et MLflow augmente le couplage opérationnel.

---

## SKILL 5 — `trendx-secrets-and-configuration`

### Rôle

Gérer les paramètres et secrets sans les exposer.

### Périmètre

- `.env`;
- identifiants ThingsBoard ;
- identifiants PostgreSQL ;
- secret Airflow ;
- mot de passe Grafana ;
- URI MLflow ;
- paramètres de rétention ;
- configuration des modèles.

### Garde-fous

- Ne jamais afficher un mot de passe dans les logs ou rapports.
- Ne jamais committer `.env`.
- Générer des mots de passe robustes.
- Vérifier les permissions du fichier.
- Masquer les secrets lors des diagnostics.
- Prévoir leur rotation.
- Privilégier Docker Secrets, Vault ou un gestionnaire équivalent en production.

### Anomalie relevée dans le document

`GF_SECURITY_ADMIN_PASSWORD: admin` constitue une configuration de démonstration, pas une configuration de production. L’agent doit la remplacer avant exposition du service.

---

# 4. SKILLS relatives aux données et à ThingsBoard

## SKILL 6 — `trendx-timescaledb-engineering`

### Rôle

Administrer le stockage temporel et créer le modèle de données.

### Capacités

- Installation et validation de l’extension TimescaleDB.
- Création d’hypertables.
- Indexation adaptée aux requêtes temporelles.
- Agrégats continus.
- Politiques de rafraîchissement.
- Compression et rétention.
- Requêtes SQL analytiques.
- Migration et évolution de schéma.
- Sauvegarde et restauration.

### Objets pris en charge

- `sensor_data`;
- `predictions`;
- `anomaly_scores`;
- `maintenance_alerts`;
- `sensor_data_hourly`.

### Validations

- Existence des tables et hypertables.
- Unicité logique des observations.
- Cohérence des timestamps.
- Performance des requêtes par device, métrique et période.
- Exécution correcte des politiques TimescaleDB.

### Amélioration recommandée

Ajouter des contraintes ou clés d’idempotence afin d’éviter les doublons, par exemple sur :

- télémétrie : `(time, device_id, metric_name)`;
- prévision : `(prediction_time, device_id, metric_name, algorithm, model_version)`;
- score : `(time, device_id, metric_name)`.

---

## SKILL 7 — `thingsboard-api-integration`

### Rôle

Fournir une intégration robuste avec l’API ThingsBoard.

### Capacités

- Authentification.
- Renouvellement du jeton.
- Découverte des devices.
- Pagination complète.
- Découverte des clés de télémétrie.
- Lecture de télémétrie historique.
- Écriture de télémétrie.
- Création, consultation et acquittement d’alarmes.
- Gestion des erreurs HTTP et des limitations.
- Validation de la compatibilité avec la version ThingsBoard CE installée.

### Risques non couverts par le code d’exemple

Le client présenté est un squelette. La skill doit notamment gérer :

- expiration du JWT ;
- pagination au-delà de 100 devices ;
- timeouts ;
- retries avec backoff ;
- réponses partielles ;
- limitation du nombre de points ;
- découpage des grandes plages temporelles ;
- déduplication ;
- erreurs d’autorisation ;
- schémas d’API différents selon la version de ThingsBoard.

---

## SKILL 8 — `trendx-incremental-telemetry-ingestion`

### Rôle

Extraire et charger la télémétrie de manière incrémentale et idempotente.

### Pipeline

1. Déterminer le dernier timestamp ingéré.
2. Calculer la prochaine fenêtre d’extraction.
3. Découper les plages trop volumineuses.
4. Appeler ThingsBoard.
5. Convertir les timestamps en UTC.
6. Transformer les valeurs en nombres.
7. Rejeter ou isoler les valeurs invalides.
8. Effectuer un upsert dans TimescaleDB.
9. Enregistrer un watermark d’ingestion.
10. Contrôler le nombre de points attendu et reçu.

### Modes requis

- backfill historique de 90 jours ;
- ingestion horaire ;
- reprise après interruption ;
- réextraction ciblée d’une période ;
- ingestion de plusieurs devices et métriques.

### Sorties

- nombre de points lus, insérés, mis à jour et rejetés ;
- début et fin de la fenêtre ;
- couverture temporelle ;
- anomalies de qualité.

---

## SKILL 9 — `trendx-data-quality-and-governance`

### Rôle

Contrôler la qualité des données avant les traitements ML.

### Contrôles

- données manquantes ;
- doublons ;
- timestamps désordonnés ;
- fréquence irrégulière ;
- trous de télémétrie ;
- valeurs constantes ;
- valeurs impossibles ou hors domaine ;
- changements d’unité ;
- dérive de capteur ;
- ruptures de distribution ;
- timezone et passage heure d’été/heure d’hiver.

### Décisions

- interpolation autorisée ou non ;
- longueur maximale d’un trou interpolable ;
- exclusion d’une période ;
- marquage d’un capteur comme non exploitable ;
- déclenchement d’une alerte de qualité.

### Nécessité métier

La procédure mentionne la validation des KPI avec le métier, mais ne définit pas de dictionnaire de données. La skill doit gérer un **registre de métriques** comprenant au minimum :

- nom ;
- unité ;
- bornes physiques ;
- fréquence attendue ;
- type d’agrégation ;
- direction d’alerte ;
- criticité ;
- politique d’interpolation.

---

# 5. SKILLS de préparation des séries temporelles

## SKILL 10 — `trendx-time-series-preprocessing`

### Rôle

Transformer la télémétrie brute en série exploitable par les modèles.

### Capacités

- Rééchantillonnage régulier.
- Agrégation.
- Interpolation contrôlée.
- Traitement des valeurs aberrantes.
- Imputation.
- Encodage des variables calendaires.
- Normalisation et transformation inverse.
- Conservation de la traçabilité des transformations.

### Sélection du scaler

Conformément au document :

- `RobustScaler` si la série contient beaucoup d’outliers ou présente une forte kurtosis ;
- `MinMaxScaler` si l’asymétrie est élevée ;
- `StandardScaler` dans les autres cas.

### Exigence supplémentaire

Les seuils proposés — outliers supérieurs à 5 %, kurtosis supérieure à 5, asymétrie supérieure à 2 — doivent être **configurables**, testés et versionnés. Ils ne doivent pas être considérés comme universels.

---

## SKILL 11 — `trendx-segmentation-and-backtesting`

### Rôle

Construire les jeux d’entraînement, validation et test sans fuite temporelle.

### Stratégies

- `FIXED`;
- `SLIDING_WINDOW`;
- `STICK_TO_END`;
- `AUTO`;
- expanding window ;
- rolling-origin evaluation.

### Capacités

- Définir l’horizon de prévision.
- Respecter strictement l’ordre temporel.
- Adapter la longueur des segments aux saisonnalités.
- Vérifier que chaque segment contient suffisamment de données.
- Agréger les métriques sur plusieurs fenêtres.
- Éviter l’utilisation d’informations futures dans l’entraînement.

### Sorties

- segments reproductibles ;
- métriques par segment ;
- métriques globales ;
- variabilité des performances ;
- intervalle d’incertitude de l’évaluation.

---

# 6. SKILLS de prévision

## SKILL 12 — `trendx-linear-regression-forecasting`

### Rôle

Produire une baseline explicable et des prévisions multivariées.

### Capacités

- Construction de variables de tendance.
- Variables calendaires et retards.
- Variables explicatives externes.
- Régularisation éventuelle.
- Intervalles de prédiction.
- Détection de colinéarité.
- Contrôle des résidus.

Cette skill doit toujours servir de **baseline**, même si un modèle plus complexe est ensuite retenu.

---

## SKILL 13 — `trendx-arima-sarima-forecasting`

### Rôle

Modéliser tendance, autocorrélation et saisonnalité.

### Capacités

- Tests de stationnarité.
- Différenciation.
- Choix de `(p,d,q)` et `(P,D,Q,s)`.
- Utilisation encadrée d’`auto_arima`.
- Diagnostic des résidus.
- Intervalles de confiance.
- Gestion des échecs de convergence.
- Fallback vers une configuration plus simple.

### Point de vigilance

`auto_arima` peut être coûteux et instable sur de nombreuses séries. L’agent doit imposer des limites de recherche et des délais maximums.

---

## SKILL 14 — `trendx-fourier-forecasting`

### Rôle

Détecter et extrapoler les composantes cycliques.

### Capacités

- Suppression ou modélisation de la tendance.
- FFT.
- Identification des fréquences dominantes.
- Détection des cycles journaliers, hebdomadaires et saisonniers.
- Filtrage du bruit.
- Reconstruction harmonique.
- Extrapolation.
- Contrôle du nombre de composantes.

### Garde-fou

La skill ne doit pas sélectionner une périodicité sans vérifier :

- la durée minimale de l’historique ;
- la stabilité de la fréquence ;
- la puissance spectrale ;
- la persistance de la saisonnalité sur plusieurs fenêtres.

---

## SKILL 15 — `trendx-prophet-forecasting`

### Rôle

Produire rapidement des prévisions avec tendance, saisonnalités et intervalles d’incertitude.

### Capacités

- Préparation du format `ds/y`.
- Configuration des saisonnalités.
- Jours fériés selon le contexte.
- Changepoints.
- Croissance linéaire ou logistique.
- Bornes `cap` et `floor`.
- Variables régressives.
- Backtesting.
- Sérialisation du modèle.

### Limite

Prophet ne doit pas être choisi automatiquement uniquement parce qu’il est simple à configurer. Il doit participer à la même compétition objective que les autres modèles.

---

## SKILL 16 — `trendx-model-selection-and-accuracy`

### Rôle

Comparer les modèles, sélectionner le meilleur et calculer les indicateurs équivalents à Trendz.

### Métriques

- MAE ;
- RMSE ;
- MAPE ou sMAPE ;
- biais moyen ;
- couverture des intervalles ;
- temps d’entraînement ;
- stabilité entre segments.

### Indicateurs spécifiques

- **Confidence Level** : part des observations satisfaisant simultanément les tolérances de valeur et de décalage temporel.
- **Confidence Band** : erreur normalisée par la plage de la télémétrie, avec statistiques par segment et percentiles configurables.

### Décision

La sélection ne devrait pas reposer uniquement sur une métrique. La skill doit permettre une fonction de score combinant :

- précision ;
- stabilité ;
- coût de calcul ;
- explicabilité ;
- couverture des intervalles ;
- contraintes métier.

### Fallbacks

Si aucun modèle n’est admissible :

- prévision naïve ;
- moyenne saisonnière ;
- dernière valeur connue ;
- maintien du dernier modèle validé.

---

# 7. SKILL de détection d’anomalies

## SKILL 17 — `trendx-unsupervised-anomaly-detection`

### Rôle

Détecter des anomalies sans étiquettes.

### Préparation

Créer des fenêtres glissantes et extraire :

- moyenne ;
- écart-type ;
- min/max ;
- pente ;
- énergie spectrale ;
- autocorrélation ;
- taux de variation ;
- résidus par rapport à une tendance ou une prévision.

### Modèles

- Isolation Forest ;
- KMeans ;
- DBSCAN ;
- détecteurs PyOD ;
- distance euclidienne ;
- éventuellement DTW pour les formes temporelles.

### Sorties

- `anomaly_score`;
- `anomaly_score_index`;
- `is_anomaly`;
- période de début et de fin ;
- facteurs explicatifs ;
- sévérité ;
- confiance du détecteur.

### Définition à formaliser

Le document donne une interprétation de `anomaly_score_index` comme « intensité × durée », mais pas sa formule exacte. La skill doit définir une formule reproductible, par exemple :

- normaliser le score ;
- regrouper les points anormaux consécutifs ;
- calculer l’aire du score au-dessus du seuil ;
- appliquer une pondération métier.

### Prévention des faux positifs

- période d’apprentissage minimale ;
- exclusion des maintenances planifiées ;
- seuil par métrique ;
- période de refroidissement ;
- regroupement des anomalies voisines ;
- recalibrage après validation métier.

---

# 8. SKILLS MLOps et orchestration

## SKILL 18 — `trendx-mlflow-model-lifecycle`

### Rôle

Tracer les expériences et gérer les versions de modèles.

### Capacités

- Création d’expériences.
- Journalisation des paramètres.
- Journalisation des métriques.
- Stockage des artefacts.
- Enregistrement du scaler, des features et des métadonnées.
- Versionnement du modèle.
- Promotion d’un modèle validé.
- Comparaison avec le modèle en production.
- Rollback.
- Reproductibilité de l’entraînement.

### Métadonnées indispensables

- device ;
- métrique ;
- période d’entraînement ;
- fréquence ;
- horizon ;
- version du code ;
- version des données ;
- hyperparamètres ;
- métriques de validation ;
- bornes métier ;
- date d’expiration ou de réentraînement.

---

## SKILL 19 — `trendx-airflow-pipeline-engineering`

### Rôle

Créer et exploiter le DAG Trendx.

### Tâches à orchestrer

1. `extract_telemetry`;
2. `normalize_data`;
3. `train_and_compete`;
4. `generate_forecast`;
5. `detect_anomalies`;
6. `writeback_thingsboard`;
7. `raise_alerts`.

### Capacités

- Génération de DAG.
- Planification séparée de l’entraînement et de l’inférence.
- Dépendances de tâches.
- Retries avec backoff.
- Timeouts.
- Pools et limites de concurrence.
- Rattrapage contrôlé.
- Paramétrage par device et métrique.
- XCom limité aux petites métadonnées.
- Passage des données volumineuses par TimescaleDB ou stockage d’artefacts.
- Notifications en cas d’échec.
- Reprise idempotente.

### Architecture recommandée

Éviter qu’un unique DAG réentraîne tous les modèles chaque heure. Séparer au minimum :

- ingestion horaire ;
- forecast et anomalies horaires ;
- entraînement quotidien ;
- maintenance et qualité quotidiennes ;
- sauvegardes.

---

# 9. SKILLS de restitution

## SKILL 20 — `thingsboard-forecast-writeback-and-alarms`

### Rôle

Réinjecter les résultats dans ThingsBoard et gérer les alarmes.

### Capacités

- Écriture des prévisions sous `_EPD_<key>`.
- Écriture éventuelle des scores d’anomalie.
- Écriture par lots.
- Gestion des timestamps futurs.
- Déduplication.
- Vérification après écriture.
- Création et mise à jour des alarmes.
- Résolution d’une alarme lorsque la condition disparaît.
- Mapping de la criticité vers les sévérités ThingsBoard.

### Garde-fous

- Ne pas écraser la télémétrie source.
- Respecter une liste blanche de clés autorisées.
- Limiter le volume d’écriture.
- Éviter la création répétée d’une même alarme.
- Conserver un lien entre l’alarme ThingsBoard et `maintenance_alerts`.
- Exiger une validation avant d’écrire sur un environnement de production lors de l’installation initiale.

---

## SKILL 21 — `trendx-grafana-observability-dashboards`

### Rôle

Provisionner Grafana et générer les tableaux de bord analytiques.

### Capacités

- Datasource PostgreSQL/TimescaleDB.
- Dashboards provisionnés en JSON.
- Variables `device_id` et `metric_name`.
- Requêtes SQL paramétrées.
- Panels :
    - réel/prédit ;
    - bande de confiance ;
    - consommation quotidienne ou hebdomadaire ;
    - heatmap d’anomalies ;
    - tableau des KPI ;
    - score d’anomalie ;
    - état du pipeline ;
    - fraîcheur des données.
- Alerting Grafana.
- Intégration dans ThingsBoard.

### Sécurité

L’intégration iframe ne doit pas conduire à activer un accès anonyme global sans analyse. La skill doit traiter :

- authentification ;
- HTTPS ;
- politique CSP ;
- cookies ;
- exposition réseau ;
- permissions sur les dashboards.

---

# 10. SKILL de production

## SKILL 22 — `trendx-production-readiness`

Cette skill peut être implémentée comme une skill composite regroupant tests, sécurité, observabilité et reprise.

### A. Tests

- Tests unitaires des transformations.
- Tests du client ThingsBoard avec réponses simulées.
- Tests d’intégration TimescaleDB.
- Tests des DAG Airflow.
- Tests de reproductibilité des modèles.
- Génération de données synthétiques.
- Injection d’anomalies connues.
- Tests de charge.
- Tests de reprise après panne.
- Tests de non-régression des métriques ML.

### B. Sécurité

- HTTPS via nginx ou Traefik.
- Rotation des secrets.
- Comptes de service à privilèges minimaux.
- Restriction des ports exposés.
- Pare-feu.
- Contrôle des images.
- Gestion des vulnérabilités.
- Protection des interfaces Airflow, MLflow et Grafana.
- Journal d’audit.
- Masquage des données sensibles.

### C. Observabilité

- Santé des conteneurs.
- Fraîcheur des données.
- Durée et taux d’échec des DAG.
- Échecs API.
- Latence d’ingestion.
- Nombre de prévisions générées.
- Distribution des scores d’anomalie.
- Dérive des données.
- Dérive des performances des modèles.
- Espace disque et croissance des hypertables.

### D. Sauvegarde et reprise

- Sauvegardes PostgreSQL.
- Sauvegarde des artefacts MLflow.
- Sauvegarde des dashboards et configurations.
- Test périodique de restauration.
- RPO/RTO documentés.
- Rollback des modèles.
- Recréation de l’environnement à partir du code.

---

# 11. Structure recommandée d’une SKILL

Chaque SKILL devrait être décrite par un contrat homogène :

```
skills/
  trendx-<nom>/
    SKILL.md
    references/
    templates/
    scripts/
    tests/
```

Son fichier `SKILL.md` devrait contenir :

1. **Objectif**
2. **Quand utiliser la skill**
3. **Quand ne pas l’utiliser**
4. **Préconditions**
5. **Entrées obligatoires**
6. **Outils et dépendances**
7. **Procédure pas à pas**
8. **Commandes ou scripts autorisés**
9. **Contrôles avant modification**
10. **Critères de succès**
11. **Gestion des erreurs**
12. **Rollback**
13. **Garde-fous de sécurité**
14. **Format de sortie**
15. **Tests de validation**

Cette standardisation permettra à l’orchestrateur de composer les SKILLS sans ambiguïté.

---

# 12. Données de contexte que l’agent doit mémoriser

Les SKILLS doivent partager un état de projet structuré, et non dépendre uniquement de la conversation :

```yaml
environment:
  name: production
  timezone: Europe/Berlin
  project_path: /opt/trendx

thingsboard:
  base_url: http://10.0.0.1:8080
  version: null
  tenant: null

targets:
  devices: []
  metrics: []
  sampling_interval: 1h
  history_days: 90
  forecast_horizon: 24h

storage:
  retention: 2y
  last_ingestion_watermark: null

ml:
  candidate_models:
    - linear
    - arima
    - fourier
    - prophet
  selected_model_by_series: {}
  limits_by_metric: {}

anomaly_detection:
  contamination: null
  window_size: null

deployment:
  phase_status: {}
  last_successful_step: null
```

Les secrets ne doivent jamais être enregistrés dans cet état en clair.

---

# 13. Niveaux d’autonomie à prévoir

Toutes les actions ne doivent pas être exécutées avec le même niveau d’autonomie.

## Actions pouvant être automatisées

- Lire l’état des services.
- Vérifier les versions et ports.
- Générer les fichiers de configuration.
- Exécuter des tests.
- Entraîner et comparer des modèles.
- Produire des dashboards.
- Générer des rapports.
- Effectuer des lectures API et SQL.

## Actions soumises à validation

- Démarrer ou arrêter la stack de production.
- Modifier le schéma de production.
- Changer une politique de rétention.
- Réinjecter de la télémétrie dans ThingsBoard.
- Activer les alarmes.
- Promouvoir un modèle en production.
- Modifier les règles de sécurité ou le reverse proxy.

## Actions nécessitant une confirmation renforcée

- Supprimer un volume.
- Purger des données.
- Réinitialiser une base.
- Écraser un modèle de production.
- Désactiver l’authentification.
- Restaurer une sauvegarde sur la production.
- Exposer publiquement Grafana, Airflow ou MLflow.

---

# 14. Priorisation pour le MVP

Le conseil final du document est pertinent : il faut éviter de construire les 22 SKILLS simultanément.

## Lot 1 — MVP obligatoire

1. `trendx-solution-orchestrator`
2. `trendx-environment-diagnostics`
3. `trendx-docker-compose-operations`
4. `trendx-secrets-and-configuration`
5. `trendx-timescaledb-engineering`
6. `thingsboard-api-integration`
7. `trendx-incremental-telemetry-ingestion`
8. `trendx-data-quality-and-governance`
9. `trendx-time-series-preprocessing`
10. `trendx-prophet-forecasting`
11. `trendx-airflow-pipeline-engineering`
12. `thingsboard-forecast-writeback-and-alarms`
13. `trendx-grafana-observability-dashboards`
14. `trendx-production-readiness`

Ce lot couvre :

> extraction → stockage → préparation → Prophet → prévision → `_EPD_` → graphique réel/prédit.
> 

## Lot 2 — Amélioration de la prévision

- segmentation et backtesting ;
- régression linéaire ;
- ARIMA/SARIMA ;
- Fourier ;
- sélection automatique ;
- MLflow complet ;
- Confidence Level et Confidence Band.

## Lot 3 — Maintenance prédictive

- extraction de features par fenêtres ;
- Isolation Forest et PyOD ;
- score intensité × durée ;
- regroupement d’événements ;
- calibration des faux positifs ;
- alarmes automatiques.

## Lot 4 — Industrialisation

- drift monitoring ;
- promotion automatique sous conditions ;
- rollback ;
- haute disponibilité ;
- sauvegardes testées ;
- durcissement réseau ;
- gouvernance et audit.

---

# 15. Lacunes du document que les SKILLS devront combler

Le document constitue une bonne procédure de cadrage, mais pas encore une spécification exécutable complète. Les SKILLS devront formaliser les éléments suivants :

1. **Dimensionnement** : aucune estimation CPU, RAM, stockage ou volumétrie.
2. **Version ThingsBoard** : les routes API doivent être vérifiées.
3. **Refresh du JWT** : absent du client d’exemple.
4. **Pagination** : seule la première page de devices est lue.
5. **Ingestion incrémentale** : aucun watermark ni mécanisme de déduplication.
6. **Idempotence** : risque de doublons dans les tables et dans ThingsBoard.
7. **Formule du Confidence Level** : les seuils ne sont pas spécifiés.
8. **Formule du Confidence Band** : définition encore conceptuelle.
9. **Formule de l’Anomaly Score Index** : « intensité × durée » doit devenir un algorithme précis.
10. **Gestion du cycle de vie des alarmes** : création prévue, résolution non décrite.
11. **Séparation des environnements** : développement, recette et production non définis.
12. **Sécurité des interfaces** : Airflow, MLflow et Grafana sont exposés directement.
13. **Gestion des dépendances Python** : `_PIP_ADDITIONAL_REQUIREMENTS` au démarrage est peu reproductible ; une image dédiée est préférable.
14. **Registre des métriques** : unités, bornes métier et fréquences attendues manquantes.
15. **Dérive des modèles** : aucun mécanisme explicite de surveillance.
16. **Rollback** : non détaillé.
17. **Tests de restauration** : seule la sauvegarde est mentionnée.
18. **Explicabilité** : nécessaire pour justifier les prévisions et les alarmes.
19. **Validation humaine** : indispensable avant activation des écritures et alertes en production.
20. **Licences et conformité** : les dépendances et les exigences de traitement des données doivent être vérifiées.

---

## Conclusion

La capacité centrale à construire n’est pas une unique skill « Trendx », mais une **skill orchestratrice s’appuyant sur 21 SKILLS spécialisées**. Les compétences les plus critiques sont celles qui garantissent :

- l’intégration robuste avec ThingsBoard ;
- l’ingestion idempotente ;
- la qualité des séries temporelles ;
- l’évaluation sans fuite temporelle ;
- la traçabilité MLflow ;
- la sécurité des écritures et alarmes ;
- l’exploitation fiable en production.

Le MVP peut commencer avec Prophet, mais l’architecture des SKILLS doit dès le départ prendre en charge le versionnement, l’idempotence, la sécurité et les critères de validation. Sans ces fondations, l’agent pourrait produire une démonstration fonctionnelle, mais pas exploiter Trendx de manière sûre et durable.