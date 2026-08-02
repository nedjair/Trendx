# Matrice de parité fonctionnelle Trendz ↔ Trendx

> Document de référence : contient, pour chaque fonctionnalité publiquement documentée de **ThingsBoard Trendz Analytics**,
> l'état de l'équivalent fonctionnel dans **Trendx**, les composants concernés, les tests d'acceptation
> prévus, les écarts connus et les dépendances éventuelles à ThingsBoard PE.
>
> Règle : une fonctionnalité n'est marquée **« terminé » que si les tests de parité passent.
> Une fonctionnalité marquée **« bloqué »** indique un point de blocage nécessitant une décision
> ou une approbation explicite.
> Règle test d'acceptation : chaque test doit être exécuté sur **≥ 2 devices** pour valider la parité multi-devices.

---

## Légende des statuts

| non commencé | Non commencé |
| partiel | Partiel / en cours |
| terminé | Terminé (tests de parité OK) |
| bloqué | Bloqué |
| hors parité | Fonctionnalité non documentée dans Trendz, hors périmètre parité |

---

## 1. Topologie & Découverte

| # | Fonctionnalité Trendz | Doc officielle | Comportement documenté | Équivalent Trendx | Composants | Statut | Tests d'acceptation | Différences connues | Dép. TB PE | Limites |
|---|------------------------|----------------|------------------------|-------------------|------------|--------|---------------------|---------------------|------------|---------|
| 1.1 | First Topology Discovery | [thingsboard.io/docs/trendz/getting-started/](https://thingsboard.io/docs/trendz/getting-started/) | Au 1er login, bouton « Discover Topology » scanne devices, assets, profils, attributs, télémétries, relations | Service `TopologyDiscoveryService` + sync complet (full) | `thingsboard` + `trendx` DB | non commencé | Démo full sync > devices > catalog | — | Non | — |
| 1.2 | Manual Topology Rediscover | [thingsboard.io/docs/trendz/connect-thingsboard/](https://www.pke-iot.expert/docs/trendz/connect-thingsboard/) | Bouton « Refresh Topology » pour détecter nouveaux types, clés | Sync incrémentale planifiable | Scheduler + diff | non commencé | Ajout nouveau profil → apparition dans catalog | — | Non | — |
| 1.3 | Incremental sync planifié | id. | Mise à jour périodique de la topologie | Job Airflow `trendx_topology_sync` | Airflow DAG | non commencé | Exécution toutes les 15min sans doublons | — | Non | — |
| 1.4 | Validation de topologie | Trendz manual modification | Utilisateur peut activer/désactiver des relations | Interface Business Entities + cases à cocher « Enabled » | Web UI Trendx | non commencé | Désactiver 1 relation → requêtes ne JOIN plus | — | Non | — |
| 1.5 | Recherche dans topologie | id. | Recherche full-text dans devices/assets/profils | API de recherche catalog | `trendx-api` routes | non commencé | Recherche partial name/type/attribut | — | Non | — |
| 1.6 | Marquage devices inactifs | — (déduit de docs | Détection de devices supprimés ou inaccessibles | Colonne `is_active` + `last_seen` dans catalog DB | non commencé | Device disparaît de TB → marqué inactif | — | Non | — |
| 1.7 | Règles d'inclusion | id. | Filtrage par tenant, customer, profil, regex, liste d'inclusion/exclusion | Règles configurables dans sync service | non commencé | Test excludes par profil | — | Non | — |

---

## 2. Business Entities

| # | Fonctionnalité Trendz | Doc officielle | Comportement documenté | Équivalent Trendx | Composants | Statut | Tests d'acceptation | Différences connues | Dép. TB PE | Limites |
|---|------------------------|----------------|------------------------|-------------------|------------|--------|---------------------|---------------------|------------|---------|
| 2.1 | Business Entity (BE) par profil | [thingsboard.io/docs/trendz/concepts/business-entities/](https://thingsboard.io/docs/trendz/concepts/business-entities/) | Un BE = groupe Devices/Assets de même type | 1 ligne `entity_profiles` → BE unique | `catalog` DB | non commencé | 1 BE par profil device créé après discovery | — | Non | — |
| 2.2 | Champs BE (nom, owner, attributs, télémétries) | id. | Chaque BE expose Entity Name, Owner, Attribute, Telemetry | Table `entity_attributes` + `telemetry_keys` liées au profil | non commencé | Tous les champs apparaissent dans Metric Explorer | — | Non | — |
| 2.3 | Relations entre BE (uniques par profil) | id. | **1 seul type de relation autorisé entre 2 profils** (pour jointure stable) | Table `entity_relations` + contrainte d'unicité per profil | non commencé | 2 relations actives entre mêmes 2 profils → message utilisateur | Trendz impose exactement 1, Trendx alertera | Non | — |
| 2.4 | Jointures par relation | id. | Agrégation multi-niveaux (Building → Apartment → Meter → Somme télémetries | `BusinessQueryEngine` avec relation path | non commencé | Test 3-niveau SUM/agg correct | — | Non | N niveaux |
| 2.5 | Filtres par BE | id. | Filtrer par BE entier | API / UI filtres dynamiques | Web UI filters | non commencé | Filtre = 1 Building → résultat correct | — | Non | — |
| 2.6 | Regroupements par BE (profil, customer, attribut) | id. | GroupBy profil/customer/attribut arbitraire | API groupBy dimensions | non commencé | GroupBy customer → 1 ligne par customer | — | Non | — |
| 2.7 | Drill-down | id. | Clic sur agrégat → détail | UI drill-down interactif | Web UI + API detail | non commencé | Clic agrégat → filtre appliqué | — | Non | — |
| 2.8 | Comparaison inter-devices | id. | Comparer plusieurs devices similaires | Série multiple + UI select devices | Web UI charts | non commencé | Superposer 5 devices même profil OK | — | Non | — |

---

## 3. Metric Explorer

| # | Fonctionnalité Trendz | Doc officielle | Comportement documenté | Équivalent Trendx | Composants | Statut | Tests d'acceptation | Différences connues | Dép. TB PE | Limites |
|---|------------------------|----------------|------------------------|-------------------|------------|--------|---------------------|---------------------|------------|---------|
| 3.1 | Sélection entités + métriques multiples | [thingsboard.io/docs/trendz/metric/overview/](https://thingsboard.io/docs/trendz/metric/overview/) | Sélection 1→N entités + 1→N métriques | Vue Metric Explorer | Web UI Explorer | non commencé | Sélection multi-device/multi-metric OK | — | Non | — |
| 3.2 | Disponibilité / période couverte | id. | Plage de données disponibles | API stats + UI info-bulle | API catalog ingestion | non commencé | Min/max timestamp par série | — | Non | — |
| 3.3 | Nombre de points + mini/maxi/moy/std/écart/somme | id. | Statistiques descriptives immédiates | API stats agrégées SQL TimescaleDB | non commencé | Toutes stats exactes vs pandas | — | Non | — |
| 3.4 | Détection données manquantes | id. | Visualiser écarts et taux NaN | Quality service + UI gaps indicator | preprocessing.quality | non commencé | Taux remplissage affiché | — | Non | — |
| 3.5 | Distribution (histogramme) | id. | Distribution des valeurs | Histogramme view | Web UI charts | non commencé | Distribution cohérente avec données | — | Non | — |
| 3.6 | Tendances temporelles | id. | Vue temporelle rapide | Line chart by default | Web UI Line | non commencé | Affichage chronologique | — | Non | — |
| 3.7 | Exploration plages (range exploration) | id. | Min/max et plages | Range explorer UI | Web UI filters | non commencé | Sélection plage graphique → série | — | Non | — |
| 3.8 | Comparaison multi-devices | id. | Même métrique sur devices différents | Multi-series line/bar | Web UI charts | non commencé | 3 devices alignés | — | Non | — |
| 3.9 | Filtres + regroupements dynamiques | id. | Filtrage & groupBy interactifs | Query builder UI | API query + UI builder | non commencé | Application filtre/groupBy sans recharger | — | Non | — |
| 3.10 | Génération résumés | id. | Résumé automatique vue Stats | Stat cards génériques + IA optionnelle | non commencé | 1 clic = résumé lisible | — | Non | — |

---

## 4. Visualisations

| # | Fonctionnalité Trendz | Doc officielle | Comportement documenté | Équivalent Trendx | Composants | Statut | Tests d'acceptation | Différences connues | Dép. TB PE | Limites |
|---|------------------------|----------------|------------------------|-------------------|------------|--------|---------------------|---------------------|------------|---------|
| 4.1 | Table simple | [thingsboard.io/docs/trendz/visualizations-tables/](https://thingsboard.io/docs/trendz/visualizations-tables/) | Grille brute/agrégée, export CSV | Table view + export | Web UI tables | non commencé | Rendu correct pagination OK | — | Non | — |
| 4.2 | Table dynamique (colonnes dynamiques / pivot | id. | colonnes dynamiques piv | Pivot table UI | Web UI pivot | non commencé | Date en colonnes, valeurs | Trendz utilise Date(Month), Trendx équivalent | Non | — |
| 4.3 | Graphique en ligne (Line) | [thingsboard.io/docs/trendz/visualizations-line/](https://thingsboard.io/docs/trendz/visualizations-line/) | Suivi temporel multi-séries | Line chart React (Recharts) | Web UI | non commencé | Zoom/pan/tooltip | — | Non | — |
| 4.4 | Barres verticales | [thingsboard.io/docs/trendz/visualizations-bar/](https://thingsboard.io/docs/trendz/visualizations-bar/) | Comparaison totaux/entités/temps | Bar chart vertical | Web UI charts | non commencé | Grouping + stacked | — | Non | — |
| 4.5 | Barres horizontales | id. | Barres horizontales | Horizontal bar | Web UI charts | non commencé | — | Non | — |
| 4.6 | Barres empilées | id. | Mode stacked bars | Stacked option | Web UI charts | non commencé | — | Non | — |
| 4.7 | Histogramme | — (distribution) | Distribution fréquence valeurs | Histogram view | Web UI charts | non commencé | — | Non | — |
| 4.8 | Graphique circulaire (Pie) | [thingsboard.io/docs/trendz/visualizations-pie/](https://thingsboard.io/docs/trendz/visualizations-pie/) | Proportions | Pie chart | Web UI charts | non commencé | <10 catégories | Non | — |
| 4.9 | Nuage de points (Scatter) | [thingsboard.io/docs/trendz/visualizations-scatter/](https://thingsboard.io/docs/trendz/visualizations-scatter/) | Corrélations XY | Scatter plot | Web UI charts | non commencé | Color by entity | — | Non | — |
| 4.10 | Heatmap | [thingsboard.io/docs/trendz/visualizations-heatmap/](https://thingsboard.io/docs/trendz/visualizations-heatmap/) | 2D couleurs intensité | Heatmap | Web UI charts | non commencé | Matrix X/Y → valeur | — | Non | — |
| 4.11 | Heatmap calendrier (hour-of-week) | id. + produits | Patterns jour × heure/semaine | Calendar heatmap view | Web UI charts | non commencé | — | Non | — |
| 4.12 | Calendrier (values/jour) | [thingsboard.io/docs/trendz/visualizations-calendar/](https://thingsboard.io/docs/trendz/visualizations-calendar/) | Mois/jour grille | Calendar view | Web UI charts | non commencé | — | Non | — |
| 4.13 | Card (KPI simple) | [thingsboard.io/docs/trendz/visualizations-card/](https://thingsboard.io/docs/trendz/visualizations-card/) | 1 métrique clé + unité | Card component | Web UI cards | non commencé | — | Non | — |
| 4.14 | Card avec comparaison PoP | id. | Card + Period-over-Period delta % | Card with delta | Web UI cards | non commencé | — | Non | — |
| 4.15 | Card avec mini-courbe | [thingsboard.io/docs/trendz/visualizations-card-with-line/](https://thingsboard.io/docs/trendz/visualizations-card-with-line/) | Card + sparkline | Sparkline card | Web UI cards | non commencé | — | Non | — |
| 4.16 | Visualisation des états | Trendz States | Timeline couleur × état | State timeline | Web UI state viz | non commencé | — | Non | — |
| 4.17 | Visualisation des anomalies | Trendz Anomaly score viz | Points colorés / score | Anomaly overlay on line | Web UI charts | non commencé | — | Non | — |
| 4.18 | Réel vs prédit | Trendz Prediction | 2 courbes + historique prédit | Forecast overlay | Web UI line | non commencé | — | Non | — |
| 4.19 | Bandes de confiance | id. | Confidence band | Intervalle confiance (upper/lower) | Web UI | non commencé | — | Non | — |
| 4.20 | Caractéristiques partagées (alias, TZ, tri, pagination, export, sauvegarde, duplication, cache, partage) | id. pour chaque viz | — | Implémenté global components | — | non commencé | — | — | Non | — |
| 4.21 | AI Card | [thingsboard.io/docs/trendz/visualizations-ai-card/](https://thingsboard.io/docs/trendz/visualizations-ai-card/) | Cumul + prévision + résumé IA | AI card + IA optionnelle | non commencé | Désactivée par défaut | Non | Optionnel LLM |

---

## 5. Filtres, regroupements, agrégations

| # | Fonctionnalité Trendz | Doc officielle | Comportement documenté | Équivalent Trendx | Composants | Statut | Tests d'acceptation | Différences connues | Dép. TB PE | Limites |
|---|------------------------|----------------|------------------------|-------------------|------------|--------|---------------------|---------------------|------------|---------|
| 5.1 | Filtrage par attributs / plage | Trendz filter docs | Conditions multiples | Filter builder | API filter parser | non commencé | — | Non | — |
| 5.2 | Filtrage par télémétrie | id. | Filtre sur valeurs ts | Query WHERE clause | API + TimescaleDB | non commencé | — | Non | — |
| 5.3 | Agrégations MIN/MAX/SUM/AVG/COUNT/UNIQ/MEDIAN/P90/P95/P99 | Trendz data grouping | Liste agrégations supportées | Mapping SQL + fonctions TimescaleDB | non commencé | Chaque agrégation vérifiée | — | Non | — |
| 5.4 | GroupBy temporel (heure/jour/sem/mois/année) | id. | Buckets de temps | Time_bucket TimescaleDB hyperfunctions | non commencé | Chaque unité OK | — | Non | — |
| 5.5 | GroupBy entité /profil /customer /attribut | id. | Dimensions arbitraires | GROUP BY dimensions | API query | non commencé | — | Non | — |
| 5.6 | Séries multiples | Line series slot | 1 ligne par entité/série | UI series slot + multi-series | non commencé | — | Non | — |
| 5.7 | Alias des séries (alias champ) | Trendz viz docs | Renommer colonnes/séries | Alias mapper UI | non commencé | — | Non | — |
| 5.8 | Fuseaux horaires | id. | Sélection TZ par vue | TZ config vue → at Time zone | non commencé | Conversions vérifiées | Non | — |

---

## 6. Champs calculés

| # | Fonctionnalité Trendz | Doc officielle | Comportement documenté | Équivalent Trendx | Composants | Statut | Tests d'acceptation | Différences connues | Dép. TB PE | Limites |
|---|------------------------|----------------|------------------------|-------------------|------------|--------|---------------------|---------------------|------------|---------|
| 6.1 | Éditeur de formules | [thingsboard.io/docs/trendz/calculations/overview/](https://thingsboard.io/docs/trendz/calculations/overview/) | Éditeur formules (JS-like) | Formula editor UI + sandbox Python | calcul champs Python isolés | non commencé | Éditeur avec validation | Trendz utilise JS, Trendx utilise Python sandboxé | Non | sandbox |
| 6.2 | Fonctions statistiques de base | id. | sum(), avg(), min()/max(), uniq() | Pandas wrapper | non commencé | Résultat identique jeu test | — | Non | — |
| 6.3 | Calculs entre métriques同 device | id. | A + B, A/B etc. | Expression evaluator | non commencé | — | Non | — |
| 6.4 | Calculs entre devices reliés (via relations | id. | Traversée relations dans formule | JOIN relation-aware eval | non commencé | 2 devices liés → formule OK | — | Non | — |
| 6.5 | Agrégations dans formules | id. | sum(metric), avg(metric) | Grouped expressions | non commencé | — | Non | — |
| 6.6 | Fonctions temporelles | id. | timeBucket, delta, deriv | Pandas rolling + fonctions temps | non commencé | — | Non | — |
| 6.7 | Formules conditionnelles (if/else) | id. | if(cond, A, B) | AST evaluator safe | non commencé | — | Non | — |
| 6.8 | Python natif isolé (sandbox) | — (inspiré docs) | Modèles personnalisés | Python RestrictedExec | CPU/mém/durée | non commencé | — | Non | Limites stricts |
| 6.9 | Aperçu & validation & débogage | id. | Exécution échantillon | Preview UI + logs | non commencé | — | Non | — |
| 6.10 | Historique d'exécution | — | Logs | Table task_runs logs | non commencé | — | Non | — |
| 6.11 | Calcul à la volée | — (on-demand view) | À chaque exécution vue | Lazy evaluation query | non commencé | — | Non | — |
| 6.12 | Matérialisation (save TB) | id. + sauvegarde TB télémetrie | Calculé stocké | calculated_field_values table | non commencé | — | Non | — |
| 6.13 | Recalcul historique | id. + backfill | Retrospectif historique | DAG Airflow recalcul | non commencé | — | Non | — |
| 6.14 | Exécution planifiée | id. Jobs / Background jobs | Périodique | Airflow DAG | non commencé | — | Non | — |
| 6.15 | Préfixe writeback configurable `_ECD_<name>` | Prompt §6.5 prompt.md | Préfixe save TB | `_ECD_` + config env var | non commencé | — | Non | — |

---

## 7. États (States)

| # | Fonctionnalité Trendz | Doc officielle | Comportement documenté | Équivalent Trendx | Composants | Statut | Tests d'acceptation | Différences connues | Dép. TB PE | Limites |
|---|------------------------|----------------|------------------------|-------------------|------------|--------|---------------------|---------------------|------------|---------|
| 7.1 | Définition états nommés | Trendz States docs | Seuils → nom/couleur/priorité | state_definitions table | UI + API | non commencé | — | Non | — |
| 7.2 | Conditions états (seuils) | id. | Conditions multiples | Condition evaluator seuils | non commencé | — | Non | — |
| 7.3 | Couleurs et priorités | id. | Couleur HTML + niveau priorité | state_definitions cols | non commencé | — | Non | — |
| 7.4 | Calcul temps passé par état | id. | Temps total/état | state_intervals hypertable | non commencé | — | Non | — |
| 7.5 | Pourcentages disponibilité/activité/arrêt | id. | % par période | SQL ratios % calculés | non commencé | — | Non | — |
| 7.6 | Transitions visualisées | id. | Timeline transitions | Timeline viz transitions | UI states viz | non commencé | — | Non | — |
| 7.7 | Analyse par heure/jour/sem/calendrier | id. | Group états × période | Pivot states × time | non commencé | — | Non | — |
| 7.8 | Comparaison inter-devices états | id. | Plusieurs devices 1 vue | Multi-device timeline | UI | non commencé | — | Non | — |

---

## 8. Prédiction / Forecasting

| # | Fonctionnalité Trendz | Doc officielle | Comportement documenté | Équivalent Trendx | Composants | Statut | Tests d'acceptation | Différences connues | Dép. TB PE | Limites |
|---|------------------------|----------------|------------------------|-------------------|------------|--------|---------------------|---------------------|------------|---------|
| 8.1 | Régression linéaire (LINEAR_REGRESSION) | [ithingsboard.com/docs/trendz/prediction/](http://www.ithingsboard.com/docs/trendz/prediction/) | Méthode LinReg | Linear Forecasting.linear | forecasting/linear.py | non commencé | MAE vs référence | Trendz utilise OLS, scikit-learn standard | Non | — |
| 8.2 | OLS Linear Regression | id. | OLS variant | OLS via statsmodels | forecasting.linear_ols | non commencé | — | — | Non | — |
| 8.3 | ARIMA / SARIMA | id. | ARIMA | pmdarima auto_arima | forecasting.arima.py | non commencé | — | Trendz ne mentionne pas SARIMA explicitement, Trendx SARIMAX | Non | — |
| 8.4 | Transformation de Fourier FOURIER_TRANSFORMATION | id. | Méthode FFour series | forecasting.fourier.py | non commencé | — | — | Non | — |
| 8.5 | Prophet | Prompt §6.7 | Prophet Meta Prophet FB | forecasting.prophet.py | non commencé | — | — | Non | — |
| 8.6 | Modèles Python personnalisés | id. | Custom upload/fichier | Perso Py registry | non commencé | — | — | Non | sandbox |
| 8.7 | Filtrage + normalisation auto | Prompt §6.7 | RobustScaler/MinMax/Standard | preprocessing.normalizer | non commencé | — | — | Non | — |
| 8.8 | Segmentation périodes/segmente | id. | Segments modèle par device/profil/global | selector strategy PER_DEVICE | PER_PROFILE/GLOBAL | AUTO | non commencé | — | — | Non | — |
| 8.9 | Limites métier min/max | id. | Cap forecasting | forecast clipping cap | non commencé | — | — | Non | — |
| 8.10 | Saisonnalités multiples | id. | yearly/weekly/yearly | Prophet yearly_seasonality config | non commencé | — | — | Non | — |
| 8.11 | Variables explicatives | id. | Régresseurs additionnels | Regressors Prophet | non commencé | — | — | Non | — |
| 8.12 | Backtesting walk-forward temporel | Prompt | Validation temporelle sans data leak | cv temporal CV | non commencé | — | — | Non | — |
| 8.13 | Comparaison réel/prédit historique | Prompt §6.7 | Case "Show Historical part | Historical overlay UI | non commencé | — | — | Non | — |
| 8.14 | Confidence Level + Band | id. | Intervalle confiance | Conf interval percentiles | UI bandes | non commencé | — | — | Non | — |
| 8.15 | Tâches d'entraînement périodiques | id. + Background Jobs | Daily retrain | Airflow DAG training | non commencé | — | — | Non | — |
| 8.16 | Tâches de prédiction périodiques | id. | Hourly forecast | Airflow DAG generation | non commencé | — | — | Non | — |
| 8.17 | Versionnement + rollback modèle | id. + MLflow | versions + champion/challenger | MLflow registry | non commencé | — | — | Non | — |
| 8.18 | Writeback prévisions dans TB `_EPD_<metric>` | §6.7 + prompt | Save TB as telemetry | TB API writeback + prefix `_EPD_` | non commencé | Désactivé par défaut | — | Non | — |
| 8.19 | Stratégie PER_DEVICE/PER_PROFILE/GLOBAL/AUTO | §6.7 prompt | Comparaison auto stratégies | Forecasting selector | non commencé | — | — | Non | — |
| 8.20 | Sélection meilleur modèle + compétition | Prompt requis | Sélection meilleur | Model selector | non commencé | — | — | Non | — |
| 8.21 | Métriques MAE/RMSE/sMAPE/MAPE (conditions)/coverage/largeur/biais | Prompt §10.3 +§11.12 | Store in MLflow metrics | ML tracking metrics mlops | non commencé | Toutes calc. vs TB PE | Non | — |

---

## 9. Détection d'anomalies

| # | Fonctionnalité Trendz | Doc officielle | Comportement documenté | Équivalent Trendx | Composants | Statut | Tests d'acceptation | Différences connues | Dép. TB PE | Limites |
|---|------------------------|----------------|------------------------|-------------------|------------|--------|---------------------|---------------------|------------|---------|
| 9.1 | Création modèle anomalie (UI) | [thingsboard.io/docs/trendz/anomaly/overview/](https://thingsboard.io/docs/trendz/anomaly/overview/) | Assistant modèle | UI wizard + API models | anomalies module + UI | non commencé | — | — | Non | — |
| 9.2 | Sélection entités + métriques | id. | 1→N devices / 1→N metrics | Model definition table | non commencé | — | — | Non | — |
| 9.3 | Période d'apprentissage config | id. | Plage référence | training_period | non commencé | — | — | Non | — |
| 9.4 | Segmentation fenêtres | id. | Fenêtres taille configurable | Segmentation features | features | non commencé | — | — | Non | — |
| 9.5 | Extraction features (moy, écart, min/max, pente, amplitude, énergie spectrale, diff fenêtre précédente, taux NaN) | §11 Prompt features minimales | 9 features+ minimales | anomalies.features.py | non commencé | Toutes features exactes | — | Non | — |
| 9.6 | Entraînement périodique | id. + jobs | Retrain régulier | Airflow anomaly_training DAG | non commencé | — | — | Non | — |
| 9.7 | Scoring + Anomaly Score (intensité déviation) | id. + §6.8 prompt | Score normalisé | anomalies.scoring.score() | non commencé | — | — | Non | — |
| 9.8 | Anomaly Score Index = intensité × durée | §6.8 Prompt spec | Score cumul | anomalies.scoring.asi() | non commencé | — | — | Non | — |
| 9.9 | Revue des résultats | Trendz Anomaly viz | UI visual | UI anomaly review | non commencé | — | — | Non | — |
| 9.10 | Scan historique rétroactif | id. + jobs | Rétroscan | DAG anomaly_scan backfill | non commencé | — | — | Non | — |
| 9.11 | Scan périodique horaire | id. | Scan continu | Airflow anomaly_scan_hourly DAG | non commencé | — | — | Non | — |
| 9.12 | Mise à jour modèle | id. + Versioning | Nouvelles versions | anomaly_model_definitions + versions | non commencé | — | — | Non | — |
| 9.13 | Sauvegarde scores dans TB télémetrie | [pke-iot.expert/docs/trendz/anomaly/anomaly-score-save-to-thingsboard/](https://www.pke-iot.expert/docs/trendz/anomaly/anomaly-score-save-to-thingsboard/) | save anom score telemetry | writeback scores TB | non commencé | Désactivé défaut | — | Non | — |
| 9.14 | Création alarmes ThingsBoard | id. + §6.8 Prompt alarms | Alarme TB | Alarm API TB CUSTOM_USER | Désactivé défaut | non commencé | — | — | Non | — |
| 9.15 | Isolation Forest | Prompt §6.8 modèles minimaux | Modèle 1 | detectors.isolation_forest | anomalies.detectors.py | non commencé | — | — | Non | — |
| 9.16 | PyOD modèles (LOF, kNN, etc.) | id. | Modèles famille PyOD | detectors.pyod() | non commencé | — | — | Non | — |
| 9.17 | KMeans / DBSCAN clustering | id. | Modèle 3 | detectors.cluster_based | non commencé | — | — | Non | — |
| 9.18 | Sensibilité + contamination + window size | id. + §6.8 | Configurable | Config UI + DB | non commencé | — | — | Non | — |
| 9.19 | Hystérésis + seuil ouverture/fermeture | §6.8 prompt spec | Anti-flapping | scoring hysteresis module | non commencé | — | — | Non | — |
| 9.20 | Cooldown + durée minimale + anti-duplication alarms | id. + §9.3 | Alarms cool | alerting.py | non commencé | — | — | Non | — |

---

## 10. Tâches & Jobs de fond

| # | Fonctionnalité Trendz | Doc officielle | Comportement documenté | Équivalent Trendx | Composants | Statut | Tests d'acceptation | Différences connues | Dép. TB PE | Limites |
|---|------------------------|----------------|------------------------|-------------------|------------|--------|---------------------|---------------------|------------|---------|
| 10.1 | Types de tâches (liste complète §6.9 Prompt) | §6.9 Prompt 20+ types | Liste 20 types | Airflow DAGs + task_runs table | non commencé | — | Trendz UI «Background jobs page, Trendx Airflow + UI Task Service | Non | — |
| 10.2 | Statut / progression | §6.9 | Pourcentage + Étape en cours | task_runs.progress API | API status | non commencé | — | Non | — |
| 10.3 | Dates (création / début / fin / durée) | id. | Horodatage | colonnes temps task_runs | non commencé | — | Non | — |
| 10.4 | Paramètres d'exécution | id. | Paramètres | params JSONB | non commencé | — | Non | — |
| 10.5 | Utilisateur/déclencheur | id. | Qui a lancé ? | user /trigger columns | non commencé | — | Non | — |
| 10.6 | Logs d'exécution | id. | Logs task | logs col logs | non commencé | — | Non | — |
| 10.7 | Compteurs entités / points / erreurs | id. | Stats | counters cols | non commencé | — | Non | — |
| 10.8 | Retry + Annulation + Relance | id. | Gestion cycle de vie | Airflow retries/cancel | non commencé | — | Non | — |

---

## 11. Cache

| # | Fonctionnalité Trendz | Doc officielle | Comportement documenté | Équivalent Trendx | Composants | Statut | Tests d'acceptation | Différences connues | Dép. TB PE | Limites |
|---|------------------------|----------------|------------------------|-------------------|------------|--------|---------------------|---------------------|------------|---------|
| 11.1 | Cache métadonnées topologie | Trendz cache docs | Devices/profils/clés | Redis + TTL | services.cache module | non commencé | — | Non | — |
| 11.2 | Cache métriques résultats | id. | Réponses agrégation queries | Redis query cache | non commencé | — | Non | — |
| 11.3 | Cache long terme (views) | id. | Sauvegarde vues fréquentes | Redis long TTL | non commencé | — | Non | — |
| 11.4 | Cache des résultats ML | — | Prédictions scores | non commencé | — | Non | — |
| 11.5 | Expiration + refresh planifié | §6.10 Prompt | Politique | Airflow cache_refresh DAG | non commencé | — | Non | — |
| 11.6 | Invalidation + nettoyage | id. | Purge sur événement | API purge endpoint | non commencé | — | Non | — |
| 11.7 | Observation taux utilisation | id. | Stats hit/miss | Redis stats | non commencé | — | Non | — |
| 11.8 | Isolation par tenant | id. | Namespace Redis tenant | Redis prefix tenant | non commencé | — | Non | — |

---

## 12. Partage & Intégration ThingsBoard

| # | Fonctionnalité Trendz | Doc officielle | Comportement documenté | Équivalent Trendx | Composants | Statut | Tests d'acceptation | Différences connues | Dép. TB PE | Limites |
|---|------------------------|----------------|------------------------|-------------------|------------|--------|---------------------|---------------------|------------|---------|
| 12.1 | Save calculated fields vers TB | §6.5 + docs | Télémétrie calculée save | TB telemetry API | writeback service + `_ECD_` | non commencé | Désactivé par défaut | — | Non | — |
| 12.2 | Save prévisions `_EPD_<metric>` | §6.7 + docs | Forecast save | TB telemetry API | non commencé | Désactivé par défaut | — | Non | — |
| 12.3 | Save anomaly scores vers TB | §6.8 + docs | Score save | TB telemetry API | non commencé | Désactivé par défaut | — | Non | — |
| 12.4 | Création alarmes TB depuis anomalies | id. + prompt | Alarm TB API alarms | alarms TB API create | non commencé | Désactivé par défaut | — | Non | — |
| 12.5 | Configuration widgets TB si API le permet | §6.11 Prompt | TB widgets | (optionnel si API TB) | non commencé | Approbation | Non | — |
| 12.6 | Intégration iframes dans TB dashboards | Trendz embed visuals docs | iframe sécurisé | reverse-proxy CSP + embed token | non commencé | — | Non | — |
| 12.7 | Liens sécurisés + tokens | id. | Liens sécurisés TB ↔ Trendx | Signed URLs tokens | non commencé | — | Non | — |
| 12.8 | Filtres par alias depuis TB dashboard alias | Trendz widget alias docs | Filtre TB→trendx alias | Query params UI | non commencé | — | — | Non | — |
| 12.9 | Navigation croisée TB ↔ Trendx | id. widget actions | Liens bidirectionnels | Deep links | non commencé | — | — | Non | — |
| 12.10 | Actions / Boutons dans widgets TB | Trendz Widget actions docs | Boutons actions | UI embedded actions | non commencé | — | Non | — |

---

## 13. Rapports

| # | Fonctionnalité Trendz | Doc officielle | Comportement documenté | Équivalent Trendx | Composants | Statut | Tests d'acceptation | Différences connues | Dép. TB PE | Limites |
|---|------------------------|----------------|------------------------|-------------------|------------|--------|---------------------|---------------------|------------|---------|
| 13.1 | Export CSV | Trendz Table docs | CSV export | endpoint export CSV | API exports | non commencé | — | Non | — |
| 13.2 | Export JSON | §6.12 Prompt | JSON | endpoint export JSON | non commencé | — | Non | — |
| 13.3 | Export PNG (captures) | §6.12 Prompt | Si possible | UI screenshot service (optionnel) | non commencé | — | Non | Optionnel |
| 13.4 | Export PDF | §6.12 Prompt | PDF | WeasyPrint ou eq | non commencé | — | Non | — |
| 13.5 | Rapports planifiés (périodiques) | id. | Périodicité | Airflow DAG trendx_reports | non commencé | — | Non | — |
| 13.6 | Multi-devices | id. | 1 rapport × N devices | reports module | non commencé | — | Non | — |
| 13.7 | Historique génération | id. | Génération passé | reports table + historique | non commencé | — | Non | — |
| 13.8 | Stockage artefacts (S3/local) | id. | Stockage artefacts | storage artefacts repo | non commencé | — | Non | — |
| 13.9 | Livraison email/webhook configurable | id. | Email / webhook | alerting / notifications | non commencé | — | Non | — |

---

## 14. Assistant IA (optionnel, documenté dans Trendz CE)

| # | Fonctionnalité Trendz | Doc officielle | Comportement documenté | Équivalent Trendx | Composants | Statut | Tests d'acceptation | Différences connues | Dép. TB PE | Limites |
|---|------------------------|----------------|------------------------|-------------------|------------|--------|---------------------|---------------------|------------|---------|
| 14.1 | Génération vues depuis langage naturel | [thingsboard.io/docs/trendz/ai-assistance-overview/](https://thingsboard.io/docs/trendz/ai-assistance-overview/) | Prompt → vue configurée | AI adapter LLM configurable | UI + backend IA | non commencé | Génération vue sur ≥2 devices depuis prompt texte | — | Non | LLM externe |
| 14.2 | Proposition filtres/agrégations | id. | Suggestions | non commencé | — | Non | — |
| 14.3 | Résumé visualisation | id. | Résumé texte | non commencé | — | Non | — |
| 14.4 | Résumé ensemble devices | id. + AI card | Group summary | non commencé | — | Non | — |
| 14.5 | Explication anomalie | id. | Texte explicatif | non commencé | — | Non | — |
| 14.6 | Assistance création champs calculés | id. + §6.13 prompt | Aide formules | non commencé | — | Non | — |
| 14.7 | Génération texte rapports | id. §6.12 prompt | Auto texte | non commencé | — | Non | — |
| 14.8 | Fournisseur LLM configurable | §6.13 Prompt | Provider plug | Config provider | non commencé | — | Non | — |
| 14.9 | NON transmission données sensibles sans approbation | §6.13 Prompt | OFF par défaut + opt-in explicite | OFF default + avertissement UI | non commencé | — | — | Non | — |

---

## 15. Sécurité & paramètres

| # | Fonctionnalité Trendz | Doc officielle | Comportement documenté | Équivalent Trendx | Composants | Statut | Tests d'acceptation | Différences connues | Dép. TB PE | Limites |
|---|------------------------|----------------|------------------------|-------------------|------------|--------|---------------------|---------------------|------------|---------|
| 15.1 | Auth via ThingsBoard (SSO) | Trendz connect | user TB auth | SSO via JWT/OAuth2 TB | non commencé | Tenant see | Non | — |
| 15.2 | Permissions tenant/customer TB | id. | Same restrictions TB | Enforcement interroge TB auth | non commencé | — | Non | — |
| 15.3 | Isolation données par tenant | §8 Prompt | Multi-tenant | Colonnes tenant_id partout | non commencé | — | — | Non | — |
| 15.4 | Audit events | §8 Prompt required tables | Table audit_events | non commencé | — | Non | — |
| 15.5 | Reverse proxy TLS + HTTPS/TLS only devant services | §12 + architecture docs | Reverse proxy Nginx/Traefik | TLS+auth | non commencé | — | Non | — |
| 15.6 | Rotation secrets | §7 Prompt + secrets | Secrets mgmt sécurisé | non commencé | — | Non | — |

---

## 16. Ingestion multi-devices & Qualité

| # | Fonctionnalité Trendz | Doc officielle | Comportement documenté | Équivalent Trendx | Composants | Statut | Tests d'acceptation | Différences connues | Dép. TB PE | Limites |
|---|------------------------|----------------|------------------------|-------------------|------------|--------|---------------------|---------------------|------------|---------|
| 16.1 | Ingestion dynamique tous devices | §4 + §9 Prompt | Sans device codé | IngestAll devices | services.ingestion.dynamic | non commencé | — | — | Non | — |
| 16.2 | Ingestion idempotente (upsert) | §9.1 Prompt AGENTS.md | Pas dedup | Unique constraint + upsert | sensor_data hypertable | non commencé | — | — | Non | — |
| 16.3 | Checkpoints indépendants device×métrique | §4 +§9 | Reprise | ingestion_checkpoints table | non commencé | — | Non | — |
| 16.4 | Traitement lots tenant/profil/device/métrique/fenêtre | §9 Prompt lots | Lots pas mémoire constante | Batch processing | non commencé | — | Non | — |
| 16.5 | Contrôle qualité NaN/doublons/fréquence/domaines métier | §10 AGENTS quality rules | QC avant ML | preprocessing.quality + dead-letter | non commencé | — | Non | — |
| 16.6 | Dead-letter / table d'erreurs | id. | Isolation erreurs | Table errors | non commencé | — | Non | — |
| 16.7 | Reprise après échec | id. | Rejouer | non commencé | — | Non | — |
| 16.8 | UTC normalisation de tous ts | §9.2 AGENTS UTC | Sans timezone mix | non commencé | — | Non | — |
| 16.9 | Canal 1: API ThingsBoard pour topologie + métadonnées writeback | §5 Prompt | API | client thingsboard | non commencé | — | Non | — |
| 16.10 | Canal 2 : PostgreSQL TB lecture-seulement pour haute volumétrie télémétrie haute performance | §5 Prompt SQL read | Haute perf TB PG read-only | PG read-only | connector | bloqué | Port 5432 refusé → nécessite action infra 10.0.0.1 pare-feu ou tunnel SSH/réplica | — | OUI Blocage | — |
| 16.11 | Fallback API ThingsBoard télémétrie si SQL indisponible | §5.3 Prompt priorité | Fallback automatique | Fallback si SQL off | partiel | API utilisée par défaut | — | Non | Moins performant |
| 16.12 | Tolérance périmétrique par device (1 casse pas les autres | §4.4 Prompt isolation | Isolation erreur | non commencé | — | Non | — |

---

## 17. Accès direct / Canaux ThingsBoard

| # | Fonctionnalité Trendz | Doc officielle | Comportement documenté | Équivalent Trendx | Composants | Statut | Tests d'acceptation | Différences connues | Dép. TB PE | Limites |
|---|------------------------|----------------|------------------------|-------------------|------------|--------|---------------------|---------------------|------------|---------|
| 17.1 | API ThingsBoard complète (auth JWT, refresh, pagination, time windows | AGENTS §9 | timeout + retry + error handling | thingsboard.client | non commencé | — | — | Non | — |
| 17.2 | PostgreSQL TB lecture seule (vraiment REVOKE write grants) | §5.2 Prompt | USER truly read-only (REVOKE INSERT/UPDATE/DELETE/...) | TB_DB_READONLY_USER | bloqué | Port fermé, USER créé | — | Approbation ouverture pare-feu OU tunnel SSH OU réplica read-only |
| 17.3 | Recherche paginée devices/assets/relations/profils/attributs | §4.1 Prompt discovery | API paginé par lots | discovery.service pagination | non commencé | — | Non | — |
| 17.4 | Écriture via API TB EXCLUSIVEMENT | §5.3 Prompt | Aucun écriture directe SQL | writeback API only | non commencé | — | — | Non | — |

---

## 18. Architecture Trendx (UI, API, Bases)

| # | Fonctionnalité Trendz | Doc officielle | Comportement documenté | Équivalent Trendx | Composants | Statut | Tests d'acceptation | Différences connues | Dép. TB PE | Limites |
|---|------------------------|----------------|------------------------|-------------------|------------|--------|---------------------|---------------------|------------|---------|
| 18.1 | Trendx UI React/TypeScript originale | §6.4 Prompt + §7 architecture | Interface Web originale (pas Grafana seul) | `trendx-ui` React frontend | non commencé | — | — | Non | — |
| 18.2 | Trendx API FastAPI | §7 architecture | API REST | `trendx-api` FastAPI | non commencé | — | — | Non | — |
| 18.3 | Bases séparées trendx / analytics / airflow / mlflow | §8 Prompt 4 bases | 4 bases PostgreSQL | non commencé | — | — | Non | — |
| 18.4 | Rôles distincts api/worker/airflow/mlflow/grafana/migration | §8 Prompt + AGENTS.md 6 rôles | 6 rôles PG | non commencé | — | — | Non | — |
| 18.5 | Hypertables + Continous aggregates + policies retention/compression | §3 Étape 3 AGENTS.md | TimescaleDB | migrations/*.sql timescaledb | non commencé | — | — | Non | — |
| 18.6 | MLflow tracking per tenant/profil/device/strategy/metric | §11 Prompt | MLflow enrichi | mlops module | non commencé | — | Non | — |
| 18.7 | Airflow DAGs 13 DAGs minimaux | §10 Prompt | 13 DAGs | dags/*.py | non commencé | — | — | Non | — |
| 18.8 | Grafana pour dashboards techniques/opérationnels SEULEMENT | §6.4 Prompt | Grafana ne remplace pas Trendx UI | grafana/ + provisioning | non commencé | — | — | Non | — |
| 18.9 | Redis cache (si nécessaire) | §7 architecture | Cache/message broker | docker-compose redis svc | non commencé | — | — | Non | — |
| 18.10 | Reverse proxy Nginx/Traefik TLS | §7 + §12 sécurité | TLS + auth | reverse-proxy | non commencé | — | — | Non | — |
| 18.11 | Health checks TOUS services versions fixes | §6.2 AGENTS.md + §7 Prompt | Docker healthcheck tous | non commencé | — | — | Non | — |
| 18.12 | Ports : TB8081 Airflow8082 MLflow5000 Grafana3000 PG5432 | §6.1 AGENTS.md ports | Ports convention | non commencé | — | — | Non | — |

---

## 19. Tests

| # | Catégorie test | §13 Prompt + §15 AGENTS.md | Composants | Statut |
|---|----------------|------------------------------|------------|--------|
| 19.1 | Tests unitaires normalisation/segments/features/métriques/KPI/scoring/serialisation/parsing TB | §15.1 AGENTS + §13 Prompt | tests/unit/*.py | non commencé |
| 19.2 | Tests intégration auth TB / extraction fenêtre / insertion Timescale / upsert dedup / run MLflow / DAG / Grafana read / writeback test device | §15.2 AGENTS + §13 Prompt | tests/integration/*.py | non commencé |
| 19.3 | Tests contrat API TB + contrat schéma SQL TB | §13 Prompt 7/8 | tests/contract/*.py | bloqué (SQL TB inaccessible) |
| 19.4 | Tests multi-devices / multi-métriques | §13 + jeu données 100 devices synthétiques §14 | tests/multi/ | non commencé |
| 19.5 | Tests de charge | §13.100 devices synthétiques Prompt §14 | tests/load/ | non commencé |
| 19.6 | Tests end-to-end bout en bout scénario complet TB→extract→store→train→predict→writeback→affichage→anomalie→alarme | §15.3 AGENTS + §13 | tests/e2e/ | non commencé |
| 19.7 | Tests de sécurité | §13 | tests/security/ | non commencé |
| 19.8 | Tests de restauration sauvegarde + rollback | §13 + §22 prod readiness | tests/restore/ | non commencé |
| 19.9 | Tests de non-régression ML (champion stable) | §15 AGENTS 10.4 + selectionneur anti promotion auto | tests/ml_noregression/ | non commencé |

---

## 20. Synthèse globale

| Domaine | Fcts totales | non commencé | partiel | terminé | bloqué | hors parité |
|---------|-------------:|-------------:|--------:|--------:|-------:|------------:|
| 1. Topologie & Découverte | 7 | 7 | 0 | 0 | 0 | 0 |
| 2. Business Entities | 8 | 8 | 0 | 0 | 0 | 0 |
| 3. Metric Explorer | 10 | 10 | 0 | 0 | 0 | 0 |
| 4. Visualisations | 21 | 21 | 0 | 0 | 0 | 0 |
| 5. Filtres / Agrégations | 8 | 8 | 0 | 0 | 0 | 0 |
| 6. Champs calculés | 15 | 15 | 0 | 0 | 0 | 0 |
| 7. États | 8 | 8 | 0 | 0 | 0 | 0 |
| 8. Prédiction | 21 | 21 | 0 | 0 | 0 | 0 |
| 9. Anomalies | 20 | 20 | 0 | 0 | 0 | 0 |
| 10. Tâches & Jobs | 8 | 8 | 0 | 0 | 0 | 0 |
| 11. Cache | 8 | 8 | 0 | 0 | 0 | 0 |
| 12. Partage TB | 10 | 10 | 0 | 0 | 0 | 0 |
| 13. Rapports | 9 | 9 | 0 | 0 | 0 | 0 |
| 14. Assistant IA | 9 | 9 | 0 | 0 | 0 | 0 |
| 15. Sécurité & paramètres | 6 | 6 | 0 | 0 | 0 | 0 |
| 16. Ingestion & Qualité | 12 | 10 | 1 | 0 | 1 | 0 |
| 17. Canaux ThingsBoard | 4 | 2 | 0 | 0 | 2 | 0 |
| 18. Architecture Trendx | 12 | 12 | 0 | 0 | 0 | 0 |
| 19. Tests | 9 | 8 | 0 | 0 | 1 | 0 |
| **TOTAL** | **205** | **200** | **1** | **0** | **4** | **0** |

> **Bloquages nécessitant une approbation :**
>
> 1. **§16.10 & §17.2** : Accès PostgreSQL ThingsBoard en lecture seule — port 5432 TCP refusé par 10.0.0.1. Options : ouverture pare-feu limitée IP 10.0.0.1, tunnel SSH, ou création réplica read-only.
> 2. **§19.3** : Tests contrat schéma SQL TB — dépend du blocage 1.
> 3. **§17.2** : Création USER PostgreSQL read-only sur 10.0.0.1 — nécessite une action sur le serveur TB.
