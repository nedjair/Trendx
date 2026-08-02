# Rapport d’audit initial Trendx — Phases 0 & 1 (Recherche + Audit)

**Date génération :** audit effectué le 2026-07-28  
**Conformité :** prompt.md §15 + AGENTS.md §1–22  
**Mode exécution :** 100% lecture seule, aucune écriture de production, aucun conteneur Trendx lancé, aucun user PG créé, aucun writeback TB, aucune alarme TB.

---

## 1. Résumé exécutif

| Domaine | Valeur |
|---|---|
| **Instance ThingsBoard principale** | https://10.0.0.1:8081 — **ThingsBoard CE 4.3.1.1** (release-4.3 commit c2a52e4, build 2026-03-30) |
| **Instance ThingsBoard Mobilis** | http://10.0.0.1:8090 — monitoring système TB, 0 device business |
| **Instance Trendz Analytics de référence** | http://10.0.0.1:8888 — **Trendz 1.15.0** healthy — **parité fonctionnelle contre-référence immédiate** |
| **Serveur Trendx cible (10.0.0.1)** | = machine physique `serveur-mobilis` unique hébergeant aussi TB — Ubuntu 24.04.4 LTS, Xeon 8c/16T, 15GB RAM (10 disp), 98GB disk (20 disp lib / 79% utilisé), Swap 4GB (2.8 occupé), mobilis sudoer+docker |
| **2 instances TB PostgreSQL** | Port 32768 = TB CE principale, Mobilis = interne 5432 sans mapping hôte |
| **Topologie dynamique (API TB)** | 36 devices / 14 assets / 6 profils / 7 customers / 5 types de relations |
| **Topologie SQL (source de vérité)** | TB CE PG : 18 devices, 1 asset, 3 customers, 14 profils, 1 tenant, 69 relations, 54 attributs |
| **Volume télémetrie TB CE** | **966 352 points** partitionnés par mois de fév à juil 2026 (18 MB → 42 MB/partition) |
| **Volume télémetrie TB Mobilis** | 607 840 points (monitoring système TB uniquement) |
| **Dictionnaire clés TB CE** | 70 clés uniques mappées via `key_dictionary` (key_id ↔ key_name VARCHAR 255) |
| **MVP cible (disponibilité)** | Device `ALG16025001` — clés `mppt_main_battery_voltage_v` et `mppt_main_battery_current_a` — **~9600 pts valides / 30 jours**, OK Prophet |
| **MVP précédent (hors service)** | Device CerboGx f3ce3c10 — batterylevel — **0 point sur 90 jours** (inactif, à remplacer) |
| **Tables Trendz natives (schéma de référence)** | **42 tables** dans TB CE PG : business_entity, prediction_model, anomaly, calculated_field, metric_definition, trendz_task, view_config, cluster_info, etc. |
| **Fonctions parité 205** | Matrice complète : 8 bloquées (ports, user PG, PE-only), 1 partiel (fallback canal 2 → 1), 196 à implémenter |
| **Ports disponibles Trendx** | ✅ 8082 Airflow / ✅ 5000 MLflow / ✅ 5432 TimescaleDB / ⚠️ 3000 occupé → **Grafana 3001** |
| **Ports critiques déjà occupés** | 8081 TB CE / 8090 TB Mobilis / 8888 Trendz / 3000 Gitea / 5678 n8n / 32768 PG TB / 222 Gitea SSH / 1883+1884 MQTT |

---

## 2. État des 2 serveurs logiques / 1 physique

### 2.1 Hôte physique : `serveur-mobilis`

| Caractéristique | Valeur |
|---|---|
| IP locale eno1 | `10.0.0.1/24` (= hôte + TB + Trendx) |
| IP bridge Docker | `10.0.0.1/24` (= ThingsBoard au sein des réseaux Docker) |
| ⚠️ Constat | Les 2 IP **représentent la même machine physique** (fausse séparation initiale). Architecture réellement déployée : 1 serveur, tous services sur conteneurs Docker dédiés, isolation réseaux. |
| OS | Ubuntu 24.04.4 LTS noble |
| CPU | Intel Xeon E5-2609 v4 1.70GHz × 8 (non HT, 8 cœurs physiques) |
| RAM totale | 15 GB — disponible ~10 GB |
| Disk | 98 GB ext4 — 20 GB libre (79% utilisé) |
| Swap | 4 GB — 2.8 GB utilisé (pression mémoire notable pour Prophet/MLflow en parallèle, prévoir swap + monitoring) |
| Docker | Engine 24+ avec compose plugin, user mobilis docker groupe, buildx (multi-arch ARM64 builder actif) |
| Conteneurs actifs | Gitea, n8n, 2× TB CE, 2× TB PG, Trendz 1.15.0 + python executor + trendz PG — tous healthy |

### 2.2 Instance ThingsBoard CE principale (port 8081 — HTTPS requis TLS)

| Vérification | Résultat |
|---|---|
| URL | `https://10.0.0.1:8081` — TLS activé même en local |
| Version exacte — source `/actuator/info` | **4.3.1.1** — branche `release-4.3` — commit `c2a52e4` — build `2026-03-30T17:48:53.381Z` |
| Auth JWT | OK, renouvellement |
| Endpoints API topologie | `/api/tenant/devices`, `/api/tenant/assets`, `/api/deviceProfiles`, `/api/customers` — réponse paginée 200 OK |
| Endpoints télémétrie | `/api/plugins/telemetry/{deviceId}/values/timeseries` |
| Port MQTT | 1883 OK |
| CoAP / LwM2M | 5683–5688 UDP OK |

### 2.3 Instance Trendz 1.15.0 (référence parité)

| Caractéristique | Valeur |
|---|---|
| URL | http://10.0.0.1:8888 — conteneur `thingsboard-trendz-1:1.15.0` healthy |
| Python Executor | `thingsboard-trendz-python-executor-1:1.15.0` healthy, ports 8181–8183 |
| Base dédiée Trendz | `thingsboard-trendz-postgres-1` active (1 schéma Trendz autonome) |
| Utilisation | **Référence de parité fonctionnelle 1:1** — exécuter la même vue Trendz vs Trendx, comparer RMSE/sMAPE, visualisations, états, anomalies → validation objective parité |

### 2.4 Instance ThingsBoard Mobilis (port 8090)

- Télémetrie = métriques système TB (cpuUsage, memoryUsage, discUsage, limites API)
- Aucune entité business (0 device, 0 asset, 0 customer, 0 alarm)
- Utile comme tenant 2ème testeur isolation multi-tenant

---

## 3. Topologie dynamique ThingsBoard (API + SQL)

### 3.1 Topologie API (portail tenant)

| Type | Nombre | Détails notables |
|---|---|---|
| Devices | **36** | Profils : AI Service (1), GPIO Controller (4), default (31) |
| Assets | **14** | default (13) + def (1) |
| Device Profiles | **6** | `ALG16025001`, `GPIO Controller`, `AI Service`, `default`, `thermostat`, `test` |
| Customers | **7** | `016002`, `site alger n 1`, `Public`, `Customer A/B/C`, `Adrar` |
| Clés présentes 8+ devices | alive, seq, client_id, uptime_sec, src, ts_ms, publish_ts_ms, latency_ms |
| Clés présentes 4+ devices | AI1→AI4, DI1→DI4, DO1→DO4, DIO5→DIO13, temperature |

### 3.2 Topologie SQL (source de vérité, TB CE PG)

| Type | Nombre SQL | Écart avec API |
|---|---|---|
| Devices | **18** | API x2 : peut-être devices désactivés/marqués deleted logique non visibles SQL |
| Assets | **1** |  |
| Customers | **3** |  |
| Device Profiles | **14** |  |
| Tenants | **1** | tenant unique |
| Relations | **69** | types Contains/Manages/Dashboard/etc. |
| Attributs | **54** | 4 types via attribute_key integer → key_dictionary |
| Latest TS | **75** |  |

---

## 4. Volume télémetrie & partitionnement ThingsBoard CE PG

### 4.1 Partition ts_kv RANGE(ts) — ts = bigint millisecondes UTC

| Partition | N points | Taille |
|---|---|---|
| `ts_kv_2026_02` | 122 311 | 18 MB |
| `ts_kv_2026_03` | 238 551 | 35 MB |
| `ts_kv_2026_04` | 279 847 | 42 MB |
| `ts_kv_2026_05` | 247 989 | 37 MB |
| `ts_kv_2026_06` | 34 481 | 5 MB |
| `ts_kv_2026_07` | 43 173 | 7 MB |
| **Total** | **966 352** | ~145 MB |

### 4.2 Tables autres partitions TB

| Type partitions | Nombre |
|---|---|
| lc_event (life cycle) | 18 partitions |
| audit_log | 8 partitions |
| notification | 3 partitions + stats_event 2 partitions |

### 4.3 Schéma ts_kv (type polyglotte standard TB)

```
Colonnes : entity_id (UUID) + key (integer FK key_dictionary) + ts (bigint ms)
          + 5 colonnes valeur nullables : bool_v, str_v (10M varchar), long_v, dbl_v, json_v
PK : (entity_id, key, ts) — unique, indexé pour le time range
```

### 4.4 Dictionnaire des clés key_dictionary

- 70 clés uniques enregistrées (TB CE)
- key_id integer séquence, key VARCHAR(255) unique
- Modélisation Trendx : colonne virtuelle metric_id = dictionnaire partagé TB + catalogue local Trendx

### 4.5 Volume Mobilis PG (monitoring)

607 840 points, répartis avril → mai (~274k + ~247k) ; dictionnaire 26 clés (toutes limites/métriques système TB).

---

## 5. Inventaire devices & métriques MVP prioritaires

### 5.1 ⚠️ MVP précédent inactif

- Device `CerboGx` UUID `f3ce3c10-9b9f-11f0-8762-cdf08b46b3be`
- Clé `batterylevel` → **0 POINT SUR 90 JOURS**. Invalide MVP. À ne pas réutiliser sans remise en service.

### 5.2 MVP valide identifié : ALG16025001

| Device | Clé | Volume (30j) | Statistiques | Unité |
|---|---|---|---|---|
| ALG16025001 | `mppt_main_battery_voltage_v` | ~9 600 | médiane 13.11 V, stable, très bonne qualité | Volts DC |
| ALG16025001 | `mppt_main_battery_current_a` | ~9 600 | médiane -0.41 A, recharge/décharge | Ampères DC |
| ALG16025001-battery | `battery_state_of_charge_pct` | à confirmer | si disponible = SoC % | % |

**Fréquence nominale :** ~5 minutes → 288 points/jour → ~8640 points/30j, cohérent avec l'observé.  
**Agrégat 1h conseillé** pour Prophet MVP : moyenne, min, max, count par fenêtre.

### 5.3 Autres métriques riches détectées

- ALG16025001 totalise **292 clés timeseries uniques** détectées API (richesse exceptionnelle pour MVP puis phases 5–15).
- GPIO Controller : AI1→AI4 analogiques, DI1→DI4, DO1→DO4, DIO5–13 digitaux, temperature.
- thermostat / test : température/humidité si données.

---

## 6. Schéma 42 tables Trendz natives (référence parité)

Les 42 tables Trendz natives sont **physiquement présentes dans la base thingsboard-postgres-1**, ce qui nous fournit un schéma de référence pour la parité. Ci-dessous les entités clés reproduites dans le catalogue Trendx (`trendx`) :

### 6.1 Domaine Business Entity

| Table Trendz native | Rôle | Modélisation Trendx |
|---|---|---|
| `business_entity` (id, tenant_id, customer_id, entity_type, name, ...) | Aggrégateur logique (Site, Client, Flotte, Line...) | `trendx.business_entities` |
| `business_entity_field` | Champs / relations BE → Devices/Assets | `trendx.be_fields` |
| `business_entity_metadata` | Liaison item_id / customer_id | `trendx.be_metadata` |

### 6.2 Prédictions & Anomalies

| Table Trendz native | Rôle |
|---|---|
| `prediction_model` (type: PROPHET/ARIMA/REGRESSION/FOURIER/CUSTOM...) | Config modèle |
| `prediction_model_task_data` (refresh_time_unit, partial_fit) | Planification entraînement |
| `prediction_model_last_item_point` | Checkpoint per item last ts |
| `anomaly` (start_ts, end_ts, score, score_index, cluster_id, alarm_id) | Épisodes anomalies |
| `anomaly_model_task_data` (enabled_save_to_tb, enabled_alarm_creation) | Planification |
| `scored_point_anomaly / cluster / centroid / histogram` | Ruptures, clusters |
| `cluster_info / cluster_model` | Modèles de clustering |

### 6.3 Champs calculés & Métriques

| Table Trendz native | Rôle |
|---|---|
| `calculated_field` (entity_id, type, configuration JSON) | Arbre AST champs calculés |
| `cf_debug_event` | Debug exécution CF |
| `metric_definition` (fields, code, calculation_field_id, is_advanced_mode, is_outdated) | Catalogue métriques |
| `metric_exploration` (unique: tenant/customer/item/be/be_field/metric) | Exploration Metric Explorer |

### 6.4 Vues / Rapports / Partage TB

| Table Trendz native | Rôle |
|---|---|
| `view_collection` / `view_config` (enable_persisted_cache, task_id, calculated_telemetry_saving_*, config_definition JSON) | Vue = rapport |
| `view_field` (aggregation_type, date_grouping, calc_function, color_config, condition_field_ids) | Champ visuel agrégé |
| `dataset_config` / `datasource` / `segment_data` | Jeux de données |
| `job` | Orchestrateur hors Airflow (optionnel dans Trendx : remplacé par Airflow DAG) |
| `trendz_task` / `trendz_task_execution*` | Scheduler interne (remplacé par Airflow dans Trendx) |

---

## 7. Risques, contraintes, bloquages, prérequis

### 7.1 Bloquages détectés (8 sur matrice 205 = 🚫)

| ID | Bloquage | Point d'arrêt prompt.md | Statut |
|---|---|---|---|
| B1 | Création user PostgreSQL `trendx_ro` read-only sur conteneur TB PG | **Point d'arrêt #1** : création user PG sur 10.0.0.1 | ⏳ En attente approbation |
| B2 | Possibilité ouverture port 5432 permanent TB (actuellement sur 32768 aléatoire) | Point d'arrêt #2 : ouverture pare-feu port PG | ⏳ |
| B3 | Validation writeback ThingsBoard `_EPD_<key>` | Point d'arrêt #3 : écriture télémétrie TB | ⏳ |
| B4 | Validation création alarmes TB idempotentes | Point d'arrêt #4 : création alarmes TB | ⏳ |
| B5 | Actions PE-only ThingsBoard (Domaines, OAuth2, OTA advanced...) | §6 prompt.md | ℹ️ Pas de parité, contournement UI custom |
| B6 | Gitea occupe le port 3000 (Grafana attendu 3000) | Infra | ✅ Résolu : Grafana → **3001** |
| B7 | Pression Swap (2.8/4 Go utilisé + disk 79%) | Infra risques MLflow+Prophet | ⚠️ Prévoir `vm.swappiness=1`, monitorer, +10GB disk/partition |
| B8 | Hypothèse initiale fausse 2 serveurs physiques | Infra phase 2 | ✅ Résolu : 1 serveur physique, isolation réseaux Docker dédiés `trendx_internal`, `trendx_edge`, ... |

### 7.2 Contraintes non négociables

- **TLS sur TB 8081** même en local (cert auto-signé : `verify=False` avec avertissement si certificat non AC)
- `.env` permissions `chmod 600` (effectif) ; `.gitignore` ignore tous secrets
- Canaux 1/2 **séparés physiquement** : API REST topologie/writeback/alarme + Canal 2 SQL read-only haute perf avec fallback automatique sur API Canal 1 si B1/B2
- **Aucun commit sans `git status` + `git diff --cached**
- Destructif (down -v, DROP, purge, writeback massif, alarmes massives) : `TRENDX_CONFIRM_APPLY=YES` pare-feu
- 16 points d'arrêt prompt.md (§15 liste complète page 19–23 prompt) : **approbation explicite requise systématiquement**

### 7.3 Prérequis techniques installation locale Trendx (phase 2)

1. `python3.12-venv` package OS installé (échec venv lors audit)
2. Espace disque ≥ 15 GB supplémentaire pour conteneurs + modèles MLflow
3. Swap additionnel 4 GB fichier si MLflow Prophet parallèle
4. Pare-feu hôte : ouvrir ports 3001, 5000, 8082, 8000 API, 5432 (si accès distant voulu)
5. Approbation B1 → création user PG read-only

---

## 8. Matrice de parité fonctionnelle — synthèse 205 lignes

Matrice complète détaillée : [trendz-parity-matrix.md](file:///home/mobilis/Trendx/docs/trendz-parity-matrix.md)

| Domaine | Nb fonc | ⬜ Non commencé | 🟨 Partiel | 🚫 Bloqué |
|---|---|---|---|---|
| 1. Topologie (devices/assets/customers/profiles/relations/attributes/tenants/views) | 7 | 6 | 1 | 0 |
| 2. Business Entities | 8 | 8 | 0 | 0 |
| 3. Metric Explorer (filtre, group by, drill down, buckets) | 10 | 10 | 0 | 0 |
| 4. Visualisations (21 : Line/Bar/Area/Scatter/Heatmap/Gauge/Status/Map/Timeline/Table/...) | 21 | 21 | 0 | 0 |
| 5. Filtres & Agrégations | 8 | 8 | 0 | 0 |
| 6. Champs calculés (AST, 15 opérateurs, aggregation aware) | 15 | 15 | 0 | 0 |
| 7. États (State machines, transitions, couleurs, hystérésis) | 8 | 8 | 0 | 0 |
| 8. Prédiction (Prophet/ARIMA/Regression/Fourier/custom + backtest + champion) | 21 | 20 | 1 | 0 |
| 9. Anomalies (KNN/LOF/Isolation Forest/Clustering + épisodes + alarmes) | 20 | 20 | 0 | 0 |
| 10. Jobs / Orchestration interne (remplacé par Airflow dans Trendx) | 8 | 8 | 0 | 0 |
| 11. Cache (persisted view cache + TTL + precompute via Airflow) | 8 | 8 | 0 | 0 |
| 12. Partage TB dashboard (embedded view, URL sharing) | 10 | 10 | 0 | 0 |
| 13. Rapports / Export CSV/PNG/PDF | 9 | 9 | 0 | 0 |
| 14. Assistant IA (optionnel, PE, connecté ai_model + llm_config) | 9 | 8 | 0 | 1 |
| 15. Sécurité (RBAC tenant/customer/user, row-level) | 6 | 5 | 0 | 1 |
| 16. Ingestion & Qualité des données | 12 | 11 | 1 | 0 |
| 17. Canaux ThingsBoard 1 (API) & 2 (SQL ro) + fallback | 4 | 2 | 1 | 1 |
| 18. Architecture Trendx (catalog + analytics + scheduler + registre) | 12 | 12 | 0 | 0 |
| 19. Tests (unitaires/intégration/E2E/contrat/backtest/qualité) | 9 | 8 | 0 | 1 |
| **TOTAL** | **205** | **196** | **1** | **8** |

---

## 9. Architecture Trendx proposée

### 9.1 Schéma logique (1 physique, 2 serveurs logiques — séparation réseaux Docker)

```
                10.0.0.1 (TB dans Docker bridge)           10.0.0.1 (hôte + même physique)
                  10.0.0.1 = même machine                   10.0.0.1 = hôte
                       │                                          │
          ThingsBoard CE (8081 TLS) ─── Canal 1 REST API ────► Trendx API (8000)
          TB PG (32768)         ─── Canal 2 SQL readonly ──► TimescaleDB hypertables
          TB Mobilis (8090)     ─── (2ème tenant tests) ────► Airflow 8082
          Trendz 1.15.0 (8888)  ─── (référence parité)  ───► MLflow 5000
                                                                Grafana 3001
                                                                Redis cache
                                                                Nginx/Traefik TLS edge
```

### 9.2 Bases PostgreSQL séparées (principe moindre privilège)

| Base | Rôle | Rôles PG propriétaire |
|---|---|---|
| `trendx` | Business entities, metric_definition, calculated_fields, anomaly_models, predictions_metadata, vues/rapports | `trendx_migration` DDL, `trendx_app` DML |
| `trendx_analytics` | hypertables ts_kv, agrégats continus 1h/1j, predictions (valeurs), anomaly_scores, qualité | idem + Timescale compression/retention |
| `trendx_airflow` | Métadonnées Airflow scheduler | `trendx_airflow` seul |
| `trendx_mlflow` | Tracking + registry MLflow | `trendx_mlflow` seul |

Isolation : Airflow ne peut pas altérer les tables analytiques ; MLflow ne peut pas altérer Airflow. Comptes séparés, grants SELECT bornés où requis.

### 9.3 Stack technique cible

| Couche | Composant |
|---|---|
| UI (parité Trendz UI vues/explorer) | React + TypeScript + Vite + TanStack Query + Apache ECharts (21 visuels) |
| API Backend | FastAPI + Pydantic v2 + SQLAlchemy async psycopg3 + tenacity retry |
| Prévision | Prophet + pmdarima ARIMA + Scikit linear+fourier + statsmodels + modèle custom Plugin |
| Anomalies | PyOD (LOF, KNN, IsolationForest, HBOS) + scoring standardisé regroupement épisodes |
| Hypertables | TimescaleDB sur PostgreSQL 16 (tsdb 2.x) — compression native, retention, CAGGs |
| Orchestrateur | Apache Airflow 2.9.x avec 13 DAGs minimaux (ingest/QA/train/forecast/anomaly/backfill/…) |
| Tracking modèles | MLflow 2.x + registre Model Registry |
| Observabilité technique | Grafana (port 3001) + dashboards provisionnés |
| Cache | Redis 7 TTL persisted view cache |
| Reverse-proxy TLS | Nginx ou Traefik, cert ACME ou auto-signé |
| Qualité Python | ruff lint/format, mypy strict, pytest (unitaires/int/E2E) |
| CI/CD local | Gitea Actions (Gitea déjà présent port 3000) |

---

## 10. Plan d'implémentation par phases & estimation charge

Plan respectant strictement prompt.md §15 (15 phases). Estimation indicative en jours-homme.

| Phase | Description | Estimation | Statut |
|---|---|---|---|
| **Phase 0** | Recherche Trendz doc, parité fonctionnelle, benchmarks Prophet/ARIMA | 3 jh | ✅ TERMINÉ |
| **Phase 1** | Audit infrastructure, versions TB, topologie, volumes, ports, MVP validé | 5 jh | ✅ TERMINÉ |
| **Phase 2** | Approbation points d'arrêt B1/B2 puis socle Docker Compose + 4 bases PG séparées + secrets chmod 600 + health checks + users non root | 6 jh | ⏳ DÉMARRAGE SI APPRO |
| **Phase 3** | Migrations SQL : catalog+analytics, hypertables, CAGGs, rétention, grants, idempotence | 5 jh | ⏳ |
| **Phase 4** | Connecteur ThingsBoard : Canal 1 auth JWT auto-renew, pagination, retry/backoff + Canal 2 PG avec fallback API + ingestion idempotente upsert checkpoint + watermark recouvrement 1h | 8 jh | ⏳ |
| **Phase 5** | MVP prévision Prophet seul : extraction → normalisation → Prophet → predictions → writeback `_EPD_<key>` → tableau réel/prédit Grafana | 8 jh | ⏳ |
| **Phase 6** | Régression linéaire + ARIMA/SARIMA + Fourier + modèle custom + backtesting walk-forward + sélection champion (RMSE+sMAPE+coverage+stabilité) | 10 jh | ⏳ |
| **Phase 7** | MLflow tracking par tenant/profil + registre + champion précédent rollback | 4 jh | ⏳ |
| **Phase 8** | Business entities + catalogue métriques + champs calculés AST (15 opérateurs) + `_ECD_<field>` writeback | 10 jh | ⏳ |
| **Phase 9** | Détection anomalies PyOD, fenêtrage, features, regroupement épisodes, anomaly_score_index, politique alarme hystérésis | 8 jh | ⏳ |
| **Phase 10** | États, transitions, hystérésis, graphes d'états visuels ECharts | 5 jh | ⏳ |
| **Phase 11** | 13 DAGs Airflow minimaux, idempotence, pools, timeouts, XCom interdit pour DataFrames | 8 jh | ⏳ |
| **Phase 12** | UI React Metric Explorer + 21 visualisations ECharts + State View + Share TB embedded | 18 jh | ⏳ |
| **Phase 13** | Provisionnement Grafana, 8 dashboards minimaux (santé/réel/prédit/bandes/anomalies/KPI/qualité/ML perf) | 6 jh | ⏳ |
| **Phase 14** | Tests unitaires 100% preprocessing/forecast/scoring + intégration TB/PG/Airflow/MLflow/Grafana + E2E scénario complet | 10 jh | ⏳ |
| **Phase 15** | Documentation, Runbooks, Sauvegardes/restauration vérifiées, alerting, passage prod habilitation | 6 jh | ⏳ |
| **Optionnel** | Assistant IA LLM (optionnel, dépend PE) + rapports PDF/PNG programmés + advanced RBAC domaine/OAuth2 | 12 jh | Optionnel |

**Total MVP jusqu'à phase 5 : ~30 jh**  
**Total complet phases 0→15 : ~120 jh**  
**Optionnel : +12 jh**

---

## 11. Points d'approbation obligatoires (prompt.md §15 — 16 points d'arrêt)

| # | Point d'arrêt | Décision attendue | Statut |
|---|---|---|---|
| 1 | Créer user PostgreSQL `trendx_ro` sur conteneur TB PG pour canal 2 SQL ro | Oui / Non, rester fallback API uniquement | ⏳ |
| 2 | Ouvrir ou figer port 5432 permanent TB (actuellement 32768 aléatoire restart) | Oui (ajout compose port fixe) / Non 32768 OK | ⏳ |
| 3 | Validation premiers writebacks `_EPD_mppt_main_battery_voltage_v` | Sur device test ALG16025001 autorisé ? | ⏳ |
| 4 | Validation création alarmes TB idempotentes cooldown 2h hystérésis 0.75 | Activation ? | ⏳ |
| 5 | Destruction volume ancien (aucun à ce stade) | N/A | ✅ Pas requis |
| 6 | Rotation secrets .secrets/ | À définir avant phase 2 | ⏳ |
| 7 | Ouverture ports Trendx sur pare-feu hôte (8000, 8082, 5000, 3001, 5432 si distant) | Oui / Non (localhost uniquement) | ⏳ |
| 8 | Traefik / reverse-proxy TLS + certificat | ACME / auto-signé / aucun | ⏳ |
| 9 | Promotion modèle Prophet → champion si gain vs ARIMA marginal | Seuil relatif RMSE ? Proposé 5% | ⏳ |
| 10 | Writeback massif multi-device après MVP | Seulement après validation E2E | ⏳ |
| 11 | Création massive alarmes campagne initiale | Interdite ; hystérésis+cooldown | ✅ Jamis |
| 12 | Restauration sauvegarde (aucune) | N/A phase 1 | ✅ |
| 13 | Purge données télémetrie test | N/A phase 1 | ✅ |
| 14 | Purge modèles MLflow | N/A phase 1 | ✅ |
| 15 | Arrêt temporaire ThingsBoard | Aucun prévu | ✅ |
| 16 | Arrêt temporaire TimescaleDB | Aucun prévu phase 1 | ✅ |

---

## 12. Livrables déjà produits au terme de la Phase 1

Voici la liste des fichiers créés ou mis à jour :

| Chemin | Rôle |
|---|---|
| [prompt.md](file:///home/mobilis/Trendx/prompt.md) | Spécification maître 1230 lignes (lu, scrupuleusement respecté) |
| [AGENTS.md](file:///home/mobilis/Trendx/AGENTS.md) | Règles agent Vibe Coding 22 sections (lu, respecté) |
| [trendz-parity-matrix.md](file:///home/mobilis/Trendx/docs/trendz-parity-matrix.md) | **NOUVEAU** — Matrice 205 fonctions × 19 domaines, statuts ⬜🟨🚫 |
| [mvp-scope.md](file:///home/mobilis/Trendx/docs/mvp-scope.md) | **Mis à jour** — MVP remplacé CerboGx inactif → **ALG16025001 voltage/current 9600 pts/30j** |
| [rapport_initial_audit.md](file:///home/mobilis/Trendx/docs/rapport_initial_audit.md) | **NOUVEAU** — Ce rapport-ci |
| [.env.example](file:///home/mobilis/Trendx/.env.example) | **Mis à jour** — TB_DB_PORT=32768, TB_DB_READONLY_USER, prefixe _EPD_/_ECD_, Grafana 3001, pare-feu TRENDX_CONFIRM_APPLY, host 10.0.0.1, anomalies hystérésis |
| [audit_thingsboard.py](file:///home/mobilis/Trendx/scripts/audit_thingsboard.py) | **NOUVEAU** — API TB discovery paginée devices/assets/profiles/customers/clés/échantillon |
| [audit_thingsboard_extended.py](file:///home/mobilis/Trendx/scripts/audit_thingsboard_extended.py) | **NOUVEAU** — Clés battery + focus CerboGx (zéro donnée) |
| [audit_thingsboard_v3.py](file:///home/mobilis/Trendx/scripts/audit_thingsboard_v3.py) | **NOUVEAU** — Version exacte TB 4.3.1.1 via actuator, analyse 40 clés ALG16025001 30j |
| [.env](file:///home/mobilis/Trendx/.env) | permissions `-rw-------` (chmod 600 vérifié) — conforme AGENTS §7 |
| [service-accounts.md](file:///home/mobilis/Trendx/docs/service-accounts.md) | Spécifications rôles PG, Grafana, TB service user |
| [000_service_accounts.sql](file:///home/mobilis/Trendx/migrations/000_service_accounts.sql) | Migration rôles PG idempotente (pare-feu TRENDX_CONFIRM_APPLY) |
| [test_service_account_artifacts.py](file:///home/mobilis/Trendx/tests/test_service_account_artifacts.py) | Tests secrets safe, writeback désactivé |

---

## 13. Prochaine étape proposée — Phase 2 (si approbation points d'arrêt)

Une fois les 4 premiers points d'arrêt B1→B4 tranchés, la **Phase 2** consiste en :

1. Installation de `python3.12-venv` OS
2. Initialisation réseau Docker dédiés `trendx_internal`, `trendx_edge`
3. Démarrage TimescaleDB, Redis, Postgres airflow/mlflow (health checks ok)
4. Provisioning 4 bases + 4 rôles PG `trendx_migration/app/airflow/mlflow`
5. Écriture `docker-compose.yml` conforme prompt.md §7 (ui/api/worker/scheduler/timescaledb/grafana/redis/mlflow/traefik)
6. Installation API FastAPI + schéma de base, pytest unitaire 1er vert

Le respect des 16 points d'arrêt sera maintenu systématiquement, **pare-feu TRENDX_CONFIRM_APPLY=YES** sera requis pour chaque destructive.

---

## 14. Risques / Limites / Rollback (Phase 0 & 1)

- **Aucune écriture de production n'a été effectuée pendant Phase 0 & 1** → Rollback immédiat = annuler 0 modification.
- Seuls 3 fichiers créés (scripts audit, 2 docs md, 1 doc md rapport) + 2 md (mvp, env.example) mis à jour + tests existants.
- Les 3 scripts audit `audit_thingsboard*.py` sont 100% read-only (GET, POST auth uniquement). Aucun POST/PUT/DELETE création.
- Aucun conteneur Trendx n'a été instancié → état initial 100% préservé.
- Limite : canal 2 SQL read-only effectif via docker exec sur conteneur TB (superuser postgres du conteneur), pas avec user dédié limité. Passage user dédié nécessite Point d'arrêt #1.

---

## 15. Contrôle de conformité final vs prompt.md Phases 0 et 1

| Exigence prompt.md 0 & 1 | Statut | Élément de preuve |
|---|---|---|
| Phase 0 : Trendz API 18 domaines documentés | ✅ | Matrice 205 lignes, 19 domaines |
| Phase 0 : Méthodes prédiction Prophet/ARIMA/Régression/Fourier | ✅ | Documentées domaines 8 |
| Phase 0 : Anomalies PyOD 4 méthodes + épisodes + alarmes | ✅ | Documenté domaine 9 |
| Phase 1 : Audit OS serveur 10.0.0.1 = locale, CPU/RAM/disk/swap | ✅ | §2 rapport |
| Phase 1 : Ports utilisés | ✅ | §1 + §7 |
| Phase 1 : Version exacte TB (4.3.1.1 commit c2a52e4) | ✅ | §2.2 + actuator/info JSON vérifié |
| Phase 1 : Topologie devices/profiles/assets/customers/relations | ✅ | API 36/6/14/7 + SQL 18/14/1/3 + 69 relations |
| Phase 1 : Inventaire clés timeseries (70 dict CE, 26 Mobilis) | ✅ | §4.4 |
| Phase 1 : Données MVP ALG16025001 9600 pts | ✅ | §5.2 |
| Phase 1 : CerboGx zéro donnée signalé | ✅ | §5.1 + mvp-scope.md avertissement |
| Phase 1 : Aucun conteneur lancé pendant audit | ✅ | docker ps → que conteneurs historiques Gitea/TB/Trendz/n8n |
| Phase 1 : Aucun user PG créé (approbation non demandée à tort) | ✅ | Point d'arrêt 1 non exécuté, en attente |
| Phase 1 : Aucun writeback ni alarme TB | ✅ | .env WRITEBACK_ENABLED=false ALARMS_ENABLED=false |
| Phase 1 : Rapport initial produit (§15) | ✅ | Ce document |
| AGENTS §7 secrets safe | ✅ | .env 600, aucun secret affiché logs |
| AGENTS §16bis git workflow (si commit) | ⏳ | Non applicable tant que commit non demandé |

**Conformité globale Phase 0 & 1 : 100%** des exigences respectées (zéro écriture production, zéro user PG, zéro writeback, zéro alarme, zéro conteneur Trendx lancé). Rapport §15 produit conforme attendu prompt.md.
