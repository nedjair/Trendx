# Plan d'implémentation — Phases 2 à 10

**Version :** 1.1 — Phase 2 (corrections post-validation)
**Date :** 2026-08-02
**Dépendances :** approbation des 6 points d'arrêt Phase 1 + validations Phase 2 conditionnelles (Q1–Q5, A–C)

---

## Phase 2 — Socle Docker Compose + base trendx + schémas

**Dépendances :** approbation points d'arrêt #1, #2, #3 + validations Q1–Q5, A–C
**Durée estimée :** 8 jours-homme

**Correction A — UNE SEULE BASE :** `trendx` avec schémas `trendx_catalog` et `trendx_analytics`. Pas de `trendx_airflow` ni `trendx_mlflow` en Phase 2.

**Correction C — APScheduler :** Worker utilise APScheduler en mode standalone, sans broker externe.

- [ ] Installation `python3.12-venv` OS
- [ ] Initialisation réseaux Docker `trendx_internal`, `trendx_edge`
- [ ] Création base `trendx` + schémas `trendx_catalog`, `trendx_analytics`
- [ ] Création rôle `trendx_ro` sur base `thingsboard` (B1)
- [ ] Création rôles `trendx_migration`, `trendx_app` sur base `trendx`
- [ ] Écriture `docker-compose.yml` conforme (reverse-proxy/api/worker uniquement, pas de TimescaleDB)
 - [ ] Écriture migrations SQL tracées dans `public.schema_version` : schéma `trendx_catalog` + `trendx_analytics` (partitionné déclaratif + BRIN)
- [ ] Partition mensuelle `trendx_analytics.ts_kv` pré-créée à +3 mois
- [ ] Agrégats horaires/jour/semaine/mois : UPSERT incrémental, jamais REFRESH MATERIALIZED VIEW (Q3)
- [ ] Installation API FastAPI + schéma de base, pytest unitaire 1er vert
- [ ] Secrets `.secrets/` chmod 600, dir 700, root
- [ ] Reverse-proxy TLS 8443 sur 127.0.0.1, auth obligatoire, HSTS, pas 80 (Q4)
- [ ] Runbook `docs/runbooks/writeback-rollback.md` validé avant toute écriture (Q5)
- [ ] Vérification espace disque avant toute migration (Correction B)
- [ ] Arrêt automatique ingestion sous `TRENDX_DISK_MIN_FREE_GB=50` implémenté

**Livrables :** docker-compose.yml, 3 Dockerfiles, migrations SQL tracées, API /health OK, runbook writeback validé

---

## Phase 3 — Connecteur ThingsBoard + ingestion

**Dépendances :** Phase 2 terminée, Canal 2 SQL accessible
**Durée estimée :** 8 jours-homme

- [ ] Client TB Canal 1 (auth JWT auto-renew, pagination, retry/backoff)
- [ ] Client TB Canal 2 (SQL RO avec fallback automatique sur Canal 1)
- [ ] Ingestion idempotente upsert + checkpoint par device/metric
- [ ] Fenêtre de recouvrement 1h
- [ ] Dédoublonnage par PK (entity_id, key, ts)
- [ ] Table d'erreurs `ingestion_errors`
- [ ] APScheduler job ingestion horaire (pas de Celery, pas de broker)
- [ ] Normalisation UTC + domaines métier

**Livrables :** connecteur TB, job ingestion, tests intégration Canal 1 + Canal 2

---

## Phase 4 — Qualité données + preprocessing

**Dépendances :** Phase 3
**Durée estimée :** 5 jours-homme

- [ ] Détection NaN / nulls / doublons
- [ ] Contrôle fréquence (attendu vs observé)
- [ ] Contrôle domaines métier (min/max par métrique)
- [ ] Politique imputation : forward fill borné → interpolation linéaire → backfill
- [ ] Rééchantillonnage 1h (moy/min/max/count)
- [ ] RobustScaler / MinMax / Standard auto
- [ ] Features temporelles (heure, jour, semaine, mois)
  - [ ] Table `data_quality` (hypertable `002` + partitionnée `011`) — **seule table réelle** ; `data_quality_report` / `data_quality_issue` du plan initial ne sont PAS créées (le code skip ces écritures, cf. `src/trendx/preprocessing/quality.py`, `src/trendx/database/models.py`). Aucune migration ne doit les introduire.

**Livrables :** module preprocessing, tests qualité, rapport qualité par device/metric

---

## Phase 5 — MVP Prévision Prophet

**Dépendances :** Phase 4
**Durée estimée :** 8 jours-homme

- [ ] Entraînement Prophet (tendance + saisonnalité quotidienne + hebdomadaire)
- [ ] Horizon 24h, fréquence 1h
- [ ] Réentraînement quotidien
- [ ] Writeback `_EPD_<metric>` (désactivé par défaut, dry-run uniquement)
- [ ] Validation MAE/RMSE/sMAPE/coverage@80%/coverage@95%/mean interval width/bias
- [ ] Sélection champion vs challenger (ARIMA, Linear, Fourier)
- [ ] MLflow tracking + Model Registry (fichier local puis conteneur)
- [ ] Dashboard Grafana réel vs prédit (optionnel)

**Livrables :** prévisions 24h, tests backtest, champion model, dashboard

---

## Phase 6 — Modèles alternatifs + compétition

**Dépendances :** Phase 5
**Durée estimée :** 10 jours-homme

- [ ] Régression linéaire (OLS scikit-learn)
- [ ] ARIMA / SARIMA (pmdarima)
- [ ] Transformation de Fourier
- [ ] Modèle Python personnalisé (sandbox)
- [ ] Backtesting walk-forward
- [ ] Sélection champion (RMSE + sMAPE + coverage + stabilité + contraintes métier)
- [ ] Rollback automatique si dégradation > seuil

**Livrables :** 4 modèles entraînables, compétition automatisée, sélection champion

---

## Phase 7 — MLflow + registre

**Dépendances :** Phase 6
**Durée estimée :** 4 jours-homme

- [ ] Tracking par tenant/profil/device/strategy/metric
- [ ] Model Registry + versions
- [ ] Champion / challenger promotion
- [ ] Rollback version précédente
- [ ] Artefacts stockés dans volume local puis conteneur MLflow

**Livrables :** MLflow opérationnel, registre modèle, API promotion

---

## Phase 8 — Business Entities + champs calculés

**Dépendances :** Phase 4
**Durée estimée :** 10 jours-homme

- [ ] CRUD Business Entities
- [ ] Relations entre BE (1 type par paire de profils)
- [ ] Jointures multi-niveaux (Building → Apartment → Meter)
- [ ] GroupBy profil / customer / attribut
- [ ] Drill-down interactif
- [ ] Éditeur formules AST (15 opérateurs)
- [ ] Calculs entre métriques / devices / relations
- [ ] Matérialisation `_ECD_<field>` (désactivé par défaut)
- [ ] Prévisualisation + debug + historique

**Livrables :** module BE, éditeur CF, writeback ECD, tests jointures

---

## Phase 9 — Détection d'anomalies

**Dépendances :** Phase 4
**Durée estimée :** 8 jours-homme

- [ ] PyOD : Isolation Forest, LOF, KNN, HBOS
- [ ] Fenêtrage + features (moy, écart, min/max, pente, amplitude, énergie spectrale, diff, taux NaN)
- [ ] Scoring : Anomaly Score (intensité) + Anomaly Score Index (intensité × durée)
- [ ] Regroupement épisodes
- [ ] Hystérésis + seuil ouverture/fermeture
- [ ] Cooldown + durée minimale + anti-duplication alarmes
- [ ] Save anomaly scores vers TB (désactivé par défaut)
- [ ] Création alarmes TB (désactivé par défaut, B4)

**Livrables :** module anomalies, DAG scan horaire + rétroactif, tests PyOD

---

## Phase 10 — États, transitions, visualisations

**Dépendances :** Phase 8
**Durée estimée :** 5 jours-homme

- [ ] Définition états nommés (seuils → nom/couleur/priorité)
- [ ] Conditions multiples (AND/OR, hystérésis)
- [ ] Calcul temps passé par état
- [ ] % disponibilité/activité/arrêt
- [ ] Transitions visualisées (timeline)
- [ ] Analyse par heure/jour/sem/calendrier
- [ ] Comparaison inter-devices états

**Livrables :** module states, visualisations timeline, tests transitions

---

## Phases 11 à 15 (optionnel / posteriori)

| Phase | Description | Durée |
|---|---|---|
| 11 | Airflow 13 DAGs minimaux | 8 jh |
| 12 | UI React Metric Explorer + 21 visualisations ECharts | 18 jh |
| 13 | Provisionnement Grafana dashboards | 6 jh |
| 14 | Tests unitaires + intégration + E2E | 10 jh |
| 15 | Documentation, runbooks, sauvegardes, passage prod | 6 jh |

---

## Estimation totale

| Jusqu'à | Durée |
|---|---|
| Phase 5 (MVP prévision) | ~30 jh |
| Phase 10 (states + viz de base) | ~56 jh |
| Phases 0→15 (complet) | ~120 jh |

---

## Points d'arrêt Phase 2 (post-corrections)

- [ ] Création rôle `trendx_ro` sur PostgreSQL ThingsBoard (B1)
- [ ] Création base `trendx` + schémas `trendx_catalog`, `trendx_analytics` (Correction A)
- [ ] Rattachement réseau `mobili_dahsboard_default`
- [ ] Validation writeback device test `ALG16025001` (B3) — dry-run uniquement
- [ ] Vérification `.env` permissions 600
- [ ] Vérification espace disque avant toute migration (Correction B)
- [ ] Arrêt automatique ingestion sous `TRENDX_DISK_MIN_FREE_GB=50` implémenté
- [ ] Runbook `docs/runbooks/writeback-rollback.md` validé (Q5)
- [ ] Partition mensuelle +3 mois pré-créée
- [ ] Schéma toujours qualifié (`trendx_analytics.ts_kv`) dans toutes les requêtes
- [ ] APScheduler configuré (Correction C)
- [ ] cpus/mem_limit/pids_limit + logging sur chaque service

---

## Addendum de réconciliation — 2026-08-13

> Document DOC-FIRST. Aucune modification fonctionnelle, migration, test ou CI n'accompagne cet addendum. Il réconcilie les sources de vérité avec l'état réel de `gitlab/master` (`a29e564`).

### État réel constaté

- **Phase 2 (socle infra, schémas, migrations 001→012)** : réalisée (commit `7214502` « phase2 » + corrections). Le découpage historique ci-dessus reste inchangé.
- **Modules qualité / preprocessing** (`quality`, `normalizer`, `resampling`, `segmentation`) : codés et couverts par des tests unitaires ; `quality.py` écrit dans la **seule** table `data_quality` (hypertable `002` + partitionnée `011` ; rétention `012`). `data_quality_report` / `data_quality_issue` ne sont PAS créés (le code skip ces écritures).
- **Modules ML** (`forecasting/*`, `anomalies/*`, `mlops`, `services/training.py`, `services/inference.py`, `services/alerting.py`) : codés et couverts par des tests unitaires, mais **non orchestrés** par le worker/scheduler (`services/tasks.py` ne contient aucune tâche ML) et sans test E2E/d'intégration pipeline.
- **Phase 3 (ingestion Canal 2 + recovery + scheduler)** : fusionnée (`ce0c42e` + `86caaa6`, MR !15). Fix migration 012 : `5ed244f` (MR !16).
- **B1 / B2 (Canal 2 read-only)** : résolus en Phase 3.

### Distinction préservée

```
Modules ML présents  ≠  Pipeline ML orchestré  ≠  E2E validé  ≠  Parité Trendz démontrée
```

Aucune fonctionnalité n'est revendiquée « terminée / parité Trendz » sans tests automatisés verts (AGENTS.md §10).

### Prochaine vague (non démarrée)

Orchestration du pipeline ML (Qualité → Preprocessing → Entraînement → Prévision → Anomalie) dans le worker/scheduler + validation bout-en-bout + couverture CI. Nécessite une validation séparée.
