# Mapping schéma Trendz → Trendx

**Version :** 1.1 — Phase 2 (corrections post-validation)  
**Date :** 2026-08-02  
**Source de vérité :** observation directe de la base `thingsboard` sur 10.0.0.1  
**Contexte :** Aucune base Trendz présente sur l'hôte → clause de repli activée.  
**Correction A :** `trendx_catalog` et `trendx_analytics` sont des schémas dans la base unique `trendx`.

**Schéma proposé :** `docs/proposed-schema.sql` est un dump pg_dump généré, NON NORMATIF.  
Il sert de référence provisoire en l'absence de dump certifié d'une instance Trendz réelle.  
Toute divergence avec un schéma Trendz existant sera traitée en priorité par `pg_dump --schema-only` si une base Trendz est découverte ultérieurement.

---

## 1. Contexte

La base ThingsBoard sur 10.0.0.1 ne contient **aucune table Trendz native** (hormis `cf_debug_event` qui est une table ThingsBoard CE standard).  
Il n'existe donc pas de schéma Trendz à reproduire à l'identique.

**Clause de repli :** En l'absence de schéma Trendz local, Trendx utilise des noms provisoires internes (`trendx_catalog`, `trendx_analytics`) dans la base `trendx` et prévoit une couche de vues de compatibilité. Aucun nom n'est inventé prétendument « Trendz ».

---

## 2. Schéma ThingsBoard observé (source de vérité)

### 2.1 Table `ts_kv` (télémétrie temps réel)

```sql
CREATE TABLE public.ts_kv (
    entity_id uuid NOT NULL,
    key       integer NOT NULL,        -- FK → key_dictionary.key_id
    ts        bigint  NOT NULL,        -- millisecondes UTC
    bool_v    boolean,
    str_v     varchar(10 000 000),
    long_v    bigint,
    dbl_v     double precision,
    json_v    json
);

-- Partitionnement : RANGE(ts) par mois
-- 14 partitions actives
-- PK : (entity_id, key, ts) — index btree unique
```

**Index existant :**
- `ts_kv_pkey` — btree sur `(entity_id, key, ts)` (UNIQUE)

### 2.2 Table `key_dictionary`

```sql
CREATE TABLE public.key_dictionary (
    key    varchar(255) NOT NULL,
    key_id integer      NOT NULL DEFAULT nextval('key_dictionary_key_id_seq'::regclass)
);

-- PK : key_dictionary_id_pkey (key)
-- UK  : key_dictionary_key_id_key (key_id)
```

### 2.3 Table `relation`

```sql
CREATE TABLE public.relation (
    from_id             uuid          NOT NULL,
    from_type           varchar(255)  NOT NULL,
    to_id               uuid          NOT NULL,
    to_type             varchar(255)  NOT NULL,
    relation_type_group varchar(255)  NOT NULL,
    relation_type       varchar(255)  NOT NULL,
    additional_info     varchar,
    version             bigint        DEFAULT 0
);

-- PK : (from_id, from_type, relation_type_group, relation_type, to_id, to_type)
-- Index : idx_relation_from_id (relation_type_group, from_type, from_id)
-- Index : idx_relation_to_id   (relation_type_group, to_type, to_id)
```

### 2.4 Tables complémentaires TB (catalogue)

| Table | Rôle Trendx |
|---|---|
| `device` | Entité Device |
| `asset` | Entité Asset |
| `customer` | Entité Customer |
| `device_profile` | Profil de device |
| `entity_view` | Vue d'entité |
| `entity_alarm` | Alarme |
| `calculated_field` | Champ calculé TB (29 existants) |
| `calculated_field_link` | Lien CF |
| `cf_debug_event` | Debug CF |
| `ai_model` | Modèle IA TB (1 existant) |
| `attribute_kv` | Attributs clé-valeur |
| `dashboard` | Dashboard TB |
| `widgets_bundle` / `widget_type` | Widgets TB |
| `tenant` | Tenant |
| `tb_user` | Utilisateur |

---

## 3. Structure Trendx à reproduire (clause de repli)

**B5 — TimescaleDB REFUSÉ :** Aucune extension. Utiliser partitionnement déclaratif par temps + index BRIN + tables d'agrégats matérialisés.

### 3.1 Schéma `trendx_catalog` dans base `trendx`

Tables héritées de Trendz (source : docs/trendz-parity-matrix.md + docs/architecture.md) :

| Table | Rôle Trendx | Colonnes clés |
|---|---|---|
| `business_entity` | Aggrégateur logique (Site, Client, Flotte…) | id, tenant_id, customer_id, entity_type, name |
| `business_entity_field` | Champs / relations BE → Devices/Assets | id, be_id, name, type, config_json |
| `business_entity_metadata` | Liaison item_id / customer_id | id, be_id, item_id, item_type, customer_id |
| `entity_relation` | Relations entre entités | id, from_id, from_type, to_id, to_type, relation_type |
| `entity_group` | Groupe d'entités | id, tenant_id, name |
| `entity_group_member` | Membre groupe | id, group_id, entity_id, entity_type |
| `metric_definition` | Catalogue métriques | id, tenant_id, name, unit, min, max, how_to_calculate |
| `metric_exploration` | Exploration Metric Explorer | id, tenant_id, customer_id, item_id, be_id, metric_id |
| `calculated_field` | Arbre AST champs calculés | id, entity_id, type, configuration JSON |
| `calculated_field_execution` | Exécution CF | id, cf_id, ts, result_json |
| `data_source` | Source de données | id, name, type, config_json |
| `prediction_model` | Config modèle | id, tenant_id, name, type, model_uri, configuration JSON |
| `prediction_model_task_data` | Planification entraînement | id, model_id, refresh_time_unit, partial_fit |
| `prediction_model_last_item_point` | Checkpoint last ts | id, model_id, item_id, ts |
| `anomaly` | Épisodes anomalies | id, detector_id, start_ts, end_ts, score, score_index |
| `anomaly_model_task_data` | Planification anomalie | id, detector_id, enabled_save_to_tb, enabled_alarm_creation |
| `scored_point_anomaly` | Points anomalies | id, detector_id, entity_id, ts, score |
| `cluster_info` | Info cluster | id, detector_id, cluster_id, center_json |
| `cluster_model` | Modèle clustering | id, detector_id, type, config_json |
| `view_config` | Vue / rapport | id, tenant_id, name, config_json, enable_persisted_cache |
| `view_field` | Champ visuel agrégé | id, view_id, aggregation_type, date_grouping, calc_function |
| `dataset_config` | Jeu de données | id, view_id, config_json |
| `trendz_task` | Tâche planifiée interne | id, type, config_json, status |
| `trendz_task_execution` | Exécution tâche | id, task_id, started, finished, status |
| `ingestion_checkpoints` | Checkpoint ingestion (propre Trendx) | pipeline, entity_id, metric_key, watermark_ts, records_processed, last_batch_id, source, updated_at (schéma `trendx_catalog`, migration 005) |
| `topology_entities`, `topology_relations`, `sync_metadata` | Découverte topologie (propre Trendx) | id, entity_id, relation_type, ... (schéma `trendx_catalog`, migration 007) |
| `audit_log` | Journal audit | id, actor, action, entity, ts |
| `app_secret` | Secret application | id, name, value_hash |
| `app_setting` | Paramètre application | id, key, value_json |
| `writeback_log` | Journal writeback | id, job_id, device_id, entity_id, key, ts_start, ts_end, nb_points, written_at, operator, dry_run |
| `prediction_model_status_history` | Historique Trendx des transitions de statut modèle | id, prediction_model_id, business_entity_id, metric_key, previous_status, new_status, changed_ts |

> **Migration 008** (`migrations/008_add_model_uri.sql`) ajoute la colonne
> `model_uri` (TEXT, nullable) à `trendx_catalog.prediction_model` afin de
> persister l'URI MLflow des modèles enregistrés via `ModelRegistry.register()`.
> Cette migration **n'a pas été appliquée ni vérifiée sur une base réelle** dans
> cet environnement (contrainte identique aux migrations précédentes) ; elle est
> idempotente (`IF NOT EXISTS`) et doit être exécutée par le rôle `trendx_migration`.

### 3.2 Schéma `trendx_analytics` dans base `trendx`

| Table | Rôle | Colonnes clés |
|---|---|---|
| `ts_kv` | Télémétrie ingérée (partitionné déclaratif) | entity_id, metric_id, ts, value_dbl, value_str, value_long, value_bool, value_json |
| `ts_kv_latest` | Dernière valeur par entité/métrique | entity_id, metric_id, ts, value_* |
| `predictions` | Valeurs prédites | entity_id, metric_id, ts, model_id, value_dbl, lower, upper |
| `anomaly_scores` | Scores anomalies | entity_id, metric_id, ts, score, score_index, detector_id |
| `data_quality` | Qualité données | entity_id, metric_id, ts, nan_ratio, gap_ratio, outlier_ratio |
| `ml_metrics` | Métriques ML | run_id, metric_name, metric_value, ts |

> **Réconciliation schéma (2026-08-27, RÉSOLU 2026-08-27 par 001 + 003) — état réel vs migrations :**
> - `migrations/001_trendz_native_schema.sql` crée le schéma `trendx_catalog` (idempotent) et y place les 58 tables du catalogue natif Trendz (`business_entity`, `prediction_model`, `trendz_task`, `trendz_task_execution`, …), la fonction `is_cached_telemetry_timestamps_do_not_intersect` et son CHECK.
> - `migrations/003_relocate_catalog.sql` (forward-only, idempotent) relocalise les bases historiques déjà migrées (catalogue en `public`) vers `trendx_catalog` via `ALTER TABLE … SET SCHEMA` (données, index, PK, UNIQUE, FK et CHECK préservés) + déplacement de la fonction + réécriture du CHECK vers `trendx_catalog.*`. Aucune perte de données.
> - `migrations/005`, `006`, `007` créent `ingestion_checkpoints`, `prediction_model_status_history`, `topology_entities`/`topology_relations`, `sync_metadata` dans `trendx_catalog` (schéma désormais créé).
> - `migrations/009`, `011`, `012` créent le schéma `trendx_analytics` (`ts_kv`, `ts_kv_latest`, `predictions`, `anomaly_scores`, `data_quality`, `ml_metrics`).
>
> **État réel :** `trendx_catalog` (catalogue 001+003, + tables propres 005/006/007) + `trendx_analytics` (009/011/012) dans la base unique `trendx`. Plus aucune table catalogue en `public` (sauf extension `pgcrypto`).
> **Écart bloquant :** RÉSOLU. Aucune fonctionnalité ne doit être revendiquée « terminée » tant que la migration 003 n'est pas exécutée sur les bases historiques (AGENTS.md §10) — à planifier en maintenance.
> Note : `docs/proposed-schema.sql` utilise `public` (historique) ; il est désormais en retard vs `schema-mapping.md` (source de vérité) et doit être mis à jour si réutilisé.

---

## 4. Mapping des types ThingsBoard → Trendx

| Type TB | Colonne TB | Type Trendx proposé |
|---|---|---|
| UUID | entity_id | uuid |
| integer (key_id) | key | integer (FK métrique interne) |
| bigint ms | ts | timestamptz (UTC) |
| boolean | bool_v | boolean |
| varchar(10M) | str_v | text |
| bigint | long_v | bigint |
| double precision | dbl_v | double precision |
| json | json_v | jsonb |

---

## 5. Règles de reproduction

1. **Mêmes noms de tables et colonnes** que Trendz quand ils existent.
2. **Mêmes types, clés, index et contraintes** quand ils sont documentés.
3. **Tables propres Trendx** (checkpoints, journal d'erreurs, audit, writeback_log) s'ajoutent sans modifier les tables héritées.
4. **Aucun renommage** de table ou colonne ThingsBoard existant.
5. **Pas de TimescaleDB** (B5). Utiliser partitionnement déclaratif + index BRIN + tables d'agrégats matérialisés.
6. **Aucune base Trendz existante** : utiliser les noms consignés dans ce document + vues de compatibilité.
7. **Schéma toujours qualifié** : `trendx_analytics.ts_kv`, jamais `ts_kv` seul.

---

## 6. Limitations connues

- Aucune base Trendz présente sur 10.0.0.1 → clause de repli activée.
- Certaines tables Trendz (ex: `job`, `segment_data`, `cluster_centroid`) n'ont pas de source de vérité locale.
- Le mapping ci-dessus est basé sur la documentation publique Trendz + l'observation TB CE.
- Toute divergence avec un schéma Trendz existant ailleurs sera traitée en priorité par `pg_dump --schema-only` si une base Trendz est découverte ultérieurement.
