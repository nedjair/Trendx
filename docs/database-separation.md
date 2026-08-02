# Séparation stricte des bases de données

**Version :** 1.1 — Phase 2 (corrections post-validation)  
**Date :** 2026-08-02  
**Règle d'or :** AGENTS.md §3  
**Correction A :** UNE SEULE BASE `trendx` avec schémas `trendx_catalog` et `trendx_analytics`. Pas de `trendx_airflow` ni `trendx_mlflow` en Phase 2.

---

## 1. Principe

La base ThingsBoard et la base Trendx sont deux bases PostgreSQL **distinctes** dans la même instance :

| Base | Schéma(s) | Rôle | Accès | Règles |
|---|---|---|---|---|
| `thingsboard` | public | Télémétrie ThingsBoard | Lecture seule via `trendx_ro` | Aucun INSERT/UPDATE/DELETE/ALTER/DROP/CREATE. Aucune migration. Aucun objet Trendx créé dedans. |
| `trendx` | `trendx_catalog` | Catalogue Trendx (métadonnées, modèles, tables Trendz) | `trendx_app` DML, `trendx_migration` DDL | Contient tables Trendz + tables propres Trendx |
| `trendx` | `trendx_analytics` | Analytique (partitionné déclaratif + BRIN + agrégats) | `trendx_app` DML, `trendx_migration` DDL | ts_kv, prédictions, anomalies, qualité |

**Correction A :** `trendx_catalog` et `trendx_analytics` sont des schémas dans la base unique `trendx`. Pas de base séparée `trendx_analytics`.

---

## 2. Interdictions formelles

### 2.1 Dans la base `thingsboard`

- ❌ Aucune table Trendx
- ❌ Aucune vue Trendx
- ❌ Aucune extension activée (timescaledb, postgres_fdw, dblink)
- ❌ Aucune modification de schéma
- ❌ Aucune jointure inter-bases
- ❌ Aucun dump/restore depuis cette base vers Trendx

### 2.2 Entre bases

- ❌ Aucune jointure inter-bases (y compris via FDW ou dblink)
- ❌ Aucune transaction touchant plusieurs bases
- ✅ Ingestion : lire `thingsboard.ts_kv` → écrire dans `trendx_analytics.ts_kv` (deux bases distinctes, ingestion séquentielle)

---

## 3. Rôles PostgreSQL (moindre privilège)

| Rôle | Bases | Schémas | Privilèges |
|---|---|---|---|
| `trendx_ro` | `thingsboard` | public | SELECT seulement. `default_transaction_read_only = on`. `statement_timeout = 60000`. `idle_in_transaction_session_timeout = 30000`. `CONNECTION LIMIT 5`. |
| `trendx_migration` | `trendx` | `trendx_catalog`, `trendx_analytics` | CONNECT + USAGE, CREATE sur schémas. Peut créer/modifier le schéma. |
| `trendx_app` | `trendx` | `trendx_catalog`, `trendx_analytics` | CONNECT + USAGE + SELECT/INSERT/UPDATE/DML. `REVOKE CREATE ON SCHEMA`. |
| `trendx_grafana` | `trendx` | `trendx_analytics` | CONNECT + USAGE + SELECT seulement. |

**Correction A :** Pas de rôle `trendx_airflow`, `trendx_mlflow` en Phase 2.

---

## 4. Pools de connexion

| Pool | Rôle | Base | Schéma | Nombre connexions max |
|---|---|---|---|---|
| Pool RO | Lecture ThingsBoard | `thingsboard` | public | 5 |
| Pool migration | Migrations DDL | `trendx` | `trendx_catalog`, `trendx_analytics` | 1 |
| Pool app | Lectures/écritures Trendx | `trendx` | `trendx_catalog`, `trendx_analytics` | 10 |
| Pool Grafana | Dashboards | `trendx` | `trendx_analytics` | 2 |

**Règle :** Un pool = un rôle = une base. Pas de partage de connexion entre bases. Schéma toujours qualifié (`trendx_analytics.ts_kv`).

---

## 5. Stratégie d'ingestion (Canal 1 vs Canal 2)

### 5.1 Canal 1 : API ThingsBoard REST

- **Usage :** topologie, métadonnées, writeback, alarmes
- **Accès :** `https://10.0.0.1:8081/api/...`
- **Auth :** JWT avec renouvellement automatique
- **Débit :** limité par TB, pas pour la télémétrie massive

### 5.2 Canal 2 : PostgreSQL ThingsBoard (lecture seule)

- **Usage :** télémétrie historique massive
- **Accès :** `mobili_dahsboard-postgres-1:5432/thingsboard`
- **Rôle :** `trendx_ro`
- **Fallback :** si Canal 2 indisponible, basculer automatiquement sur Canal 1 API
- **Protection :** `statement_timeout = 60000`, `default_transaction_read_only = on`

### 5.3 Ingestion Trendx

```
Canal 2 (SQL RO) ou Canal 1 (API)
        │
        ▼
  checkpoint (dernier ts ingéré)
        │
        ▼
  préprocessing (NaN, doublons, domaines)
        │
        ▼
  trendx_analytics.ts_kv (partitionné déclaratif)
```

---

## 6. Sauvegardes

| Base | Schéma | Fréquence | Rétention | Méthode |
|---|---|---|---|---|
| `thingsboard` | public | Non modifiable | — | Gérée par ThingsBoard |
| `trendx` | `trendx_catalog` + `trendx_analytics` | Quotidienne | 180 jours (brut) + 2 ans (horaire) + 5 ans (jour/semaine) | `pg_dump` |

**Règle :** Les sauvegardes Trendx ne doivent jamais écraser les sauvegardes ThingsBoard.  
**Procédure :** Sauvegarde testée hors pointe, avec vérification de la restauration.

---

## 7. Vérification de séparation

Script de vérification automatisée (à exécuter après chaque déploiement) :

```sql
-- 1. Vérifier qu'aucune table Trendx n'existe dans thingsboard
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_catalog = 'thingsboard'
  AND table_name NOT IN (
    'device','asset','customer','device_profile','entity_view',
    'entity_alarm','attribute_kv','dashboard','widget_type',
    'widgets_bundle','widgets_bundle_widget','tenant','tb_user',
    'relation','key_dictionary','ts_kv','ts_kv_latest',
    'calculated_field','calculated_field_link','cf_debug_event',
    'ai_model','admin_settings','audit_log','component_descriptor',
    'customer','device_credentials','domain','edge','error_event',
    'event','lc_event','mobile_app','mobile_app_bundle',
    'notification','notification_request','notification_rule',
    'notification_target','notification_template','oauth2_client',
    'ota_package','queue','queue_stats','resource','rpc',
    'rule_chain','rule_node','rule_node_state','stats_event',
    'tb_schema_settings','user_auth_settings','user_credentials',
    'user_settings','widget_type_info_view','tenant_profile',
    'api_usage_state','alarm','alarm_comment','alarm_info',
    'alarm_types','api_key','device_info_active_attribute_view',
    'device_info_active_ts_view','device_info_view',
    'edge_active_attribute_view','qr_code_settings',
    'domain_oauth2_client','mobile_app_bundle_oauth2_client',
    'ts_kv_indefinite'
  );

-- 2. Vérifier que trendx_ro ne peut pas écrire
SET ROLE trendx_ro;
INSERT INTO thingsboard.device (id) VALUES (gen_random_uuid()); -- doit ÉCHOUER
RESET ROLE;

-- 3. Vérifier les schémas Trendx dans trendx
SELECT schema_name FROM information_schema.schemata
WHERE catalog_name = 'trendx'
  AND schema_name IN ('trendx_catalog', 'trendx_analytics');

-- 4. Vérifier qu'aucun objet Trendx n'existe dans thingsboard
SELECT count(*) FROM information_schema.tables
WHERE table_catalog = 'thingsboard'
  AND table_schema = 'public'
  AND table_name LIKE 'trendx_%' OR table_name LIKE 'business_%';
```
