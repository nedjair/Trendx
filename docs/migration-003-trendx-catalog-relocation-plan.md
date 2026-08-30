# Plan de migration — Relocalisation du catalogue Trendz dans `trendx_catalog` (B7 / Option A)

- **ID de décision :** `open-questions.md` §1 blocage **B7**
- **Cible validée :** **Option A** — créer une migration dédiée `trendx_catalog` et relocaliser les tables concernées (décision opérateur 2026-08-27)
- **Statut de ce document :** **IMPLÉMENTÉ (2026-08-27)** — modifications appliquées : `migrations/001` (crée `trendx_catalog`, idempotent), `migrations/003_relocate_catalog.sql` (créé), `migrations/008` (→ `trendx_catalog.prediction_model`), tests `tests/integration/test_migration_001/003/008`, docs alignées (`architecture.md`, `database-separation.md`, `schema-mapping.md`, `Makefile`, `open-questions.md`). **Aucun SQL n'a été exécuté sur la base de production** ; l'exécution de `003` sur les bases historiques reste une étape de maintenance planifiée (jamais automatique). Validation locale réalisée sur PostgreSQL jetable (docker) : chaîne vierge 001→003→005→006→007→008 OK, scénario historique OK, idempotence OK, 58 tables + 17 FK + fonction + CHECK intègres, aucune perte de données.
- **Sources de vérité :**
  - `migrations/001_trendz_native_schema.sql` (DDL catalogue, ~58 tables en `public`)
  - `migrations/005,006,007,008,009,011,012_*.sql`
  - `docs/architecture.md:124`, `docs/database-separation.md:22`, `docs/schema-mapping.md:101-169` (écart déjà documenté)
  - `.gitlab-ci.yml:79-80,108,121-123,244-245` (application des migrations)

---

## 1. Décision enregistrée

Option A validée comme **cible architecturale**. Option B (garder `00x` dans `public` + préfixer `001`) rejetée : elle pérennise un mélange et introduit une convention de nommage artificielle.

L'implémentation concrète n'est **pas** démarrée : la présente section + §2-§10 constituent l'audit et le plan détaillé à valider.

---

## 2. Analyse de la cause racine (pourquoi 005/006/007 étaient bloquées)

1. **`trendx_catalog` n'est créé par aucune migration.** `migrations/001` place les tables catalogue dans `public`. `005/006/007` créent leurs objets dans `trendx_catalog` **sans** `CREATE SCHEMA`. En base vierge, elles échouent sauf schéma préexistant. Aujourd'hui le schéma n'existe que via un `CREATE SCHEMA IF NOT EXISTS trendx_catalog;` ad-hoc dans `.gitlab-ci.yml:79` et `:244` (et dans les tests). Ce hors-chain crée une dépendance cachée : tout déploiement qui n'exécute pas ce pré-script CASCADE.
2. **Incohérence de Bounded Context.** Le code et les docs attendent le catalogue en `trendx_catalog` :
   - `Makefile:265` et `scripts/restore-test.sh:153` interrogent `trendx_catalog.business_entity`.
   - `docs/schema-mapping.md:144` attend `model_uri` sur `trendx_catalog.prediction_model`.
   - `.gitlab-ci.yml:104` note explicitement `trendx_catalog.prediction_model` « while the table lives in public ».
   - `migrations/008` (ligne 21) fait `ALTER TABLE public.prediction_model` → référence cassée après la remise en cohérence.
3. **`migrations/001` est NON-idempotente** (`CREATE TABLE public.*` sans `IF NOT EXISTS` ; `.gitlab-ci.yml:286` le confirme). Toute retarget doit donc être doublée d'une migration de relocalisation distincte et idempotente pour les bases **déjà** peuplées.

La migration dédiée (§5) corrige les deux : elle crée le schéma dans la chaîne ET relocalise les objets existants.

---

## 3. Audit technique — inventaire des objets à relocaliser (depuis `001`)

### 3.1 Tables `public.*` → `trendx_catalog.*` (58 tables)

```
agent_ai, anomaly, anomaly_model_task_data, api_key, business_entity,
business_entity_field, business_entity_field_metadata, business_entity_metadata,
cached_telemetry, cached_telemetry_point, calculation_field, calculation_field_task_data,
cluster_example, cluster_info, cluster_model, custom_prediction_model, custom_prompt,
custom_prompt_metadata, custom_view_settings, dataset_config, datasource, domain_tenant_pair,
latest_telemetry, licence_data, llm_config, llm_settings, llm_settings_chat_type_link,
manual_dataset, metric_definition, metric_definition_metadata, metric_exploration, ml_properties,
prediction_model, prediction_model_last_item_point, prediction_model_task_data, relation,
scored_point_anomaly, scored_point_centroid, scored_point_cluster, scored_point_histogram,
segment_data, trendz_system_property, trendz_task, trendz_task_execution,
trendz_task_execution_progress_step, trendz_task_execution_request,
trendz_task_execution_state_record, trendz_task_scheduling_state_record,
trendz_task_sequence, trendz_task_sequence_item, user_metadata, user_record,
view_assistance_chat, view_assistance_chat_message, view_assistance_token_usage,
view_collection, view_config, view_field
```
(1 PK + N index par table : déplacés automatiquement avec `ALTER TABLE … SET SCHEMA`.)

### 3.2 Contraintes présentes dans `001`

- **PK** : 1 par table (`*_pkey`), toutes en `public`.
- **UNIQUE** : `api_key(tenant_id)`, `api_key(token)`, `business_entity_field_metadata(unique)`, `business_entity_metadata(unique)`, `calculation_field(tenant_id,name)`, `cached_telemetry(...)`, `custom_view_settings(domain)`, `domain_tenant_pair(tenant_id,domain)`, `latest_telemetry(...)`, `licence_data(tenant_id)`, `llm_settings(tenant_id)`, `metric_exploration(raw_unique)`, `trendz_system_property(property_key)`, `trendz_task(reference_type,reference_key)`, `trendz_task_execution_request(task_id,execution_id)`, `trendz_task_execution_state_record(execution_id)`, `trendz_task_scheduling_state_record(task_id)`, `user_metadata(unique)`, `user_record(tenant_id,customer_id,user_id)`, `view_assistance_chat(pkey)`, `view_assistance_token_usage(pkey)`. Déplacées avec les tables.
- **CHECK** : `cached_telemetry.is_timestamps_do_not_intersect_constraint` → référence `public.is_cached_telemetry_timestamps_do_not_intersect` (voir §3.4).
- **FK (17, toutes INTRA-ensemble `001`)** — donc conservées intactes par `SET SCHEMA` (la table référencée fait partie du même lot) :
  - `business_entity_field.business_entity_id → business_entity`
  - `cluster_example.cluster_info_id → cluster_info`
  - `cluster_info.cluster_model_id → cluster_model`
  - `cluster_model.dataset_config_id → dataset_config`
  - `cluster_model.properties_id → ml_properties`
  - `trendz_task_sequence_item.fk_sequence → trendz_task_sequence`
  - `trendz_task_sequence_item.fk_task_reference → trendz_task`
  - `llm_settings_chat_type_link.llm_config_id → llm_config`
  - `llm_settings_chat_type_link.llm_setting_id → llm_settings`
  - `llm_settings.default_llm_config_id → llm_config` (ON DELETE SET NULL)
  - `relation.business_entity_id → business_entity`
  - `relation.related_entity_id → business_entity`
  - `trendz_task_execution_progress_step.execution_id → trendz_task_execution`
  - `trendz_task_execution_progress_step.parent_step_id → trendz_task_execution_progress_step`
  - `trendz_task_execution_request.task_id → trendz_task`
  - `trendz_task_execution.task_id → trendz_task`
  - `view_assistance_chat_message.chat_id → view_assistance_chat`

### 3.3 Séquences, vues, triggers, materialized views

- **Séquences** : aucune dans `001` (toutes les PK sont `uuid` générées hors DB ; aucun `serial`/`nextval`/`SEQUENCE`). Rien à relocaliser sur ce point.
- **Vues / materialized views** : aucune dans `001`.
- **Triggers** : aucun dans `001`.

### 3.4 Fonction + CHECK (point de vigilance unique)

- Fonction `public.is_cached_telemetry_timestamps_do_not_intersect(...)` (corps PL/pgSQL référence `cached_telemetry` **non qualifié** + `starts_with` built-in).
- CHECK `cached_telemetry.is_timestamps_do_not_intersect_constraint` l'appelle en `public.…`.
- **Décision proposée :** recréer la fonction dans `trendx_catalog` avec `SET search_path = trendx_catalog, public`, et recréer le CHECK en `trendx_catalog.is_cached_telemetry_timestamps_do_not_intersect`. Alternative (plus conservative) : laisser la fonction en `public` (le CHECK qualifié `public.…` reste valide ; le corps résout `cached_telemetry` via le `search_path` de session qui inclut `trendx_catalog`). **Recommandation : déplacer la fonction** pour respecter le Bounded Context ; garder `public` seulement si validation contraire.

### 3.5 Extension

- `pgcrypto` créée `WITH SCHEMA public` dans `001`. **Ne pas déplacer** (extension partagée, utilisée par `public` et le reste). Reste en `public`.

### 3.6 Références SQL / code à vérifier (hors `001`)

- `migrations/008_add_model_uri.sql` : `ALTER TABLE public.prediction_model` → **doit devenir `trendx_catalog.prediction_model`** (§5.3).
- `Makefile:167` : garde d'idempotence `SELECT … table_schema='public' AND table_name='business_entity'` → **doit devenir `trendx_catalog`** (sinon 001 rejoué en boucle).
- `tests/integration/test_migration_001_native_schema.py:94` : assertion `table_schema = 'public'` → **doit devenir `trendx_catalog`**.
- `.gitlab-ci.yml:79,244` : `CREATE SCHEMA IF NOT EXISTS trendx_catalog` ad-hoc → devient **redondant mais inoffensif** (à conserver en belt-and-suspenders ou à retirer).
- Code applicatif : vérifier qu'aucune requête ne résout `business_entity`/`relation`/`prediction_model`/… **sans** `search_path` incluant `trendx_catalog` (le `Makefile` et `restore-test.sh` les qualifient déjà → forte probabilité que le code qualifie aussi ; à confirmer par grep `FROM business_entity`/`JOIN relation`/`prediction_model` avant exécution).

---

## 4. Dépendances validées

| Type | Constat | Impact relocalisation |
|---|---|---|
| PK / UNIQUE | intra `001` | déplacés avec `SET SCHEMA` |
| FK (17) | intra `001` | préservées (référencé dans le lot) |
| Index | liés aux tables | déplacés avec `SET SCHEMA` |
| Séquences | aucune | — |
| Vues / MV | aucune | — |
| Triggers | aucun | — |
| Fonction + CHECK | 1, référence croisée | voir §3.4 |
| Extension `pgcrypto` | `public` | reste `public` |

---

## 5. Conception recommandée (Option A)

### 5.1 `migrations/001_trendz_native_schema.sql` — retarget + idempotence (base vierge)

- Ajouter en tête (après `SET` pg_dump) : `CREATE SCHEMA IF NOT EXISTS trendx_catalog;`
- Remplacer `public.` → `trendx_catalog.` sur **toutes** les `CREATE TABLE`, `CREATE INDEX`, `ALTER TABLE … ADD CONSTRAINT`, la fonction et le CHECK.
- Idempotence : `CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`. Pour les contraintes (PG n'a pas `ADD CONSTRAINT IF NOT EXISTS`), utiliser des `DO $$` guards ou accepter l'application unique sur schéma vierge (comportement actuel de CI). **Recommandation :** `IF NOT EXISTS` + guards `DO` pour robustesse.
- `pgcrypto` : reste `public`.

### 5.2 `migrations/003_relocate_catalog.sql` — NOUVELLE migration (bases existantes)

- Emplacement : **slot `003` libre** (`.gitlab-ci.yml:108` « intentionally absent » ; le glob `:121` l'inclut déjà → sera appliquée dans la chaîne `001→…→006→009…` **avant** `005/006/007/008`).
- `CREATE SCHEMA IF NOT EXISTS trendx_catalog;`
- Boucle `DO $$` sur la liste §3.1 : pour chaque `t`, si `public.t` existe ET `trendx_catalog.t` n'existe pas → `ALTER TABLE public.t SET SCHEMA trendx_catalog`. Sinon no-op (idempotent, forward-only).
- Fonction + CHECK : selon §3.4 (recréer en `trendx_catalog` si décidé, garder `public` sinon), avec guards d'idempotence.
- **Rien ne déplace `trendx_catalog → public`** (forward-only).

### 5.3 `migrations/008_add_model_uri.sql` — retarget

- `ALTER TABLE public.prediction_model` → `ALTER TABLE trendx_catalog.prediction_model` (garder `ADD COLUMN IF NOT EXISTS`, idempotent). Résout l'incohérence `.gitlab-ci.yml:104`.

### 5.4 Ordre d'application (cohérent avec CI)

`000 → 001 → 002 → 003 → 004 → 005 → 006 → 007 → 008 → 009 → 010 → 011 → 012`
- Base vierge : `001` crée en `trendx_catalog`, `003` no-op. `005/006/007/008` trouvent le catalogue au bon endroit.
- Base existante : `001` déjà en `public` (trackée appliquée), `003` relocalise, puis `005/006/007/008` OK.

### 5.5 Filet de sécurité `search_path`

- `ALTER ROLE trendx_app SET search_path = trendx_catalog, trendx_analytics, public;` (et `trendx_ro`, `trendx_migration` cohérents) pour couvrir toute requête non qualifiée résiduelle.

---

## 6. Esquisse DDL proposée (À VALIDER — non exécutée)

```sql
-- migrations/003_relocate_catalog.sql
-- Relocalise le catalogue Trendz (migration 001) de public -> trendx_catalog.
-- Forward-only, idempotent. Ne jamais déplacer en sens inverse.
\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS trendx_catalog;

DO $$
DECLARE
  t text;
  tables text[] := ARRAY[
    'agent_ai','anomaly','anomaly_model_task_data','api_key','business_entity',
    'business_entity_field','business_entity_field_metadata','business_entity_metadata',
    'cached_telemetry','cached_telemetry_point','calculation_field','calculation_field_task_data',
    'cluster_example','cluster_info','cluster_model','custom_prediction_model','custom_prompt',
    'custom_prompt_metadata','custom_view_settings','dataset_config','datasource','domain_tenant_pair',
    'latest_telemetry','licence_data','llm_config','llm_settings','llm_settings_chat_type_link',
    'manual_dataset','metric_definition','metric_definition_metadata','metric_exploration','ml_properties',
    'prediction_model','prediction_model_last_item_point','prediction_model_task_data','relation',
    'scored_point_anomaly','scored_point_centroid','scored_point_cluster','scored_point_histogram',
    'segment_data','trendz_system_property','trendz_task','trendz_task_execution',
    'trendz_task_execution_progress_step','trendz_task_execution_request',
    'trendz_task_execution_state_record','trendz_task_scheduling_state_record',
    'trendz_task_sequence','trendz_task_sequence_item','user_metadata','user_record',
    'view_assistance_chat','view_assistance_chat_message','view_assistance_token_usage',
    'view_collection','view_config','view_field'
  ];
BEGIN
  FOREACH t IN ARRAY tables LOOP
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema='public' AND table_name=t)
       AND NOT EXISTS (SELECT 1 FROM information_schema.tables
                       WHERE table_schema='trendx_catalog' AND table_name=t)
    THEN
      EXECUTE format('ALTER TABLE public.%I SET SCHEMA trendx_catalog', t);
    END IF;
  END LOOP;
END $$;

-- Fonction + CHECK (si décision §3.4 = déplacer la fonction) :
--   DROP FUNCTION IF EXISTS public.is_cached_telemetry_timestamps_do_not_intersect(...) ;
--   CREATE FUNCTION trendx_catalog.is_cached_telemetry_timestamps_do_not_intersect(...)
--     RETURNS boolean LANGUAGE plpgsql SET search_path = trendx_catalog, public AS $$ … $$ ;
--   ALTER TABLE trendx_catalog.cached_telemetry
--     DROP CONSTRAINT IF EXISTS is_timestamps_do_not_intersect_constraint;
--   ALTER TABLE trendx_catalog.cached_telemetry
--     ADD CONSTRAINT is_timestamps_do_not_intersect_constraint
--       CHECK (trendx_catalog.is_cached_telemetry_timestamps_do_not_intersect(...));
-- (corps de fonction = copie exacte de migrations/001 lignes 44-62)
```

Modifications `001` (pattern) : `s/public\./trendx_catalog./g` sur tables/index/constraints/fonction/CHECK + ajout `CREATE SCHEMA IF NOT EXISTS trendx_catalog;` + `IF NOT EXISTS` sur `CREATE TABLE`/`CREATE INDEX`.
Modification `008` : `public.prediction_model` → `trendx_catalog.prediction_model`.

---

## 7. Idempotence & rollback

- **Idempotence** : `CREATE SCHEMA IF NOT EXISTS` + guards `EXISTS/NOT EXISTS` sur chaque `ALTER TABLE SET SCHEMA` + `IF NOT EXISTS` sur objets `001`/`008`. Ré-application = no-op.
- **Forward-only** : aucune étape ne ramène `trendx_catalog → public`.
- **Rollback (manuel, documenté, NON automatique)** : script `scripts/003_rollback.sql` (à rédiger) qui reverse `SET SCHEMA` + restaure fonction/CHECK en `public`, précédé d'un `pg_dump` de `trendx`. Jamais exécuté sans validation.

---

## 8. Tests PostgreSQL réels (fail-closed)

1. **Mettre à jour** `tests/integration/test_migration_001_native_schema.py:94` : `table_schema='public'` → `'trendx_catalog'` (001 retarget).
2. **Ajouter** `tests/integration/test_migration_003_relocate_catalog.py` (vrai PostgreSQL, `ON_ERROR_STOP`) :
   - appliquer `001` (→ `public`), puis `003`, puis Assert :
     - chaque table §3.1 existe dans `trendx_catalog` ;
     - aucune des tables §3.1 ne reste dans `public` (test négatif) ;
     - FK/CHECK valides (requête sur `cached_telemetry` n'échoue pas ; `information_schema.table_constraints` liste les 17 FK en `trendx_catalog`) ;
     - ré-application de `003` = no-op (idempotence).
3. **Étendre** `scripts/restore-test.sh` / `Makefile doctor` : déjà `trendx_catalog.business_entity` → reste vert ; ajouter assertion « 0 table catalogue dans `public` ».
4. **CI** : `test-migration-007` et `test-migration-008` continuent de fonctionner (`003` crée le schéma, `008` cible `trendx_catalog.prediction_model`).

---

## 9. Mises à jour documentaires (étape implémentation, après validation)

- `docs/architecture.md:124` : remplacer la note « écart bloquant … à corriger par une migration dédiée » par « résolu par `migrations/003_relocate_catalog.sql` + retarget `001` (B7/Option A) ».
- `docs/database-separation.md:22` : même résolution.
- `docs/schema-mapping.md:162-168` : marquer l'écart comme résolu ; noter que `proposed-schema.sql` reste `public` (non normatif, à ne pas prendre pour source).
- `docs/open-questions.md` §1 B7 : passer de « ❓ Nécessite décision » à « ✅ Option A validée — implémentée via `003` » (après exécution réussie).
- `Makefile:167` : garde `public.business_entity` → `trendx_catalog.business_entity`.
- `.gitlab-ci.yml:79,244` : conserver (inoffensif) ou retirer (schéma désormais créé par la chaîne).

---

## 10. Checklist de validation avant exécution (AGENTS.md § point d'arrêt)

- [ ] Opérateur valide ce plan (et le choix §3.4 fonction : déplacer vs garder `public`).
- [ ] Accès DB `trendx` (rôle `trendx_migration`) confirmé pour exécuter `003` (point d'arrêt : création/modif objet DB)
- [ ] `pg_dump` de `trendx` réalisé avant toute écriture (rollback §7)
- [ ] Espace disque vérifié (`TRENDX_DISK_MIN_FREE_GB`) — `ALTER TABLE SET SCHEMA` est métadonné, sans copie de données, empreinte négligeable
- [ ] Grep code applicatif confirmant qualification `trendx_catalog.*` (§3.6)
- [ ] `001`/`008` retarget + `003` créés, `make` lint/tests verts en CI locale avant prod

**Aucune écriture SQL n'est effectuée tant que la case 1 (validation du présent plan) n'est pas cochée.**
