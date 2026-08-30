-- ============================================================================
-- Trendx Catalogue — Relocalisation du catalogue dans le schéma trendx_catalog
-- Database : trendx
-- Migrations concernées : 001 (tables du catalogue native Trendz)
-- Type : forward-only, idempotent. NE JAMAIS DETRUIRE DE DONNÉES.
-- ----------------------------------------------------------------------------
-- Contexte (B7 / open-questions) :
--   La migration 001 créait autrefois toutes les tables du catalogue dans le
--   schéma `public`. Le schéma `trendx_catalog` n'était jamais créé par la
--   chaîne de migrations (uniquement via un bootstrap CI ad hoc), ce qui
--   bloquait 005/006/007 (schéma introuvable) et laissait 008 incohérent
--   (public.prediction_model vs trendx_catalog.prediction_model).
--
--   La migration 001 (révisée) crée désormais `trendx_catalog` et y place ses
--   objets. Cette migration 003 relocalise, sur une BASE EXISTANTE déjà
--   migrée (état historique : catalogue en `public`), les 58 tables du
--   catalogue + la fonction + le CHECK associé vers `trendx_catalog`, de façon
--   idempotente et sans perte de données (ALTER TABLE ... SET SCHEMA déplace
--   les données, index, contraintes et FK intra-ensemble).
--
-- Sur une base VIERGE (001 déjà cible trendx_catalog), 003 est un no-op sûr :
--   aucune table n'est dans `public`, la fonction est déjà dans
--   `trendx_catalog`, le CHECK réécrit reste identique.
--
-- Décision opérateur (2026-08-27) : la fonction
--   is_cached_telemetry_timestamps_do_not_intersect est DÉPLACÉE dans
--   trendx_catalog (Bounded Context propre) ; son CHECK est réécrit pour
--   référencer la fonction qualifiée trendx_catalog.* afin d'être robuste au
--   pg_dump/restore (la contrainte est stockée par OID, mais le texte doit
--   rester resolvable).
-- ============================================================================

-- 1) Schéma cible (idempotent ; 001 le crée aussi, doublon sans effet).
CREATE SCHEMA IF NOT EXISTS trendx_catalog;

-- 2) Relocalisation des 58 tables du catalogue de public -> trendx_catalog.
--    SET SCHEMA déplace tables, données, index, PK, UNIQUE, FK et CHECK
--    (les 17 FK sont toutes intra-ensemble catalogue, donc préservées).
--    Gardé par EXISTS : si la table n'est plus dans public (déjà migrée),
--    l'instruction est ignorée -> idempotent et réexécutable.
DO $$
DECLARE
    t text;
    catalog_tables text[] := ARRAY[
        'agent_ai',
        'anomaly',
        'anomaly_model_task_data',
        'api_key',
        'business_entity',
        'business_entity_field',
        'business_entity_field_metadata',
        'business_entity_metadata',
        'cached_telemetry',
        'cached_telemetry_point',
        'calculation_field',
        'calculation_field_task_data',
        'cluster_example',
        'cluster_info',
        'cluster_model',
        'custom_prediction_model',
        'custom_prompt',
        'custom_prompt_metadata',
        'custom_view_settings',
        'dataset_config',
        'datasource',
        'domain_tenant_pair',
        'latest_telemetry',
        'licence_data',
        'llm_config',
        'llm_settings',
        'llm_settings_chat_type_link',
        'manual_dataset',
        'metric_definition',
        'metric_definition_metadata',
        'metric_exploration',
        'ml_properties',
        'prediction_model',
        'prediction_model_last_item_point',
        'prediction_model_task_data',
        'relation',
        'scored_point_anomaly',
        'scored_point_centroid',
        'scored_point_cluster',
        'scored_point_histogram',
        'segment_data',
        'trendz_system_property',
        'trendz_task',
        'trendz_task_execution',
        'trendz_task_execution_progress_step',
        'trendz_task_execution_request',
        'trendz_task_execution_state_record',
        'trendz_task_scheduling_state_record',
        'trendz_task_sequence',
        'trendz_task_sequence_item',
        'user_metadata',
        'user_record',
        'view_assistance_chat',
        'view_assistance_chat_message',
        'view_assistance_token_usage',
        'view_collection',
        'view_config',
        'view_field'
    ];
BEGIN
    FOREACH t IN ARRAY catalog_tables LOOP
        IF EXISTS (
            SELECT 1 FROM pg_tables
            WHERE schemaname = 'public' AND tablename = t
        ) THEN
            EXECUTE format('ALTER TABLE public.%I SET SCHEMA trendx_catalog', t);
        END IF;
    END LOOP;
END $$;

-- 3) Relocalisation de la fonction métier dans trendx_catalog (Bounded Context).
--    Le CHECK de cached_telemetry en dépend (par OID) : déplacer la fonction
--    est autorisé et ne casse pas la contrainte. Gardé par EXISTS.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
          AND p.proname = 'is_cached_telemetry_timestamps_do_not_intersect'
    ) THEN
        EXECUTE
            'ALTER FUNCTION public.is_cached_telemetry_timestamps_do_not_intersect('
            || 'uuid, uuid, character varying, bigint, bigint, character varying) '
            || 'SET SCHEMA trendx_catalog';
    END IF;
END $$;

-- 4) Réécriture du CHECK pour référencer la fonction qualifiée trendx_catalog.*.
--    Idempotent (DROP CONSTRAINT IF EXISTS + ADD CONSTRAINT). La fonction est
--    désormais dans trendx_catalog (cf. étape 3) donc la référence résout.
--    Recrée la contrainte à l'identique ; les lignes existantes la satisfont
--    déjà (sinon l'ADD échouerait volontairement, signalant une corruption).
ALTER TABLE trendx_catalog.cached_telemetry
    DROP CONSTRAINT IF EXISTS is_timestamps_do_not_intersect_constraint;

ALTER TABLE trendx_catalog.cached_telemetry
    ADD CONSTRAINT is_timestamps_do_not_intersect_constraint
    CHECK (trendx_catalog.is_cached_telemetry_timestamps_do_not_intersect(
        business_entity_field_id,
        item_id,
        date_aggregation_type,
        end_ts,
        start_ts,
        'function'::character varying
    ));

-- 5) Droits : la fonction appartient au DDL de migration et est exécutable par
--    trendx_app (appelée par le CHECK au moment des écritures). Idempotent.
ALTER FUNCTION trendx_catalog.is_cached_telemetry_timestamps_do_not_intersect(
    uuid, uuid, character varying, bigint, bigint, character varying
) OWNER TO trendx_migration;

GRANT EXECUTE ON FUNCTION trendx_catalog.is_cached_telemetry_timestamps_do_not_intersect(
    uuid, uuid, character varying, bigint, bigint, character varying
) TO trendx_app;
