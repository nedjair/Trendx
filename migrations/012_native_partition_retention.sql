-- ============================================================================
-- Trendx Analytics : native PostgreSQL partition retention (DROP PARTITION)
-- B5 Lot 2 — Étape 012
-- ----------------------------------------------------------------------------
-- Profil : PostgreSQL natif (B5) — TimescaleDB INTERDITE en profil minimal.
--   Cette migration ajoute UNIQUEMENT la maintenance de rétention native.
--   Elle ne touche AUCUNE structure TimescaleDB existante si celle-ci est
--   présente (guarde d'extension) : elle n'appelle aucune primitive de
--   l'extension TimescaleDB, et ne recrée/modifie aucune table de ce profil.
--
-- Objectif : purger les partitions mensuelles SUFFISAMMENT anciennes des cinq
-- parents partitionnés de trendx_analytics :
--   1. ts_kv           : 180 jours  -> 6 mois  (plan Trendx)
--   2. predictions     : 1 an      -> 12 mois (plan Trendx)
--   3. anomaly_scores  : 12 mois (défaut documenté — non explicit dans le plan)
--   4. data_quality    : 12 mois (défaut documenté — non explicit dans le plan)
--   5. ml_metrics      : 12 mois (défaut documenté — non explicit dans le plan)
--
-- CHOIX DES DÉFAUTS POUR LES TROIS TABLES ML (anomaly_scores / data_quality /
-- ml_metrics) :
--   Le plan d'implémentation (docs/implementation-plan.md) ne fixe explicitement
--   que deux fenêtres de rétention dans ce périmètre :
--     * ts_kv = 180 jours (~6 mois)
--     * predictions = 1 an (12 mois)
--   Les trois tables ML (anomaly_scores, data_quality, ml_metrics) ne sont PAS
--   explicitement chiffrées dans le plan. Par cohérence et parité avec le
--   profil TimescaleDB (politique de rétention native par défaut à 12 mois sur
--   les hypertables ML), on retient un DÉFAUT de 12 mois pour ces trois tables.
--   Ce choix est centralisé dans la table de configuration RETENTION_MONTHS
--   ci-dessous et reste CONFIGURABLE (ALTER de la constante) sans ajouter de
--   variable .env (interdit par l'Étape 012).
--
-- RÈGLE DE CALCUL DU SEUIL (mensuelle, par table) :
--   current_month = date_trunc('month', now())
--   seuil = current_month - (keep_months - 1) mois
--   on ne supprime QUE les partitions dont le mois de DÉBUT est STRICTEMENT
--   antérieur au seuil. Le mois courant et les (keep_months-1) mois précédents
--   sont donc CONSERVÉS.
--   Example keep_months = 3 :
--     current          -> KEEP
--     current - 1      -> KEEP
--     current - 2      -> KEEP
--     current - 3      -> DROP (seuil = current - 2 ; current - 3 < seuil)
--
-- GARDE-FOUS (critiques) :
--   A. AUCUNE partition future n'est supprimée. Les partitions créées par
--      ensure_partitions_forward(3) (current .. current+3) restent intactes car
--      leur mois de début >= current_month > seuil.
--   B. Le mois courant est toujours conservé (seuil <= current_month - 1).
--   C. keep_months <= 0 : aucun DROP, retour propre sans erreur.
--   D. Aucune partition ancienne : no-op sans erreur.
--   E. Idempotence : deux exécutions consécutives sont sûres (la seconde ne
--      trouve plus rien à supprimer).
--   F. AUCUNE partition DEFAULT n'est créée ni supprimée (on filtre relkind et
--      on n'appelle jamais CREATE TABLE ... DEFAULT PARTITION).
--   G. Tables absentes : comportement sûr (parent introuvable -> on saute).
--
-- IDENTIFICATION DES PARTITIONS :
--   via pg_inherits / pg_class / pg_namespace, restreint à trendx_analytics,
--   exclusion des tables étrangères (relkind != 'r' pour l'enfant) et des
--   partitions DEFAULT. Tout DDL dynamique utilise format('%I.%I', ...) :
--   AUCUNE interpolation de chaîne brute dans du SQL dynamique.
--
-- OWNERSHIP / SECURITY DEFINER (hérité de l'Étape 011) :
--   ensure_partitions_forward() (004) et cette fonction sont SECURITY DEFINER,
--   propriétaire trendx_migration. Les partitions enfants étant créées par la
--   fonction forward (qui s'exécute comme trendx_migration), elles appartiennent
--   à trendx_migration : le DROP (qui s'exécute aussi comme trendx_migration)
--   possède donc bien les objets qu'il détruit. Aucun droit DDL n'est accordé
--   directement à trendx_app.
--
-- CONCURRENCE :
--   pg_advisory_xact_lock(clé stable) pour empêcher deux opérations de
--   maintenance des partitions concurrentes (multi-worker / multi-processus).
--
-- Idempotence :
--   * garde TimescaleDB -> saut total (002 fournit déjà la rétention TSDB) ;
--   * CREATE OR REPLACE FUNCTION -> ré-applicable ;
--   * GRANT idempotent (IF EXISTS trendx_app).
-- ============================================================================

SET client_encoding = 'UTF8';
SET standard_conforming_strings = ON;
SET check_function_bodies = FALSE;
SET client_min_messages = WARNING;

DO $$
BEGIN
  -- En présence de TimescaleDB, 002 fournit déjà la rétention native des
  -- tables du profil. On n'introduit AUCUNE dépendance et on ne recrée/modifie
  -- AUCUNE structure TimescaleDB existante.
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
    RAISE NOTICE '[trendx-migrate] TimescaleDB détecté, saut de 012_native_partition_retention.sql';
    RETURN;
  END IF;

  CREATE SCHEMA IF NOT EXISTS trendx_analytics;
END
$$;

-- ============================================================================
-- drop_old_partitions(p_keep_months DEFAULT NULL)
-- ----------------------------------------------------------------------------
-- Purge native par DROP TABLE de la partition enfant (JAMAIS DELETE ligne à
-- ligne). Si p_keep_months est NULL, utilise la configuration par défaut par
-- table (RETENTION_MONTHS). Si p_keep_months est fourni (>0), il s'applique
-- UNIFORMÉMENT aux cinq parents (mode override de test/réglage fin).
--
-- Retourne un jsonb décrivant les partitions supprimées, par table et au total.
-- ============================================================================
CREATE OR REPLACE FUNCTION trendx_analytics.drop_old_partitions(
    p_keep_months integer DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = trendx_analytics, pg_temp
AS $$
DECLARE
    -- Configuration de rétention par table (mois). ts_kv = 6 (180 j),
    -- predictions = 12 (1 an), les 3 tables ML = 12 (défaut documenté).
    RETENTION_MONTHS CONSTANT jsonb := jsonb_build_object(
        'ts_kv',            6,
        'predictions',      12,
        'anomaly_scores',   12,
        'data_quality',     12,
        'ml_metrics',       12
    );
    v_current_month  timestamptz := date_trunc('month', now());
    v_keep          integer;
    v_tab            text;
    v_schema         text;
    v_parent_oid     oid;
    v_threshold      timestamptz;
    v_part           text;
    v_part_oid       oid;
    v_part_start     timestamptz;
    v_dropped        text[] := ARRAY[]::text[];
    v_per_table      jsonb := '{}'::jsonb;
    v_table_dropped  text[];
BEGIN
    -- Verrou transactionnel : empêche deux maintenances de partitions
    -- concurrentes (multi-worker / multi-processus). Clé stable dédiée.
    PERFORM pg_advisory_xact_lock(912012);

    FOR v_tab IN
        SELECT jsonb_object_keys(RETENTION_MONTHS)
    LOOP
        v_table_dropped := ARRAY[]::text[];

        -- Résolution du parent (relkind='p') dans trendx_analytics.
        SELECT c.oid, n.nspname
            INTO v_parent_oid, v_schema
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = v_tab
          AND c.relkind = 'p'
          AND n.nspname = 'trendx_analytics';

        -- Garde-fou G : table absente ou non partitionnée -> on saute, no-op.
        IF v_parent_oid IS NULL THEN
            v_per_table := v_per_table || jsonb_build_object(v_tab, jsonb_build_array());
            CONTINUE;
        END IF;

        -- Fenêtre de rétention : override (p_keep_months) ou défaut par table.
        IF p_keep_months IS NOT NULL AND p_keep_months > 0 THEN
            v_keep := p_keep_months;
        ELSE
            v_keep := (RETENTION_MONTHS ->> v_tab)::integer;
        END IF;

        -- Garde-fou C : keep_months <= 0 -> aucun DROP, no-op propre.
        IF v_keep <= 0 THEN
            v_per_table := v_per_table || jsonb_build_object(v_tab, jsonb_build_array());
            CONTINUE;
        END IF;

        -- Seuil : on conserve le mois courant + (keep_months - 1) mois
        -- précédents. On ne supprime que les partitions dont le mois de DÉBUT
        -- est STRICTEMENT antérieur au seuil.
        v_threshold := v_current_month - make_interval(months => (v_keep - 1));

        -- Parcours des enfants réels du parent, exclusifs de toute DEFAULT.
        FOR v_part, v_part_oid, v_part_start IN
            SELECT ch.relname,
                   ch.oid,
                   -- borne basse de la partition (FROM) reconstituée via
                   -- pg_get_expr sur la contrainte de partitionnement.
                   (regexp_match(
                       pg_get_expr(ch.relpartbound, ch.oid),
                       'FROM \(''([^'']+)''\)'
                   ))[1]::timestamptz
            FROM pg_inherits i
            JOIN pg_class ch ON ch.oid = i.inhrelid
            WHERE i.inhparent = v_parent_oid
              -- enfant réel uniquement (pas table étrangère, pas DEFAULT).
              AND ch.relkind = 'r'
              -- exclusion explicit des partitions DEFAULT de tout parent.
              AND NOT pg_get_expr(ch.relpartbound, ch.oid) ILIKE '%DEFAULT%'
        LOOP
            -- Partition sans borne détectable (anormale) : on ne la touche pas.
            IF v_part_start IS NULL THEN
                CONTINUE;
            END IF;

            -- Règle : DROP uniquement si mois de début < seuil.
            -- -> jamais le mois courant (B), jamais le future (A),
            --    jamais +3 mois (A, car >= current_month > seuil).
            IF v_part_start < v_threshold THEN
                -- Garde-fou F : aucune DEFAULT. Déjà filtrée, double protection.
                IF pg_get_expr(
                       (SELECT c.relpartbound FROM pg_class c WHERE c.oid = v_part_oid),
                       v_part_oid
                   ) ILIKE '%DEFAULT%' THEN
                    CONTINUE;
                END IF;

                EXECUTE format('DROP TABLE %I.%I', v_schema, v_part);
                v_table_dropped := v_table_dropped || (v_schema || '.' || v_part);
                v_dropped := v_dropped || (v_schema || '.' || v_part);
            END IF;
        END LOOP;

        v_per_table := v_per_table || jsonb_build_object(v_tab, to_jsonb(v_table_dropped));
    END LOOP;

    RETURN jsonb_build_object(
        'keep_months_override', p_keep_months,
        'current_month',       v_current_month,
        'dropped',             to_jsonb(v_dropped),
        'dropped_count',       coalesce(array_length(v_dropped, 1), 0),
        'per_table',           v_per_table,
        'checked_at',          clock_timestamp()
    );
END;
$$;

-- ============================================================================
-- Transfert de propriété + privilèges (idempotent, sauté si TimescaleDB)
-- ============================================================================
DO $$
DECLARE
    v_role_exists boolean;
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
    RAISE NOTICE '[trendx-migrate] TimescaleDB détecté, saut du transfert de propriété 012';
    RETURN;
  END IF;

  -- Rôle trendx_migration (si absent) — nécessite superuser ou createrole ;
  -- en CI le runner (trendx_app = POSTGRES_USER) est superuser.
  SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trendx_migration')
    INTO v_role_exists;
  IF NOT v_role_exists THEN
    BEGIN
      CREATE ROLE trendx_migration LOGIN PASSWORD 'trendx_migration_pass';
    EXCEPTION WHEN duplicate_object THEN
      NULL;
    END;
    SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trendx_migration')
      INTO v_role_exists;
  END IF;

  IF v_role_exists THEN
    -- trendx_app membre de trendx_migration (condition requise pour le chemin
    -- SECURITY DEFINER et le transfert de propriété). Silencieux si échec.
    IF EXISTS (
         SELECT 1 FROM pg_roles a
         JOIN pg_auth_members m ON m.member = a.oid
         JOIN pg_roles r ON r.oid = m.roleid
         WHERE a.rolname = 'trendx_app' AND r.rolname = 'trendx_migration'
       ) IS NOT TRUE
       AND EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trendx_app') THEN
      BEGIN
        EXECUTE 'GRANT trendx_migration TO trendx_app';
      EXCEPTION WHEN OTHERS THEN
        NULL;
      END;
    END IF;

    -- La fonction de rétention appartient à trendx_migration (chemin SD).
    ALTER FUNCTION trendx_analytics.drop_old_partitions(integer) OWNER TO trendx_migration;

    -- CREATE + USAGE sur le schéma pour que le chemin SD puisse référencer /
    -- détruire les enfants de trendx_analytics.
    GRANT CREATE, USAGE ON SCHEMA trendx_analytics TO trendx_migration;
  END IF;
END;
$$;

-- Exécution : accorder EXECUTE à trendx_app (le rôle applicatif / worker) ;
-- aucun droit DDL direct. Idempotent (IF EXISTS).
REVOKE ALL ON FUNCTION trendx_analytics.drop_old_partitions(integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION trendx_analytics.drop_old_partitions(integer) TO trendx_app;
