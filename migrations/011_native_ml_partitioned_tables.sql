-- ============================================================================
-- Trendx Analytics : native PostgreSQL partitioned tables for ML results
-- (predictions, anomaly_scores, data_quality, ml_metrics)
-- Base : trendx_analytics
-- Profil : PostgreSQL natif (B5) — TimescaleDB INTERDITE en profil minimal.
-- ============================================================================
-- Contexte (B5 Lot 2 — Étape 011) :
--   migration 002 (TimescaleDB) est un NO-OP en PG natif (son corps est gardé
--   par la présence de l'extension timescaledb). En PG natif, 002 ne crée donc
--   AUCUNE des tables ML. Or ensure_partitions_forward() (définie dans 004)
--   itère sur ['ts_kv','predictions','anomaly_scores','data_quality','ml_metrics']
--   et lève 'table partitionnée introuvable' dès qu'une table n'est pas
--   partitionnée. En PG natif seule ts_kv (009/010) existe et est partitionnée ;
--   les 4 tables ML manquantes font échouer la maintenance forward.
--
--   Cette migration 011 fournit UNIQUEMENT les PARENTS partitionnés natifs
--   attendus par 004. Elle ne réimplémente PAS de logique de maintenance :
--   ensure_partitions_forward() (004) reste l'unique mécanisme forward.
--
-- Contrat colonnes / PK : IDENTIQUE à migrations/002 (hors garde TSDB).
--   - predictions     : PARTITION BY RANGE (ts)          [PK inclut ts]
--   - anomaly_scores  : PARTITION BY RANGE (ts)          [PK inclut ts]
--   - data_quality    : PARTITION BY RANGE (period)      [PK inclut period]
--                       (002 utilise period comme colonne temporelle du hypertable)
--   - ml_metrics      : PARTITION BY RANGE (ts), SANS PK [002 n'a pas de PK]
--
--   ensure_partitions_forward() est indépendant du nom de la colonne de
--   partition : il utilise date_trunc('month', now()) comme bornes, valides
--   pour toute colonne de partition timestamptz (ts OU period).
--
-- Idempotence :
--   * garde TimescaleDB -> saut total (002 a déjà créé les hypertables en TSDB) ;
--   * CREATE SCHEMA / TABLE IF NOT EXISTS -> ré-applicable sans erreur ;
--   * GRANT idempotent (IF EXISTS trendx_app).
--
-- Ordre CI : APRÈS 010. Ne touche PAS ts_kv (Lot 1 validé).
-- Ne crée PAS de partition DEFAULT, PAS de partition historique arbitraire
-- (ensure_partitions_forward() crée les partitions mensuelles forward).
-- ============================================================================

SET client_encoding = 'UTF8';
SET standard_conforming_strings = ON;
SET check_function_bodies = FALSE;
SET client_min_messages = WARNING;

DO $$
BEGIN
  -- En présence de TimescaleDB, 002 a déjà créé ces tables comme hypertables
  -- (relkind='p', déjà partie partitionnée). On évite toute table concurrente
  -- en trendx_analytics et on laisse 002 propriétaire de ces objets.
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
    RAISE NOTICE '[trendx-migrate] TimescaleDB détecté, saut de 011_native_ml_partitioned_tables.sql';
    RETURN;
  END IF;

  CREATE SCHEMA IF NOT EXISTS trendx_analytics;

  -- 1) predictions — partitionné par ts
  CREATE TABLE IF NOT EXISTS trendx_analytics.predictions (
      ts TIMESTAMPTZ NOT NULL,
      entity_id UUID NOT NULL,
      metric_key TEXT NOT NULL,
      forecast_generated_at TIMESTAMPTZ NOT NULL,
      model_id UUID,
      model_used TEXT NOT NULL,
      run_id BIGINT,
      value DOUBLE PRECISION NOT NULL,
      lower_bound DOUBLE PRECISION,
      upper_bound DOUBLE PRECISION,
      horizon_step INTEGER NOT NULL,
      frequency TEXT NOT NULL DEFAULT '1h',
      mlflow_run_id TEXT,
      written_back BOOLEAN NOT NULL DEFAULT FALSE,
      writeback_key TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (ts, entity_id, metric_key, forecast_generated_at, model_id)
  ) PARTITION BY RANGE (ts);

  -- 2) anomaly_scores — partitionné par ts
  CREATE TABLE IF NOT EXISTS trendx_analytics.anomaly_scores (
      ts TIMESTAMPTZ NOT NULL,
      entity_id UUID NOT NULL,
      metric_key TEXT NOT NULL,
      detector_id UUID,
      algorithm TEXT,
      raw_score DOUBLE PRECISION NOT NULL,
      normalized_score DOUBLE PRECISION,
      threshold DOUBLE PRECISION,
      is_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
      episode_id BIGINT,
      severity TEXT,
      PRIMARY KEY (ts, entity_id, metric_key, detector_id)
  ) PARTITION BY RANGE (ts);

  -- 3) data_quality — partitionné par period (colonne de partitionnement de 002)
  CREATE TABLE IF NOT EXISTS trendx_analytics.data_quality (
      period TIMESTAMPTZ NOT NULL,
      entity_id UUID NOT NULL,
      metric_key TEXT NOT NULL,
      period_length TEXT NOT NULL,
      expected_points INTEGER,
      actual_points INTEGER,
      null_points INTEGER,
      duplicate_points INTEGER,
      gap_points INTEGER,
      outlier_points INTEGER,
      completeness_ratio DOUBLE PRECISION,
      min_v DOUBLE PRECISION,
      max_v DOUBLE PRECISION,
      avg_v DOUBLE PRECISION,
      stddev_v DOUBLE PRECISION,
      p10 DOUBLE PRECISION,
      p90 DOUBLE PRECISION,
      PRIMARY KEY (period, entity_id, metric_key, period_length)
  ) PARTITION BY RANGE (period);

  -- 4) ml_metrics — partitionné par ts ; 002 n'a PAS de PK -> aucune contrainte
  CREATE TABLE IF NOT EXISTS trendx_analytics.ml_metrics (
      ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      entity_id UUID,
      metric_key TEXT,
      model_id UUID,
      run_type TEXT NOT NULL,
      window_label TEXT,
      metric_name TEXT NOT NULL,
      metric_value DOUBLE PRECISION NOT NULL,
      extra JSONB
  ) PARTITION BY RANGE (ts);

  -- DML applicatif : accordé à trendx_app. Les GRANTs sur le parent
  -- sont hérités par les partitions futures créées par ensure_partitions_forward().
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trendx_app') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE
        ON trendx_analytics.predictions,
           trendx_analytics.anomaly_scores,
           trendx_analytics.data_quality,
           trendx_analytics.ml_metrics
        TO trendx_app;
  END IF;
END;
$$;

-- ============================================================================
-- Transfert de propriété OBLIGATOIRE vers trendx_migration
-- ----------------------------------------------------------------------------
-- ensure_partitions_forward() (004) est SECURITY DEFINER, propriétaire
-- trendx_migration : elle s'exécute DONC avec les droits de trendx_migration,
-- et non de l'appelant. Pour 'CREATE TABLE ... PARTITION OF <parent>', le
-- propriétaire effectif (trendx_migration) DOIT ÊTRE PROPRIÉTAIRE du parent
-- (la propriété du schéma + CREATE ne suffit pas). Si les 4 parents ML
-- restent propriété de trendx_app, la fonction ne lève PAS d'erreur mais
-- n'attache AUCUNE partition (silencieusement) -> échec latent en CI.
--
-- 009/010 placent déjà ts_kv sous trendx_migration ; 011 doit en faire
-- autant pour les 4 tables ML, SANS dépendre de l'étape de bootstrap CI
-- (création du rôle trendx_migration / GRANT trendx_migration TO trendx_app)
-- qui peut être sautée ou retardée. D'où le bloc auto-suffisant ci-dessous :
--   1) crée trendx_migration si absent ;
--   2) donne trendx_app comme MEMBRE de trendx_migration (permet le transfert
--      de propriété et rend le chemin SECURITY DEFINER opérationnel) ;
--   3) transfère la propriété des 4 parents à trendx_migration ;
--   4) accorde CREATE, USAGE sur le schéma à trendx_migration.
-- Idempotent et ré-applicable. Sauté si TimescaleDB (002 est propriétaire).
-- ============================================================================
DO $$
DECLARE
  v_role_exists boolean;
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
    RAISE NOTICE '[trendx-migrate] TimescaleDB détecté, saut du transfert de propriété 011';
    RETURN;
  END IF;

  -- 1) rôle trendx_migration (si absent). Nécessite superuser ou createrole ;
  --    en CI le runner (trendx_app = POSTGRES_USER) est superuser.
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
    -- 2) trendx_app membre de trendx_migration : condition requise pour que le
    --    transfert de propriété (ci-dessous) et le chemin SECURITY DEFINER
    --    soient autorisés. On ne l'ajoute QUE si ce n'est pas déjà le cas, et
    --    on ignore silencieusement l'échec (un runner non-superuser sans
    --    privilège ne peut pas s'auto-octroyer l'appartenance ; le bootstrap CI
    --    s'en charge, ou le runner est déjà membre). L'important est le
    --    transfert de propriété en 3), qui réussit si le runner est superuser
    --    (cas CI : POSTGRES_USER=trendx_app) ou déjà membre de trendx_migration.
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

    -- 3) transfert de propriété des parents vers trendx_migration.
    --    Les 4 tables ML (créées ci-dessus) PLUS ts_kv (créée par 009/010,
    --    propriété trendx_app) : ensure_partitions_forward() (004) itère sur
    --    les 5 et s'exécute comme trendx_migration, donc TOUS les parents
    --    doivent lui appartenir pour que l'attachement de partition réussisse.
    --    ts_kv n'existe qu'en profil natif (009/010) ; le IF EXISTS évite
    --    l'erreur en profil TimescaleDB (où 002 l'a créée comme hypertable).
    ALTER TABLE trendx_analytics.predictions    OWNER TO trendx_migration;
    ALTER TABLE trendx_analytics.anomaly_scores OWNER TO trendx_migration;
    ALTER TABLE trendx_analytics.data_quality   OWNER TO trendx_migration;
    ALTER TABLE trendx_analytics.ml_metrics     OWNER TO trendx_migration;
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'trendx_analytics' AND c.relname = 'ts_kv' AND c.relkind = 'p') THEN
      ALTER TABLE trendx_analytics.ts_kv OWNER TO trendx_migration;
    END IF;

    -- 4) CREATE + USAGE sur le schéma : CREATE permet de créer les partitions
    --    enfants ; USAGE permet à la fonction SECURITY DEFINER de référencer le
    --    parent pour l'attachement PARTITION OF.
    GRANT CREATE, USAGE ON SCHEMA trendx_analytics TO trendx_migration;
  END IF;
END;
$$;
