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

  -- Propriété / droits (modèle identique à 004) :
  --   ensure_partitions_forward() (004) est SECURITY DEFINER, propriétaire
  --   trendx_migration ; elle crée les partitions enfants via
  --   'CREATE TABLE ... PARTITION OF <parent>'. Dans PostgreSQL, attacher une
  --   partition exige la PROPRIÉTÉ du parent (pas seulement CREATE sur le
  --   schéma). On transfère donc la propriété des 4 parents à trendx_migration
  --   (comme 004 le fait pour ts_kv_hourly/_daily/_weekly), ce qui rend la
  --   maintenance forward opérationnelle SANS modifier 004 ni 002.
  --   Les partitions futures hériteront de cette propriété.
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trendx_migration') THEN
    ALTER TABLE trendx_analytics.predictions    OWNER TO trendx_migration;
    ALTER TABLE trendx_analytics.anomaly_scores OWNER TO trendx_migration;
    ALTER TABLE trendx_analytics.data_quality   OWNER TO trendx_migration;
    ALTER TABLE trendx_analytics.ml_metrics     OWNER TO trendx_migration;
    -- CREATE + USAGE sur le schéma (idempotent) : necessaires au cas où le
    -- schéma existerait deja sans ces droits pour trendx_migration. CREATE
    -- permet de créer les partitions enfants ; USAGE permet à la fonction
    -- SECURITY DEFINER de référencer le parent pour l'attachement PARTITION OF.
    GRANT CREATE, USAGE ON SCHEMA trendx_analytics TO trendx_migration;
  END IF;

  -- DML applicatif : accordé à trendx_app. Les GRANTs sur le parent PARENT
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
