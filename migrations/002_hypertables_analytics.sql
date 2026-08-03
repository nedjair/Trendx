-- ============================================================================
-- Trendx Analytics : Hypertables TimescaleDB, CAGGs, policies
-- Base : trendx_analytics
-- Propriétaire : trendx_migration (DDL)
-- Droits lecture/écriture : trendx_app (DML)
-- ============================================================================
-- Toutes les tables utilisent CREATE TABLE IF NOT EXISTS pour idempotence.
-- ============================================================================

SET client_encoding = 'UTF8';
SET standard_conforming_strings = ON;
SET check_function_bodies = FALSE;
SET client_min_messages = WARNING;

-- Guarde anti-échec si TimescaleDB nest pas installé (profil minimal PG natif)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
    RAISE NOTICE '[trendx-migrate] TimescaleDB non installé, saut de 002_hypertables_analytics.sql';
    RETURN;
  END IF;
END;
$$;

-- TimescaleDB est déjà activé par 010-create-databases.sh dans entrypoint TSDB.
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- ============================================================================
-- 1. Table principale ts_kv — télémétrie brute (équivalent ts_kv de TB)
-- ============================================================================
CREATE TABLE IF NOT EXISTS ts_kv (
    ts TIMESTAMPTZ NOT NULL,
    entity_id UUID NOT NULL,
    metric_key TEXT NOT NULL,
    str_v TEXT,
    long_v BIGINT,
    dbl_v DOUBLE PRECISION,
    bool_v BOOLEAN,
    json_v JSONB,
    source TEXT NOT NULL DEFAULT 'thingsboard',
    ingestion_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts, entity_id, metric_key)
);

SELECT create_hypertable('ts_kv', 'ts',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day',
    create_default_indexes => TRUE
);

CREATE INDEX IF NOT EXISTS idx_ts_kv_entity_metric_ts
    ON ts_kv (entity_id, metric_key, ts DESC) WITH (timescaledb.transaction_per_chunk = TRUE);
CREATE INDEX IF NOT EXISTS idx_ts_kv_entity_source_ts
    ON ts_kv (entity_id, source, ts DESC);

-- ============================================================================
-- 2. Dernières valeurs connues par entité/métrique
-- ============================================================================
CREATE TABLE IF NOT EXISTS ts_kv_latest (
    entity_id UUID NOT NULL,
    metric_key TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    str_v TEXT,
    long_v BIGINT,
    dbl_v DOUBLE PRECISION,
    bool_v BOOLEAN,
    json_v JSONB,
    source TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (entity_id, metric_key)
);

-- ============================================================================
-- 3. Prévisions stockées
-- ============================================================================
CREATE TABLE IF NOT EXISTS predictions (
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
);

SELECT create_hypertable('predictions', 'ts',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 month',
    create_default_indexes => TRUE
);
CREATE INDEX IF NOT EXISTS idx_predictions_latest
    ON predictions (entity_id, metric_key, forecast_generated_at DESC, ts);

-- ============================================================================
-- 4. Scores d'anomalies (vue par point)
-- ============================================================================
CREATE TABLE IF NOT EXISTS anomaly_scores (
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
);

SELECT create_hypertable('anomaly_scores', 'ts',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 week',
    create_default_indexes => TRUE
);

-- ============================================================================
-- 5. Rapports qualité données
-- ============================================================================
CREATE TABLE IF NOT EXISTS data_quality (
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
);

SELECT create_hypertable('data_quality', 'period',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 month',
    create_default_indexes => TRUE
);

-- ============================================================================
-- 6. Métriques d'entraînement / exécution ML par run
-- ============================================================================
CREATE TABLE IF NOT EXISTS ml_metrics (
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    entity_id UUID,
    metric_key TEXT,
    model_id UUID,
    run_type TEXT NOT NULL,  -- TRAIN / BACKTEST / FORECAST
    window_label TEXT,
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    extra JSONB
);
SELECT create_hypertable('ml_metrics', 'ts',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 month',
    create_default_indexes => TRUE
);

-- ============================================================================
-- CAGGs 1/5 : Agrégat horaire (valeurs numériques dbl_v)
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS ts_kv_hourly
WITH (timescaledb.continuous, timescaledb.materialized_only = false)
AS
SELECT
    time_bucket('1 hour'::interval, ts) AS bucket,
    entity_id,
    metric_key,
    count(*)                                          AS samples_count,
    avg(dbl_v)                                        AS avg_v,
    min(dbl_v)                                        AS min_v,
    max(dbl_v)                                        AS max_v,
    stddev(dbl_v)                                     AS stddev_v,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY dbl_v)  AS median_v,
    percentile_cont(0.9) WITHIN GROUP (ORDER BY dbl_v)  AS p90_v,
    percentile_cont(0.1) WITHIN GROUP (ORDER BY dbl_v)  AS p10_v,
    sum(dbl_v) FILTER (WHERE dbl_v IS NOT NULL)       AS sum_v,
    last(dbl_v, ts)                                   AS last_v,
    first(dbl_v, ts)                                  AS first_v
FROM ts_kv
WHERE dbl_v IS NOT NULL
GROUP BY bucket, entity_id, metric_key
WITH NO DATA;

-- ============================================================================
-- CAGGs 2/5 : Agrégat 6 heures
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS ts_kv_6hourly
WITH (timescaledb.continuous, timescaledb.materialized_only = false)
AS
SELECT
    time_bucket('6 hours'::interval, ts) AS bucket,
    entity_id,
    metric_key,
    count(*)                                          AS samples_count,
    avg(dbl_v)                                        AS avg_v,
    min(dbl_v)                                        AS min_v,
    max(dbl_v)                                        AS max_v,
    stddev(dbl_v)                                     AS stddev_v,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY dbl_v)  AS median_v,
    sum(dbl_v) FILTER (WHERE dbl_v IS NOT NULL)       AS sum_v,
    last(dbl_v, ts)                                   AS last_v
FROM ts_kv
WHERE dbl_v IS NOT NULL
GROUP BY bucket, entity_id, metric_key
WITH NO DATA;

-- ============================================================================
-- CAGGs 3/5 : Agrégat journalier
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS ts_kv_daily
WITH (timescaledb.continuous, timescaledb.materialized_only = false)
AS
SELECT
    time_bucket('1 day'::interval, ts) AS bucket,
    entity_id,
    metric_key,
    count(*)                                          AS samples_count,
    avg(dbl_v)                                        AS avg_v,
    min(dbl_v)                                        AS min_v,
    max(dbl_v)                                        AS max_v,
    stddev(dbl_v)                                     AS stddev_v,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY dbl_v)  AS median_v,
    sum(dbl_v) FILTER (WHERE dbl_v IS NOT NULL)       AS sum_v,
    integral(timevector(ts, dbl_v))                   AS integral_v
FROM ts_kv
WHERE dbl_v IS NOT NULL
GROUP BY bucket, entity_id, metric_key
WITH NO DATA;

-- ============================================================================
-- CAGGs 4/5 : Agrégat hebdomadaire
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS ts_kv_weekly
WITH (timescaledb.continuous, timescaledb.materialized_only = false)
AS
SELECT
    time_bucket('1 week'::interval, ts) AS bucket,
    entity_id,
    metric_key,
    count(*)                    AS samples_count,
    avg(dbl_v)                  AS avg_v,
    min(dbl_v)                  AS min_v,
    max(dbl_v)                  AS max_v,
    stddev(dbl_v)               AS stddev_v,
    sum(dbl_v) FILTER (WHERE dbl_v IS NOT NULL) AS sum_v
FROM ts_kv
WHERE dbl_v IS NOT NULL
GROUP BY bucket, entity_id, metric_key
WITH NO DATA;

-- ============================================================================
-- CAGGs 5/5 : Agrégat mensuel
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS ts_kv_monthly
WITH (timescaledb.continuous, timescaledb.materialized_only = false)
AS
SELECT
    time_bucket('1 month'::interval, ts) AS bucket,
    entity_id,
    metric_key,
    count(*)                    AS samples_count,
    avg(dbl_v)                  AS avg_v,
    min(dbl_v)                  AS min_v,
    max(dbl_v)                  AS max_v,
    sum(dbl_v) FILTER (WHERE dbl_v IS NOT NULL) AS sum_v
FROM ts_kv
WHERE dbl_v IS NOT NULL
GROUP BY bucket, entity_id, metric_key
WITH NO DATA;

-- ============================================================================
-- Refresh policies CAGGs (continu ou périodique)
-- ============================================================================
SELECT add_continuous_aggregate_policy('ts_kv_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset   => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '15 minutes',
    if_not_exists => TRUE
);

SELECT add_continuous_aggregate_policy('ts_kv_6hourly',
    start_offset => INTERVAL '1 day',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

SELECT add_continuous_aggregate_policy('ts_kv_daily',
    start_offset => INTERVAL '3 days',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '3 hours',
    if_not_exists => TRUE
);

SELECT add_continuous_aggregate_policy('ts_kv_weekly',
    start_offset => INTERVAL '14 days',
    end_offset   => INTERVAL '1 day',
    schedule_interval => INTERVAL '12 hours',
    if_not_exists => TRUE
);

SELECT add_continuous_aggregate_policy('ts_kv_monthly',
    start_offset => INTERVAL '2 months',
    end_offset   => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- ============================================================================
-- Rétention 2 ans (télémétrie brute) + compression segment by
-- ============================================================================
SELECT add_retention_policy('ts_kv', INTERVAL '2 years', if_not_exists => TRUE);
SELECT add_retention_policy('predictions', INTERVAL '3 years', if_not_exists => TRUE);
SELECT add_retention_policy('anomaly_scores', INTERVAL '2 years', if_not_exists => TRUE);
SELECT add_retention_policy('data_quality', INTERVAL '3 years', if_not_exists => TRUE);
SELECT add_retention_policy('ml_metrics', INTERVAL '5 years', if_not_exists => TRUE);

ALTER TABLE ts_kv SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'entity_id, metric_key, source',
    timescaledb.compress_orderby = 'ts DESC'
);
ALTER TABLE predictions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'entity_id, metric_key, model_used',
    timescaledb.compress_orderby = 'ts DESC'
);
ALTER TABLE anomaly_scores SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'entity_id, metric_key, algorithm',
    timescaledb.compress_orderby = 'ts DESC'
);

SELECT add_compression_policy('ts_kv', INTERVAL '2 weeks', if_not_exists => TRUE);
SELECT add_compression_policy('predictions', INTERVAL '1 month', if_not_exists => TRUE);
SELECT add_compression_policy('anomaly_scores', INTERVAL '1 month', if_not_exists => TRUE);

-- ============================================================================
-- Droits trendx_app : DML
-- ============================================================================
GRANT CONNECT ON DATABASE trendx_analytics TO trendx_app;
GRANT USAGE ON SCHEMA public TO trendx_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO trendx_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO trendx_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO trendx_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO trendx_app;

-- ============================================================================
-- Registre schema_version (base analytics)
-- ============================================================================
CREATE TABLE IF NOT EXISTS schema_version (
    migration_name TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    checksum TEXT,
    execution_seconds INTEGER,
    status TEXT NOT NULL DEFAULT 'OK'
);

INSERT INTO schema_version (migration_name, checksum, execution_seconds, status)
VALUES ('002_hypertables_analytics.sql', 'trendx-phase2-hypercaggs', 0, 'OK')
ON CONFLICT (migration_name) DO NOTHING;
