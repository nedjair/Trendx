-- ============================================================
-- 004_aggregates_partitions.sql
-- Phase 2 — correctifs bloquants 1 et 3
--
-- Bloquant 1 : automatisation des partitions mensuelles à +3 mois
--   - trendx_analytics.ensure_partitions_forward(min_months)
--     (SECURITY DEFINER, propriétaire trendx_migration,
--      exécutable par trendx_app via le scheduler du worker)
--   - trendx_analytics.partition_coverage(min_months)
--     (lecture seule, utilisée par `make doctor`)
--
-- Bloquant 3 : agrégats en TABLES alimentées par UPSERT incrémental
--   avec filigrane. JAMAIS de REFRESH MATERIALIZED VIEW.
--   - ts_kv_hourly / ts_kv_daily / ts_kv_weekly (tables)
--   - trendx_analytics.aggregate_watermarks (filigrane par agrégat)
--   - trendx_analytics.refresh_aggregate('hourly|daily|weekly')
--
-- Propriétaire des objets : trendx_migration.
-- À appliquer avec : psql -1 -U postgres -d trendx -f 004_aggregates_partitions.sql
-- ============================================================

BEGIN;

-- 1) Suppression des vues matérialisées (remplacées par des tables)
DROP MATERIALIZED VIEW IF EXISTS trendx_analytics.ts_kv_hourly;
DROP MATERIALIZED VIEW IF EXISTS trendx_analytics.ts_kv_daily;
DROP MATERIALIZED VIEW IF EXISTS trendx_analytics.ts_kv_weekly;

-- 2) Tables d'agrégats (UPSERT incrémental, colonnes conformes aux MVs supprimées)

CREATE TABLE trendx_analytics.ts_kv_hourly (
    bucket         timestamptz   NOT NULL,
    entity_id      uuid          NOT NULL,
    metric_key     text          NOT NULL,
    samples_count  bigint        NOT NULL DEFAULT 0,
    avg_v          double precision,
    min_v          double precision,
    max_v          double precision,
    stddev_v       double precision,
    median_v       double precision,
    p90_v          double precision,
    p10_v          double precision,
    sum_v          double precision,
    PRIMARY KEY (bucket, entity_id, metric_key)
);
CREATE INDEX idx_ts_kv_hourly_entity ON trendx_analytics.ts_kv_hourly (entity_id, metric_key, bucket);

CREATE TABLE trendx_analytics.ts_kv_daily (
    bucket         timestamptz   NOT NULL,
    entity_id      uuid          NOT NULL,
    metric_key     text          NOT NULL,
    samples_count  bigint        NOT NULL DEFAULT 0,
    avg_v          double precision,
    min_v          double precision,
    max_v          double precision,
    stddev_v       double precision,
    median_v       double precision,
    sum_v          double precision,
    PRIMARY KEY (bucket, entity_id, metric_key)
);
CREATE INDEX idx_ts_kv_daily_entity ON trendx_analytics.ts_kv_daily (entity_id, metric_key, bucket);

CREATE TABLE trendx_analytics.ts_kv_weekly (
    bucket         timestamptz   NOT NULL,
    entity_id      uuid          NOT NULL,
    metric_key     text          NOT NULL,
    samples_count  bigint        NOT NULL DEFAULT 0,
    avg_v          double precision,
    min_v          double precision,
    max_v          double precision,
    stddev_v       double precision,
    sum_v          double precision,
    PRIMARY KEY (bucket, entity_id, metric_key)
);
CREATE INDEX idx_ts_kv_weekly_entity ON trendx_analytics.ts_kv_weekly (entity_id, metric_key, bucket);

-- 3) Filigranes par agrégat (une ligne par agrégat)
CREATE TABLE trendx_analytics.aggregate_watermarks (
    aggregate_name      text        PRIMARY KEY,
    watermark           timestamptz,
    last_refresh_start  timestamptz,
    last_refresh_end    timestamptz,
    last_refresh_rows   bigint      NOT NULL DEFAULT 0,
    updated_at          timestamptz NOT NULL DEFAULT now()
);

-- 4) Rafraîchissement d'un agrégat : UPSERT incrémental sur fenêtre bornée.
--    - horaire  : fenêtre 3 heures  (job toutes les 15 min)
--    - journalier: fenêtre 2 jours  (job toutes les heures)
--    - hebdomadaire: fenêtre 7 jours (job 1x/jour hors pointe)
--    La fenêtre démarre en début de bucket afin de recalculer complètement
--    chaque bucket ouvert (aucune perte sur les données tardives).
CREATE OR REPLACE FUNCTION trendx_analytics.refresh_aggregate(p_agg text)
RETURNS jsonb
LANGUAGE plpgsql
SET search_path = trendx_analytics, pg_temp
AS $$
DECLARE
    v_table         text;
    v_bucket        text;
    v_window_start  timestamptz;
    v_sel           text;
    v_cols          text;
    v_upd           text;
    v_rows          bigint;
    v_start         timestamptz := clock_timestamp();
BEGIN
    v_table := 'ts_kv_' || p_agg;
    CASE p_agg
        WHEN 'hourly' THEN
            v_bucket        := 'hour';
            v_window_start  := date_trunc('hour', clock_timestamp() - interval '3 hours');
            v_sel           := 'count(*), avg(dbl_v), min(dbl_v), max(dbl_v), stddev(dbl_v),'
                               ' percentile_cont(0.5) WITHIN GROUP (ORDER BY dbl_v),'
                               ' percentile_cont(0.9) WITHIN GROUP (ORDER BY dbl_v),'
                               ' percentile_cont(0.1) WITHIN GROUP (ORDER BY dbl_v),'
                               ' sum(dbl_v)';
            v_cols          := 'samples_count, avg_v, min_v, max_v, stddev_v, median_v, p90_v, p10_v, sum_v';
        WHEN 'daily' THEN
            v_bucket        := 'day';
            v_window_start  := date_trunc('day', clock_timestamp() - interval '2 days');
            v_sel           := 'count(*), avg(dbl_v), min(dbl_v), max(dbl_v), stddev(dbl_v),'
                               ' percentile_cont(0.5) WITHIN GROUP (ORDER BY dbl_v),'
                               ' sum(dbl_v)';
            v_cols          := 'samples_count, avg_v, min_v, max_v, stddev_v, median_v, sum_v';
        WHEN 'weekly' THEN
            v_bucket        := 'week';
            v_window_start  := date_trunc('week', clock_timestamp() - interval '7 days');
            v_sel           := 'count(*), avg(dbl_v), min(dbl_v), max(dbl_v), stddev(dbl_v), sum(dbl_v)';
            v_cols          := 'samples_count, avg_v, min_v, max_v, stddev_v, sum_v';
        ELSE
            RAISE EXCEPTION 'agrégat inconnu: % (attendu hourly|daily|weekly)', p_agg;
    END CASE;

    v_upd := (SELECT string_agg(c || ' = EXCLUDED.' || c, ', ')
              FROM unnest(string_to_array(v_cols, ', ')) AS c);

    EXECUTE format(
        'INSERT INTO %I (bucket, entity_id, metric_key, %s) '
        'SELECT date_trunc(%L, ts) AS bucket, entity_id, metric_key, %s '
        'FROM ts_kv '
        'WHERE dbl_v IS NOT NULL AND ts >= %L '
        'GROUP BY 1, 2, 3 '
        'ON CONFLICT (bucket, entity_id, metric_key) DO UPDATE SET %s',
        v_table, v_cols, v_bucket, v_sel, v_window_start, v_upd
    );
    GET DIAGNOSTICS v_rows = ROW_COUNT;

    INSERT INTO aggregate_watermarks
        (aggregate_name, watermark, last_refresh_start, last_refresh_end, last_refresh_rows, updated_at)
    VALUES
        (p_agg, clock_timestamp(), v_start, clock_timestamp(), v_rows, clock_timestamp())
    ON CONFLICT (aggregate_name) DO UPDATE SET
        watermark          = EXCLUDED.watermark,
        last_refresh_start = EXCLUDED.last_refresh_start,
        last_refresh_end   = EXCLUDED.last_refresh_end,
        last_refresh_rows  = EXCLUDED.last_refresh_rows,
        updated_at         = EXCLUDED.updated_at;

    RETURN jsonb_build_object(
        'aggregate',    p_agg,
        'bucket',       v_bucket,
        'window_start', v_window_start,
        'rows_upserted', v_rows,
        'started_at',   v_start,
        'finished_at',  clock_timestamp()
    );
END;
$$;

-- 5) Garantie des partitions mensuelles à +3 mois (appelée par le scheduler).
--    SECURITY DEFINER : s'exécute avec les droits de trendx_migration
--    afin que trendx_app (DML uniquement) puisse créer les partitions.
CREATE OR REPLACE FUNCTION trendx_analytics.ensure_partitions_forward(p_min_months integer DEFAULT 3)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = trendx_analytics, pg_temp
AS $$
DECLARE
    v_tables  text[] := ARRAY['ts_kv','predictions','anomaly_scores','data_quality','ml_metrics'];
    v_tab     text;
    v_schema  text;
    v_bound   timestamptz;
    v_target  timestamptz;
    v_part    text;
    v_created text[] := ARRAY[]::text[];
BEGIN
    v_target := date_trunc('month', clock_timestamp())
              + make_interval(months => p_min_months);

    FOR v_tab IN SELECT unnest(v_tables) LOOP
        SELECT n.nspname INTO v_schema
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = v_tab AND c.relkind = 'p';

        IF v_schema IS NULL THEN
            RAISE EXCEPTION 'table partitionnée introuvable: %', v_tab;
        END IF;

        FOR v_bound IN
            SELECT generate_series(
                       date_trunc('month', clock_timestamp()),
                       v_target,
                       interval '1 month'
                   )
        LOOP
            v_part := v_tab || '_' || to_char(v_bound, 'YYYY_MM');
            IF NOT EXISTS (
                SELECT 1
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = v_part AND n.nspname = v_schema
            ) THEN
                EXECUTE format(
                    'CREATE TABLE %I.%I PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L)',
                    v_schema, v_part, v_schema, v_tab,
                    v_bound, v_bound + interval '1 month'
                );
                v_created := v_created || (v_schema || '.' || v_part);
            END IF;
        END LOOP;
    END LOOP;

    RETURN jsonb_build_object(
        'min_months',    p_min_months,
        'target_bound',  v_target + interval '1 month',
        'created',       v_created,
        'checked_at',    clock_timestamp()
    );
END;
$$;

-- 6) Contrôle de couverture (lecture seule, pour `make doctor`)
CREATE OR REPLACE FUNCTION trendx_analytics.partition_coverage(p_min_months integer DEFAULT 3)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SET search_path = trendx_analytics, pg_temp
AS $$
DECLARE
    v_tables  text[] := ARRAY['ts_kv','predictions','anomaly_scores','data_quality','ml_metrics'];
    v_tab     text;
    v_target  timestamptz;
    v_missing integer := 0;
    v_last    text;
    v_max     text;
    v_rows    jsonb := '[]'::jsonb;
    v_row     jsonb;
BEGIN
    v_target := date_trunc('month', clock_timestamp())
              + make_interval(months => p_min_months);

    FOR v_tab IN SELECT unnest(v_tables) LOOP
        SELECT ch.relname,
               (regexp_match(pg_get_expr(ch.relpartbound, ch.oid),
                             'TO \(''([^'']+)''\)'))[1]::timestamptz::text
        INTO v_last, v_max
        FROM pg_inherits i
        JOIN pg_class p   ON p.oid  = i.inhparent
        JOIN pg_class ch  ON ch.oid = i.inhrelid
        JOIN pg_namespace pn ON pn.oid = p.relnamespace
        WHERE p.relname = v_tab AND pn.nspname = 'trendx_analytics'
        ORDER BY ch.relname DESC
        LIMIT 1;

        v_row := jsonb_build_object('table', v_tab, 'last_partition', v_last, 'max_bound', v_max);
        v_rows := v_rows || v_row;
    END LOOP;

    SELECT count(*) INTO v_missing
    FROM (
        SELECT x.t AS t, g.b AS b
        FROM (SELECT unnest(v_tables) AS t) x
        CROSS JOIN generate_series(
            date_trunc('month', clock_timestamp()),
            v_target,
            interval '1 month'
        ) AS g(b)
    ) need
    WHERE NOT EXISTS (
        SELECT 1
        FROM pg_inherits i
        JOIN pg_class p   ON p.oid  = i.inhparent
        JOIN pg_class ch  ON ch.oid = i.inhrelid
        JOIN pg_namespace pn ON pn.oid = p.relnamespace
        WHERE p.relname = need.t AND pn.nspname = 'trendx_analytics'
          AND ch.relname = need.t || '_' || to_char(need.b, 'YYYY_MM')
    );

    RETURN jsonb_build_object(
        'min_months',   p_min_months,
        'target_bound', v_target + interval '1 month',
        'missing',      v_missing,
        'tables',       v_rows
    );
END;
$$;

-- 7) Propriété (trendx_migration) et privilèges (trendx_app)
ALTER TABLE trendx_analytics.ts_kv_hourly OWNER TO trendx_migration;
ALTER TABLE trendx_analytics.ts_kv_daily OWNER TO trendx_migration;
ALTER TABLE trendx_analytics.ts_kv_weekly OWNER TO trendx_migration;
ALTER TABLE trendx_analytics.aggregate_watermarks OWNER TO trendx_migration;

ALTER FUNCTION trendx_analytics.refresh_aggregate(text) OWNER TO trendx_migration;
ALTER FUNCTION trendx_analytics.ensure_partitions_forward(integer) OWNER TO trendx_migration;
ALTER FUNCTION trendx_analytics.partition_coverage(integer) OWNER TO trendx_migration;

GRANT SELECT, INSERT, UPDATE, DELETE
    ON trendx_analytics.ts_kv_hourly,
       trendx_analytics.ts_kv_daily,
       trendx_analytics.ts_kv_weekly,
       trendx_analytics.aggregate_watermarks
    TO trendx_app;

REVOKE ALL ON FUNCTION trendx_analytics.refresh_aggregate(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION trendx_analytics.ensure_partitions_forward(integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION trendx_analytics.partition_coverage(integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION trendx_analytics.refresh_aggregate(text) TO trendx_app;
GRANT EXECUTE ON FUNCTION trendx_analytics.ensure_partitions_forward(integer) TO trendx_app;
GRANT EXECUTE ON FUNCTION trendx_analytics.partition_coverage(integer) TO trendx_app;

-- 8) Pré-création immédiate des partitions manquantes (mois courant → +3 mois)
SELECT trendx_analytics.ensure_partitions_forward(3);

COMMIT;
