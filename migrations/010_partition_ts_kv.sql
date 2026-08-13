-- ============================================================================
-- Trendx Analytics : declarative partitioning of raw telemetry (ts_kv) — B5
-- Profil : PostgreSQL natif (B5) — TimescaleDB INTERDITE en profil minimal.
-- ============================================================================
-- Poursuit migration 009 (tables natives ts_kv / ts_kv_latest). 009 crée
-- trendx_analytics.ts_kv comme table plate (relkind='r') ; cette migration 010
-- converts it into a declarative PARTITIONED table :
--   * PARTITION BY RANGE (ts) ;
--   * partitions mensuelles ts_kv_YYYY_MM ;
--   * initial horizon from the oldest month of existing data up to
--     (current month + 3 months) ;
--   * index BRIN sur ts (idx_ts_kv_brin_ts) ;
--   * AUCUNE partition DEFAULT.
--
-- Idempotence :
--   * saute si TimescaleDB présent (002 fournit déjà le hypertable) ;
--   * saute si ts_kv absente (créée par 009 en natif uniquement) ;
--   * ne reconvertit pas si ts_kv est déjà partitionnée (relkind='p') ;
--   * recrée seulement les partitions manquantes (IF NOT EXISTS) ;
--   * CREATE INDEX IF NOT EXISTS pour le BRIN.
--
-- Structural contract (identical to 009) strictly preserved :
--   colonnes, PK (ts, entity_id, metric_key), defaults, et droits ts_kv_latest
--   et tables d'agrégats (004) non touchés. ts_kv_latest reste non partitionnée.
-- ============================================================================

SET client_encoding = 'UTF8';
SET standard_conforming_strings = ON;
SET check_function_bodies = FALSE;
SET client_min_messages = WARNING;

DO $$
DECLARE
    v_kind      char;
    v_cur       timestamptz := date_trunc('month', now());
    v_data_min  timestamptz;
    v_data_max  timestamptz;
    v_start     timestamptz;
    v_end       timestamptz;
    v_bound     timestamptz;
    v_part      text;
BEGIN
    -- Si TimescaleDB est présent, 002 fournit déjà ts_kv (hypertable). On ne
    -- touche pas au profil TSDB et on n'introduit aucune dépendance.
    IF EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'
    ) THEN
        RAISE NOTICE '[trendx-migrate] TimescaleDB détecté, saut de 010_partition_ts_kv.sql';
        RETURN;
    END IF;

    -- ts_kv doit exister (créée par 009 en natif). Sinon, sortie propre.
    SELECT c.relkind INTO v_kind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'trendx_analytics' AND c.relname = 'ts_kv';

    IF v_kind IS NULL THEN
        RAISE NOTICE '[trendx-migrate] trendx_analytics.ts_kv absente, saut de 010';
        RETURN;
    END IF;

    -- Déjà partitionnée : on passe directement aux partitions + BRIN.
    IF v_kind <> 'r' THEN
        NULL;
    ELSE
        -- Conversion table plate -> table partitionnée (stratégie swap).
        -- LIKE ... INCLUDING ALL reproduit colonnes, types, defaults, PK et
        -- contraintes NOT NULL. La PK (ts, entity_id, metric_key) inclut la
        -- partition key ts, so it stays valid on the partitioned table.
        CREATE TABLE trendx_analytics._ts_kv_parted (
            LIKE trendx_analytics.ts_kv INCLUDING ALL
        ) PARTITION BY RANGE (ts);

        INSERT INTO trendx_analytics._ts_kv_parted
            SELECT * FROM trendx_analytics.ts_kv;

        DROP TABLE trendx_analytics.ts_kv;
        ALTER TABLE trendx_analytics._ts_kv_parted RENAME TO ts_kv;
    END IF;

    -- Partition coverage : from the oldest month of existing data
    -- up to max(current month + 3 months, max data month). No
    -- partition DEFAULT. Le COPY lors de la conversion ne peut donc jamais
    -- échouer sur une ligne hors fenêtre.
    SELECT date_trunc('month', min(ts)), date_trunc('month', max(ts))
        INTO v_data_min, v_data_max
        FROM trendx_analytics.ts_kv;

    v_start := least(coalesce(v_data_min, v_cur), v_cur);
    v_end   := greatest(v_cur + interval '3 months', coalesce(v_data_max, v_cur));

    FOR v_bound IN
        SELECT generate_series(v_start, v_end, interval '1 month')
    LOOP
        v_part := 'ts_kv_' || to_char(v_bound, 'YYYY_MM');
        IF NOT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = v_part AND n.nspname = 'trendx_analytics'
        ) THEN
            EXECUTE format(
                'CREATE TABLE trendx_analytics.%I PARTITION OF trendx_analytics.ts_kv '
                'FOR VALUES FROM (%L) TO (%L)',
                v_part, v_bound, v_bound + interval '1 month'
            );
        END IF;
    END LOOP;

    -- Index BRIN sur ts (partitionné : un BRIN par partition). Idempotent.
    CREATE INDEX IF NOT EXISTS idx_ts_kv_brin_ts
        ON trendx_analytics.ts_kv USING brin (ts) WITH (pages_per_range = 128);

    -- Droits DML pour le rôle applicatif (si présent), conformément à 009.
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trendx_app') THEN
        GRANT SELECT, INSERT, UPDATE, DELETE
            ON trendx_analytics.ts_kv TO trendx_app;
    END IF;
END
$$;
