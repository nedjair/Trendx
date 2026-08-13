-- ============================================================================
-- Trendx Analytics : native PostgreSQL tables for raw telemetry (ts_kv)
-- Profil : PostgreSQL natif (B5) — TimescaleDB INTERDITE en profil minimal.
-- ============================================================================
-- Complète migration 002, qui est un NO-OP en PG natif (son corps est gardé
-- par la présence de l'extension TimescaleDB). En PG natif, 002 ne crée donc
-- AUCUNE table ts_kv, ce qui bloque l'ingestion (blocage P0). Cette migration
-- 009 fournit les tables cibles de l'ingestion en PostgreSQL natif.
--
-- Base : trendx_analytics
-- Contrat : identique à migrations/002 (colonnes, PK, nullabilité).
-- Idempotence : CREATE SCHEMA/TABLE IF NOT EXISTS + garde NOT EXISTS timescaledb.
--
-- Ordre dans la chaîne CI (001 -> 002 -> 003(skip) -> 004 -> 005 -> 006 -> 009) :
-- 009 s'exécute APRÈS 004. Elle ne crée PAS de table partitionnée, donc
-- ensure_partitions_forward() (définie dans 004, attend une table PARTITIONNEE)
-- n'est pas déclenchée et la chaîne native reste intacte. La conformité B5
-- COMPLÈTE (partitionnement déclaratif + index BRIN + agrégats matérialisés)
-- reste un travail séparé, hors périmètre de ce correctif minimal.
-- ============================================================================

SET client_encoding = 'UTF8';
SET standard_conforming_strings = ON;
SET check_function_bodies = FALSE;
SET client_min_messages = WARNING;

DO $$
BEGIN
  -- Si TimescaleDB est présent, 002 fournit déjà ts_kv (hypertable, en schéma
  -- public résolu via search_path). On évite une table concurrente en
  -- trendx_analytics en sautant cette migration.
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
    RAISE NOTICE '[trendx-migrate] TimescaleDB détecté, saut de 009_native_ts_kv.sql';
    RETURN;
  END IF;

  CREATE SCHEMA IF NOT EXISTS trendx_analytics;

  -- --------------------------------------------------------------------------
  -- 1. Table principale ts_kv — télémétrie brute (équivalent ts_kv de TB)
  --    Contrat identique à migrations/002 (hors garde TSDB).
  -- --------------------------------------------------------------------------
  CREATE TABLE IF NOT EXISTS trendx_analytics.ts_kv (
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

  -- --------------------------------------------------------------------------
  -- 2. Dernières valeurs connues par entité/métrique
  -- --------------------------------------------------------------------------
  CREATE TABLE IF NOT EXISTS trendx_analytics.ts_kv_latest (
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

  -- Droits DML pour le rôle applicatif (si présent), conformément à 002.
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trendx_app') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE
      ON trendx_analytics.ts_kv TO trendx_app;
    GRANT SELECT, INSERT, UPDATE, DELETE
      ON trendx_analytics.ts_kv_latest TO trendx_app;
  END IF;
END
$$;
