-- ============================================================================
-- Trendx Catalogue + Analytics — Étape 013
-- alert_rule, alert_incident (TrendX-owned) + prediction_model enrichment
-- ============================================================================
-- Profil : PostgreSQL natif (TimescaleDB INTERDITE en profil minimal).
--
-- Contrat (validé par 013-A) :
--   * alert_rule / alert_incident : objets TrendX-owned (n'existent pas dans
--     Trendz 1.15.0). entity_id est TEXT (device TB id), volontairement SANS
--     FK vers business_entity, conformément au contrat. rule_id référence
--     alert_rule(id) ON DELETE SET NULL uniquement.
--   * prediction_model : table Trendz-héritée. Enrichissement ADDITIF
--     (frequency, algorithm, scaler, hyperparameters) ; colonnes NULL,
--     ADD COLUMN IF NOT EXISTS ; aucune modification de colonne existante,
--     aucun renommage.
--   * Aucune forecast_series, aucune writeback_batch, aucune prediction_run.
--   * Aucun DROP, aucune modification de 000-012.
--
-- Idempotence : CREATE TABLE / ADD COLUMN / CREATE INDEX / GRANT en
-- IF NOT EXISTS. Ré-applicable sur DB fraîche (000->013) et sur DB 000->012
-- existante, et N fois sans erreur.
-- ============================================================================

SET client_encoding = 'UTF8';
SET standard_conforming_strings = ON;
SET check_function_bodies = FALSE;
SET client_min_messages = WARNING;

-- 1) alert_rule (trendx_catalog) ------------------------------------------------
CREATE TABLE IF NOT EXISTS trendx_catalog.alert_rule (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                 TEXT NOT NULL,
    entity_id            TEXT,
    metric_key           TEXT,
    rule_type            TEXT NOT NULL,
    condition_json       JSONB NOT NULL DEFAULT '{}'::jsonb,
    cooldown_seconds     INTEGER NOT NULL DEFAULT 7200,
    min_duration_seconds INTEGER NOT NULL DEFAULT 60,
    open_threshold       DOUBLE PRECISION,
    close_threshold      DOUBLE PRECISION,
    severity             TEXT NOT NULL DEFAULT 'MEDIUM',
    is_active            BOOLEAN NOT NULL DEFAULT TRUE,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_alert_rule_active ON trendx_catalog.alert_rule (is_active);
CREATE INDEX IF NOT EXISTS ix_alert_rule_entity ON trendx_catalog.alert_rule (entity_id);

-- 2) alert_incident (trendx_catalog) -------------------------------------------
CREATE TABLE IF NOT EXISTS trendx_catalog.alert_incident (
    id                   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    logical_key          TEXT NOT NULL UNIQUE,
    rule_id              UUID REFERENCES trendx_catalog.alert_rule(id) ON DELETE SET NULL,
    entity_id            TEXT NOT NULL,
    metric_key           TEXT,
    severity             TEXT NOT NULL,
    status               TEXT NOT NULL DEFAULT 'ACTIVE',
    external_tb_alarm_id UUID,
    opened_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged_at      TIMESTAMPTZ,
    cleared_at           TIMESTAMPTZ,
    closed_at            TIMESTAMPTZ,
    acknowledged_by      TEXT,
    last_value           DOUBLE PRECISION,
    open_reason          TEXT,
    close_reason         TEXT,
    additional_info      JSONB NOT NULL DEFAULT '{}'::jsonb,
    opened_count         INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS ix_alert_incident_status ON trendx_catalog.alert_incident (status);
CREATE INDEX IF NOT EXISTS ix_alert_incident_entity ON trendx_catalog.alert_incident (entity_id);

-- 3) prediction_model — enrichissement additif (Trendz-hérité) ------------------
ALTER TABLE trendx_catalog.prediction_model
    ADD COLUMN IF NOT EXISTS frequency       TEXT,
    ADD COLUMN IF NOT EXISTS algorithm       TEXT,
    ADD COLUMN IF NOT EXISTS scaler          JSONB,
    ADD COLUMN IF NOT EXISTS hyperparameters JSONB;

-- 4) Permissions (pattern existant : rôle trendx_app, IF EXISTS) ---------------
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trendx_app') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE
        ON trendx_catalog.alert_rule,
           trendx_catalog.alert_incident
        TO trendx_app;
  END IF;
END;
$$;
