-- ============================================================================
-- Trendx Catalogue — Étape 014
-- scheduler_run, scheduler_heartbeat (TrendX-owned, scheduler B1)
-- ============================================================================
-- Profil : PostgreSQL natif (TimescaleDB INTERDITE en profil minimal).
--
-- Contrat :
--   * scheduler_run / scheduler_heartbeat : objets TrendX-owned (n'existent
--     pas dans Trendz 1.15.0). task_id est UUID NULLABLE, volontairement SANS
--     FK vers trendz_task : les jobs `direct` n'ont pas de tâche, et la purge
--     de rétention des tâches ne doit jamais effacer l'historique des runs.
--   * Création additive uniquement ; statuts/CHECK fermés
--     (running|ok|failed, direct|enqueue).
--   * Aucun DROP, aucune modification de 000-013, aucune table ThingsBoard.
--
-- Idempotence : CREATE TABLE / CREATE INDEX / GRANT en IF NOT EXISTS.
-- Ré-applicable sur DB fraîche (000->014) et sur DB 000->013 existante,
-- et N fois sans erreur.
-- ============================================================================

SET client_encoding = 'UTF8';
SET standard_conforming_strings = ON;
SET check_function_bodies = FALSE;
SET client_min_messages = WARNING;

-- 1) scheduler_run (trendx_catalog) ------------------------------------------
CREATE TABLE IF NOT EXISTS trendx_catalog.scheduler_run (
    run_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id               TEXT NOT NULL,
    job_type             TEXT NOT NULL,
    status               TEXT NOT NULL CHECK (status IN ('running', 'ok', 'failed')),
    triggered_at         TIMESTAMPTZ NOT NULL,
    finished_at          TIMESTAMPTZ,
    error                TEXT,
    instance_id          TEXT NOT NULL,
    mode                 TEXT NOT NULL DEFAULT 'direct' CHECK (mode IN ('direct', 'enqueue')),
    task_id              UUID
);

CREATE INDEX IF NOT EXISTS ix_scheduler_run_job_triggered
    ON trendx_catalog.scheduler_run (job_id, triggered_at DESC);

-- 2) scheduler_heartbeat (trendx_catalog) ------------------------------------
CREATE TABLE IF NOT EXISTS trendx_catalog.scheduler_heartbeat (
    instance_id          TEXT PRIMARY KEY,
    leader               BOOLEAN NOT NULL DEFAULT FALSE,
    heartbeat_ts         TIMESTAMPTZ NOT NULL,
    version              TEXT,
    host                 TEXT
);

-- 3) Permissions (pattern existant : rôle trendx_app, IF EXISTS) --------------
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trendx_app') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE
        ON trendx_catalog.scheduler_run,
           trendx_catalog.scheduler_heartbeat
        TO trendx_app;
  END IF;
END;
$$;
