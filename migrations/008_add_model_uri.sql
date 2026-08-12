-- ============================================================================
-- Trendx Catalogue — Add model_uri column to prediction_model
-- Database : trendx
-- Schema : trendx_catalog
-- DDL owner : trendx_migration
-- Read/write privileges : trendx_app
-- Read-only privileges : trendx_ro
--
-- Goal: persist the MLflow model URI returned by TrainingService so the
-- champion registered via ModelRegistry.register() can later be reloaded by
-- URI. Previously register() accepted the model_uri argument but silently
-- dropped it (no column, not forwarded to repo.create()).
--
-- NOTE: this migration has NOT been applied nor verified against a real
-- database in this environment (same constraint as migrations 001-007, which
-- are applied out-of-band). It is idempotent (IF NOT EXISTS) and must be run
-- by the trendx_migration role. The new column inherits the table's existing
-- ownership and grants (trendx_app / trendx_ro); no per-column GRANT needed.
-- ============================================================================

ALTER TABLE trendx_catalog.prediction_model
    ADD COLUMN IF NOT EXISTS model_uri TEXT;

COMMENT ON COLUMN trendx_catalog.prediction_model.model_uri IS
    'MLflow model URI (runs:/<run_id>/model) persisté lors de register().';
