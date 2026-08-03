-- Migration Trendx : table de checkpoints d'ingestion
-- Scope : trendx_catalog (schéma dédié aux entités métier)
-- Ces tables sont propres à Trendx, sans modifier les tables héritées de Trendz.

CREATE TABLE IF NOT EXISTS trendx_catalog.ingestion_checkpoints (
    pipeline          text      NOT NULL,
    entity_id         text      NOT NULL,
    metric_key        text      NOT NULL,
    watermark_ts      timestamptz NOT NULL,
    records_processed bigint    NOT NULL DEFAULT 0,
    last_batch_id     text,
    source            text,
    updated_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ingestion_checkpoints_pk PRIMARY KEY (pipeline, entity_id, metric_key)
);

CREATE INDEX IF NOT EXISTS ingestion_checkpoints_entity_idx
    ON trendx_catalog.ingestion_checkpoints (entity_id, metric_key);

COMMENT ON TABLE trendx_catalog.ingestion_checkpoints IS
    'Points de reprise par pipeline/device/metric pour l''ingestion incrémentale Trendx.';
