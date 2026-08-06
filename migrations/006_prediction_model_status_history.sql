-- Historique Trendx des transitions de statut des modèles.
-- Ne modifie pas la table héritée prediction_model.

CREATE TABLE IF NOT EXISTS trendx_catalog.prediction_model_status_history (
    id                  uuid        NOT NULL DEFAULT gen_random_uuid(),
    prediction_model_id uuid        NOT NULL,
    business_entity_id  uuid        NOT NULL,
    metric_key          text        NOT NULL,
    previous_status     text,
    new_status          text        NOT NULL,
    changed_ts          bigint      NOT NULL,
    CONSTRAINT prediction_model_status_history_pk PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS prediction_model_status_history_lookup_idx
    ON trendx_catalog.prediction_model_status_history
       (business_entity_id, metric_key, changed_ts DESC);

COMMENT ON TABLE trendx_catalog.prediction_model_status_history IS
    'Historique Trendx des transitions champion/challenger pour permettre un rollback sûr.';
