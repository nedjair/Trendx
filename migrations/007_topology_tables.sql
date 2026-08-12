-- ============================================================================
-- Trendx Catalogue — Topology discovery tables (path B)
-- Database : trendx
-- Schema : trendx_catalog
-- DDL owner : trendx_migration
-- Read/write privileges : trendx_app
-- Read-only privileges : trendx_ro
--
-- Goal: create the topology persistence tables (entities, relations, sync
-- metadata) in the official catalog schema. No opportunistic creation by the
-- runtime worker (no more create_all); no schema CREATE grant to trendx_app;
-- the public schema is never created or used.
-- ============================================================================

CREATE TABLE IF NOT EXISTS trendx_catalog.topology_entities (
    id              SERIAL      PRIMARY KEY,
    entity_type     VARCHAR(32) NOT NULL,
    entity_id       VARCHAR(64) NOT NULL,
    name            VARCHAR(255) NOT NULL,
    label           VARCHAR(255) DEFAULT '',
    entity_data     JSON,
    attributes      JSON,
    telemetry_keys  JSON,
    first_seen      TIMESTAMP,
    last_seen       TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_topology_entities_entity_id
    ON trendx_catalog.topology_entities (entity_id);

CREATE TABLE IF NOT EXISTS trendx_catalog.topology_relations (
    id              SERIAL      PRIMARY KEY,
    from_type       VARCHAR(32) NOT NULL,
    from_id         VARCHAR(64) NOT NULL,
    to_type         VARCHAR(32) NOT NULL,
    to_id           VARCHAR(64) NOT NULL,
    relation_type   VARCHAR(64) NOT NULL,
    relation_data   JSON
);

CREATE TABLE IF NOT EXISTS trendx_catalog.sync_metadata (
    id          SERIAL       PRIMARY KEY,
    sync_key    VARCHAR(128) NOT NULL UNIQUE,
    sync_value  TEXT         DEFAULT ''
);

-- Propriété DDL
ALTER TABLE trendx_catalog.topology_entities  OWNER TO trendx_migration;
ALTER TABLE trendx_catalog.topology_relations OWNER TO trendx_migration;
ALTER TABLE trendx_catalog.sync_metadata      OWNER TO trendx_migration;

ALTER SEQUENCE trendx_catalog.topology_entities_id_seq  OWNER TO trendx_migration;
ALTER SEQUENCE trendx_catalog.topology_relations_id_seq OWNER TO trendx_migration;
ALTER SEQUENCE trendx_catalog.sync_metadata_id_seq      OWNER TO trendx_migration;

-- Droits d'écriture (aucun CREATE de schéma)
GRANT SELECT, INSERT, UPDATE, DELETE ON trendx_catalog.topology_entities  TO trendx_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON trendx_catalog.topology_relations TO trendx_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON trendx_catalog.sync_metadata      TO trendx_app;

-- nextval() sur les séquences SERIAL pour trendx_app
GRANT SELECT, USAGE ON SEQUENCE trendx_catalog.topology_entities_id_seq  TO trendx_app;
GRANT SELECT, USAGE ON SEQUENCE trendx_catalog.topology_relations_id_seq TO trendx_app;
GRANT SELECT, USAGE ON SEQUENCE trendx_catalog.sync_metadata_id_seq      TO trendx_app;

-- Droits lecture seule
GRANT SELECT ON trendx_catalog.topology_entities  TO trendx_ro;
GRANT SELECT ON trendx_catalog.topology_relations TO trendx_ro;
GRANT SELECT ON trendx_catalog.sync_metadata      TO trendx_ro;
