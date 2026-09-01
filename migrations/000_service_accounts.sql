-- ============================================================================
-- Trendx 000 — Service accounts & schemas (SINGLE-DB design)
-- ----------------------------------------------------------------------------
-- Base cible unique : trendx (une seule base PostgreSQL, conformément au
-- profil minimal TRENDX_PROFILE=minimal et à la règle de séparation stricte
-- des bases : plus aucune base séparée (analytics / airflow / mlflow).
--
-- Cette migration :
--   * crée les schémas trendx_catalog et trendx_analytics si nécessaire ;
--   * applique les grants de connexion et d'usage sur la base trendx et les
--     deux schémas aux rôles RÉELLEMENT créés par l'infrastructure
--     (trendx_app, trendx_migration, trendx_grafana, trendx_ro) ;
--   * est idempotente (CREATE SCHEMA IF NOT EXISTS, GRANT idempotents) ;
--   * ne crée AUCUN rôle et AUCUN mot de passe (les rôles sont provisionnés
--     par le bootstrap d'infrastructure, jamais ici) ;
--   * ne contient AUCUN credential en dur ;
--   * ne bascule jamais vers une base séparée (pas de changement de base).
--
-- Les rôles attendus (créés par l'infrastructure, pas par ce fichier) :
--   trendx_migration  : propriétaire DDL (CREATE dans les deux schémas)
--   trendx_app        : DML applicatif (lecture/écriture)
--   trendx_grafana    : datasource read-only (analytics)
--   trendx_ro         : lecture seule (rôle d'exploitation)
-- ============================================================================

SET client_min_messages = WARNING;

-- Schémas de la base unique trendx.
CREATE SCHEMA IF NOT EXISTS trendx_catalog;
CREATE SCHEMA IF NOT EXISTS trendx_analytics;

-- Grants conditionnels : seuls les rôles existants sont concernés, ce qui rend
-- ce fichier idempotent et sans erreur même si un rôle n'est pas encore créé.
DO $$
DECLARE
  r       text;
  db_name text := current_database();
BEGIN
  FOREACH r IN ARRAY ARRAY['trendx_app', 'trendx_migration', 'trendx_grafana', 'trendx_ro']
  LOOP
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
      -- Connexion à la base unique trendx.
      EXECUTE format('GRANT CONNECT ON DATABASE %I TO %I', db_name, r);
      -- Usage des deux schémas.
      EXECUTE format('GRANT USAGE ON SCHEMA trendx_catalog TO %I', r);
      EXECUTE format('GRANT USAGE ON SCHEMA trendx_analytics TO %I', r);

      -- trendx_migration est le propriétaire DDL : CREATE + defaults futures.
      IF r = 'trendx_migration' THEN
        EXECUTE format('GRANT CREATE ON SCHEMA trendx_catalog TO %I', r);
        EXECUTE format('GRANT CREATE ON SCHEMA trendx_analytics TO %I', r);
        EXECUTE format(
          'ALTER DEFAULT PRIVILEGES IN SCHEMA trendx_catalog GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO trendx_app'
        );
        EXECUTE format(
          'ALTER DEFAULT PRIVILEGES IN SCHEMA trendx_analytics GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO trendx_app'
        );
        EXECUTE format(
          'ALTER DEFAULT PRIVILEGES IN SCHEMA trendx_catalog GRANT USAGE, SELECT ON SEQUENCES TO trendx_app'
        );
        EXECUTE format(
          'ALTER DEFAULT PRIVILEGES IN SCHEMA trendx_analytics GRANT USAGE, SELECT ON SEQUENCES TO trendx_app'
        );
      END IF;
    END IF;
  END LOOP;
END
$$;

-- Grafana n'a aucun privilège d'écriture PostgreSQL : elle consomme le schéma
-- analytics uniquement via son datasource read-only (rôle trendx_grafana).
