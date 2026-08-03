-- NON-DESTRUCTIVE rollback template. Review and run only after confirmation.
-- DROP OWNED does not drop databases or roles, but revokes object ownership/grants.
\set ON_ERROR_STOP on
\connect :ANALYTICS_DB_NAME
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM :"ANALYTICS_DB_USER", :"GRAFANA_DB_USER";
REVOKE ALL PRIVILEGES ON SCHEMA public FROM :"ANALYTICS_DB_USER", :"GRAFANA_DB_USER";
-- DROP OWNED BY :"MIGRATION_USER"; -- uncomment only after an explicit review
