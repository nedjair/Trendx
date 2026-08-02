#!/usr/bin/env bash
# ==================================================================
# Trendx — Initialisation PostgreSQL existant
# ATTENTION : ce script ne doit être exécuté qu'après approbation
# explicite (création bases + rôles + extensions).
# ==================================================================
set -euo pipefail

echo "[trendx-init] ======================================="
echo "[trendx-init] 010-create-databases.sh : création base trendx + schémas"
echo "[trendx-init] ======================================="

# --- Conteneur PostgreSQL existant ---
PG_CONTAINER="${PG_EXTERNAL_CONTAINER:-mobili_dahsboard-postgres-1}"

# --- Utilitaires ---------------------------------------------------
psql_cmd=(docker exec -i "$PG_CONTAINER" psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB")

create_role_if_missing() {
  local role="$1" password="$2"
  echo "[trendx-init]  ➜ rôle $role (si absent)"
  "${psql_cmd[@]}" <<EOSQL 2>/dev/null || true
    DO \$\$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${role}') THEN
        CREATE ROLE ${role} WITH LOGIN PASSWORD '${password}';
      END IF;
    END
    \$\$;
EOSQL
}

create_db_if_missing() {
  local db="$1" owner="$2"
  echo "[trendx-init]  ➜ base $db (owner=$owner)"
  "${psql_cmd[@]}" <<EOSQL 2>/dev/null || true
    SELECT 'CREATE DATABASE ${db} OWNER ${owner}'
    WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = '${db}')\gexec
EOSQL
}

grant_connect_and_schema() {
  local db="$1" user="$2"
  echo "[trendx-init]  ➜ grants $user sur $db"
  docker exec -i "$PG_CONTAINER" psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$db" <<EOSQL
    -- Connexion
    REVOKE ALL ON DATABASE ${db} FROM PUBLIC;
    GRANT CONNECT ON DATABASE ${db} TO ${user};
    GRANT ALL ON SCHEMA public TO ${user};
    -- Tables futures (idempotent pas de défaut on force ALTER DEFAULT)
    ALTER DEFAULT PRIVILEGES FOR ROLE ${user} IN SCHEMA public
      GRANT ALL ON TABLES TO ${user};
    ALTER DEFAULT PRIVILEGES FOR ROLE ${user} IN SCHEMA public
      GRANT ALL ON SEQUENCES TO ${user};
EOSQL
}

create_extension_if_missing() {
  local db="$1" ext="$2"
  echo "[trendx-init]  ➜ extension $ext sur $db (APPROBATION REQUISE)"
  docker exec -i "$PG_CONTAINER" psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$db" <<EOSQL
    CREATE EXTENSION IF NOT EXISTS ${ext};
EOSQL
}

# --- 1. Rôles (3 rôles séparés) -----------------------------------
echo "[trendx-init] === 1/5 Rôles dédiés ==="
# Les mots de passe proviennent de .secrets/service-accounts.env ou .env
create_role_if_missing trendx_migration "${TRENDX_MIGRATION_PASSWORD:-CHANGE_ME}"
create_role_if_missing trendx_app       "${TRENDX_APP_PASSWORD:-CHANGE_ME}"
create_role_if_missing trendx_ro        "${TB_DB_READONLY_PASSWORD:-CHANGE_ME}"

# --- 2. Base unique + schémas --------------------------------------
echo "[trendx-init] === 2/5 Création base trendx ==="
create_db_if_missing trendx   trendx_migration

# --- 3. Extensions ------------------------------------------------
echo "[trendx-init] === 3/5 Extensions ==="
# TimescaleDB nécessite une approbation explicite (AGENTS.md)
if [ "${TRENDX_TIMESCALEDB_ENABLED:-false}" = "true" ]; then
  create_extension_if_missing trendx timescaledb
  create_extension_if_missing postgres   timescaledb
else
  echo "[trendx-init]    TimescaleDB non activé (TRENDX_TIMESCALEDB_ENABLED != true)"
fi
create_extension_if_missing trendx pg_stat_statements
create_extension_if_missing postgres   pg_stat_statements

# --- 4. Grants ----------------------------------------------------
echo "[trendx-init] === 4/5 Grants ==="
grant_connect_and_schema trendx   trendx_migration
grant_connect_and_schema trendx   trendx_app

# trendx_app = droit lecture+écriture, pas DDL (sauf migration)
docker exec -i "$PG_CONTAINER" psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname trendx <<EOSQL
  REVOKE CREATE ON SCHEMA public FROM trendx_app;
EOSQL

# --- 5. Validation ------------------------------------------------
echo "[trendx-init] === 5/5 Validation ==="
if [ "${TRENDX_TIMESCALEDB_ENABLED:-false}" = "true" ]; then
  TS_OK=$(docker exec -i "$PG_CONTAINER" psql --username "$POSTGRES_USER" --dbname trendx -tAc "SELECT count(*) FROM pg_extension WHERE extname='timescaledb'")
  echo "[trendx-init]    TimescaleDB dans trendx : $TS_OK"
else
  echo "[trendx-init]    TimescaleDB désactivé"
fi
echo "[trendx-init] === Terminée ======================================="
echo "[trendx-init] 1 base : trendx (schémas trendx_catalog, trendx_analytics)"
echo "[trendx-init] 3 rôles : trendx_migration (DDL) | trendx_app (DML) | trendx_ro (READ-ONLY TB)"
echo "[trendx-init] ======================================================"
