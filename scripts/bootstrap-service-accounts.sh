#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

# Default mode is local validation. Applying changes requires both --apply and
# TRENDX_CONFIRM_APPLY=YES. No ThingsBoard writeback or alarm operation exists here.
# SC1007 volontaire : préfixe CDPATH= vide neutralisant CDPATH pour cd (portabilité POSIX).
# shellcheck disable=SC1007
ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
ENV_FILE=${ENV_FILE:-"$ROOT_DIR/.env"}
MODE=check
[[ ${1:-} == "--apply" ]] && MODE=apply
[[ ${1:-} == "--help" ]] && { printf '%s\n' "Usage: $0 [--check|--apply]"; exit 0; }
[[ ${1:-} == "--check" || -z ${1:-} ]] || { echo "Usage: $0 [--check|--apply]" >&2; exit 2; }

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

require_cmd() { command -v "$1" >/dev/null || { echo "Missing command: $1" >&2; exit 1; }; }
require_var() { [[ -n ${!1:-} ]] || { echo "Missing variable: $1" >&2; exit 1; }; }
valid_ident() { [[ $1 =~ ^[a-z_][a-z0-9_]{0,62}$ ]]; }
secret_file=${TRENDX_CREDENTIALS_FILE:-.secrets/service-accounts.env}
[[ $secret_file = /* ]] || secret_file="$ROOT_DIR/$secret_file"

for name in ANALYTICS_DB_NAME AIRFLOW_DB_NAME MLFLOW_DB_NAME; do
  require_var "$name"; valid_ident "${!name}" || { echo "Invalid database identifier: $name" >&2; exit 1; }
done
for name in ANALYTICS_DB_USER AIRFLOW_DB_USER; do
  require_var "$name"; valid_ident "${!name}" || { echo "Invalid role identifier: $name" >&2; exit 1; }
done
MIGRATION_USER=${TRENDX_MIGRATION_USER:-trendx_migration}
GRAFANA_DB_USER=${GRAFANA_DB_USER:-trendx_grafana}
for name in MIGRATION_USER GRAFANA_DB_USER; do valid_ident "${!name}" || { echo "Invalid role identifier: $name" >&2; exit 1; }; done

if [[ $MODE == check ]]; then
  require_cmd psql; require_cmd curl; require_cmd jq; require_cmd openssl
  [[ ! -f "$secret_file" || $(stat -c '%a' "$secret_file") == 600 ]] || { echo "Credentials file must be mode 600" >&2; exit 1; }
  echo "Validation passed; no remote action performed."
  exit 0
fi
[[ ${TRENDX_CONFIRM_APPLY:-} == YES ]] || { echo "Refusing apply: set TRENDX_CONFIRM_APPLY=YES explicitly." >&2; exit 1; }
require_cmd psql; require_cmd curl; require_cmd jq; require_cmd openssl
require_var PG_ADMIN_HOST; require_var PG_ADMIN_USER; require_var PG_ADMIN_DB

mkdir -p "$(dirname "$secret_file")"
touch "$secret_file"; chmod 600 "$secret_file"
# SC1090 nécessaire : fichier de secrets local à chemin variable (créé plus haut, chmod 600).
# shellcheck disable=SC1090
source "$secret_file" 2>/dev/null || true
random_password() { openssl rand -base64 48 | tr -dc 'A-Za-z0-9_@%+=' | cut -c1-40; }
persist_secret() { local key=$1 value=$2; if ! grep -q "^${key}=" "$secret_file"; then printf '%s=%q\n' "$key" "$value" >>"$secret_file"; fi; }
APP_PASSWORD=${TRENDX_APP_PASSWORD:-${ANALYTICS_DB_PASSWORD:-$(random_password)}}
AIRFLOW_PASSWORD=${TRENDX_AIRFLOW_PASSWORD:-${AIRFLOW_DB_PASSWORD:-$(random_password)}}
MLFLOW_PASSWORD=${TRENDX_MLFLOW_PASSWORD:-${MLFLOW_DB_PASSWORD:-$(random_password)}}
GRAFANA_PASSWORD=${TRENDX_GRAFANA_PASSWORD:-$(random_password)}
MIGRATION_PASSWORD=${TRENDX_MIGRATION_PASSWORD:-$(random_password)}
for pair in "TRENDX_APP_PASSWORD=$APP_PASSWORD" "TRENDX_AIRFLOW_PASSWORD=$AIRFLOW_PASSWORD" "TRENDX_MLFLOW_PASSWORD=$MLFLOW_PASSWORD" "TRENDX_GRAFANA_PASSWORD=$GRAFANA_PASSWORD" "TRENDX_MIGRATION_PASSWORD=$MIGRATION_PASSWORD"; do persist_secret "${pair%%=*}" "${pair#*=}"; done

export PGPASSWORD=${PG_ADMIN_PASSWORD:-}
psql_admin=(psql --host="$PG_ADMIN_HOST" --port="${PG_ADMIN_PORT:-5432}" --username="$PG_ADMIN_USER" --dbname="$PG_ADMIN_DB" --no-psqlrc --set=ON_ERROR_STOP=1)
for role in "$ANALYTICS_DB_USER" "$AIRFLOW_DB_USER" "$MLFLOW_DB_USER" "$GRAFANA_DB_USER" "$MIGRATION_USER"; do
  valid_ident "$role" || { echo "Invalid role identifier" >&2; exit 1; }
done
sql_literal() { local value=${1//\'/\'\'}; printf "'%s'" "$value"; }
role_sql=$(mktemp); chmod 600 "$role_sql"; trap 'rm -f "$role_sql"' EXIT
{
  printf "SELECT format('CREATE ROLE %%I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION PASSWORD %%L', %s, %s) WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=%s) \\gexec\n" "$(sql_literal "$ANALYTICS_DB_USER")" "$(sql_literal "$APP_PASSWORD")" "$(sql_literal "$ANALYTICS_DB_USER")"
  printf "ALTER ROLE %s NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION PASSWORD %s;\n" "$ANALYTICS_DB_USER" "$(sql_literal "$APP_PASSWORD")"
  printf "SELECT format('CREATE ROLE %%I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION PASSWORD %%L', %s, %s) WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=%s) \\gexec\n" "$(sql_literal "$AIRFLOW_DB_USER")" "$(sql_literal "$AIRFLOW_PASSWORD")" "$(sql_literal "$AIRFLOW_DB_USER")"
  printf "ALTER ROLE %s NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION PASSWORD %s;\n" "$AIRFLOW_DB_USER" "$(sql_literal "$AIRFLOW_PASSWORD")"
  printf "SELECT format('CREATE ROLE %%I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION PASSWORD %%L', %s, %s) WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=%s) \\gexec\n" "$(sql_literal "$MLFLOW_DB_USER")" "$(sql_literal "$MLFLOW_PASSWORD")" "$(sql_literal "$MLFLOW_DB_USER")"
  printf "ALTER ROLE %s NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION PASSWORD %s;\n" "$MLFLOW_DB_USER" "$(sql_literal "$MLFLOW_PASSWORD")"
  printf "SELECT format('CREATE ROLE %%I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION PASSWORD %%L', %s, %s) WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=%s) \\gexec\n" "$(sql_literal "$GRAFANA_DB_USER")" "$(sql_literal "$GRAFANA_PASSWORD")" "$(sql_literal "$GRAFANA_DB_USER")"
  printf "ALTER ROLE %s NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION PASSWORD %s;\n" "$GRAFANA_DB_USER" "$(sql_literal "$GRAFANA_PASSWORD")"
  printf "SELECT format('CREATE ROLE %%I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION PASSWORD %%L', %s, %s) WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=%s) \\gexec\n" "$(sql_literal "$MIGRATION_USER")" "$(sql_literal "$MIGRATION_PASSWORD")" "$(sql_literal "$MIGRATION_USER")"
  printf "ALTER ROLE %s NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION PASSWORD %s;\n" "$MIGRATION_USER" "$(sql_literal "$MIGRATION_PASSWORD")"
} >"$role_sql"
"${psql_admin[@]}" -f "$role_sql"
for db in "$ANALYTICS_DB_NAME" "$AIRFLOW_DB_NAME" "$MLFLOW_DB_NAME"; do
  db_exists=$("${psql_admin[@]}" -At -v dbname="$db" -c "SELECT 1 FROM pg_database WHERE datname = :'dbname';")
  if [[ $db_exists != 1 ]]; then
    "${psql_admin[@]}" -v dbname="$db" -v owner="$MIGRATION_USER" -c 'CREATE DATABASE :"dbname" OWNER :"owner"'
  fi
done
"${psql_admin[@]}" -v ANALYTICS_DB_NAME="$ANALYTICS_DB_NAME" -v AIRFLOW_DB_NAME="$AIRFLOW_DB_NAME" -v MLFLOW_DB_NAME="$MLFLOW_DB_NAME" \
  -v ANALYTICS_DB_USER="$ANALYTICS_DB_USER" -v AIRFLOW_DB_USER="$AIRFLOW_DB_USER" -v MLFLOW_DB_USER="$MLFLOW_DB_USER" \
  -v GRAFANA_DB_USER="$GRAFANA_DB_USER" -v MIGRATION_USER="$MIGRATION_USER" -f "$ROOT_DIR/migrations/000_service_accounts.sql"

echo "PostgreSQL service accounts and database grants applied."
[[ ${GRAFANA_API_ENABLED:-false} == true ]] && bash "$ROOT_DIR/scripts/configure-grafana-service-account.sh"
[[ ${TB_ACCOUNT_PROVISIONING_ENABLED:-false} == true ]] && bash "$ROOT_DIR/scripts/configure-thingsboard-service-user.sh"
